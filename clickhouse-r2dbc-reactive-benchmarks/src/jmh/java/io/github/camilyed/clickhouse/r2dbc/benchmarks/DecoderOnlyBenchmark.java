package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QuerySettings;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Diagnostic isolation benchmark for {@link StreamingScanBenchmark}'s confirmed, growing regression
 * (see ROADMAP.md's Phase 5 "Optimization phase" section — hypotheses H0/H1): decodes the exact same
 * bytes from memory, no network involved on either side, isolating pure row-decode/materialization
 * cost from transport/bridge cost (which {@link TransportOnlyStreamingBenchmark} isolates the other
 * way). Together the two answer "where does the gap actually live" instead of guessing from
 * {@code StreamingScanBenchmark}'s combined number alone.
 *
 * <p>The response body for {@code rows} is captured once in {@code @Setup(Level.Trial)} — a real
 * query against {@link PointQueryTable}, fully aggregated into one {@code byte[]} via {@code
 * .aggregate().asByteArray()} (the same operator this transport's own {@code killQueryBestEffort}
 * uses) — outside the measured region. Both {@code @Benchmark} methods then decode that same
 * in-memory payload repeatedly. Deliberately handed to each decoder as a single chunk rather than
 * replaying original network fragmentation: this isolates decode cost from chunk-boundary effects on
 * purpose, since chunking is exactly what {@link TransportOnlyStreamingBenchmark} exists to cover.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class DecoderOnlyBenchmark {

  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;

  /** Row-count tiers — same shape as {@link StreamingScanBenchmark}'s, for a like-for-like split. */
  @Param({"10000", "100000", "1000000"})
  public long rows;

  private byte[] capturedResponseBytes;

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and captures one full response body
   * for {@code rows} — the only network call this benchmark class ever makes, run once per trial,
   * outside the measured window.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(), BenchmarkEnvironment.username(),
            BenchmarkEnvironment.password());
    capturedResponseBytes =
        transport
            .query(ClickHouseQuery.of(SELECT_ALL_SQL))
            .aggregate()
            .asByteArray()
            .block(Duration.ofSeconds(60));
  }

  /** This driver's production decode path, {@link RowBinaryDecoder#decodeRows}, over captured bytes. */
  @Benchmark
  public void ourDriver(final Blackhole blackhole) {
    final Flux<ByteBuffer> body = Flux.just(ByteBuffer.wrap(capturedResponseBytes));
    final long rowCount = RowBinaryDecoder.decodeRows(body).count().block(Duration.ofSeconds(60));
    blackhole.consume(rowCount);
  }

  /**
   * client-v2's own reader, constructed directly over the captured bytes (no {@code Client}/{@code
   * QueryResponse} involved) — the same {@code RowBinaryWithNamesAndTypesFormatReader} class and
   * settings {@link RowBinaryDecoder#decodeRows} itself wraps, so this measures client-v2's decode
   * cost in isolation exactly as directly as this driver's own decode path is measured above.
   */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    long rowCount = 0;
    try (ByteArrayInputStream input = new ByteArrayInputStream(capturedResponseBytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader =
          new RowBinaryWithNamesAndTypesFormatReader(
              input,
              new QuerySettings()
                  .setUseTimeZone("UTC")
                  .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1"),
              new BinaryStreamReader.DefaultByteBufferAllocator());
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
        blackhole.consume(reader.getString(2));
        blackhole.consume(reader.getBigDecimal(3));
        rowCount++;
      }
    }
    blackhole.consume(rowCount);
  }
}

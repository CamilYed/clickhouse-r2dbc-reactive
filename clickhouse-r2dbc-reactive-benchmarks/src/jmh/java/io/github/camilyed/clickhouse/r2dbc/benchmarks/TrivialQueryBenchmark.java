package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Level 1 ("raw transport + decode") floor: {@code SELECT 1}, no table, no storage engine, no
 * {@code MergeTree} lookup — just the smallest possible request/response this driver's transport
 * and client-v2's transport can each make. Where {@link PointQueryBenchmark} measures point-query
 * latency (protocol + a real row lookup), this class isolates protocol/connection overhead alone,
 * per docs/PERFORMANCE.md's Phase 5 "Core workload set" (recommended run before {@code
 * StreamingScanBenchmark} — the next benchmark after this one).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TrivialQueryBenchmark {

  private static final String SELECT_1_SQL = "SELECT 1";

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;

  /** Starts the shared container once per JMH trial. No dataset — {@code SELECT 1} needs none. */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            BenchmarkEnvironment.username(),
            BenchmarkEnvironment.password());
    decodingScheduler = RowDecodingScheduler.defaults();
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .build();
  }

  /** Releases both clients' connection pools, this driver's decode scheduler, and its transport. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    decodingScheduler.dispose();
    ourTransport.dispose();
  }

  /**
   * This driver: {@link ClickHouseHttpTransport#query} + {@link RowBinaryDecoder#decode} — the real
   * production decode path (off {@link #ourTransport}'s Netty event loop, via {@link
   * #decodingScheduler}), not the scheduler-free {@link RowBinaryDecoder#decodeRows} test/benchmark
   * shortcut, since this benchmark's source is a live network response, not an in-memory one.
   * {@link ResponseCompression#LZ4} here, not {@code NONE} — {@link #ourTransport} is built via
   * {@link ClickHouseHttpTransport#ClickHouseHttpTransport(String, String, String)}, which resolves
   * {@link io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions#defaults()}'s own
   * default of {@link ResponseCompression#LZ4} (sends {@code compress=1}); telling the decoder
   * {@code NONE} while the transport actually requests LZ4 feeds it compressed bytes it can't
   * parse.
   */
  @Benchmark
  public void thisDriver(final Blackhole blackhole) {
    final Flux<ByteBuffer> body =
        ourTransport.query(ClickHouseQuery.of(SELECT_1_SQL)).asByteArray().map(ByteBuffer::wrap);
    final DecodedRow row =
        RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.LZ4)
            .flatMapMany(DecodedResult::rows)
            .blockFirst(Duration.ofSeconds(10));
    blackhole.consume(row);
  }

  /** client-v2: {@link Client#query} + {@link ClickHouseBinaryFormatReader}. */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    try (QueryResponse response = clientV2.query(SELECT_1_SQL).get(10, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
      }
    }
  }
}

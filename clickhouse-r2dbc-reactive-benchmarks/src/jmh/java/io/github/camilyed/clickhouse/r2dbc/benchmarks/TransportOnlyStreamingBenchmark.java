package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.io.InputStream;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Diagnostic isolation benchmark for {@link StreamingScanBenchmark}'s confirmed, growing regression
 * (see docs/PERFORMANCE.md's Phase 5 "Optimization phase" section — hypotheses H2/H3): consumes the
 * exact same query's response bytes, but never constructs a {@code RowBinaryWithNamesAndTypes}
 * decoder on either side. This isolates "HTTP transport + bridge/copy overhead" from "row
 * decode/materialization overhead" — if this benchmark shows the two drivers roughly matched, the
 * gap {@code StreamingScanBenchmark} measures lives in decode, not transport; if this benchmark
 * alone reproduces a meaningful share of that gap, transport/bridge cost (H2/H3) is a real
 * contributor too.
 *
 * <p>This driver: {@link ClickHouseHttpTransport#query} as normal, but the response {@code Flux} is
 * only summed for byte count — no {@link
 * io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder} involved at all, so this
 * exercises exactly the same {@code ByteBuf -> byte[]} path (H3) production code takes ({@code
 * ClickHouseResult} uses the identical {@code .asByteArray()} shape) without decode on top of it.
 *
 * <p>client-v2: {@link QueryResponse#getInputStream()} — its own lowest-level access to the raw
 * response body, read in a plain byte-counting loop, no {@code ClickHouseBinaryFormatReader}
 * involved either. This is the fairest available "raw bytes, no decode" comparison client-v2's
 * public API offers.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TransportOnlyStreamingBenchmark {

  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;

  private static final int READ_BUFFER_SIZE = 8192;

  /**
   * Row-count tiers — same shape as {@link StreamingScanBenchmark}'s, for a like-for-like split.
   */
  @Param({"10000", "100000", "1000000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;

  /** Starts the shared container and seeds {@link PointQueryTable}. */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            BenchmarkEnvironment.username(),
            BenchmarkEnvironment.password());
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .build();
  }

  /** Releases client-v2's connection pool and this driver's transport. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    ourTransport.dispose();
  }

  /** This driver: sums response chunk lengths, no {@code RowBinaryDecoder} involved. */
  @Benchmark
  public void thisDriver(final Blackhole blackhole) {
    final long totalBytes =
        ourTransport
            .query(ClickHouseQuery.of(SELECT_ALL_SQL))
            .asByteArray()
            .reduce(0L, (accumulated, chunk) -> accumulated + chunk.length)
            .block(Duration.ofSeconds(60));
    blackhole.consume(totalBytes);
  }

  /** client-v2: drains {@link QueryResponse#getInputStream()} directly, no row reader involved. */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    long totalBytes = 0;
    try (QueryResponse response = clientV2.query(SELECT_ALL_SQL).get(60, TimeUnit.SECONDS);
        InputStream body = response.getInputStream()) {
      final byte[] buffer = new byte[READ_BUFFER_SIZE];
      int bytesRead;
      while ((bytesRead = body.read(buffer)) != -1) {
        totalBytes += bytesRead;
      }
    }
    blackhole.consume(totalBytes);
  }
}

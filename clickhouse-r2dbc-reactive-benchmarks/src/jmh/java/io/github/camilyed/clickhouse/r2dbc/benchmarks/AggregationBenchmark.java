package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Level 1 "analytical aggregation" — the query shape docs/PERFORMANCE.md's Phase 5 "Query mix"
 * design named but never built: {@code GROUP BY} + {@code count()}/{@code avg()}/{@code
 * quantile()}, ClickHouse's actual core use case, not a synthetic point/scan stand-in. Unlike
 * {@link StreamingScanBenchmark} (result size grows with {@code rows}) or {@link
 * PointQueryBenchmark} (a single row), this benchmark's result set is always small and fixed (100
 * groups) regardless of {@code rows} — what scales with {@code rows} instead is the amount of work
 * ClickHouse itself does server-side to compute the aggregation. That makes this class measure
 * something neither of the other two do: small-result-set decode/round-trip overhead layered on top
 * of a query whose *server-side* cost scales with the input tier, rather than a client-side decode
 * cost that scales with the result size.
 *
 * <p>Reuses {@link PointQueryTable} rather than a dedicated table — no schema needs a literal
 * {@code category} column when {@code id % 100} already buckets a uniform {@code id} range
 * (1..{@code rows}) into 100 roughly-even groups, giving {@code GROUP BY} real work to do without
 * adding another dataset class this suite would need to keep in sync with {@link PointQueryTable}'s
 * own seeding.
 *
 * <p>{@code quantile(0.95)(...)} is explicitly wrapped in {@code toFloat64(...)} rather than
 * applied directly to the {@code Decimal(18,4)} {@code amount} column — ClickHouse's {@code
 * quantile} function's return type for a {@code Decimal} input isn't pinned down across versions
 * the way {@code avg()}'s always-{@code Float64} return is (checked directly against ClickHouse's
 * own aggregate function docs), so casting explicitly keeps the wire type — and therefore which
 * typed getter {@link #clientV2} reads it with — deterministic instead of guessed.
 *
 * <p><b>Not yet compiled or run in this session</b> (no JDK 21 available) — same caveat as every
 * other production/benchmark change made this way in this project; the {@code
 * ClickHouseBinaryFormatReader#getDouble(int)} call in {@link #clientV2} is the one genuinely new
 * API surface this class exercises that no earlier benchmark in this suite has proven — everything
 * else ({@code getLong}, this driver's plain {@code decodeRows}/blackhole-consume pattern) mirrors
 * {@link StreamingScanBenchmark}/{@link PointQueryBenchmark} exactly.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AggregationBenchmark {

  private static final String AGGREGATION_SQL =
      "SELECT id % 100 AS category, count() AS cnt, avg(amount) AS avg_amount, "
          + "quantile(0.95)(toFloat64(amount)) AS p95_amount FROM "
          + PointQueryTable.NAME
          + " GROUP BY category ORDER BY cnt DESC";

  /**
   * Row-count tiers — see docs/PERFORMANCE.md's Phase 5 "Dataset" table for what each tier is for.
   * Unlike {@link StreamingScanBenchmark}, the result set this benchmark decodes is always 100 rows
   * regardless of {@code rows}; these tiers instead vary how much server-side aggregation work
   * ClickHouse does to produce that fixed-size result.
   */
  @Param({"10000", "100000", "1000000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;

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
    decodingScheduler = RowDecodingScheduler.defaults();
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .build();
  }

  /** Releases client-v2's connection pool and this driver's decode scheduler. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    decodingScheduler.dispose();
  }

  /**
   * This driver: {@link ClickHouseHttpTransport#query} + {@link RowBinaryDecoder#decode} — the real
   * production decode path (off the Netty event loop, via {@link #decodingScheduler}), not the
   * scheduler-free {@link RowBinaryDecoder#decodeRows} test/benchmark shortcut — draining and
   * blackhole-consuming every decoded group row.
   */
  @Benchmark
  public void ourDriver(final Blackhole blackhole) {
    final Flux<ByteBuffer> body =
        ourTransport.query(ClickHouseQuery.of(AGGREGATION_SQL)).asByteArray().map(ByteBuffer::wrap);
    final long rowCount =
        RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.NONE)
            .flatMapMany(DecodedResult::rows)
            .doOnNext(blackhole::consume)
            .count()
            .block(Duration.ofSeconds(30));
    blackhole.consume(rowCount);
  }

  /**
   * client-v2: {@link Client#query} + {@link ClickHouseBinaryFormatReader}, reading all four
   * projected columns per group row — {@code category}/{@code cnt} via {@code getLong} (the same
   * getter {@link StreamingScanBenchmark} already proved correct for a {@code UInt64} column),
   * {@code avg_amount}/{@code p95_amount} via {@code getDouble} (both guaranteed {@code Float64} by
   * the {@code SELECT}'s own casts — see this class's Javadoc).
   */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    long rowCount = 0;
    try (QueryResponse response = clientV2.query(AGGREGATION_SQL).get(30, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
        blackhole.consume(reader.getLong(2));
        blackhole.consume(reader.getDouble(3));
        blackhole.consume(reader.getDouble(4));
        rowCount++;
      }
    }
    blackhole.consume(rowCount);
  }
}

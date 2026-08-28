package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * Variant A of the ROADMAP.md Phase 12 "Formal latency-path-isolation plan (2026-08-25)" A/B/C/D
 * ladder — the exact current production path ({@link ClickHouseHttpTransport} → {@code
 * response.asByteArray().map(ByteBuffer::wrap)} → {@link
 * io.github.camilyed.clickhouse.r2dbc.core.FluxInputStreamBridge} → {@link RowDecodingScheduler} →
 * client-v2's blocking RowBinary reader → {@link DecodedRow}), faithfully reproduced from {@link
 * TrivialQueryBenchmark}/{@link PointQueryBenchmark}/{@link StreamingScanBenchmark} rather than
 * simplified, with one deliberate difference from those three: both sides' connection pool and this
 * driver's decoder worker count are pinned to {@link #POOL_SIZE} explicitly (those three establish
 * shared, ongoing trend baselines and are intentionally left at each driver's own default pool
 * size, which is why this is a separate class rather than an edit to them — see their own Javadoc
 * headers).
 *
 * <p><b>Scenarios</b> (three {@code @Benchmark} methods per driver, not a {@code @Param}, since
 * {@link #thisDriverStream10k} needs its own time-to-first-row {@link org.HdrHistogram.Histogram}
 * the way {@link StreamingScanBenchmark} does — a shape a shared {@code @Param}-driven method can't
 * express as cleanly): {@code SELECT 1} (protocol floor, no table), a single-row parameterized
 * point lookup against {@link PointQueryTable}, and a full 10,000-row streaming scan of the same
 * table. Deliberately not the 1M-row scan {@link StreamingScanBenchmark} already covers — this
 * ladder is about fixed per-query overhead, which a large scan amortizes away.
 *
 * <p><b>Concurrency</b> is driven by JMH's {@code -t}/{@code --threads} CLI flag against this
 * class's shared {@link Scope#Benchmark} state, not a field here — run once at {@code -t 1}
 * (isolates fixed overhead, minimal queueing) and once at {@code -t 8} (full matched-pool
 * utilization, {@code == }{@link #POOL_SIZE}) per the plan's "Concurrency" section; 32/128 are
 * explicitly out of scope for this pass (queueing at those levels can hide a ~100µs fixed cost).
 *
 * <p>Variants B/C/D (byte-copy avoidance, transport-acquisition-before-decoder-admission prototype,
 * and the optional benchmark-local {@code BinaryInput} adapter) are separate classes added
 * alongside this one as each is built — see ROADMAP.md's Phase 12 section for the full plan and
 * status.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LatencyPathVariantABenchmark {

  private static final String SELECT_1_SQL = "SELECT 1";

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;

  private static final long STREAM_ROWS = 10_000;

  /**
   * Both sides' connection pool, and this driver's decoder worker count, pinned to this value — the
   * plan's "start at pool=8" baseline, matching this project's own established default ({@code
   * ClickHouseConnectionFactory} couples decoder worker count to resolved pool size the same way in
   * production). Not a {@code @Param}: the plan explicitly defers 32/128 to a later pass.
   */
  private static final int POOL_SIZE = 8;

  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;
  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  /**
   * Starts the shared container, seeds {@link PointQueryTable} at {@link #STREAM_ROWS}, and
   * configures both drivers with the same {@link #POOL_SIZE}-connection pool — this driver via the
   * {@code (baseUrl, Authentication, maxConnections)} constructor {@link
   * BoundedPoolConcurrencyBenchmark} already established this pattern with, client-v2 via {@code
   * Client.Builder#setMaxConnections}/{@code enableConnectionPool}. {@code useAsyncRequests(true)}
   * is required, not optional — see {@link BoundedPoolConcurrencyBenchmark}'s Javadoc for the cloud
   * run that surfaced why client-v2 defaults to synchronous execution otherwise.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(STREAM_ROWS);
    ids = PointQueryTable.deterministicIds(STREAM_ROWS, ID_POOL_SIZE, ID_SEED);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            Authentication.basic(BenchmarkEnvironment.username(), BenchmarkEnvironment.password()),
            POOL_SIZE);
    decodingScheduler = RowDecodingScheduler.withWorkerCount(POOL_SIZE);
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .enableConnectionPool(true)
            .setMaxConnections(POOL_SIZE)
            .useAsyncRequests(true)
            .build();
  }

  /** Releases both clients' connection pools, this driver's decode scheduler, and its transport. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    decodingScheduler.dispose();
    ourTransport.dispose();
  }

  /** This driver, {@code SELECT 1} — see {@link TrivialQueryBenchmark#thisDriver} for the shape. */
  @Benchmark
  public void thisDriverSelect1(final Blackhole blackhole) {
    final Flux<ByteBuffer> body =
        ourTransport.query(ClickHouseQuery.of(SELECT_1_SQL)).asByteArray().map(ByteBuffer::wrap);
    final DecodedRow row =
        RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.LZ4)
            .flatMapMany(DecodedResult::rows)
            .blockFirst(Duration.ofSeconds(10));
    blackhole.consume(row);
  }

  /** client-v2, {@code SELECT 1} — see {@link TrivialQueryBenchmark#clientV2} for the shape. */
  @Benchmark
  public void clientV2Select1(final Blackhole blackhole) throws Exception {
    try (QueryResponse response = clientV2.query(SELECT_1_SQL).get(10, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
      }
    }
  }

  /** This driver, point lookup — see {@link PointQueryBenchmark#thisDriver} for the shape. */
  @Benchmark
  public void thisDriverPoint(final Blackhole blackhole) {
    final long id = nextId();
    final ClickHouseQuery query =
        ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    final DecodedRow row =
        RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.LZ4)
            .flatMapMany(DecodedResult::rows)
            .blockFirst(Duration.ofSeconds(10));
    blackhole.consume(row);
  }

  /** client-v2, point lookup — see {@link PointQueryBenchmark#clientV2} for the shape. */
  @Benchmark
  public void clientV2Point(final Blackhole blackhole) throws Exception {
    final long id = nextId();
    try (QueryResponse response =
        clientV2.query(SELECT_BY_ID_SQL, Map.of("id", id)).get(10, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        blackhole.consume(reader.getString(1));
        blackhole.consume(reader.getBigDecimal(2));
      }
    }
  }

  /**
   * This driver, full {@link #STREAM_ROWS}-row scan — see {@link StreamingScanBenchmark#thisDriver}
   * for the shape. Time-to-first-row is not tracked separately here (unlike {@link
   * StreamingScanBenchmark}): this benchmark's own {@code SampleTime} measurement is what the
   * A/B/C/D table compares across variants, not a TTFR breakdown, which stays {@link
   * StreamingScanBenchmark}'s job for the tiers it already owns (10k/100k/1M).
   */
  @Benchmark
  public void thisDriverStream10k(final Blackhole blackhole) {
    final Flux<ByteBuffer> body =
        ourTransport.query(ClickHouseQuery.of(SELECT_ALL_SQL)).asByteArray().map(ByteBuffer::wrap);
    final long rowCount =
        RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.LZ4)
            .flatMapMany(DecodedResult::rows)
            .doOnNext(blackhole::consume)
            .count()
            .block(Duration.ofSeconds(60));
    blackhole.consume(rowCount);
  }

  /**
   * client-v2, full {@link #STREAM_ROWS}-row scan — see {@link StreamingScanBenchmark#clientV2}.
   */
  @Benchmark
  public void clientV2Stream10k(final Blackhole blackhole) throws Exception {
    long rowCount = 0;
    try (QueryResponse response = clientV2.query(SELECT_ALL_SQL).get(60, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
        blackhole.consume(reader.getString(2));
        blackhole.consume(reader.getBigDecimal(3));
        rowCount++;
      }
    }
    blackhole.consume(rowCount);
  }

  /**
   * Advances through the pre-generated {@link #ids} pool — see {@link PointQueryBenchmark#nextId}.
   */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

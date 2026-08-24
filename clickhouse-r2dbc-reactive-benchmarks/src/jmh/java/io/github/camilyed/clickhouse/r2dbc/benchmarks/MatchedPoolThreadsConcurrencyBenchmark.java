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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * The pool-matched {@code @Threads(8)} control experiment proposed in docs/PERFORMANCE.md's
 * 2026-08-19 section ("Proposed control experiment, not yet built") — isolates one of the two
 * variables {@link ConcurrencyBenchmark} left entangled. {@link ConcurrencyBenchmark}'s
 * {@code @Threads(8)} run showed this driver *losing* on mean through p99.9 while winning
 * dramatically on the tail, but that run had two differences from {@link
 * BoundedPoolConcurrencyBenchmark} (which shows no such regression) at once: blocking calls vs.
 * non-blocking, <em>and</em> unmatched default connection pools vs. a matched 8-connection pool.
 * Neither benchmark alone tells you which variable caused the regression.
 *
 * <p>This class changes exactly one of those two variables relative to {@link
 * ConcurrencyBenchmark}: both drivers still use {@code @Threads(8)} blocking calls (same as {@link
 * ConcurrencyBenchmark}), but both are now given the identical {@link #POOL_SIZE}-connection budget
 * {@link BoundedPoolConcurrencyBenchmark} uses — this driver via the {@code (baseUrl,
 * Authentication, maxConnections)} constructor, client-v2 via {@code
 * Client.Builder#setMaxConnections}/{@code enableConnectionPool}. If the mean/p99 regression
 * disappears here with pools matched but callers still blocking, that isolates the pool mismatch as
 * the cause, not blocking-vs-non-blocking calling style; if it persists, blocking calls themselves
 * are implicated instead.
 *
 * <p>Deliberately blocking on this driver's side (a {@code Flux} built and immediately {@code
 * .blockFirst()}'d per JMH worker thread) even though that is not this driver's idiomatic
 * concurrency shape — see {@link BoundedPoolConcurrencyBenchmark}'s Javadoc for the non-blocking
 * {@code Flux.flatMap} shape that actually plays to this driver's strengths. The point of this
 * class is a same-resources, same-calling-style control, not a demonstration of either driver's
 * best case.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MatchedPoolThreadsConcurrencyBenchmark {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /**
   * Physical connection pool size, matched on both sides — the same value {@link
   * BoundedPoolConcurrencyBenchmark#POOL_SIZE} uses, so this control experiment and that benchmark
   * differ only in calling style (blocking {@code @Threads(8)} here vs. non-blocking {@code
   * Flux.flatMap} there), not in pool budget.
   */
  private static final int POOL_SIZE = 8;

  /** Row-count tier for {@link PointQueryTable} — kept fixed; pool/thread count is the axis. */
  @Param({"10000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;
  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and configures both drivers with
   * the same {@link #POOL_SIZE}-connection pool — this driver via the {@code (baseUrl,
   * Authentication, maxConnections)} constructor, client-v2 via {@code
   * Client.Builder#setMaxConnections}/{@code enableConnectionPool}, exactly as {@link
   * BoundedPoolConcurrencyBenchmark#setUpTrial} does.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    ids = PointQueryTable.deterministicIds(rows, ID_POOL_SIZE, ID_SEED);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            Authentication.basic(BenchmarkEnvironment.username(), BenchmarkEnvironment.password()),
            POOL_SIZE);
    decodingScheduler = RowDecodingScheduler.defaults();
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .enableConnectionPool(true)
            .setMaxConnections(POOL_SIZE)
            .build();
  }

  /** Releases both clients' connection pools and this driver's decode scheduler. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    decodingScheduler.dispose();
  }

  /**
   * This driver under {@code @Threads(8)} with a matched {@link #POOL_SIZE}-connection pool: each
   * worker thread blocks on its own {@link RowBinaryDecoder#decode} call — the real production
   * decode path (off the Netty event loop, via the shared {@link #decodingScheduler}), not the
   * scheduler-free {@link RowBinaryDecoder#decodeRows} test/benchmark shortcut this class used
   * before its own scheduler-fairness gap was found (see {@link ConcurrencyBenchmark}'s Javadoc for
   * the same fix and why the previously "3-fork confirmed" numbers here need re-running) —
   * otherwise identical calling shape to {@link ConcurrencyBenchmark#thisDriver}, only the pool
   * size changed.
   */
  @Benchmark
  @Threads(8)
  public void thisDriver(final Blackhole blackhole) {
    final long id = nextId();
    final ClickHouseQuery query =
        ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    final DecodedRow row =
        RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.NONE)
            .flatMapMany(DecodedResult::rows)
            .blockFirst(Duration.ofSeconds(10));
    blackhole.consume(row);
  }

  /**
   * client-v2 under {@code @Threads(8)} with a matched {@link #POOL_SIZE}-connection pool: each
   * worker thread issues its own blocking {@link Client#query} call, identical calling shape to
   * {@link ConcurrencyBenchmark#clientV2} — only the pool size changed.
   */
  @Benchmark
  @Threads(8)
  public void clientV2(final Blackhole blackhole) throws Exception {
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

  /** Advances through the pre-generated {@link #ids} pool — thread-safe via {@link AtomicLong}. */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

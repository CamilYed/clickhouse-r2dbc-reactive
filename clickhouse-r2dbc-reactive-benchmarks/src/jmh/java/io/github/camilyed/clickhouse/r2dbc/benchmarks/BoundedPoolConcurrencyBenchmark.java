package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
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
import java.util.concurrent.CompletableFuture;
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
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Level 3, the shape {@link ConcurrencyBenchmark}'s {@code @Threads(N)} class deliberately does
 * <em>not</em> cover: a deliberately <b>small, bounded</b> connection pool ({@link #POOL_SIZE}
 * connections) serving {@link #concurrency} logical queries at once — this driver via {@code
 * Flux.flatMap}'s own concurrency control (no blocked platform thread per logical query, JMH's own
 * calling thread issues and awaits all of them), client-v2 via its own async {@code
 * CompletableFuture}-returning {@link Client#query} (its internal HTTP client executor handles the
 * concurrency, not caller-provided threads either). Both sides configured with the same {@link
 * #POOL_SIZE} connections. This is the actual "~11 concurrent queries per user action" scenario
 * that motivated this project (see docs/PERFORMANCE.md's Phase 5 "Concurrency/burst" design) —
 * answers whether a small pool serving many more logical queries than it has connections behaves
 * better on this driver's non-blocking pipeline than on client-v2's, not just how the two compare
 * with platform-thread count and connection count both equal (that's {@link ConcurrencyBenchmark}'s
 * job).
 *
 * <p><b>Small first pass, not the full matrix.</b> Pool size is fixed at {@link #POOL_SIZE} (not
 * parameterized) and {@link #concurrency} only sweeps 8/32/128 — a deliberately small run to get a
 * first real signal before committing to a larger sweep (more concurrency levels, multiple pool
 * sizes), given every benchmark on this page already takes real wall-clock time and this project
 * has already been burned once by treating a single small-tier run as conclusive. Widen the matrix
 * only once this first pass actually shows something worth digging into further.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BoundedPoolConcurrencyBenchmark {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** Physical connection pool size — fixed and deliberately small, see the class's own Javadoc. */
  private static final int POOL_SIZE = 8;

  /**
   * How many logical concurrent queries to issue over the fixed {@link #POOL_SIZE}-connection pool.
   */
  @Param({"8", "32", "128"})
  public int concurrency;

  private static final long ROWS = 10_000;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;
  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and configures both drivers with
   * the same {@link #POOL_SIZE}-connection pool — this driver via the {@code (baseUrl,
   * Authentication, maxConnections)} constructor added specifically for this benchmark, client-v2
   * via {@code Client.Builder#setMaxConnections}/{@code enableConnectionPool}.
   *
   * <p>{@code useAsyncRequests(true)} is required here, not optional: client-v2's {@code
   * ClientConfigProperties.ASYNC_OPERATIONS} defaults to {@code false}, under which {@code
   * Client#query(...)} runs the whole blocking HTTP round trip synchronously on the calling thread
   * before ever handing back a (by then already-completed) {@code CompletableFuture}. Left at the
   * default, every client-v2 query below the {@code concurrency}-wide {@code flatMap} would execute
   * serially regardless of {@link #concurrency}/{@link #POOL_SIZE} — see {@link
   * ClientV2PointQueryClient}'s Javadoc for the cloud run that surfaced this (flat throughput and
   * flat per-query latency across every concurrency level, consistent with one sequential worker,
   * not {@link #POOL_SIZE}).
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(ROWS);
    ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
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

  /**
   * This driver: issues {@link #concurrency} logical point lookups via {@code Flux.range(...)
   * .flatMap(..., concurrency)} — every logical query is subscribed immediately; {@link
   * #POOL_SIZE}-many run with a live connection at once, the rest queue inside Reactor Netty's
   * pool/{@link ClickHouseHttpTransport}'s pipeline without occupying a JMH worker thread each. One
   * {@code @Benchmark} invocation is "issue and await all {@link #concurrency} logical queries."
   */
  @Benchmark
  public void thisDriver(final Blackhole blackhole) {
    final long completed =
        Flux.range(0, concurrency)
            .flatMap(ignored -> singlePointQuery(), concurrency)
            .count()
            .block(Duration.ofSeconds(60));
    blackhole.consume(completed);
  }

  /**
   * Uses {@link RowBinaryDecoder#decode} — the real production decode path, off the Netty event
   * loop via {@link #decodingScheduler} — rather than the scheduler-free {@link
   * RowBinaryDecoder#decodeRows} shortcut. That distinction matters most exactly here: this method
   * runs inside a non-blocking {@code Flux.flatMap} pipeline over live Netty responses, so skipping
   * the scheduler hop would mean blocking decode work runs directly on the event-loop thread this
   * benchmark's whole non-blocking premise depends on staying free.
   */
  private Mono<DecodedRow> singlePointQuery() {
    final long id = nextId();
    final ClickHouseQuery query =
        ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    // LZ4, not NONE - ourTransport resolves TransportOptions.defaults()'s own LZ4 default (sends
    // compress=1); NONE here would feed the decoder compressed bytes it can't parse.
    return RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.LZ4)
        .flatMapMany(DecodedResult::rows)
        .next();
  }

  /**
   * client-v2: issues {@link #concurrency} logical point lookups via {@link Client#query}'s own
   * {@code CompletableFuture}-returning async API (not one blocking call per JMH worker thread —
   * client-v2's internal HTTP client executor drives the actual concurrency, bounded by the same
   * {@link #POOL_SIZE}-connection pool configured in {@link #setUpTrial}), then awaits all of them.
   */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    @SuppressWarnings("unchecked")
    final CompletableFuture<Void>[] futures = new CompletableFuture[concurrency];
    for (int i = 0; i < concurrency; i++) {
      final long id = nextId();
      futures[i] =
          clientV2
              .query(SELECT_BY_ID_SQL, Map.of("id", id))
              .thenAccept(
                  response -> {
                    try (response) {
                      final ClickHouseBinaryFormatReader reader =
                          clientV2.newBinaryFormatReader(response);
                      while (reader.next() != null) {
                        blackhole.consume(reader.getString(1));
                        blackhole.consume(reader.getBigDecimal(2));
                      }
                    } catch (final Exception e) {
                      // Consumer<QueryResponse> (thenAccept's functional interface) declares no
                      // checked exceptions, but QueryResponse#close() declares `throws Exception`
                      // (try-with-resources' implicit close call) — rethrown unchecked so
                      // CompletableFuture.allOf's join/get still surfaces the original failure.
                      throw new RuntimeException(e);
                    }
                  });
    }
    CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);
  }

  /** Advances through the pre-generated {@link #ids} pool — thread-safe via {@link AtomicLong}. */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

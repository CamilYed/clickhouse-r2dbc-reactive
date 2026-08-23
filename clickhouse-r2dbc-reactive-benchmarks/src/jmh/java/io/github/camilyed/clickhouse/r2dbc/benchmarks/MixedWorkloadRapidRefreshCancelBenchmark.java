package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Part 1 of the "mixed heavy-workload rapid-refresh" scenario (real cancel-on-refresh semantics) —
 * {@link #users} concurrent logical user sessions, each repeatedly picking one of {@link
 * #HEAVY_QUERY_COUNT} distinct heavy analytical queries, firing it, then — after a short {@link
 * #THINK_TIME} shorter than these queries typically take — abandoning it for a new one, exactly
 * like a user rapidly hitting refresh on a dashboard before the previous load finished. A separate,
 * not-yet-built {@code MixedWorkloadRapidRefreshPileUpBenchmark} is planned as the other named
 * variant (no cancellation — old and new queries both run to completion) — a separate class rather
 * than a flag on this one, per this project's own "don't collapse different questions into one
 * parameterized benchmark" practice (the same reasoning {@code
 * BoundedPoolConcurrencyBenchmark}/{@code ConcurrencyBenchmark} already follow for blocking vs.
 * non-blocking).
 *
 * <p><b>Deliberately asymmetric connection pooling.</b> {@link #ourTransport} is built with no
 * explicit {@code maxConnections} at all, leaving Reactor Netty's own default pool sizing in effect
 * — the point being that this driver's real cancellation frees connections promptly enough that it
 * doesn't need a hand-tuned pool to survive rapid refreshes. {@link #clientV2}, by contrast, gets
 * an explicit, generous {@link #CLIENT_V2_POOL_SIZE}: the scenario under test is specifically
 * whether client-v2's pool still gets exhausted by discarded-but-still-running queries piling up
 * even when it's sized generously, not whether a small pool alone explains any slowdown (that
 * variable was already isolated and ruled out by {@code MatchedPoolThreadsConcurrencyBenchmark} —
 * see README's "Connection pooling" section).
 *
 * <p><b>Why {@code Flux.switchMap} is the harness, not a hand-rolled cancel loop.</b> {@code
 * switchMap} cancels the previous inner {@link Mono} the instant a new source tick arrives —
 * exactly "the user refreshed, drop what was in flight." For this driver, that cancellation reaches
 * all the way down to {@link ClickHouseHttpTransport} tearing down the connection and issuing a
 * best-effort {@code KILL QUERY} (see the driver's own cancellation behavior, README's "Known
 * limitations"). For client-v2, {@code Mono.fromFuture}'s cancellation calls {@code
 * CompletableFuture#cancel(true)} on the underlying future — but a plain {@code supplyAsync}-backed
 * future does not actually interrupt the executor thread already running the query, a well-known
 * {@code CompletableFuture} gotcha, so cancelling it only stops the *caller* from observing the
 * result; it does not free the connection or stop ClickHouse server-side any sooner. This benchmark
 * does not attempt to directly observe that background work (not reliably possible through
 * client-v2's public API alone — a {@code whenComplete} callback on an already-cancelled future
 * never fires from the real completion, since {@code CompletableFuture} only honors the first
 * completer); instead it measures the *effect*: total wall-clock time for the whole burst to
 * settle, and how many of the {@link #REFRESHES_PER_USER} refreshes per user actually survive to
 * completion (logged per iteration). If client-v2's discarded-but-still-running queries keep
 * competing for {@link #CLIENT_V2_POOL_SIZE} connections and server resources, that shows up as
 * worse settling time/lower survival count here, without needing to instrument client-v2's
 * internals directly.
 *
 * <p><b>{@code onBackpressureLatest()} is load-bearing, not decorative.</b> A first version fed
 * {@code Flux.interval(...)} straight into {@code switchMap} and saw ~100% survival on both drivers
 * regardless of {@link #THINK_TIME} — instrumented with per-tick timestamps, the real gap between
 * ticks turned out to match each query's full duration, not {@link #THINK_TIME}. Root cause: {@code
 * switchMap}'s no-prefetch variant only requests the next source item from upstream once the
 * current inner sequence has fully terminated, so a demand-gated source like {@code Flux.interval}
 * effectively can't tick faster than the current query finishes — turning the intended
 * overlapping-refresh race into a plain sequential queue. {@code onBackpressureLatest()} decouples
 * the two: it requests unbounded from {@code Flux.interval} so ticks keep firing on schedule,
 * buffering only the freshest one for {@code switchMap} to pick up (and silently dropping any that
 * go stale in between) — which is also the semantically correct behavior for "always show me the
 * latest refresh," not an artifact worth removing.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MixedWorkloadRapidRefreshCancelBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(MixedWorkloadRapidRefreshCancelBenchmark.class);

  /**
   * Row count each heavy query aggregates over. History: escalating this (1M -> 5M -> 20M, plus
   * more joins) to force queries slow enough to race against {@link #THINK_TIME} repeatedly hit
   * either "still 100% survival, too fast" or {@code MEMORY_LIMIT_EXCEEDED} on the shared
   * container. {@link #buildHeavyQueries()} now controls duration directly via {@code sleep()}
   * instead, so this only needs to be large enough for a realistic join/aggregation shape, not
   * large enough to be slow by scan cost alone — settled back at a size that never caused memory
   * pressure in any earlier run.
   */
  private static final long HEAVY_QUERY_ROWS = 1_000_000;

  /** How many distinct heavy query shapes {@link #buildHeavyQueries()} generates. */
  private static final int HEAVY_QUERY_COUNT = 12;

  /**
   * How many rapid-refresh cycles each simulated user performs per {@code @Benchmark} call —
   * "kilkanaście" (a dozen-plus), not just enough to prove the mechanism once, so a real session's
   * worth of pileup pressure has a chance to build on client-v2's side.
   */
  private static final int REFRESHES_PER_USER = 15;

  /**
   * Delay between one refresh and the next — deliberately short relative to each heavy query's
   * fixed {@link #HEAVY_QUERY_SLEEP_SECONDS} duration, to force cancellation on nearly every
   * refresh once {@code onBackpressureLatest()} (see this class's own Javadoc) lets ticks actually
   * fire on schedule.
   */
  private static final Duration THINK_TIME = Duration.ofMillis(5);

  /**
   * client-v2's connection pool size — deliberately generous (not matched to this driver's, which
   * uses Reactor Netty's own default instead — see this class's own Javadoc) so that any exhaustion
   * observed here is caused by discarded-but-still-running queries piling up, not by an
   * artificially small pool.
   */
  private static final int CLIENT_V2_POOL_SIZE = 32;

  /** How many concurrent simulated user sessions — a small first pass, not a full sweep. */
  @Param({"32"})
  public int users;

  private String[] heavyQueries;
  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;
  private final AtomicLong ourDriverIssued = new AtomicLong();
  private final AtomicLong ourDriverCompletions = new AtomicLong();
  private final AtomicLong clientV2Issued = new AtomicLong();
  private final AtomicLong clientV2Completions = new AtomicLong();

  /**
   * Starts the shared server, seeds a heavy-query-sized {@link PointQueryTable}, builds both
   * clients — deliberately asymmetric pooling, see this class's own Javadoc.
   *
   * <p>{@code useAsyncRequests(true)} is required for the scenario this class actually describes:
   * client-v2's {@code ASYNC_OPERATIONS} config defaults to {@code false}, under which {@code
   * Client#query(...)} runs synchronously on the calling thread and returns an
   * already-{@code completedFuture} — not the {@code supplyAsync}-backed future this class's own
   * Javadoc reasons about (the "cancel doesn't interrupt the executor thread" gotcha it describes
   * doesn't even apply to an already-completed future). Left at the default, every {@link #users}
   * session would run strictly sequentially on the single thread driving {@code switchMap}, not
   * "many concurrent users refreshing" — see {@link ClientV2PointQueryClient}'s Javadoc for the
   * cloud run that first surfaced this class of bug.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(HEAVY_QUERY_ROWS);
    HeavyWorkloadDimensionTables.seed();
    heavyQueries = buildHeavyQueries();
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            Authentication.basic(BenchmarkEnvironment.username(), BenchmarkEnvironment.password()));
    decodingScheduler = RowDecodingScheduler.defaults();
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .enableConnectionPool(true)
            .setMaxConnections(CLIENT_V2_POOL_SIZE)
            .useAsyncRequests(true)
            .build();
  }

  /**
   * Releases client-v2's connection pool and this driver's decode scheduler at the end of the
   * trial.
   */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    decodingScheduler.dispose();
  }

  /** Logs and resets each side's refresh-survival count once per measurement iteration. */
  @TearDown(Level.Iteration)
  public void tearDownIteration() {
    logSurvivalRate("ourDriver", ourDriverCompletions.getAndSet(0), ourDriverIssued.getAndSet(0));
    logSurvivalRate("clientV2", clientV2Completions.getAndSet(0), clientV2Issued.getAndSet(0));
  }

  /**
   * This driver: {@link #users} concurrent user sessions, each rapid-refreshing via {@code
   * switchMap} — real cancellation reaches {@link ClickHouseHttpTransport}.
   */
  @Benchmark
  public long ourDriver() {
    return Flux.range(0, users)
        .flatMap(
            user ->
                userRefreshLoop(
                    user, this::ourDriverHeavyQuery, ourDriverIssued, ourDriverCompletions),
            users)
        .then(Mono.just(0L))
        .block(Duration.ofSeconds(120));
  }

  /**
   * client-v2: {@link #users} concurrent user sessions, each rapid-refreshing via {@code switchMap}
   * wrapped around {@link Client#query(String)}'s {@code CompletableFuture} — see this class's own
   * Javadoc for why cancellation here only stops the caller from observing the result, not
   * necessarily the background work.
   */
  @Benchmark
  public long clientV2() {
    return Flux.range(0, users)
        .flatMap(
            user ->
                userRefreshLoop(
                    user, this::clientV2HeavyQuery, clientV2Issued, clientV2Completions),
            users)
        .then(Mono.just(0L))
        .block(Duration.ofSeconds(120));
  }

  /**
   * Each inner query emits exactly one {@code true} if it survives to completion — switchMap
   * silently drops a cancelled inner sequence's signals entirely, so counting {@code onNext}
   * emissions on the flattened {@link Flux} (rather than {@code doOnComplete}, which would only
   * fire once for the whole per-user loop) is what lets the {@code completions} counter actually
   * reflect how many of {@link #REFRESHES_PER_USER} refreshes per user survived, not just whether
   * the loop finished. {@code issued} counts every tick fired (not just survivors) so {@link
   * #logSurvivalRate} can report a correct ratio regardless of how many {@code @Benchmark}
   * invocations happened inside one measurement iteration — {@code issued} is only reset in {@link
   * #tearDownIteration}, same as {@code completions}, deliberately not once per invocation.
   */
  private Mono<Void> userRefreshLoop(
      final int user,
      final Function<String, Mono<Boolean>> heavyQuery,
      final AtomicLong issued,
      final AtomicLong completions) {
    return Flux.interval(Duration.ZERO, THINK_TIME)
        .take(REFRESHES_PER_USER)
        .doOnNext(ignored -> issued.incrementAndGet())
        .onBackpressureLatest()
        .switchMap(
            tick -> heavyQuery.apply(heavyQueries[(user + tick.intValue()) % HEAVY_QUERY_COUNT]))
        .doOnNext(ignored -> completions.incrementAndGet())
        .then();
  }

  /**
   * Uses {@link RowBinaryDecoder#decode} — the real production decode path, off {@link
   * #ourTransport}'s Netty event loop via {@link #decodingScheduler} — rather than the
   * scheduler-free {@link RowBinaryDecoder#decodeRows} shortcut. Matters especially here: {@link
   * #users} concurrent sessions all share the same handful of Netty event-loop threads, so blocking
   * decode work directly on one of them (as {@code decodeRows} would) risks stalling every other
   * user's in-flight query, not just the one being decoded.
   */
  private Mono<Boolean> ourDriverHeavyQuery(final String sql) {
    final Flux<ByteBuffer> body =
        ourTransport.query(ClickHouseQuery.of(sql)).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.NONE)
        .flatMapMany(DecodedResult::rows)
        .then(Mono.just(Boolean.TRUE));
  }

  private Mono<Boolean> clientV2HeavyQuery(final String sql) {
    return Mono.fromFuture(clientV2.query(sql))
        .flatMap(
            response ->
                Mono.fromCallable(() -> drain(response)).subscribeOn(Schedulers.boundedElastic()));
  }

  private boolean drain(final QueryResponse response) throws Exception {
    try (QueryResponse closeable = response;
        ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(closeable)) {
      while (reader.next() != null) {
        // intentionally discarded - only whether decoding ran to completion matters here
      }
    }
    return true;
  }

  /**
   * How long every heavy query's mandatory {@code sleep()} holds it open — a single fixed value,
   * not a per-query spread, so every refresh in this benchmark is exactly as likely to be raced by
   * the next {@link #THINK_TIME} tick, and so results stay directly comparable/reproducible run to
   * run (same reason {@link PointQueryTable#deterministicIds} avoids {@code Math.random()}).
   */
  private static final double HEAVY_QUERY_SLEEP_SECONDS = 2.0;

  /**
   * 12 distinct heavy six-table queries — {@link PointQueryTable} (the fact table) joined against
   * four dimensions ({@link HeavyWorkloadDimensionTables#BUCKET_DIM_NAME} at varying cardinality
   * 10-120, {@link HeavyWorkloadDimensionTables#REGION_DIM_NAME}, {@link
   * HeavyWorkloadDimensionTables#SEGMENT_DIM_NAME}, {@link
   * HeavyWorkloadDimensionTables#CHANNEL_DIM_NAME}), plus a fifth table touched via a scalar
   * subquery ({@link HeavyWorkloadDimensionTables#TIER_THRESHOLD_NAME}, filtering {@code amount}
   * against a per-query tier threshold) — six tables total per query, a varying {@code WHERE}
   * selectivity (~1/7 of rows), and a varying tier per query, so all 12 queries genuinely differ in
   * cost, filter, and result shape.
   *
   * <p><b>Wrapped in a {@code (SELECT sleep(...))} scalar subquery</b> — a real run at {@link
   * #HEAVY_QUERY_ROWS} rows showed 100% survival on both drivers even with a 5ms {@link
   * #THINK_TIME}: the actual per-query cost (row scan + join + memory pressure) was the only lever
   * controlling duration, so making the race reliable meant either scaling rows/joins further (the
   * path that led to repeated {@code MEMORY_LIMIT_EXCEEDED}) or decoupling duration from data
   * volume entirely. {@code sleep()} does the latter: ClickHouse's docs note it runs once per
   * block, not once per row, so calling it inside a scalar subquery with no {@code FROM} guarantees
   * exactly one sleep per query execution regardless of how many rows the real aggregation touches
   * — giving deterministic, tunable duration without needing a large or memory-heavy dataset at
   * all. Every query uses the same {@link #HEAVY_QUERY_SLEEP_SECONDS} — see that field's Javadoc
   * for why a fixed value beats a per-query spread here.
   */
  private static String[] buildHeavyQueries() {
    final String[] queries = new String[HEAVY_QUERY_COUNT];
    for (int i = 0; i < HEAVY_QUERY_COUNT; i++) {
      final int bucketModulus = 10 * (i + 1);
      final int filterRemainder = i % 7;
      final int tierId = i % HeavyWorkloadDimensionTables.TIER_THRESHOLD_ROWS;
      final String aggregation =
          "SELECT bd.bucket_name AS bucket, rd.region_name AS region, sd.segment_name AS segment, "
              + "cd.channel_name AS channel, count() AS cnt, avg(p.amount) AS avg_amount, "
              + "quantile(0.95)(toFloat64(p.amount)) AS p95_amount "
              + "FROM "
              + PointQueryTable.NAME
              + " AS p "
              + "INNER JOIN "
              + HeavyWorkloadDimensionTables.BUCKET_DIM_NAME
              + " AS bd ON p.id % "
              + bucketModulus
              + " = bd.bucket_id "
              + "INNER JOIN "
              + HeavyWorkloadDimensionTables.REGION_DIM_NAME
              + " AS rd ON p.id % 5 = rd.region_id "
              + "INNER JOIN "
              + HeavyWorkloadDimensionTables.SEGMENT_DIM_NAME
              + " AS sd ON p.id % 8 = sd.segment_id "
              + "INNER JOIN "
              + HeavyWorkloadDimensionTables.CHANNEL_DIM_NAME
              + " AS cd ON p.id % 4 = cd.channel_id "
              + "WHERE p.id % 7 = "
              + filterRemainder
              + " AND p.amount > (SELECT min_amount FROM "
              + HeavyWorkloadDimensionTables.TIER_THRESHOLD_NAME
              + " WHERE tier_id = "
              + tierId
              + ") "
              + "GROUP BY bucket, region, segment, channel ORDER BY cnt DESC";
      queries[i] =
          "SELECT (SELECT sleep("
              + HEAVY_QUERY_SLEEP_SECONDS
              + ")) AS delayMarker, * FROM ("
              + aggregation
              + ")";
    }
    return queries;
  }

  private void logSurvivalRate(final String driverName, final long completions, final long issued) {
    LOG.info("{} refreshes survived to completion: {}/{}", driverName, completions, issued);
  }
}

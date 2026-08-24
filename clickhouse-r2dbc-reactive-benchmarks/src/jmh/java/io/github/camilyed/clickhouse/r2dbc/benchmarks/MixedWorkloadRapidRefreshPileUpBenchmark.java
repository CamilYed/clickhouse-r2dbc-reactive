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
 * Part 2 of the "mixed heavy-workload rapid-refresh" scenario (Phase 11 PR4, see ROADMAP.md) — the
 * named companion to {@link MixedWorkloadRapidRefreshCancelBenchmark}: same {@link #users}
 * concurrent sessions, same {@link #HEAVY_QUERY_COUNT} heavy queries, same {@link #THINK_TIME}
 * refresh cadence, same deliberately asymmetric pooling ({@link #ourTransport} at Reactor Netty's
 * own default, {@link #clientV2} at an explicit generous {@link #CLIENT_V2_POOL_SIZE}) — but where
 * that class cancels the previous in-flight query the instant a new refresh tick fires ({@code
 * switchMap}), this class lets every tick's query run to completion regardless of how many later
 * ticks have already fired ({@code flatMap}) — old and new queries genuinely pile up, competing for
 * the same physical pool/server resources at once, the way a user rapidly hitting refresh would
 * behave against a client with no request-cancellation support at all. A separate class rather than
 * a flag on the cancel variant, per this project's own "don't collapse different questions into one
 * parameterized benchmark" practice (the same reasoning {@link
 * MixedWorkloadRapidRefreshCancelBenchmark}'s own Javadoc already gives for why it, in turn, isn't
 * a flag on {@code BoundedPoolConcurrencyBenchmark}/{@code ConcurrencyBenchmark}).
 *
 * <p>Every constant below (rows, query shapes, pool sizes, think time, sleep duration) is copied
 * from {@link MixedWorkloadRapidRefreshCancelBenchmark} unchanged rather than shared via a common
 * base class, so the two scenarios stay directly, visually comparable without a reader needing to
 * cross-reference a third file to know what {@link #THINK_TIME} or {@link #CLIENT_V2_POOL_SIZE}
 * actually is — the same "duplication over indirection for benchmark harness code" choice this
 * module makes elsewhere (see e.g. every {@code @State}-split benchmark's near-identical {@code
 * ThisDriverState}/{@code ClientV2State} pair).
 *
 * <p><b>Why {@code Flux.flatMap} (default, unbounded-ish concurrency) is the harness, not {@code
 * switchMap}.</b> {@code flatMap} subscribes to as many inner sequences as its concurrency limit
 * allows (Reactor's default is 256, far above this class's own {@link #REFRESHES_PER_USER}=15 per
 * user) without waiting for an earlier inner sequence to finish before starting the next one — the
 * opposite of {@code switchMap}'s cancel-the-previous behavior. Unlike {@link
 * MixedWorkloadRapidRefreshCancelBenchmark}'s own Javadoc, which documents needing {@code
 * onBackpressureLatest()} to stop {@code switchMap}'s per-inner-completion demand gating from
 * silently pacing {@code Flux.interval} down to one tick per query duration, {@code flatMap}'s own
 * default concurrency is high enough relative to {@link #REFRESHES_PER_USER} that it keeps
 * requesting new ticks from {@code Flux.interval} on schedule without needing the same operator —
 * dropping {@code onBackpressureLatest()} here is deliberate, not an oversight, since dropping any
 * tick at all would undercount how many queries this scenario is meant to pile up.
 *
 * <p><b>What "survival" means here.</b> {@link MixedWorkloadRapidRefreshCancelBenchmark} logs a
 * survival ratio because {@code switchMap} silently drops most refreshes' results — the interesting
 * number there is how many survive. Here, nothing is ever cancelled, so every issued refresh is
 * expected to eventually complete and the survival ratio should read at or near 100% on both
 * drivers by construction; it is logged anyway, in the same format, purely so a reviewer can
 * sanity-check that assumption against a real run rather than trusting the reasoning blind. The
 * actual metric this class exists to produce is JMH's own {@link Mode#SampleTime} wall-clock score
 * for the whole {@link #users}-session burst to settle — if client-v2's discarded-but-still-running
 * queries from the cancel variant were competing for {@link #CLIENT_V2_POOL_SIZE} connections and
 * server resources, forcing every refresh to actually run to completion here should make that
 * competition maximal and its cost fully visible in the settling time, not just inferable from a
 * lower survival count.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MixedWorkloadRapidRefreshPileUpBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(MixedWorkloadRapidRefreshPileUpBenchmark.class);

  /** Matches {@link MixedWorkloadRapidRefreshCancelBenchmark#HEAVY_QUERY_ROWS} exactly. */
  private static final long HEAVY_QUERY_ROWS = 1_000_000;

  /** How many distinct heavy query shapes {@link #buildHeavyQueries()} generates. */
  private static final int HEAVY_QUERY_COUNT = 12;

  /** Matches {@link MixedWorkloadRapidRefreshCancelBenchmark#REFRESHES_PER_USER} exactly. */
  private static final int REFRESHES_PER_USER = 15;

  /** Matches {@link MixedWorkloadRapidRefreshCancelBenchmark#THINK_TIME} exactly. */
  private static final Duration THINK_TIME = Duration.ofMillis(5);

  /** Matches {@link MixedWorkloadRapidRefreshCancelBenchmark#CLIENT_V2_POOL_SIZE} exactly. */
  private static final int CLIENT_V2_POOL_SIZE = 32;

  /** How many concurrent simulated user sessions — matches the cancel variant's own value. */
  @Param({"32"})
  public int users;

  private String[] heavyQueries;
  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;
  private final AtomicLong thisDriverIssued = new AtomicLong();
  private final AtomicLong thisDriverCompletions = new AtomicLong();
  private final AtomicLong clientV2Issued = new AtomicLong();
  private final AtomicLong clientV2Completions = new AtomicLong();

  /**
   * Starts the shared server, seeds a heavy-query-sized {@link PointQueryTable}, builds both
   * clients — deliberately asymmetric pooling, see this class's own Javadoc. {@code
   * useAsyncRequests(true)} is required for the same reason {@link
   * MixedWorkloadRapidRefreshCancelBenchmark#setUpTrial()}'s own Javadoc explains.
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
    logSurvivalRate(
        "thisDriver", thisDriverCompletions.getAndSet(0), thisDriverIssued.getAndSet(0));
    logSurvivalRate("clientV2", clientV2Completions.getAndSet(0), clientV2Issued.getAndSet(0));
  }

  /**
   * This driver: {@link #users} concurrent user sessions, each rapid-refreshing via {@code flatMap}
   * — every refresh runs to completion, piling up against {@link #ourTransport}'s pool.
   */
  @Benchmark
  public long thisDriver() {
    return Flux.range(0, users)
        .flatMap(
            user ->
                userRefreshLoop(
                    user, this::thisDriverHeavyQuery, thisDriverIssued, thisDriverCompletions),
            users)
        .then(Mono.just(0L))
        .block(Duration.ofSeconds(120));
  }

  /**
   * client-v2: {@link #users} concurrent user sessions, each rapid-refreshing via {@code flatMap}
   * wrapped around {@link Client#query(String)}'s {@code CompletableFuture} — every refresh runs to
   * completion, piling up against {@link #clientV2}'s pool.
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
   * Unlike {@link MixedWorkloadRapidRefreshCancelBenchmark#userRefreshLoop}'s {@code switchMap},
   * {@code flatMap} here lets every one of {@link #REFRESHES_PER_USER} ticks' inner queries run
   * concurrently to completion rather than cancelling the previous one — see this class's own
   * Javadoc for why no {@code onBackpressureLatest()} is needed. {@code issued}/{@code completions}
   * are tracked the same way as the cancel variant purely for log-format symmetry - see this
   * class's own Javadoc section "What survival means here" for why the ratio itself isn't the
   * interesting number in this variant.
   */
  private Mono<Void> userRefreshLoop(
      final int user,
      final Function<String, Mono<Boolean>> heavyQuery,
      final AtomicLong issued,
      final AtomicLong completions) {
    return Flux.interval(Duration.ZERO, THINK_TIME)
        .take(REFRESHES_PER_USER)
        .doOnNext(ignored -> issued.incrementAndGet())
        .flatMap(
            tick -> heavyQuery.apply(heavyQueries[(user + tick.intValue()) % HEAVY_QUERY_COUNT]))
        .doOnNext(ignored -> completions.incrementAndGet())
        .then();
  }

  /**
   * Uses {@link RowBinaryDecoder#decode} — the real production decode path, off {@link
   * #ourTransport}'s Netty event loop via {@link #decodingScheduler} — see {@link
   * MixedWorkloadRapidRefreshCancelBenchmark#thisDriverHeavyQuery} for why this matters especially
   * under many concurrent sessions sharing the same event-loop threads.
   */
  private Mono<Boolean> thisDriverHeavyQuery(final String sql) {
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

  /** Matches {@link MixedWorkloadRapidRefreshCancelBenchmark#HEAVY_QUERY_SLEEP_SECONDS} exactly. */
  private static final double HEAVY_QUERY_SLEEP_SECONDS = 2.0;

  /**
   * Identical query shapes to {@link MixedWorkloadRapidRefreshCancelBenchmark#buildHeavyQueries()}
   * — see that method's own Javadoc for the full six-table join/{@code sleep()} rationale, copied
   * here unchanged rather than shared, per this class's own Javadoc on duplication.
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

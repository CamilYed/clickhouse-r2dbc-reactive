package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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

/**
 * The headline application-level comparison — see {@code CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md}
 * in full, sections 6-9 and 19(PR2) specifically. Answers: <b>with the same physical connection
 * limit and the same sustained logical concurrency, how many real point queries per second can each
 * public client path complete, and what latency distribution do individual queries see?</b>
 *
 * <p>Fixes the methodological gaps every earlier concurrency benchmark in this module has (plan
 * section 2): {@link OurDriverPointQueryClient}/{@link ClientV2PointQueryClient} go through public
 * SPI only, never {@code ClickHouseHttpTransport}/{@code RowBinaryDecoder} directly; both sides map
 * to the identical {@link PointResult} record; the physical pool size is explicit and equal on both
 * sides; and — unlike {@link BoundedPoolConcurrencyBenchmark}, whose JMH percentiles are
 * <em>burst</em> latency (one JMH operation = a whole {@code concurrency}-sized batch) — this class
 * separately records genuine <em>per-query</em> latency via {@link Recorder}, because JMH's own
 * {@link Mode#Throughput} score here only ever reports the aggregate rate, never an individual
 * query's latency distribution.
 *
 * <h2>What the JMH Score means</h2>
 *
 * Each {@code @Benchmark} method issues {@link #REQUESTS_PER_INVOCATION} logical point queries,
 * bounded to {@code concurrency} in flight at once via {@code Flux.flatMap(..., concurrency)} — for
 * {@code concurrency=128}, as soon as one query completes another is already available, producing
 * sustained closed-loop pressure rather than one short burst. {@link OperationsPerInvocation}
 * <b>multiplies</b> JMH's own measured invocation rate (batches/sec) by {@link
 * #REQUESTS_PER_INVOCATION}, so the printed {@code Score} is already <b>logical queries/sec</b>,
 * not "invocations (batches) per second".
 *
 * <p><b>Empirically verified</b> against the 2026-08-23 trusted cloud run's raw {@code
 * results.json}: at {@code concurrency=8} (where logical concurrency exactly matches {@code
 * poolSize}, so the queueing-theory identity {@code throughput ≈ concurrency / mean-latency}
 * applies directly), client-v2's reported score (≈889 ops/s) matches {@code 8 /
 * mean-per-query-latency-in-seconds} to within the expected noise band — not the wildly different
 * number {@code raw-invocations/sec} (≈0.22/sec, three orders of magnitude off) would have produced
 * if the annotation weren't applying its multiplication correctly. Separately, both drivers' scores
 * stay flat (roughly 800–890 ops/s) across {@code concurrency=8/32/128} in that same run — exactly
 * the signature of a benchmark correctly bottlenecked by the matched {@code poolSize}-connection
 * pool rather than by logical concurrency, which is only a coherent observation if the reported
 * unit really is logical queries/sec throughout. No further verification needed before trusting
 * this class's {@code Score} column as reported.
 *
 * <h2>What the logged per-query latency means</h2>
 *
 * Each side's {@code Recorder} records each individual query's wall-clock latency (subscribe to
 * mapped {@link PointResult}), reset and logged once per JMH measurement iteration via {@link
 * Recorder#getIntervalHistogram()} — this is the number to read for "how long does one query take",
 * not JMH's own {@code Score}/{@code Error}, which only describes the aggregate throughput of the
 * whole sustained workload.
 *
 * <h2>What the logged TRUE MERGED per-query latency means</h2>
 *
 * Phase 11 PR4 (see ROADMAP.md): each state also accumulates every measurement iteration's interval
 * histogram into one running, never-reset {@link Histogram} via {@link Histogram#add(Histogram)} —
 * an exact, lossless merge, since every histogram involved shares the same {@link
 * #LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS}/precision settings — logged once per fork at {@link
 * Level#Trial} teardown. Unlike the per-iteration lines above (an approximate mean of however many
 * iterations a fork ran), this line's percentiles are computed over every sample the fork actually
 * recorded. With {@code forks=1} that's already an exact global percentile for that {@code (driver,
 * concurrency)} combination; with {@code forks>1} (the trusted CI profile) {@code analyze.py} still
 * averages this line's value across forks, but that is now only averaging a handful of
 * already-exact per-fork percentiles, not dozens of approximate per-iteration ones.
 *
 * <h2>Why {@link ThisDriverState}/{@link ClientV2State} are separate {@code @State} classes</h2>
 *
 * Earlier versions of this class used one shared {@code @State(Scope.Benchmark)} holding both
 * clients, built and prewarmed together in a single {@code @Setup(Level.Trial)} regardless of which
 * {@code @Benchmark} method a given JMH fork was actually about to run. That's wasted setup cost by
 * itself, but it stops being just wasted cost once Phase 11 PR2's resource-model measurement
 * (thread count, process CPU, RSS) runs per fork: a fork profiling only {@link
 * #thisDriver(ThisDriverState)} would still have client-v2's connection pool and executor threads
 * alive in the same process, polluting exactly the numbers that measurement exists to capture.
 * Splitting the client/recorder state per driver means JMH only instantiates and {@code @Setup}s
 * the state a given fork's {@code @Benchmark} method actually declares as a parameter — see JMH's
 * own state-injection contract (a {@code @State} class not referenced, directly or transitively, by
 * the executing benchmark method is never constructed). The environment bootstrap ({@link
 * BenchmarkEnvironment}, {@link PointQueryTable#seed}) stays duplicated across both state classes
 * rather than factored out to a third shared state, since both are already idempotent/synchronized
 * (see their own Javadoc) and a shared state would reintroduce the same fork-pollution problem this
 * split exists to avoid.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class PublicApiMatchedPoolThroughputBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(PublicApiMatchedPoolThroughputBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** How many logical point queries one {@code @Benchmark} invocation issues and awaits. */
  private static final int REQUESTS_PER_INVOCATION = 4096;

  /** How many prewarm queries each side runs before measurement — see the plan's section 5. */
  private static final int PREWARM_CALLS = 64;

  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /** This driver's client, connection pool, and per-query latency recorder, isolated per fork. */
  @State(Scope.Benchmark)
  public static class ThisDriverState {

    /**
     * Physical connection pool size — not yet swept, see the plan's section 6. Kept in lockstep
     * with {@link ClientV2State#poolSize} so both sides always sweep the same values.
     */
    @Param({"8"})
    public int poolSize;

    /** How many logical queries are allowed in flight at once, bounded by {@link #poolSize}. */
    @Param({"8", "32", "128"})
    public int concurrency;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private OurDriverPointQueryClient client;
    private Recorder latencyRecorder;
    private Histogram mergedHistogram;

    /**
     * Starts the shared/external server, seeds {@link PointQueryTable}, builds this driver's client
     * at an explicit {@link #poolSize}, then prewarms it — moves DNS resolution, class loading, and
     * first connection-pool expansion out of what gets measured.
     */
    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client = new OurDriverPointQueryClient(poolSize);
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
      mergedHistogram = new Histogram(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    /**
     * Logs this fork's exact merged per-query latency (see the class Javadoc's "TRUE MERGED"
     * section), then closes the client's connection and disposes its owning factory.
     */
    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("thisDriver", mergedHistogram);
      client.close();
    }

    /**
     * Merges this iteration's interval histogram into {@link #mergedHistogram}, then logs and
     * resets this driver's per-query latency distribution.
     */
    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      final Histogram interval = latencyRecorder.getIntervalHistogram();
      mergedHistogram.add(interval);
      logLatencySummary("thisDriver", interval);
    }

    /**
     * Advances through the pre-generated {@link #ids} pool - thread-safe via {@link AtomicLong}.
     */
    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** client-v2's client, connection pool, and per-query latency recorder, isolated per fork. */
  @State(Scope.Benchmark)
  public static class ClientV2State {

    /**
     * Physical connection pool size — not yet swept, see the plan's section 6. Kept in lockstep
     * with {@link ThisDriverState#poolSize} so both sides always sweep the same values.
     */
    @Param({"8"})
    public int poolSize;

    /** How many logical queries are allowed in flight at once, bounded by {@link #poolSize}. */
    @Param({"8", "32", "128"})
    public int concurrency;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private ClientV2PointQueryClient client;
    private Recorder latencyRecorder;
    private Histogram mergedHistogram;

    /**
     * Starts the shared/external server, seeds {@link PointQueryTable}, builds client-v2's client
     * at an explicit {@link #poolSize}, then prewarms it — moves DNS resolution, class loading, and
     * first connection-pool expansion out of what gets measured.
     */
    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client = new ClientV2PointQueryClient(poolSize);
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
      mergedHistogram = new Histogram(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    /**
     * Logs this fork's exact merged per-query latency (see the class Javadoc's "TRUE MERGED"
     * section), then closes client-v2's client (and its connection pool).
     */
    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("clientV2", mergedHistogram);
      client.close();
    }

    /**
     * Merges this iteration's interval histogram into {@link #mergedHistogram}, then logs and
     * resets client-v2's per-query latency distribution.
     */
    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      final Histogram interval = latencyRecorder.getIntervalHistogram();
      mergedHistogram.add(interval);
      logLatencySummary("clientV2", interval);
    }

    /**
     * Advances through the pre-generated {@link #ids} pool - thread-safe via {@link AtomicLong}.
     */
    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** This driver, through the public R2DBC SPI only - see {@link OurDriverPointQueryClient}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long thisDriver(final ThisDriverState state) {
    return runWorkload(state.client, state.latencyRecorder, state.concurrency, state::nextId);
  }

  /** client-v2, through its public async API only - see {@link ClientV2PointQueryClient}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long clientV2(final ClientV2State state) {
    return runWorkload(state.client, state.latencyRecorder, state.concurrency, state::nextId);
  }

  /**
   * Issues {@link #REQUESTS_PER_INVOCATION} logical point queries bounded to {@code concurrency} in
   * flight, reduces every result's {@link PointResult#checksum()} into one order-independent sum -
   * see the plan's section 8 for why a checksum, not {@code Blackhole.consume} per query, is the
   * correctness barrier here: it forces both sides to have actually decoded real values regardless
   * of completion order under concurrency, without depending on which thread JMH's own {@code
   * Blackhole} is invoked from.
   */
  private long runWorkload(
      final PointQueryClient client,
      final Recorder latencyRecorder,
      final int concurrency,
      final LongSupplier idSupplier) {
    return Flux.range(0, REQUESTS_PER_INVOCATION)
        .flatMap(ignored -> timedQuery(client, latencyRecorder, idSupplier), concurrency)
        .reduce(0L, Long::sum)
        .block(Duration.ofMinutes(1));
  }

  private Mono<Long> timedQuery(
      final PointQueryClient client,
      final Recorder latencyRecorder,
      final LongSupplier idSupplier) {
    final long id = idSupplier.getAsLong();
    final long startNanos = System.nanoTime();
    return client
        .query(id)
        .doOnNext(
            result ->
                latencyRecorder.recordValue(Math.max((System.nanoTime() - startNanos) / 1000, 0)))
        .map(PointResult::checksum);
  }

  private static void logLatencySummary(final String driverName, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    // p99.99 is deliberately not logged here - the plan's section 9 guard ("don't interpret p99.99
    // below 100k samples") isn't met at REQUESTS_PER_INVOCATION=4096 per iteration/thread, so it
    // would correspond to roughly one sample and shouldn't be treated as evidence of anything.
    LOG.info(
        "{} per-query latency (µs, n={}): mean={}, p50={}, p90={}, p95={}, p99={}, p99.9={}, max={}",
        driverName,
        histogram.getTotalCount(),
        String.format("%.1f", histogram.getMean()),
        histogram.getValueAtPercentile(50),
        histogram.getValueAtPercentile(90),
        histogram.getValueAtPercentile(95),
        histogram.getValueAtPercentile(99),
        histogram.getValueAtPercentile(99.9),
        histogram.getMaxValue());
  }

  /**
   * Logs one fork's exact merged per-query latency distribution — see the class Javadoc's "What the
   * logged TRUE MERGED per-query latency means" section. A distinct log message (containing the
   * literal text {@code "TRUE MERGED"}) rather than reusing {@link #logLatencySummary}, so {@code
   * analyze.py} can tell the two kinds of line apart by regex without any ambiguity.
   */
  private static void logMergedLatencySummary(final String driverName, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    LOG.info(
        "{} TRUE MERGED per-query latency (µs, n={}): mean={}, p50={}, p90={}, p95={}, p99={},"
            + " p99.9={}, max={}",
        driverName,
        histogram.getTotalCount(),
        String.format("%.1f", histogram.getMean()),
        histogram.getValueAtPercentile(50),
        histogram.getValueAtPercentile(90),
        histogram.getValueAtPercentile(95),
        histogram.getValueAtPercentile(99),
        histogram.getValueAtPercentile(99.9),
        histogram.getMaxValue());
  }
}

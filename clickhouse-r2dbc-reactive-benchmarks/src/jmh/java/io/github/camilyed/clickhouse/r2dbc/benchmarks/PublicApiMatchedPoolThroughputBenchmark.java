package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * bounded to {@link #concurrency} in flight at once via {@code Flux.flatMap(..., concurrency)} —
 * for {@code concurrency=128}, as soon as one query completes another is already available,
 * producing sustained closed-loop pressure rather than one short burst. {@link
 * OperationsPerInvocation} <b>multiplies</b> JMH's own measured invocation rate (batches/sec) by
 * {@link #REQUESTS_PER_INVOCATION}, so the printed {@code Score} is already <b>logical
 * queries/sec</b>, not "invocations (batches) per second".
 *
 * <p><b>Empirically verified</b> against the 2026-08-23 trusted cloud run's raw {@code
 * results.json}: at {@code concurrency=8} (where logical concurrency exactly matches {@link
 * #poolSize}, so the queueing-theory identity {@code throughput ≈ concurrency / mean-latency}
 * applies directly), client-v2's reported score (≈889 ops/s) matches {@code 8 /
 * mean-per-query-latency-in-seconds} to within the expected noise band — not the wildly different
 * number {@code raw-invocations/sec} (≈0.22/sec, three orders of magnitude off) would have produced
 * if the annotation weren't applying its multiplication correctly. Separately, both drivers' scores
 * stay flat (roughly 800–890 ops/s) across {@code concurrency=8/32/128} in that same run — exactly
 * the signature of a benchmark correctly bottlenecked by the matched {@link #poolSize}-connection
 * pool rather than by logical concurrency, which is only a coherent observation if the reported
 * unit really is logical queries/sec throughout. No further verification needed before trusting
 * this class's {@code Score} column as reported.
 *
 * <h2>What the logged per-query latency means</h2>
 *
 * {@link #ourDriverLatencyRecorder}/{@link #clientV2LatencyRecorder} record each individual query's
 * wall-clock latency (subscribe to mapped {@link PointResult}), reset and logged once per JMH
 * measurement iteration via {@link Recorder#getIntervalHistogram()} — this is the number to read
 * for "how long does one query take", not JMH's own {@code Score}/{@code Error}, which only
 * describes the aggregate throughput of the whole sustained workload.
 */
@State(Scope.Benchmark)
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

  /**
   * Physical connection pool size, equal and explicit on both sides — not yet swept, see the plan's
   * section 6.
   */
  @Param({"8"})
  public int poolSize;

  /**
   * How many logical queries are allowed in flight at once, bounded by {@link #poolSize}
   * connections.
   */
  @Param({"8", "32", "128"})
  public int concurrency;

  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  private OurDriverPointQueryClient ourDriverClient;
  private ClientV2PointQueryClient clientV2Client;

  private Recorder ourDriverLatencyRecorder;
  private Recorder clientV2LatencyRecorder;

  /**
   * Starts the shared/external server, seeds {@link PointQueryTable}, builds both clients with an
   * identical, explicit {@link #poolSize}, then prewarms both equally before any measurement -
   * moves DNS resolution, class loading, and first connection-pool expansion out of what gets
   * measured.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(ROWS);
    ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);

    ourDriverClient = new OurDriverPointQueryClient(poolSize);
    clientV2Client = new ClientV2PointQueryClient(poolSize);

    ourDriverClient.prewarm(ids, PREWARM_CALLS);
    clientV2Client.prewarm(ids, PREWARM_CALLS);

    ourDriverLatencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    clientV2LatencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
  }

  /** Closes both clients' connections/pools at the end of the trial. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    ourDriverClient.close();
    clientV2Client.close();
  }

  /** Logs and resets each side's per-query latency distribution once per measurement iteration. */
  @TearDown(Level.Iteration)
  public void tearDownIteration() {
    logLatencySummary("ourDriver", ourDriverLatencyRecorder.getIntervalHistogram());
    logLatencySummary("clientV2", clientV2LatencyRecorder.getIntervalHistogram());
  }

  /** This driver, through the public R2DBC SPI only - see {@link OurDriverPointQueryClient}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long ourDriver() {
    return runWorkload(ourDriverClient, ourDriverLatencyRecorder);
  }

  /** client-v2, through its public async API only - see {@link ClientV2PointQueryClient}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long clientV2() {
    return runWorkload(clientV2Client, clientV2LatencyRecorder);
  }

  /**
   * Issues {@link #REQUESTS_PER_INVOCATION} logical point queries bounded to {@link #concurrency}
   * in flight, reduces every result's {@link PointResult#checksum()} into one order-independent sum
   * - see the plan's section 8 for why a checksum, not {@code Blackhole.consume} per query, is the
   * correctness barrier here: it forces both sides to have actually decoded real values regardless
   * of completion order under concurrency, without depending on which thread JMH's own {@code
   * Blackhole} is invoked from.
   */
  private long runWorkload(final PointQueryClient client, final Recorder latencyRecorder) {
    return Flux.range(0, REQUESTS_PER_INVOCATION)
        .flatMap(ignored -> timedQuery(client, latencyRecorder), concurrency)
        .reduce(0L, Long::sum)
        .block(Duration.ofMinutes(1));
  }

  private Mono<Long> timedQuery(final PointQueryClient client, final Recorder latencyRecorder) {
    final long id = nextId();
    final long startNanos = System.nanoTime();
    return client
        .query(id)
        .doOnNext(
            result ->
                latencyRecorder.recordValue(Math.max((System.nanoTime() - startNanos) / 1000, 0)))
        .map(PointResult::checksum);
  }

  /** Advances through the pre-generated {@link #ids} pool - thread-safe via {@link AtomicLong}. */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
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
}

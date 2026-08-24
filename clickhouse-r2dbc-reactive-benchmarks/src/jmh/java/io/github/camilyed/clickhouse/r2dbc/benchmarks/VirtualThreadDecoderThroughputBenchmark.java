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
 * Phase 11's JDK 21 virtual-thread decoder experiment (see ROADMAP.md, "Later (deferred, was
 * blocked on Phase 11)" section, 2026-08-24): the follow-up benchmark that widened both {@code
 * decoderWorkerCount} and {@code transportPendingAcquireMaxCount} together (see {@link
 * DecoderAndPendingAcquireWidenedThroughputBenchmark}) proved, via JFR ({@code jdk.ThreadPark}),
 * that decode is I/O-wait-dominated — a {@code RowDecodingScheduler} worker spends the overwhelming
 * majority of its time blocked reading network bytes, not doing CPU work. That finding motivates a
 * different fix than widening the platform-thread pool further: run decode tasks on JDK 21 virtual
 * threads instead, which park/unpark cheaply and don't hold a platform thread while blocked — see
 * {@code RowDecodingScheduler#virtualThreads(int)}'s Javadoc for the full motivation and this
 * driver's own pinning-risk analysis of client-v2's decode-path source.
 *
 * <p>Compares this driver against <b>itself</b>, same pool size, same query, same concurrency sweep
 * as {@link DecoderWorkerCountThroughputBenchmark} — only the decoder's thread type differs. {@link
 * PlatformThreadDecoderState} is today's default (bounded platform-thread pool, {@link
 * #WORKER_COUNT} workers, coupled to {@link #POOL_SIZE}); {@link VirtualThreadDecoderState} runs
 * the exact same worker-count cap on JDK 21 virtual threads instead, via {@link
 * OurDriverPointQueryClient.VirtualThreadDecoder}. Unlike {@link
 * DecoderAndPendingAcquireWidenedThroughputBenchmark}, the worker count is <b>not</b> widened here
 * — this class isolates thread type as the one variable, not capacity, since the JFR finding this
 * class exists to test is about resource cost per worker, not about needing more of them.
 *
 * <p>This class alone does not answer the pinning question definitively — that needs the trusted CI
 * profile run with {@code -Djdk.tracePinnedThreads=full} enabled (see {@code
 * .github/workflows/benchmark.yml}) so any pinned-thread stack trace shows up in {@code
 * raw-stdout.log} rather than silently degrading tail latency. If throughput and every percentile
 * come back statistically indistinguishable from {@link PlatformThreadDecoderState}, that is itself
 * a useful (if unglamorous) result: same behavior, lower thread count, no capacity or latency
 * regression. If virtual threads instead show elevated tail latency without any pinning trace, that
 * points at scheduler/carrier-thread contention rather than pinning as the cause.
 *
 * <p>Reuses {@link DecoderWorkerCountThroughputBenchmark}'s exact merged-HdrHistogram-per-fork
 * logging shape ({@code TRUE MERGED} tag) for the same trustworthiness-at-{@code forks=3} reason.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class VirtualThreadDecoderThroughputBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(VirtualThreadDecoderThroughputBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** How many logical point queries one {@code @Benchmark} invocation issues and awaits. */
  private static final int REQUESTS_PER_INVOCATION = 4096;

  /** How many prewarm queries each side runs before measurement. */
  private static final int PREWARM_CALLS = 64;

  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /** Matches {@link DecoderWorkerCountThroughputBenchmark}'s own {@code POOL_SIZE} value. */
  private static final int POOL_SIZE = 8;

  /**
   * Deliberately equal to {@link #POOL_SIZE} on both sides of this comparison — thread type is the
   * one variable this class isolates, not worker-count headroom (that question already has its own
   * benchmark, {@link DecoderAndPendingAcquireWidenedThroughputBenchmark}).
   */
  private static final int WORKER_COUNT = POOL_SIZE;

  /** This driver with today's default: decode on a bounded platform-thread pool. */
  @State(Scope.Benchmark)
  public static class PlatformThreadDecoderState {

    @Param({"8", "32", "128"})
    public int concurrency;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private OurDriverPointQueryClient client;
    private Recorder latencyRecorder;
    private Histogram mergedHistogram;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client =
          new OurDriverPointQueryClient(
              new OurDriverPointQueryClient.ExplicitDecoderWorkerCount(POOL_SIZE, WORKER_COUNT));
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
      mergedHistogram = new Histogram(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("platformThreadDecoder", mergedHistogram);
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      final Histogram interval = latencyRecorder.getIntervalHistogram();
      mergedHistogram.add(interval);
      logLatencySummary("platformThreadDecoder", interval);
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** This driver with decode running on JDK 21 virtual threads, same worker-count cap. */
  @State(Scope.Benchmark)
  public static class VirtualThreadDecoderState {

    @Param({"8", "32", "128"})
    public int concurrency;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private OurDriverPointQueryClient client;
    private Recorder latencyRecorder;
    private Histogram mergedHistogram;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client =
          new OurDriverPointQueryClient(
              new OurDriverPointQueryClient.VirtualThreadDecoder(POOL_SIZE, WORKER_COUNT));
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
      mergedHistogram = new Histogram(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("virtualThreadDecoder", mergedHistogram);
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      final Histogram interval = latencyRecorder.getIntervalHistogram();
      mergedHistogram.add(interval);
      logLatencySummary("virtualThreadDecoder", interval);
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** This driver, decode on a bounded platform-thread pool — today's default behavior. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long platformThreadDecoder(final PlatformThreadDecoderState state) {
    return runWorkload(state.client, state.latencyRecorder, state.concurrency, state::nextId);
  }

  /** This driver, decode on JDK 21 virtual threads, same worker-count cap as the baseline. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long virtualThreadDecoder(final VirtualThreadDecoderState state) {
    return runWorkload(state.client, state.latencyRecorder, state.concurrency, state::nextId);
  }

  /**
   * Issues {@link #REQUESTS_PER_INVOCATION} logical point queries bounded to {@code concurrency} in
   * flight, reduces every result's {@link PointResult#checksum()} into one order-independent sum -
   * same correctness barrier {@link DecoderWorkerCountThroughputBenchmark} uses.
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

  private static void logLatencySummary(final String label, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    LOG.info(
        "{} per-query latency (µs, n={}): mean={}, p50={}, p90={}, p95={}, p99={}, p99.9={}, max={}",
        label,
        histogram.getTotalCount(),
        String.format("%.1f", histogram.getMean()),
        histogram.getValueAtPercentile(50),
        histogram.getValueAtPercentile(90),
        histogram.getValueAtPercentile(95),
        histogram.getValueAtPercentile(99),
        histogram.getValueAtPercentile(99.9),
        histogram.getMaxValue());
  }

  private static void logMergedLatencySummary(final String label, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    LOG.info(
        "{} TRUE MERGED per-query latency (µs, n={}): mean={}, p50={}, p90={}, p95={}, p99={},"
            + " p99.9={}, max={}",
        label,
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

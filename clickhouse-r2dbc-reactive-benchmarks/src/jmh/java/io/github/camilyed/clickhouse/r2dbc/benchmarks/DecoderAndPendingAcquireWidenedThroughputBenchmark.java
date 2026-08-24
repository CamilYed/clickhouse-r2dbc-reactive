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
 * Phase 11 PR5 follow-up (see ROADMAP.md): {@link DecoderWorkerCountThroughputBenchmark}'s trusted
 * run (2026-08-24) found that widening {@code decoderWorkerCount} alone shrank this driver's
 * p90-p99 tail at {@code concurrency=8} (matching {@code poolSize}) by roughly 10-12%, exactly as
 * hypothesized — but at {@code concurrency=32} and {@code concurrency=128}, the widened side
 * produced <b>zero</b> successful measurements: every fork, every iteration, failed with {@code
 * reactor.netty...PoolAcquirePendingLimitException: Pending acquire queue has reached its maximum
 * size of 16}, while the coupled (today's default) side ran cleanly at the same concurrencies.
 *
 * <p>Root cause, confirmed against {@code RowBinaryDecoder.decode}'s source, not just inferred from
 * the failure: {@code Mono.fromCallable(() -> newReader(source, compression))
 * .subscribeOn(reactorScheduler)} is where the transport response stream — and with it the
 * underlying connection acquisition and request send that produce it — first gets subscribed to.
 * {@code RowDecodingScheduler}'s worker count is therefore not only a decode-throughput knob; it's
 * incidentally this driver's real admission-control gate on how many queries can start touching the
 * connection pool at once. Coupling it to {@code poolSize} (the default) keeps that number under
 * Reactor Netty's own pending-acquire-queue limit ({@code 2 × maxConnections} by default); widening
 * it alone removes that incidental protection without replacing it with anything, so at concurrency
 * above the pool size the pending-acquire-queue limit becomes a hard failure instead of the added
 * latency PR5 originally set out to shrink.
 *
 * <p><b>Working theory this class exists to test:</b> today's shape is two queues in series — the
 * decoder's own bounded-elastic queue, then Reactor Netty's pending-acquire queue. Tandem queueing
 * is a known way to compound tail latency beyond what either queue alone would produce at the same
 * throughput, which matches the observed PR4 symptom shape (p50 tied, only p90-p99 diverges). If
 * that theory holds, widening the decoder <i>and</i> {@link
 * io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactoryProvider#TRANSPORT_PENDING_ACQUIRE_MAX_COUNT}
 * together — collapsing back to one effective queue (the physical connection pool, the real,
 * unavoidable bottleneck either way) — should recover the {@code concurrency=8} tail-latency win at
 * {@code concurrency=32}/{@code 128} too, without the outright failures widening the decoder alone
 * produced. {@link #WIDENED_PENDING_ACQUIRE_MAX_COUNT} is sized with the same "deliberately
 * generous, not minimal-tuned" philosophy {@link
 * DecoderWorkerCountThroughputBenchmark#WIDENED_DECODER_WORKER_COUNT} already uses, so a real
 * effect has every chance to show up before this investigation tries to find the smallest number
 * that still helps.
 *
 * <p>Deliberately a separate class rather than a third {@code @State}/{@code @Benchmark} pair
 * bolted onto {@link DecoderWorkerCountThroughputBenchmark}: that class's own two states already
 * answer "does widening the decoder alone help or hurt" — a question this class does not need to
 * re-ask. This class's only job is the follow-up question the first run's failure raised, compared
 * against {@link DecoderWorkerCountThroughputBenchmark}'s already-recorded {@code coupledDecoder}
 * numbers (see ROADMAP.md's Phase 11 PR5 entry), not re-measured here. Reuses the same true-merged-
 * HdrHistogram-per-fork logging pattern ({@code logMergedLatencySummary}, tagged {@code TRUE
 * MERGED} in the log) that makes the p90/p99 comparison trustworthy at {@code forks=3}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DecoderAndPendingAcquireWidenedThroughputBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(DecoderAndPendingAcquireWidenedThroughputBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** How many logical point queries one {@code @Benchmark} invocation issues and awaits. */
  private static final int REQUESTS_PER_INVOCATION = 4096;

  /** How many prewarm queries each side runs before measurement. */
  private static final int PREWARM_CALLS = 64;

  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /** Matches {@link DecoderWorkerCountThroughputBenchmark#POOL_SIZE}. */
  private static final int POOL_SIZE = 8;

  /** Matches {@link DecoderWorkerCountThroughputBenchmark#WIDENED_DECODER_WORKER_COUNT}. */
  private static final int WIDENED_DECODER_WORKER_COUNT = POOL_SIZE * 4;

  /**
   * Comfortably above the worst case this benchmark ever needs — the highest {@code concurrency}
   * tested (128) minus {@link #POOL_SIZE} (8) is 120 acquisitions that could need to queue at once;
   * this is roughly double that with room to spare, deliberately generous rather than a minimal
   * tuned value, same reasoning as {@link #WIDENED_DECODER_WORKER_COUNT}'s own sizing.
   */
  private static final int WIDENED_PENDING_ACQUIRE_MAX_COUNT = 256;

  /** This driver with both the decoder and the pending-acquire queue widened together. */
  @State(Scope.Benchmark)
  public static class WidenedDecoderAndPendingAcquireState {

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
              new OurDriverPointQueryClient.ExplicitDecoderWorkerCountAndPendingAcquireLimit(
                  POOL_SIZE, WIDENED_DECODER_WORKER_COUNT, WIDENED_PENDING_ACQUIRE_MAX_COUNT));
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
      mergedHistogram = new Histogram(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("widenedDecoderAndPendingAcquire", mergedHistogram);
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      final Histogram interval = latencyRecorder.getIntervalHistogram();
      mergedHistogram.add(interval);
      logLatencySummary("widenedDecoderAndPendingAcquire", interval);
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** This driver, decoder and pending-acquire queue both explicitly widened together. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long widenedDecoderAndPendingAcquire(final WidenedDecoderAndPendingAcquireState state) {
    return runWorkload(state.client, state.latencyRecorder, state.concurrency, state::nextId);
  }

  /**
   * Issues {@link #REQUESTS_PER_INVOCATION} logical point queries bounded to {@code concurrency} in
   * flight, reduces every result's {@link PointResult#checksum()} into one order-independent sum -
   * same correctness barrier {@link PublicApiMatchedPoolThroughputBenchmark} uses.
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

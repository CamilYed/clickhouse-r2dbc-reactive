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
 * Phase 11 PR5 (see ROADMAP.md): {@link PublicApiMatchedPoolThroughputBenchmark}'s first trusted
 * merged-histogram run (2026-08-24) found this driver's p90-p99 per-query latency running 15-25%
 * behind client-v2's at every tested concurrency (8/32/128), while p50 stayed tied and {@code
 * gc.time} stayed equal between drivers despite this driver allocating ~3.3x less per query —
 * ruling out GC pauses as the cause and pointing at decode-worker queueing (the {@code
 * RowDecodingScheduler} this driver hands off decode work to, fixed at exactly {@code
 * transportMaxConnections} workers — see {@code docs/operations/connection-pooling.md}'s "The
 * decode worker pool tracks this pool's size, not the CPU core count") as the more likely tail-
 * latency driver: a query whose decode has to wait for a free worker, because every worker happens
 * to be busy decoding another concurrent query's response at that instant, pays that wait as pure
 * added latency with no corresponding allocation cost — exactly the signature this run showed.
 *
 * <p>This class tests that hypothesis directly, comparing this driver against <b>itself</b>, same
 * pool size, same query, same concurrency sweep as {@link PublicApiMatchedPoolThroughputBenchmark},
 * only {@link ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT} differs — {@link
 * CoupledDecoderState} leaves it at the driver's long-standing default (coupled 1:1 to {@code
 * poolSize}, i.e. today's behavior, unchanged); {@link WidenedDecoderState} explicitly widens it to
 * {@link #WIDENED_DECODER_WORKER_COUNT}, four times the pool size, via the new {@link
 * OurDriverPointQueryClient.ExplicitDecoderWorkerCount} constructor — while the physical connection
 * pool itself stays fixed at {@link #POOL_SIZE} on both sides, so this isolates decode-worker
 * headroom as the one variable, not a wider connection pool (which {@link
 * PublicApiMatchedPoolThroughputBenchmark}'s matched-pool comparison already holds fixed on
 * purpose). Deliberately does not involve client-v2 at all — that comparison already exists
 * elsewhere; this class's own numbers should be read alongside it, not instead of it, the same
 * reasoning {@link ClientV2ExecutorAggressivenessBenchmark}'s own Javadoc gives for its own
 * compare-against-itself shape.
 *
 * <p>If widening the decoder measurably shrinks the p90-p99 gap without hurting throughput or p50,
 * that is the evidence Phase 11 PR5 needs to decide whether decoupling the decoder from the
 * connection pool by default is worth pursuing as a real driver change, not just a benchmark-only
 * knob. If it doesn't move the tail at all, decode-worker queueing is not the explanation and this
 * result rules it out just as usefully — either way, this benchmark itself is the "measure it" half
 * of Phase 11's "one evidence-driven optimization, followed by an exact-same-config trusted re-run"
 * requirement, not the optimization decision itself.
 *
 * <p>Per-driver {@code @State} split ({@link CoupledDecoderState}/{@link WidenedDecoderState}) for
 * the same reason {@link PublicApiMatchedPoolThroughputBenchmark} splits its own states — see that
 * class's Javadoc section "Why ThisDriverState/ClientV2State are separate @State classes". Also
 * reuses that class's merged-HdrHistogram-per-fork logging ({@code logMergedLatencySummary}, tagged
 * {@code TRUE MERGED} in the log) — the p90/p99 comparison this class exists to make is exactly the
 * number that logging line makes trustworthy at {@code forks=3}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DecoderWorkerCountThroughputBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(DecoderWorkerCountThroughputBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** How many logical point queries one {@code @Benchmark} invocation issues and awaits. */
  private static final int REQUESTS_PER_INVOCATION = 4096;

  /** How many prewarm queries each side runs before measurement. */
  private static final int PREWARM_CALLS = 64;

  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /** Matches {@link PublicApiMatchedPoolThroughputBenchmark}'s own {@code poolSize} value. */
  private static final int POOL_SIZE = 8;

  /**
   * Four times {@link #POOL_SIZE} — deliberately generous headroom, not a tuned/minimal value, so a
   * real effect (if decode-worker queueing is really the tail-latency driver) has every chance to
   * show up clearly before this class tries to find the smallest number that still helps.
   */
  private static final int WIDENED_DECODER_WORKER_COUNT = POOL_SIZE * 4;

  /** This driver with the decoder left at its default, pool-coupled worker count. */
  @State(Scope.Benchmark)
  public static class CoupledDecoderState {

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
      client = new OurDriverPointQueryClient(POOL_SIZE);
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
      mergedHistogram = new Histogram(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("coupledDecoder", mergedHistogram);
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      final Histogram interval = latencyRecorder.getIntervalHistogram();
      mergedHistogram.add(interval);
      logLatencySummary("coupledDecoder", interval);
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** This driver with the decoder explicitly widened to {@link #WIDENED_DECODER_WORKER_COUNT}. */
  @State(Scope.Benchmark)
  public static class WidenedDecoderState {

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
              new OurDriverPointQueryClient.ExplicitDecoderWorkerCount(
                  POOL_SIZE, WIDENED_DECODER_WORKER_COUNT));
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
      mergedHistogram = new Histogram(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("widenedDecoder", mergedHistogram);
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      final Histogram interval = latencyRecorder.getIntervalHistogram();
      mergedHistogram.add(interval);
      logLatencySummary("widenedDecoder", interval);
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** This driver, decoder coupled 1:1 to the connection pool — today's default behavior. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long coupledDecoder(final CoupledDecoderState state) {
    return runWorkload(state.client, state.latencyRecorder, state.concurrency, state::nextId);
  }

  /** This driver, decoder explicitly widened to {@link #WIDENED_DECODER_WORKER_COUNT}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long widenedDecoder(final WidenedDecoderState state) {
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

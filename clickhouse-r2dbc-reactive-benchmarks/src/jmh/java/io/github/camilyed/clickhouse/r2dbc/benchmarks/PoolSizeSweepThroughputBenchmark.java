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
 * Phase 11 PR3 (see ROADMAP.md): {@link PublicApiMatchedPoolThroughputBenchmark} tests exactly one
 * matched pool size ({@code poolSize=8}) across a concurrency sweep. This class instead fixes
 * {@code concurrency} at {@link #CONCURRENCY} (the largest tier the headline benchmark itself uses)
 * and sweeps {@code poolSize} across {@code 4/8/16/32} — a real scalability curve answering "how
 * much pool headroom does each side actually need before more of it stops helping", not just a
 * single matched-pool snapshot. At {@code poolSize=32} (equal to {@link #CONCURRENCY}), neither
 * side should see any pool-driven queueing at all — the natural upper end of this curve.
 *
 * <p>Manual-only by design: swept across four pool sizes instead of the headline benchmark's one,
 * this class is meaningfully more expensive to run at the trusted profile's 3 forks/5 warmup
 * iterations. Deliberately not added to {@code benchmark.yml}'s weekly schedule (which stays pinned
 * to {@code PublicApiMatchedPoolThroughputBenchmark} regardless of what this file adds to the
 * {@code workflow_dispatch} benchmark dropdown) — run it by hand when the question this class
 * answers is actually the one being asked.
 *
 * <p>Same per-driver {@code @State} split as {@link PublicApiMatchedPoolThroughputBenchmark} — see
 * that class's Javadoc section "Why ThisDriverState/ClientV2State are separate @State classes" for
 * why, and the same correctness barrier ({@link PointResult#checksum()} reduced
 * order-independently).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class PoolSizeSweepThroughputBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(PoolSizeSweepThroughputBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** How many logical point queries one {@code @Benchmark} invocation issues and awaits. */
  private static final int REQUESTS_PER_INVOCATION = 4096;

  /** How many prewarm queries each side runs before measurement. */
  private static final int PREWARM_CALLS = 64;

  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /**
   * Fixed at the sweep's own top {@code poolSize} tier (32), so the largest pool size measured here
   * sees genuinely zero pool-driven queueing (a clean upper bound for the curve) while the smaller
   * tiers each show a real, comparable amount of it.
   */
  private static final int CONCURRENCY = 32;

  /**
   * This driver's client and per-query latency recorder, isolated per fork, swept over pool size.
   */
  @State(Scope.Benchmark)
  public static class ThisDriverState {

    @Param({"4", "8", "16", "32"})
    public int poolSize;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private OurDriverPointQueryClient client;
    private Recorder latencyRecorder;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client = new OurDriverPointQueryClient(poolSize);
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      logLatencySummary("thisDriver", poolSize, latencyRecorder.getIntervalHistogram());
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** client-v2's client and per-query latency recorder, isolated per fork, swept over pool size. */
  @State(Scope.Benchmark)
  public static class ClientV2State {

    @Param({"4", "8", "16", "32"})
    public int poolSize;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private ClientV2PointQueryClient client;
    private Recorder latencyRecorder;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client = new ClientV2PointQueryClient(poolSize);
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      logLatencySummary("clientV2", poolSize, latencyRecorder.getIntervalHistogram());
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** This driver, through the public R2DBC SPI only - see {@link OurDriverPointQueryClient}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long thisDriver(final ThisDriverState state) {
    return runWorkload(state.client, state.latencyRecorder, state::nextId);
  }

  /** client-v2, through its public async API only - see {@link ClientV2PointQueryClient}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long clientV2(final ClientV2State state) {
    return runWorkload(state.client, state.latencyRecorder, state::nextId);
  }

  private long runWorkload(
      final PointQueryClient client,
      final Recorder latencyRecorder,
      final LongSupplier idSupplier) {
    return Flux.range(0, REQUESTS_PER_INVOCATION)
        .flatMap(ignored -> timedQuery(client, latencyRecorder, idSupplier), CONCURRENCY)
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

  private static void logLatencySummary(
      final String driverName, final int poolSize, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    LOG.info(
        "{} poolSize={} per-query latency (µs, n={}): mean={}, p50={}, p90={}, p95={}, p99={},"
            + " p99.9={}, max={}",
        driverName,
        poolSize,
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

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
 * Phase 11 PR3 (see ROADMAP.md): {@link PublicApiMatchedPoolThroughputBenchmark}'s cloud-verified
 * result found client-v2 ahead of this driver on throughput and per-query latency at a matched
 * connection pool. One open question that result alone can't answer: how much of that edge comes
 * from client-v2's async dispatch defaulting to {@code Executors.newCachedThreadPool()} — a pool
 * willing to spin up a fresh thread for every concurrent request rather than queueing behind a
 * fixed worker count — versus something architectural. This class isolates exactly that one
 * variable, comparing client-v2 against <b>itself</b>, same pool size, same query, same concurrency
 * sweep, only the executor differs; see {@link ClientV2PointQueryClient}'s own Javadoc for the
 * constructor pair this drives ({@link ClientV2PointQueryClient#ClientV2PointQueryClient(int)} vs.
 * {@link
 * ClientV2PointQueryClient#ClientV2PointQueryClient(ClientV2PointQueryClient.FixedExecutorPoolSize)}).
 *
 * <p>Deliberately does not involve this driver at all — that comparison already exists in {@link
 * PublicApiMatchedPoolThroughputBenchmark}, whose published numbers this class's own numbers should
 * be read alongside, not instead of. {@code poolSize}/{@code concurrency} intentionally match that
 * class's own values (8, and 8/32/128) so the two are visually comparable without needing to
 * cross-reference different axes.
 *
 * <p>Per-driver {@code @State} split ({@link CachedExecutorState}/{@link FixedExecutorState}) for
 * the same reason {@link PublicApiMatchedPoolThroughputBenchmark} splits its own states — see that
 * class's Javadoc section "Why ThisDriverState/ClientV2State are separate @State classes".
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ClientV2ExecutorAggressivenessBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClientV2ExecutorAggressivenessBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** How many logical point queries one {@code @Benchmark} invocation issues and awaits. */
  private static final int REQUESTS_PER_INVOCATION = 4096;

  /** How many prewarm queries each side runs before measurement. */
  private static final int PREWARM_CALLS = 64;

  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /**
   * client-v2 left at its own default {@code Executors.newCachedThreadPool()}, isolated per fork.
   */
  @State(Scope.Benchmark)
  public static class CachedExecutorState {

    /** Matches {@link PublicApiMatchedPoolThroughputBenchmark#ThisDriverState}'s own value. */
    @Param({"8"})
    public int poolSize;

    @Param({"8", "32", "128"})
    public int concurrency;

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
      logLatencySummary("clientV2CachedExecutor", latencyRecorder.getIntervalHistogram());
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** client-v2 with a {@code Executors.newFixedThreadPool(poolSize)}, isolated per fork. */
  @State(Scope.Benchmark)
  public static class FixedExecutorState {

    @Param({"8"})
    public int poolSize;

    @Param({"8", "32", "128"})
    public int concurrency;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private ClientV2PointQueryClient client;
    private Recorder latencyRecorder;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client =
          new ClientV2PointQueryClient(
              new ClientV2PointQueryClient.FixedExecutorPoolSize(poolSize));
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      logLatencySummary("clientV2FixedExecutor", latencyRecorder.getIntervalHistogram());
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** client-v2, its own default cached executor - the baseline every other benchmark measures. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long clientV2CachedExecutor(final CachedExecutorState state) {
    return runWorkload(state.client, state.latencyRecorder, state.concurrency, state::nextId);
  }

  /** client-v2, a fixed-size executor matched to {@link CachedExecutorState#poolSize}. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long clientV2FixedExecutor(final FixedExecutorState state) {
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
}

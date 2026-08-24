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
 * Phase 11 PR3 (see ROADMAP.md): {@link PublicApiMatchedPoolThroughputBenchmark} deliberately opens
 * one logical {@code Connection} at {@code @Setup} and reuses it for every query — the right shape
 * for isolating physical-pool concurrency from logical-connection churn (see that class's own
 * Javadoc and {@link OurDriverPointQueryClient}'s), but not the shape most Spring applications
 * actually use. Spring's {@code DatabaseClient} acquires a fresh logical connection from the {@code
 * ConnectionFactory} per operation and releases it afterward — this class measures that shape
 * instead, via {@link OurDriverConnectionPerOperationPointQueryClient}, <b>alongside</b> the
 * existing one-{@code Connection} benchmark, not replacing it.
 *
 * <p>client-v2's side is unchanged from {@link
 * ClientV2PointQueryClient#ClientV2PointQueryClient(int)} — client-v2 has no separate
 * logical-connection object to open/close per operation in the first place, so its existing shape
 * already <em>is</em> "per operation" in the sense this class cares about; the point of this
 * benchmark is entirely to measure whether this driver's R2DBC {@code Connection} indirection costs
 * something material when acquired/released on every call, the way an application built on Spring
 * Data R2DBC would actually do it, not to introduce a second client-v2 variant.
 *
 * <p>{@code poolSize}/{@code concurrency} match {@link PublicApiMatchedPoolThroughputBenchmark}'s
 * own values so the two are directly, visually comparable — same per-driver {@code @State} split as
 * that class, for the same reason (see its Javadoc section "Why ThisDriverState/ClientV2State are
 * separate @State classes").
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ConnectionPerOperationThroughputBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConnectionPerOperationThroughputBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /** How many logical point queries one {@code @Benchmark} invocation issues and awaits. */
  private static final int REQUESTS_PER_INVOCATION = 4096;

  /** How many prewarm queries each side runs before measurement. */
  private static final int PREWARM_CALLS = 64;

  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /** This driver's connection-per-operation client and recorder, isolated per fork. */
  @State(Scope.Benchmark)
  public static class ThisDriverState {

    /** Matches {@link PublicApiMatchedPoolThroughputBenchmark}'s own {@code poolSize}. */
    @Param({"8"})
    public int poolSize;

    @Param({"8", "32", "128"})
    public int concurrency;

    private long[] ids;
    private final AtomicLong idCursor = new AtomicLong();
    private OurDriverConnectionPerOperationPointQueryClient client;
    private Recorder latencyRecorder;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
      client = new OurDriverConnectionPerOperationPointQueryClient(poolSize);
      client.prewarm(ids, PREWARM_CALLS);
      latencyRecorder = new Recorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      client.close();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      logLatencySummary("thisDriver", latencyRecorder.getIntervalHistogram());
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /** client-v2's client and recorder, isolated per fork - unchanged shape, see class Javadoc. */
  @State(Scope.Benchmark)
  public static class ClientV2State {

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
      logLatencySummary("clientV2", latencyRecorder.getIntervalHistogram());
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }

  /**
   * This driver, {@code factory.create()}/{@code connection.close()} per query - see {@link
   * OurDriverConnectionPerOperationPointQueryClient}.
   */
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

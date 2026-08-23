package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

/**
 * Every other concurrency/throughput benchmark in this module (deliberately) matches both drivers
 * to an identical, artificially small connection pool — the right choice for isolating calling
 * style or pool-sizing effects from each other, but it also means this module has never measured
 * what an application gets by just using each driver <b>out of the box</b>: this driver's Reactor
 * Netty {@code ConnectionProvider} default ({@code max(availableProcessors, 8) * 2}, at least 16 —
 * see docs/operations/connection-pooling.md's "Reactor Netty's own defaults" table) against
 * client-v2's own default of 10 (its {@code ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS}).
 *
 * <p><b>Why the queries are artificially slowed down.</b> Every existing point-query benchmark in
 * this module finishes a query in low single-digit milliseconds — fast enough that even a
 * deliberately tiny 8-connection pool rarely has a chance to actually queue anything before the
 * next slot frees up (see {@code docs/operations/connection-pooling.md}'s "Is it worth setting
 * maxConnections yourself?" section on why headroom this generous rarely bites in practice). A
 * default-vs-default comparison at that query speed would mostly just remeasure protocol-floor
 * noise, not pool behavior. {@link OurDriverDefaultPoolSlowQueryClient}/{@link
 * ClientV2DefaultPoolSlowQueryClient} both append {@code sleep(}{@link #sleepSeconds}{@code )} to
 * the query — a real, deterministic, server-side hold — long enough that a {@code concurrency}
 * value above the pool size actually forces queueing on the smaller side, which is the whole point
 * of comparing two <em>different</em> pool sizes rather than a matched one.
 *
 * <h2>What one JMH sample means</h2>
 *
 * One {@code @Benchmark} invocation issues {@link #concurrency} logical point queries via {@code
 * Flux.flatMap(..., concurrency)} and awaits the whole batch — {@link Mode#SampleTime}, not {@link
 * Mode#Throughput} with {@code @OperationsPerInvocation}, because each batch here is inherently
 * seconds-long (see above), the same reasoning {@link BoundedPoolConcurrencyBenchmark} already uses
 * for its own burst-latency measurement. A sample is therefore "wall-clock time to drain this many
 * concurrent {@code sleepSeconds}-long queries through this driver's own default pool" — not a
 * genuine per-query latency (see {@link PublicApiMatchedPoolThroughputBenchmark} for that), a
 * whole-batch completion time, exactly like {@link BoundedPoolConcurrencyBenchmark}'s own
 * percentiles.
 *
 * <p><b>Small first pass, not the full matrix</b> — {@code concurrency} only sweeps 8/32 (not the
 * 128 tier every matched-pool benchmark in this module uses): at {@code sleepSeconds=1.0} and
 * client-v2's 10-connection default, a 128-wide batch alone would take {@code ceil(128/10) * 1.0s ≈
 * 13s} to drain, which multiplies out to an impractically long trusted run across 5 warmup +
 * several measurement iterations × 3 forks × 2 drivers × 2 {@code sleepSeconds} values. Widen once
 * this first pass shows something worth digging into further, same reasoning as {@link
 * BoundedPoolConcurrencyBenchmark}'s own Javadoc.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DefaultPoolSlowQueryThroughputBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultPoolSlowQueryThroughputBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /**
   * How many sequential prewarm calls each side runs before measurement — deliberately far below
   * {@link PointQueryClient}'s other callers' usual 64, since each call here costs a real {@link
   * #sleepSeconds}-long round trip and prewarm is sequential/blocking by design (see {@link
   * PointQueryClient#prewarm}); enough to establish the first physical connection(s) and warm up
   * class loading without paying for dozens of artificially slow round trips.
   */
  private static final int PREWARM_CALLS = 4;

  private static final Duration BATCH_TIMEOUT = Duration.ofSeconds(60);

  /** How many logical concurrent queries one batch/sample issues against each side's own pool. */
  @Param({"8", "32"})
  public int concurrency;

  /** How long each query holds server-side via {@code sleep(...)} — see the class Javadoc. */
  @Param({"0.5", "1.0"})
  public double sleepSeconds;

  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  private OurDriverDefaultPoolSlowQueryClient ourDriverClient;
  private ClientV2DefaultPoolSlowQueryClient clientV2Client;

  /**
   * Starts the shared/external server, seeds {@link PointQueryTable}, builds both clients at their
   * own default pool size (see the class Javadoc), then prewarms both.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(ROWS);
    ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);

    ourDriverClient = new OurDriverDefaultPoolSlowQueryClient(sleepSeconds);
    clientV2Client = new ClientV2DefaultPoolSlowQueryClient(sleepSeconds);

    LOG.info(
        "Default pools: ourDriver ~{} (Reactor Netty's max(availableProcessors,8)*2 on this"
            + " runner), client-v2 10 (its own fixed default) - sleepSeconds={}",
        Math.max(Runtime.getRuntime().availableProcessors(), 8) * 2,
        sleepSeconds);

    ourDriverClient.prewarm(ids, PREWARM_CALLS);
    clientV2Client.prewarm(ids, PREWARM_CALLS);
  }

  /** Closes both clients' connections/pools at the end of the trial. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    ourDriverClient.close();
    clientV2Client.close();
  }

  /** This driver, through the public R2DBC SPI, left at its own default pool size. */
  @Benchmark
  public long ourDriver() {
    return runBatch(ourDriverClient);
  }

  /** client-v2, through its public async API, left at its own default pool size. */
  @Benchmark
  public long clientV2() {
    return runBatch(clientV2Client);
  }

  /**
   * Issues {@link #concurrency} logical, {@link #sleepSeconds}-slowed point queries and reduces
   * every result's {@link PointResult#checksum()} into one order-independent sum - the same
   * correctness barrier {@link PublicApiMatchedPoolThroughputBenchmark} uses, forcing both sides to
   * have actually decoded real values regardless of completion order under concurrency.
   */
  private long runBatch(final PointQueryClient client) {
    return Flux.range(0, concurrency)
        .flatMap(ignored -> singleQuery(client), concurrency)
        .reduce(0L, Long::sum)
        .block(BATCH_TIMEOUT);
  }

  private Mono<Long> singleQuery(final PointQueryClient client) {
    return client.query(nextId()).map(PointResult::checksum);
  }

  /** Advances through the pre-generated {@link #ids} pool - thread-safe via {@link AtomicLong}. */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

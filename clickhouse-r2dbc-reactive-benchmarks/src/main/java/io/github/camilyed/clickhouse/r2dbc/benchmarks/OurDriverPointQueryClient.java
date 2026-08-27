package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactoryProvider;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Parameters;
import java.math.BigDecimal;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link PointQueryClient} through this driver's public R2DBC SPI only — {@code ConnectionFactory}
 * → {@code Connection} → {@code Statement} → {@code Result} → {@code Row}, exactly what an
 * application built on this driver calls. See {@code CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md}
 * section 7 for why the headline benchmark must go through here, not {@code
 * ClickHouseHttpTransport} directly — that's what every earlier concurrency benchmark in this
 * module did, and section 2.1 of the plan names that as the main methodological gap this class
 * fixes.
 *
 * <p>Opens exactly one logical {@link Connection} at construction and reuses it for every {@link
 * #query(long)} call, rather than the plan's own skeleton code (a fresh {@code
 * connectionFactory.create()}/{@code close()} per query via {@code Mono.usingWhen}). This driver's
 * R2DBC {@code Connection} is a thin logical wrapper over the shared, pool-sized {@code
 * ClickHouseHttpTransport} — opening/closing one per query would measure logical-wrapper allocation
 * churn on top of the physical-pool concurrency behavior this benchmark actually exists to isolate,
 * and {@code PublicApiPointQueryBenchmark} (in this module's {@code src/jmh/java}) already
 * established this exact reuse-one-connection shape for the same reason. Documented explicitly, per
 * the plan's own instruction not to silently change benchmark semantics from its skeleton.
 *
 * <p>{@link #close()} disposes both the logical {@link Connection} and the owning {@link
 * ClickHouseConnectionFactory} — the factory, not the connection, is what actually owns the Reactor
 * Netty transport pool and the {@code RowDecodingScheduler} worker pool, so closing only the
 * connection leaves both running for the lifetime of the JVM. Mirrors {@link
 * ClientV2PointQueryClient#close()}, which disposes client-v2's {@code Client} — the equivalent
 * pool-owning object on that side.
 *
 * <p>Six constructors, six distinct scenarios: {@link #OurDriverPointQueryClient(int)} matches this
 * driver to an explicit pool size for a fair matched-pool comparison; {@link
 * #OurDriverPointQueryClient(double)} instead leaves this driver at its own default pool and slows
 * every query down via {@code sleep(...)} — see {@link DefaultPoolSlowQueryThroughputBenchmark}'s
 * Javadoc for why that second scenario exists; {@link
 * #OurDriverPointQueryClient(ExplicitDecoderWorkerCount)} matches an explicit pool size like the
 * first constructor, but additionally overrides {@link
 * ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT} independently of it — see {@link
 * DecoderWorkerCountThroughputBenchmark}'s Javadoc for why that third scenario exists (Phase 11
 * PR5, ROADMAP.md); {@link
 * #OurDriverPointQueryClient(ExplicitDecoderWorkerCountAndPendingAcquireLimit)} builds on the third
 * by additionally overriding {@link
 * ClickHouseConnectionFactoryProvider#TRANSPORT_PENDING_ACQUIRE_MAX_COUNT} — see that class's
 * Javadoc for why PR5's own trusted run made this fourth scenario necessary; {@link
 * #OurDriverPointQueryClient(VirtualThreadDecoder)} matches the third constructor's shape exactly
 * (explicit pool size + explicit decoder worker count) but additionally sets {@link
 * ClickHouseConnectionFactoryProvider#DECODER_USE_VIRTUAL_THREADS} — see {@link
 * VirtualThreadDecoderThroughputBenchmark}'s Javadoc for why this fifth scenario exists; {@link
 * #OurDriverPointQueryClient(NativeDecoder)} matches the first constructor's shape exactly
 * (explicit pool size, otherwise default configuration) but additionally sets {@link
 * ClickHouseConnectionFactoryProvider#ROW_DECODER} to {@code "native"} — see {@link
 * NativeDecoder}'s Javadoc for why this sixth scenario exists. All six share the same query/close
 * logic below, only the SQL text and pool configuration differ.
 */
final class OurDriverPointQueryClient implements PointQueryClient {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private final ClickHouseConnectionFactory factory;
  private final Connection connection;
  private final String selectSql;

  /**
   * Pairs an explicit {@code poolSize} with an explicit {@code decoderWorkerCount} independent of
   * it — a distinct parameter type (rather than a second {@code int} parameter on {@link
   * #OurDriverPointQueryClient(int)}) so a call site self-documents "these two are deliberately
   * different numbers", per the same "no boolean/flag parameters that silently change behavior"
   * reasoning {@link ClientV2PointQueryClient.FixedExecutorPoolSize} already follows for a similar
   * disambiguation need on the client-v2 side.
   */
  record ExplicitDecoderWorkerCount(int poolSize, int decoderWorkerCount) {}

  /**
   * Adds an explicit {@code pendingAcquireMaxCount} on top of {@link ExplicitDecoderWorkerCount}'s
   * two fields — a third, distinct parameter type (rather than a third {@code int} parameter
   * grafted onto that record) for the same self-documenting reason that record itself exists: a
   * call site should say "these three are deliberately different numbers", not leave a reader
   * counting positional {@code int}s. Exists specifically to test Phase 11 PR5's follow-up
   * hypothesis (see {@link DecoderAndPendingAcquireWidenedThroughputBenchmark}'s Javadoc): that
   * widening {@code decoderWorkerCount} alone removed one queue from a tandem pair (decoder queue,
   * then Reactor Netty's own pending-acquire queue) but left the second queue's default capacity
   * too small to hold what the first queue used to hold back — so widening both together, not just
   * the decoder, is the actual fix to test.
   */
  record ExplicitDecoderWorkerCountAndPendingAcquireLimit(
      int poolSize, int decoderWorkerCount, int pendingAcquireMaxCount) {}

  /**
   * Pairs an explicit {@code poolSize} with an explicit {@code decoderWorkerCount}, same shape as
   * {@link ExplicitDecoderWorkerCount} — a distinct type rather than a boolean flag on that record,
   * per the same "no boolean/flag parameters that silently change behavior" reasoning, so a call
   * site self-documents "this scenario runs the decoder on virtual threads" rather than reading a
   * bare {@code true} at the call site. Exists specifically for {@link
   * VirtualThreadDecoderThroughputBenchmark}'s comparison against {@link
   * ExplicitDecoderWorkerCount} — see that class's Javadoc.
   */
  record VirtualThreadDecoder(int poolSize, int decoderWorkerCount) {}

  /**
   * Pairs an explicit {@code poolSize} with {@link ClickHouseConnectionFactoryProvider#ROW_DECODER}
   * explicitly set to {@code "native"} instead of the default {@code "clickhouse"} — a distinct
   * type rather than a boolean flag on {@link #OurDriverPointQueryClient(int)}, per the same "no
   * boolean/flag parameters that silently change behavior" reasoning the other records above
   * already follow. Exists specifically so {@code PublicApiMatchedPoolThroughputBenchmark} can
   * measure whether {@code RowBinaryDecoderMode.NATIVE} — confirmed faster in isolation by {@code
   * DecoderOnlyBenchmark} (see that class's Javadoc for the 2026-08-27 trusted decoder-only
   * numbers) — actually moves the public, end-to-end point-query latency/throughput, not just the
   * isolated decode step.
   */
  record NativeDecoder(int poolSize) {}

  /**
   * Opens one logical connection against a {@code ConnectionFactory} sized to {@code poolSize}
   * physical connections.
   */
  OurDriverPointQueryClient(final int poolSize) {
    this.selectSql = SELECT_BY_ID_SQL;
    this.factory =
        ClickHouseConnectionFactory.from(
            baseOptions()
                .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, poolSize)
                .build());
    this.connection = openConnection(factory);
  }

  /**
   * Opens one logical connection against a {@code ConnectionFactory} sized to {@link
   * ExplicitDecoderWorkerCount#poolSize()} physical connections, with {@link
   * ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT} explicitly set to {@link
   * ExplicitDecoderWorkerCount#decoderWorkerCount()} instead of the default (which would otherwise
   * couple it to {@code poolSize}) — see that option's own Javadoc for why a caller would ever want
   * the two to differ.
   */
  OurDriverPointQueryClient(final ExplicitDecoderWorkerCount explicitDecoderWorkerCount) {
    this.selectSql = SELECT_BY_ID_SQL;
    this.factory =
        ClickHouseConnectionFactory.from(
            baseOptions()
                .option(
                    ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS,
                    explicitDecoderWorkerCount.poolSize())
                .option(
                    ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT,
                    explicitDecoderWorkerCount.decoderWorkerCount())
                .build());
    this.connection = openConnection(factory);
  }

  /**
   * Opens one logical connection against a {@code ConnectionFactory} sized to {@link
   * ExplicitDecoderWorkerCountAndPendingAcquireLimit#poolSize()} physical connections, with both
   * {@link ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT} and {@link
   * ClickHouseConnectionFactoryProvider#TRANSPORT_PENDING_ACQUIRE_MAX_COUNT} explicitly overridden
   * — see {@link ExplicitDecoderWorkerCountAndPendingAcquireLimit}'s own Javadoc for why a caller
   * would ever need to set both together rather than just the decoder.
   */
  OurDriverPointQueryClient(final ExplicitDecoderWorkerCountAndPendingAcquireLimit explicitLimits) {
    this.selectSql = SELECT_BY_ID_SQL;
    this.factory =
        ClickHouseConnectionFactory.from(
            baseOptions()
                .option(
                    ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS,
                    explicitLimits.poolSize())
                .option(
                    ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT,
                    explicitLimits.decoderWorkerCount())
                .option(
                    ClickHouseConnectionFactoryProvider.TRANSPORT_PENDING_ACQUIRE_MAX_COUNT,
                    explicitLimits.pendingAcquireMaxCount())
                .build());
    this.connection = openConnection(factory);
  }

  /**
   * Opens one logical connection against a {@code ConnectionFactory} sized to {@link
   * VirtualThreadDecoder#poolSize()} physical connections, with the decode pool run on JDK 21
   * virtual threads via {@link ClickHouseConnectionFactoryProvider#DECODER_USE_VIRTUAL_THREADS} and
   * capped at {@link VirtualThreadDecoder#decoderWorkerCount()} — the virtual-thread counterpart to
   * {@link #OurDriverPointQueryClient(ExplicitDecoderWorkerCount)}, same worker-count contract,
   * different thread type. See {@link VirtualThreadDecoderThroughputBenchmark}'s Javadoc for why
   * this constructor exists.
   */
  OurDriverPointQueryClient(final VirtualThreadDecoder virtualThreadDecoder) {
    this.selectSql = SELECT_BY_ID_SQL;
    this.factory =
        ClickHouseConnectionFactory.from(
            baseOptions()
                .option(
                    ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS,
                    virtualThreadDecoder.poolSize())
                .option(
                    ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT,
                    virtualThreadDecoder.decoderWorkerCount())
                .option(ClickHouseConnectionFactoryProvider.DECODER_USE_VIRTUAL_THREADS, true)
                .build());
    this.connection = openConnection(factory);
  }

  /**
   * Opens one logical connection against a {@code ConnectionFactory} sized to {@link
   * NativeDecoder#poolSize()} physical connections, with {@link
   * ClickHouseConnectionFactoryProvider#ROW_DECODER} explicitly set to {@code "native"} — otherwise
   * identical to {@link #OurDriverPointQueryClient(int)}, so a benchmark comparing the two isolates
   * {@code rowDecoder} as the only variable, per {@link NativeDecoder}'s own Javadoc.
   */
  OurDriverPointQueryClient(final NativeDecoder nativeDecoder) {
    this.selectSql = SELECT_BY_ID_SQL;
    this.factory =
        ClickHouseConnectionFactory.from(
            baseOptions()
                .option(
                    ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS,
                    nativeDecoder.poolSize())
                .option(ClickHouseConnectionFactoryProvider.ROW_DECODER, "native")
                .build());
    this.connection = openConnection(factory);
  }

  /**
   * Opens one logical connection against a {@code ConnectionFactory} left at this driver's own
   * default {@code transportMaxConnections} (Reactor Netty's {@code ConnectionProvider} default,
   * {@code max(availableProcessors, 8) * 2}, at least 16 — see
   * docs/operations/connection-pooling.md's "Reactor Netty's own defaults" table). Every query
   * additionally selects {@code sleep(sleepSeconds)} (ignored in the mapped result) to give the
   * physical pool something to actually queue behind — see {@link
   * DefaultPoolSlowQueryThroughputBenchmark}'s Javadoc.
   */
  OurDriverPointQueryClient(final double sleepSeconds) {
    this.selectSql =
        "SELECT label, amount, sleep("
            + sleepSeconds
            + ") FROM "
            + PointQueryTable.NAME
            + " WHERE id = {id:UInt64}";
    this.factory = ClickHouseConnectionFactory.from(baseOptions().build());
    this.connection = openConnection(factory);
  }

  /**
   * Package-private, not {@code private}: shared with {@link
   * OurDriverConnectionPerOperationPointQueryClient}, which needs the same base options for its own
   * {@code factory.create()}-per-query variant — see that class's Javadoc for why it exists
   * alongside this one instead of as a third constructor here.
   */
  static ConnectionFactoryOptions.Builder baseOptions() {
    return ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.HOST, BenchmarkEnvironment.host())
        .option(ConnectionFactoryOptions.PORT, BenchmarkEnvironment.port())
        .option(ConnectionFactoryOptions.USER, BenchmarkEnvironment.username())
        .option(ConnectionFactoryOptions.PASSWORD, BenchmarkEnvironment.password());
  }

  private static Connection openConnection(final ClickHouseConnectionFactory factory) {
    return Mono.from(factory.create()).block(Duration.ofSeconds(10));
  }

  @Override
  public Mono<PointResult> query(final long id) {
    return Flux.from(connection.createStatement(selectSql).bind("id", Parameters.in(id)).execute())
        .flatMap(
            result ->
                Flux.from(
                    result.map(
                        (row, metadata) ->
                            new PointResult(
                                row.get("label", String.class),
                                row.get("amount", BigDecimal.class)))))
        .single();
  }

  @Override
  public void close() {
    Mono.from(connection.close()).block(Duration.ofSeconds(10));
    factory.dispose();
  }
}

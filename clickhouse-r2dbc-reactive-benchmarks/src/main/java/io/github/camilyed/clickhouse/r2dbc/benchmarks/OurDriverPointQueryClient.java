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
 * <p>Three constructors, three distinct scenarios: {@link #OurDriverPointQueryClient(int)} matches
 * this driver to an explicit pool size for a fair matched-pool comparison; {@link
 * #OurDriverPointQueryClient(double)} instead leaves this driver at its own default pool and slows
 * every query down via {@code sleep(...)} — see {@link DefaultPoolSlowQueryThroughputBenchmark}'s
 * Javadoc for why that second scenario exists; {@link
 * #OurDriverPointQueryClient(ExplicitDecoderWorkerCount)} matches an explicit pool size like the
 * first constructor, but additionally overrides {@link
 * ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT} independently of it — see {@link
 * DecoderWorkerCountThroughputBenchmark}'s Javadoc for why that third scenario exists (Phase 11
 * PR5, ROADMAP.md). All three share the same query/close logic below, only the SQL text and pool
 * configuration differ.
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

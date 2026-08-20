package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactoryProvider;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
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
 */
final class OurDriverPointQueryClient implements PointQueryClient {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private final Connection connection;

  /**
   * Opens one logical connection against a {@code ConnectionFactory} sized to {@code poolSize}
   * physical connections.
   */
  OurDriverPointQueryClient(final int poolSize) {
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, BenchmarkEnvironment.host())
            .option(ConnectionFactoryOptions.PORT, BenchmarkEnvironment.port())
            .option(ConnectionFactoryOptions.USER, BenchmarkEnvironment.username())
            .option(ConnectionFactoryOptions.PASSWORD, BenchmarkEnvironment.password())
            .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, poolSize)
            .build();
    final ConnectionFactory factory = ClickHouseConnectionFactory.from(options);
    this.connection = Mono.from(factory.create()).block(Duration.ofSeconds(10));
  }

  @Override
  public Mono<PointResult> query(final long id) {
    return Flux.from(
            connection.createStatement(SELECT_BY_ID_SQL).bind("id", Parameters.in(id)).execute())
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
  }
}

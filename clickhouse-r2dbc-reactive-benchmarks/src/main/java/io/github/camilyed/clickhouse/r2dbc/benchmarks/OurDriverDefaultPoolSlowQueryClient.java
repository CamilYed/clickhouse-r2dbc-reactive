package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Parameters;
import java.math.BigDecimal;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link PointQueryClient} through this driver's public R2DBC SPI, deliberately <b>not</b> given an
 * explicit {@code transportMaxConnections} — see {@link DefaultPoolSlowQueryThroughputBenchmark}'s
 * Javadoc for why an unmatched, out-of-the-box pool is the point of that benchmark rather than a
 * methodology gap. Every earlier public-SPI point-query benchmark ({@link
 * OurDriverPointQueryClient}) pins both sides to an identical, artificially small pool specifically
 * to isolate calling-style differences from pool-size differences — this class isolates the
 * opposite question: what happens when each driver is left at its own default, under a query load
 * slow enough (see the constructor's {@code sleepSeconds} parameter) for a small pool to actually
 * queue.
 */
final class OurDriverDefaultPoolSlowQueryClient implements PointQueryClient {

  private final Connection connection;
  private final String selectSql;

  /**
   * Opens one logical connection against a {@code ConnectionFactory} left at this driver's own
   * default {@code transportMaxConnections} (Reactor Netty's {@code ConnectionProvider} default,
   * {@code max(availableProcessors, 8) * 2}, at least 16 — see
   * docs/operations/connection-pooling.md's "Reactor Netty's own defaults" table). Every query
   * additionally selects {@code sleep(sleepSeconds)} (ignored in the mapped result) to give the
   * physical pool something to actually queue behind — see the owning benchmark's Javadoc.
   */
  OurDriverDefaultPoolSlowQueryClient(final double sleepSeconds) {
    this.selectSql =
        "SELECT label, amount, sleep("
            + sleepSeconds
            + ") FROM "
            + PointQueryTable.NAME
            + " WHERE id = {id:UInt64}";
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, BenchmarkEnvironment.host())
            .option(ConnectionFactoryOptions.PORT, BenchmarkEnvironment.port())
            .option(ConnectionFactoryOptions.USER, BenchmarkEnvironment.username())
            .option(ConnectionFactoryOptions.PASSWORD, BenchmarkEnvironment.password())
            .build();
    final ConnectionFactory factory = ClickHouseConnectionFactory.from(options);
    this.connection = Mono.from(factory.create()).block(Duration.ofSeconds(10));
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
  }
}

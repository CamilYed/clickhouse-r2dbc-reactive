package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactoryProvider;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Parameters;
import java.math.BigDecimal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link PointQueryClient} through this driver's public R2DBC SPI, using the plan's own original
 * skeleton shape — {@code factory.create()} → {@code Statement} → {@code Result} → {@code
 * connection.close()}, once per {@link #query(long)} call, via {@code Mono.usingWhen} — rather than
 * {@link OurDriverPointQueryClient}'s reuse-one-logical-connection shape. See Phase 11 PR3
 * (ROADMAP.md): this is the calling shape Spring's own {@code DatabaseClient} actually uses
 * (acquire a connection per operation from the {@code ConnectionFactory}, run the operation,
 * release it), so it exists alongside {@link OurDriverPointQueryClient} — not replacing it, since
 * that class's own reuse-one-connection shape remains the right choice for isolating physical-pool
 * concurrency behavior from logical-connection churn (see its Javadoc).
 *
 * <p>A separate class rather than a third constructor overload on {@link
 * OurDriverPointQueryClient}, since the difference here isn't just configuration (SQL text/pool
 * size, the axis those two constructors already vary along) but the shape of {@link #query(long)}
 * itself.
 *
 * <p>{@link #close()} disposes the {@link ClickHouseConnectionFactory} — the transport pool/decoder
 * scheduler it owns — since there is no single long-lived {@link Connection} field here to close
 * separately; every {@link Connection} this class ever opens is already closed, per query, inside
 * {@link #query(long)} itself.
 */
final class OurDriverConnectionPerOperationPointQueryClient implements PointQueryClient {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private final ClickHouseConnectionFactory factory;

  /** Builds a {@code ConnectionFactory} sized to {@code poolSize} physical connections. */
  OurDriverConnectionPerOperationPointQueryClient(final int poolSize) {
    this.factory =
        ClickHouseConnectionFactory.from(
            OurDriverPointQueryClient.baseOptions()
                .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, poolSize)
                .build());
  }

  @Override
  public Mono<PointResult> query(final long id) {
    return Mono.usingWhen(
        factory.create(),
        connection ->
            Flux.from(
                    connection
                        .createStatement(SELECT_BY_ID_SQL)
                        .bind("id", Parameters.in(id))
                        .execute())
                .flatMap(
                    result ->
                        Flux.from(
                            result.map(
                                (row, metadata) ->
                                    new PointResult(
                                        row.get("label", String.class),
                                        row.get("amount", BigDecimal.class)))))
                .single(),
        connection -> Mono.from(connection.close()));
  }

  @Override
  public void close() {
    factory.dispose();
  }
}

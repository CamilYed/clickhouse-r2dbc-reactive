package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.infrastructure;

import org.springframework.boot.CommandLineRunner;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

/**
 * Creates the {@code order_events} table on startup if it doesn't already exist.
 *
 * <p>Spring Data R2DBC has no schema-generation/migration mechanism of its own (unlike Spring Data
 * JPA's {@code ddl-auto}) — a ClickHouse table also needs an {@code ENGINE}/{@code ORDER BY} clause
 * that has no ANSI-SQL equivalent a generic tool could infer, so a hand-written, idempotent {@code
 * CREATE TABLE IF NOT EXISTS} run once at startup is the pragmatic choice for a demo this size; a
 * real application would more likely use a dedicated migration step.
 */
@Component
class OrderEventsSchemaInitializer implements CommandLineRunner {

  private final DatabaseClient databaseClient;

  OrderEventsSchemaInitializer(final DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  @Override
  public void run(final String... args) {
    databaseClient
        .sql(
            "CREATE TABLE IF NOT EXISTS order_events ("
                + "id UUID, "
                + "customer_id UUID, "
                + "category LowCardinality(String), "
                + "tags Array(String), "
                + "amount Decimal(18,4), "
                + "discount Nullable(Decimal(18,4)), "
                + "status Enum8('PLACED' = 1, 'PAID' = 2, 'CANCELLED' = 3), "
                + "client_ip IPv4, "
                + "occurred_at DateTime64(3)"
                + ") ENGINE = MergeTree ORDER BY occurred_at")
        .fetch()
        .rowsUpdated()
        .block();
  }
}

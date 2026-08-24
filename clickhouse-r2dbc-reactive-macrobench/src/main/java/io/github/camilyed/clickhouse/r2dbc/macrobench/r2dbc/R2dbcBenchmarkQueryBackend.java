package io.github.camilyed.clickhouse.r2dbc.macrobench.r2dbc;

import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.BenchmarkQueryBackend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.PointRow;
import io.github.camilyed.clickhouse.r2dbc.macrobench.dataset.BenchmarkDataset;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link BenchmarkQueryBackend} through this driver, via Spring's {@link DatabaseClient} - the
 * same "fully pre-formed SQL, literal values, no {@code .bind(...)}" style {@code
 * examples/spring-boot-webflux-demo}'s {@code DatabaseClientOrderEventRepository} already uses
 * (see that class's Javadoc for why {@code .bind(...)} doesn't work against this driver's {@code
 * {name:Type}} parameter syntax through {@link DatabaseClient} yet). Every value embedded by this
 * class is a driver-generated {@code long} (never user input), so no literal-escaping is needed
 * the way the demo's string/array values require.
 */
final class R2dbcBenchmarkQueryBackend implements BenchmarkQueryBackend {

  private final DatabaseClient databaseClient;

  R2dbcBenchmarkQueryBackend(final DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  @Override
  public Backend kind() {
    return Backend.R2DBC;
  }

  @Override
  public Mono<PointRow> point(final long id) {
    return databaseClient
        .sql(
            "SELECT id, label, amount FROM " + BenchmarkDataset.POINT_TABLE + " WHERE id = " + id)
        .map(R2dbcBenchmarkQueryBackend::toPointRow)
        .one();
  }

  @Override
  public Mono<List<CategoryTotal>> analytics() {
    return databaseClient
        .sql(BenchmarkDataset.ANALYTICS_SQL)
        .map(R2dbcBenchmarkQueryBackend::toCategoryTotal)
        .all()
        .collectList();
  }

  @Override
  public Flux<PointRow> stream(final long limit) {
    return databaseClient
        .sql(
            "SELECT id, label, amount FROM "
                + BenchmarkDataset.POINT_TABLE
                + " ORDER BY id LIMIT "
                + limit)
        .map(R2dbcBenchmarkQueryBackend::toPointRow)
        .all();
  }

  private static PointRow toPointRow(final io.r2dbc.spi.Readable row) {
    return new PointRow(
        row.get("id", Long.class), row.get("label", String.class), row.get("amount", BigDecimal.class));
  }

  private static CategoryTotal toCategoryTotal(final io.r2dbc.spi.Readable row) {
    return new CategoryTotal(
        row.get("category", String.class),
        row.get("order_count", Long.class),
        row.get("total_amount", BigDecimal.class));
  }
}

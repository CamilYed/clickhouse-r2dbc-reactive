package io.github.camilyed.clickhouse.r2dbc.macrobench.api.fakes;

import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.BenchmarkQueryBackend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.PointRow;
import java.math.BigDecimal;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An in-memory {@link BenchmarkQueryBackend} for {@code BenchmarkController} tests - no real
 * ClickHouse, no mocking of the interface, just a fixed answer for each scenario, per CLAUDE.md's
 * "in-memory fakes instead of mocks" testing rule.
 */
public final class InMemoryBenchmarkQueryBackend implements BenchmarkQueryBackend {

  private final Backend kind;

  public InMemoryBenchmarkQueryBackend(final Backend kind) {
    this.kind = kind;
  }

  @Override
  public Backend kind() {
    return kind;
  }

  @Override
  public Mono<PointRow> point(final long id) {
    return Mono.just(new PointRow(id, "fake-label", BigDecimal.TEN));
  }

  @Override
  public Mono<List<CategoryTotal>> analytics() {
    return Mono.just(List.of(new CategoryTotal("fake-category", 1, BigDecimal.ONE)));
  }

  @Override
  public Flux<PointRow> stream(final long limit) {
    return Flux.just(new PointRow(1, "fake-label", BigDecimal.ONE));
  }
}

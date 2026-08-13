package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The persistence port for {@link OrderEvent} — owned by the domain, implemented by an
 * infrastructure adapter ({@code DatabaseClientOrderEventRepository}), the same Ports & Adapters
 * split the driver itself uses between {@code core} and its transport/connector adapters (see the
 * root project's {@code CLAUDE.md}, Architecture section). {@code api.OrderEventController} depends
 * only on this interface, never on {@code DatabaseClient} directly.
 */
public interface OrderEventRepository {

  /** Persists a new event. */
  Mono<Void> save(OrderEvent event);

  /** All events, oldest first. */
  Flux<OrderEvent> findAll();

  /** How many events exist in total. */
  Mono<Long> count();

  /**
   * The total {@code amount} per {@code category}, highest total first — an aggregation query, the
   * kind of thing ClickHouse is actually for, not a per-row lookup.
   */
  Flux<CategoryTotal> totalAmountByCategory();
}

package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.fakes;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEvent;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEventRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * In-memory {@link OrderEventRepository} whose {@link #findAll()} stream is supplied directly by
 * the caller, so a test can control the exact timing between elements (e.g. via {@code
 * Flux#delayElements}) — something a real ClickHouse query's own timing cannot deterministically
 * guarantee. Exists specifically for {@code OrderEventStreamingControllerTest}, which needs that
 * control to prove the HTTP layer writes events incrementally rather than buffering them.
 *
 * <p>Only {@link #findAll()} is exercised by that test, so the other {@link OrderEventRepository}
 * methods intentionally fail fast rather than silently returning a value nothing configured.
 */
public final class InMemoryOrderEventRepository implements OrderEventRepository {

  private final Flux<OrderEvent> eventsToStream;

  public InMemoryOrderEventRepository(final Flux<OrderEvent> eventsToStream) {
    this.eventsToStream = eventsToStream;
  }

  @Override
  public Mono<Void> save(final OrderEvent event) {
    throw new UnsupportedOperationException(
        "not needed by the streaming tests this fake exists for");
  }

  @Override
  public Flux<OrderEvent> findAll() {
    return eventsToStream;
  }

  @Override
  public Mono<Long> count() {
    throw new UnsupportedOperationException(
        "not needed by the streaming tests this fake exists for");
  }

  @Override
  public Flux<CategoryTotal> totalAmountByCategory() {
    throw new UnsupportedOperationException(
        "not needed by the streaming tests this fake exists for");
  }
}

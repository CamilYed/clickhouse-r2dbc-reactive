package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEvent;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEventRepository;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * REST endpoint proving {@code clickhouse-r2dbc-reactive} works end to end through Spring's own
 * R2DBC integration — deliberately thin, depending only on the {@link OrderEventRepository} port,
 * never on {@code DatabaseClient}/SQL directly (that lives in the {@code infrastructure} adapter).
 *
 * <p>{@link #byCategory()} is the point of this demo not being "just CRUD": it proves a real
 * aggregation query (a {@code GROUP BY}) round-trips correctly too, which is the kind of query
 * ClickHouse is actually built for.
 */
@RestController
class OrderEventController {

  private final OrderEventRepository orderEvents;

  OrderEventController(final OrderEventRepository orderEvents) {
    this.orderEvents = orderEvents;
  }

  /**
   * Records a new order event with a freshly generated id and the current timestamp — see {@link
   * OrderEvent}'s Javadoc for why the id is always supplied by this method rather than left for
   * ClickHouse to generate (it can't).
   */
  @PostMapping("/order-events")
  Mono<OrderEvent> create(@RequestBody final CreateOrderEventRequest request) {
    final OrderEvent event =
        new OrderEvent(
            UUID.randomUUID(),
            request.customerId(),
            request.category(),
            request.tags(),
            request.amount(),
            Optional.ofNullable(request.discount()),
            request.status(),
            request.clientIp(),
            Instant.now());
    return orderEvents.save(event).thenReturn(event);
  }

  /**
   * Streams every event, oldest first — the response body is written as it arrives from ClickHouse,
   * not buffered into a single in-memory list first.
   */
  @GetMapping("/order-events")
  Flux<OrderEvent> all() {
    return orderEvents.findAll();
  }

  /** The total number of recorded events. */
  @GetMapping("/order-events/count")
  Mono<Long> count() {
    return orderEvents.count();
  }

  /** Total order amount per category, highest first — see this class's own Javadoc. */
  @GetMapping("/order-events/analytics/by-category")
  Flux<CategoryTotal> byCategory() {
    return orderEvents.totalAmountByCategory();
  }
}

package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.builders;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEvent;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Test Data Builder for the domain {@link OrderEvent} record — sane defaults for every field,
 * override only what a given scenario actually cares about (see CLAUDE.md's "Test Data Builder"
 * pattern).
 */
public final class OrderEventTestBuilder {

  private UUID id = UUID.randomUUID();
  private UUID customerId = UUID.randomUUID();
  private String category = "electronics";
  private List<String> tags = List.of("featured");
  private BigDecimal amount = new BigDecimal("100.00");
  private Optional<BigDecimal> discount = Optional.empty();
  private OrderStatus status = OrderStatus.PLACED;
  private String clientIp = "127.0.0.1";
  private Instant occurredAt = Instant.now();

  private OrderEventTestBuilder() {}

  public static OrderEventTestBuilder anOrderEvent() {
    return new OrderEventTestBuilder();
  }

  public OrderEventTestBuilder withId(final UUID id) {
    this.id = id;
    return this;
  }

  public OrderEventTestBuilder withCustomerId(final UUID customerId) {
    this.customerId = customerId;
    return this;
  }

  public OrderEventTestBuilder withCategory(final String category) {
    this.category = category;
    return this;
  }

  public OrderEventTestBuilder withTags(final List<String> tags) {
    this.tags = tags;
    return this;
  }

  public OrderEventTestBuilder withAmount(final String amount) {
    this.amount = new BigDecimal(amount);
    return this;
  }

  public OrderEventTestBuilder withDiscount(final String discount) {
    this.discount = Optional.of(new BigDecimal(discount));
    return this;
  }

  public OrderEventTestBuilder withNoDiscount() {
    this.discount = Optional.empty();
    return this;
  }

  public OrderEventTestBuilder withStatus(final OrderStatus status) {
    this.status = status;
    return this;
  }

  public OrderEventTestBuilder withClientIp(final String clientIp) {
    this.clientIp = clientIp;
    return this;
  }

  public OrderEventTestBuilder withOccurredAt(final Instant occurredAt) {
    this.occurredAt = occurredAt;
    return this;
  }

  public OrderEvent build() {
    return new OrderEvent(
        id, customerId, category, tags, amount, discount, status, clientIp, occurredAt);
  }
}

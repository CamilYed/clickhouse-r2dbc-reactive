package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.builders;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.CreateOrderEventRequest;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderStatus;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/**
 * Test Data Builder for {@link CreateOrderEventRequest} — sane defaults for every field, override
 * only what a given scenario actually cares about (see CLAUDE.md's "Test Data Builder" pattern).
 */
public final class CreateOrderEventRequestTestBuilder {

  private UUID customerId = UUID.randomUUID();
  private String category = "electronics";
  private List<String> tags = List.of("featured");
  private BigDecimal amount = new BigDecimal("100.00");
  private BigDecimal discount;
  private OrderStatus status = OrderStatus.PLACED;
  private String clientIp = "127.0.0.1";

  private CreateOrderEventRequestTestBuilder() {}

  public static CreateOrderEventRequestTestBuilder aCreateOrderEventRequest() {
    return new CreateOrderEventRequestTestBuilder();
  }

  public CreateOrderEventRequestTestBuilder withCustomerId(final UUID customerId) {
    this.customerId = customerId;
    return this;
  }

  public CreateOrderEventRequestTestBuilder withCategory(final String category) {
    this.category = category;
    return this;
  }

  public CreateOrderEventRequestTestBuilder withTags(final List<String> tags) {
    this.tags = tags;
    return this;
  }

  public CreateOrderEventRequestTestBuilder withAmount(final String amount) {
    this.amount = new BigDecimal(amount);
    return this;
  }

  public CreateOrderEventRequestTestBuilder withDiscount(final String discount) {
    this.discount = new BigDecimal(discount);
    return this;
  }

  public CreateOrderEventRequestTestBuilder withNoDiscount() {
    this.discount = null;
    return this;
  }

  public CreateOrderEventRequestTestBuilder withStatus(final OrderStatus status) {
    this.status = status;
    return this;
  }

  public CreateOrderEventRequestTestBuilder withClientIp(final String clientIp) {
    this.clientIp = clientIp;
    return this;
  }

  public CreateOrderEventRequest build() {
    return new CreateOrderEventRequest(
        customerId, category, tags, amount, discount, status, clientIp);
  }
}

package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEvent;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderStatus;
import org.assertj.core.api.AbstractAssert;

/**
 * Custom (tailor-made) assertion for {@link OrderEvent}, so test failures read like domain
 * statements instead of {@code expected true but was false} (see CLAUDE.md's "Custom assertion"
 * pattern).
 */
public final class OrderEventAssert extends AbstractAssert<OrderEventAssert, OrderEvent> {

  private OrderEventAssert(final OrderEvent actual) {
    super(actual, OrderEventAssert.class);
  }

  public static OrderEventAssert assertThatOrderEvent(final OrderEvent actual) {
    return new OrderEventAssert(actual);
  }

  public OrderEventAssert hasCategory(final String expected) {
    isNotNull();
    assertThat(actual.category()).as("category").isEqualTo(expected);
    return this;
  }

  public OrderEventAssert hasTags(final String... expected) {
    isNotNull();
    assertThat(actual.tags()).as("tags").containsExactly(expected);
    return this;
  }

  public OrderEventAssert hasStatus(final OrderStatus expected) {
    isNotNull();
    assertThat(actual.status()).as("status").isEqualTo(expected);
    return this;
  }

  public OrderEventAssert hasClientIp(final String expected) {
    isNotNull();
    assertThat(actual.clientIp()).as("clientIp").isEqualTo(expected);
    return this;
  }

  /** Asserts {@code amount} is numerically equal to {@code expected}, ignoring scale. */
  public OrderEventAssert hasAmount(final String expected) {
    isNotNull();
    assertThat(actual.amount()).as("amount").isEqualByComparingTo(expected);
    return this;
  }

  /** Asserts {@code discount} is present and numerically equal to {@code expected}. */
  public OrderEventAssert hasDiscount(final String expected) {
    isNotNull();
    assertThat(actual.discount()).as("discount").isPresent();
    assertThat(actual.discount().orElseThrow()).as("discount").isEqualByComparingTo(expected);
    return this;
  }

  public OrderEventAssert hasNoDiscount() {
    isNotNull();
    assertThat(actual.discount()).as("discount").isEmpty();
    return this;
  }
}

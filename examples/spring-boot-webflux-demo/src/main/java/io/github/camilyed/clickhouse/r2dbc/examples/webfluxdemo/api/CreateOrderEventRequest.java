package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderStatus;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/**
 * The wire request body for {@code POST /order-events}.
 *
 * <p>A top-level, public record rather than nested inside {@link OrderEventController} — the DTO is
 * a genuine public boundary (Spring's JSON codec deserializes it, and this module's own test
 * support code, {@code CreateOrderEventRequestTestBuilder} in the {@code api.builders} sub-package,
 * constructs it), so it needs to be visible outside this package, unlike the controller itself.
 */
public record CreateOrderEventRequest(
    UUID customerId,
    String category,
    List<String> tags,
    BigDecimal amount,
    BigDecimal discount,
    OrderStatus status,
    String clientIp) {}

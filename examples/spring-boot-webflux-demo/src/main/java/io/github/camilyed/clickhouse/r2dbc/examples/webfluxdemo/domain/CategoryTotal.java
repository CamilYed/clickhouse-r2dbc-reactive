package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain;

import java.math.BigDecimal;

/**
 * The total order amount for one {@code category}, as returned by {@link
 * OrderEventRepository#totalAmountByCategory()} — the result of an aggregation query, not a stored
 * row, which is exactly the kind of query ClickHouse is actually built for and a plain CRUD demo
 * would never exercise.
 */
public record CategoryTotal(String category, BigDecimal totalAmount) {}

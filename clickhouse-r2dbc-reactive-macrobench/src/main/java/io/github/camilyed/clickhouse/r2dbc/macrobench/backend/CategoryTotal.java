package io.github.camilyed.clickhouse.r2dbc.macrobench.backend;

import java.math.BigDecimal;

/** One row of the analytics scenario's {@code GROUP BY category} join+aggregation result. */
public record CategoryTotal(String category, long orderCount, BigDecimal totalAmount) {}

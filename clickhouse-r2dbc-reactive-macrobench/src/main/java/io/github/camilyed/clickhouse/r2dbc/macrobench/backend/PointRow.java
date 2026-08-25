package io.github.camilyed.clickhouse.r2dbc.macrobench.backend;

import java.math.BigDecimal;

/** One row from the seeded point-query/stream table ({@code id}, {@code label}, {@code amount}). */
public record PointRow(long id, String label, BigDecimal amount) {}

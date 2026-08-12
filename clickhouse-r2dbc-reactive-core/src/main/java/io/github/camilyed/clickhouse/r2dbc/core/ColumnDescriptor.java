package io.github.camilyed.clickhouse.r2dbc.core;

/**
 * One column's wire-reported name and ClickHouse type string, in result-set (wire) order.
 *
 * <p>{@code typeName} is ClickHouse's own type string as sent in the {@code
 * RowBinaryWithNamesAndTypes} header (e.g. {@code "Nullable(Int32)"}, {@code "Array(String)"}), not
 * a Java type — see {@link DecodedResult} for why this deliberately stops short of predicting a
 * Java class per column.
 */
public record ColumnDescriptor(String name, String typeName) {}

package io.github.camilyed.clickhouse.r2dbc.core;

import java.util.List;
import reactor.core.publisher.Flux;

/**
 * A decoded ClickHouse result: the column schema, in wire order, alongside the stream of decoded
 * rows — both read from one {@link RowBinaryDecoder#decode} subscription.
 *
 * <p>{@code columns} deliberately carries ClickHouse's own type string per column ({@link
 * ColumnDescriptor#typeName()}), not a predicted Java class: which Java type client-v2 decodes a
 * given ClickHouse type into is an internal decision of its {@code BinaryStreamReader} (a
 * hand-written switch over {@code ClickHouseDataType}), not a static function of {@code
 * ClickHouseColumn} that this project can read off safely without duplicating — and possibly
 * drifting from — that internal switch. A caller that needs a Java type per column should derive
 * one from an actually-decoded row's values instead.
 *
 * <p>Each {@link DecodedRow} in {@code rows} carries values only, in the same wire order as {@code
 * columns} — resolving a column name to a value is {@code columns}' job (find the index once per
 * result, not once per row), not something {@code rows} does for a caller. See {@link DecodedRow}'s
 * Javadoc for why rows stopped being self-describing {@code Map}s.
 */
public record DecodedResult(List<ColumnDescriptor> columns, Flux<DecodedRow> rows) {}

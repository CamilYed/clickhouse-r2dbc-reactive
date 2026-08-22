package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain;

/**
 * The lifecycle of an {@link OrderEvent}, stored as ClickHouse's {@code Enum8('PLACED' = 1, 'PAID'
 * = 2, 'CANCELLED' = 3)}.
 *
 * <p>See {@code DatabaseClientOrderEventRepository}'s Javadoc for a real driver rough edge this
 * type once surfaced: driver releases before {@code 0.2.1} decoded {@code Enum8}/{@code Enum16}
 * columns as client-v2's own internal {@code EnumValue} type, not a plain {@link String}, so
 * reading one back through R2DBC's {@code Row.get(name, Class)} needed {@code Object.class} plus
 * {@code toString()} rather than {@code String.class} directly. Fixed in core's {@code
 * ListDecodingRowBinaryReader} (see ROADMAP.md's Phase 8 item 1) and published in {@code 0.2.1},
 * which this demo now depends on.
 */
public enum OrderStatus {
  PLACED,
  PAID,
  CANCELLED
}

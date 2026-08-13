package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain;

/**
 * The lifecycle of an {@link OrderEvent}, stored as ClickHouse's {@code Enum8('PLACED' = 1, 'PAID'
 * = 2, 'CANCELLED' = 3)}.
 *
 * <p>See {@code DatabaseClientOrderEventRepository}'s Javadoc for a real driver rough edge this
 * type surfaced: this driver decodes {@code Enum8}/{@code Enum16} columns as client-v2's own
 * internal {@code EnumValue} type, not a plain {@link String} — so reading one back through R2DBC's
 * {@code Row.get(name, Class)} means asking for {@code Object.class} and calling {@code toString()}
 * on the result, then parsing that into this enum, rather than asking for {@code String.class}
 * directly (which throws {@link ClassCastException}, since the decoded value isn't actually a
 * {@code String}).
 */
public enum OrderStatus {
  PLACED,
  PAID,
  CANCELLED
}

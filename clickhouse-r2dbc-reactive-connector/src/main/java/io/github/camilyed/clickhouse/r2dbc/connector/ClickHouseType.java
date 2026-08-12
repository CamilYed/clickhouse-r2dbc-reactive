package io.github.camilyed.clickhouse.r2dbc.connector;

import io.r2dbc.spi.Type;

/**
 * A column's database type, exposing ClickHouse's own wire type name via {@link #getName()}.
 *
 * <p>{@link #getJavaType()} is fixed to {@link Object}. Unlike {@link
 * io.r2dbc.spi.ReadableMetadata#getJavaType()} (optional, may return {@code null}), {@link
 * Type#getJavaType()} is mandatory — and predicting the exact Java class client-v2 decodes a given
 * ClickHouse type into, ahead of reading any row, would mean duplicating its {@code
 * BinaryStreamReader} decode switch here, an internal detail this project has deliberately not
 * taken on (see {@code core.DecodedResult}'s Javadoc). {@link ClickHouseColumnMetadata} does not
 * use this class for its own {@code getJavaType()} for that reason, and returns the spec's default
 * ({@code null}, "type not available") instead.
 */
record ClickHouseType(String name) implements Type {

  @Override
  public Class<?> getJavaType() {
    return Object.class;
  }

  @Override
  public String getName() {
    return name;
  }
}

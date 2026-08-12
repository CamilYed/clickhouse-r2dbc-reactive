package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Type;

/**
 * One column's metadata, backed by {@code core}'s {@link ColumnDescriptor}.
 *
 * <p>{@link #getJavaType()} is left at {@link io.r2dbc.spi.ReadableMetadata}'s own default
 * ({@code null}, "type not available") rather than guessed — see {@link ColumnDescriptor}'s
 * Javadoc for why a Java type per column isn't available without decoding a row.
 */
final class ClickHouseColumnMetadata implements ColumnMetadata {

  private final ColumnDescriptor descriptor;

  ClickHouseColumnMetadata(final ColumnDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public String getName() {
    return descriptor.name();
  }

  @Override
  public Type getType() {
    return new ClickHouseType(descriptor.typeName());
  }
}

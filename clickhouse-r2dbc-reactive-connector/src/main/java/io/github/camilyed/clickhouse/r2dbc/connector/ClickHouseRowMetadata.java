package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import java.util.List;
import java.util.NoSuchElementException;

/** {@link RowMetadata} over a fixed, wire-ordered list of {@code core}'s {@link ColumnDescriptor}s. */
final class ClickHouseRowMetadata implements RowMetadata {

  private final List<ClickHouseColumnMetadata> columnMetadatas;

  ClickHouseRowMetadata(final List<ColumnDescriptor> columns) {
    this.columnMetadatas = columns.stream().map(ClickHouseColumnMetadata::new).toList();
  }

  @Override
  public ColumnMetadata getColumnMetadata(final int index) {
    return columnMetadatas.get(index);
  }

  @Override
  public ColumnMetadata getColumnMetadata(final String name) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    return columnMetadatas.stream()
        .filter(column -> column.getName().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new NoSuchElementException("No column named '" + name + "'"));
  }

  @Override
  public List<? extends ColumnMetadata> getColumnMetadatas() {
    return columnMetadatas;
  }
}

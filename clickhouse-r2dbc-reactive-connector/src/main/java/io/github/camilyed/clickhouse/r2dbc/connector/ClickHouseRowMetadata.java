package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * {@link RowMetadata} over a fixed, wire-ordered list of {@code core}'s {@link ColumnDescriptor}s.
 *
 * <p>Also the name→index lookup {@link ClickHouseRow} uses to resolve {@link
 * io.r2dbc.spi.Row#get(String, Class)} against a {@link
 * io.github.camilyed.clickhouse.r2dbc.core.DecodedRow}'s positional values — built once here, in
 * the constructor, rather than re-derived per row/per call the way it was before this class held an
 * index map at all.
 */
final class ClickHouseRowMetadata implements RowMetadata {

  private final List<ClickHouseColumnMetadata> columnMetadatas;
  private final Map<String, Integer> indexByName;

  ClickHouseRowMetadata(final List<ColumnDescriptor> columns) {
    this.columnMetadatas = columns.stream().map(ClickHouseColumnMetadata::new).toList();
    this.indexByName = new HashMap<>();
    for (int index = 0; index < columns.size(); index++) {
      indexByName.put(canonicalize(columns.get(index).name()), index);
    }
  }

  @Override
  public ColumnMetadata getColumnMetadata(final int index) {
    return columnMetadatas.get(index);
  }

  @Override
  public ColumnMetadata getColumnMetadata(final String name) {
    return columnMetadatas.get(indexOf(name));
  }

  @Override
  public List<? extends ColumnMetadata> getColumnMetadatas() {
    return columnMetadatas;
  }

  /** The wire-order index of the column named {@code name} (case-insensitive). */
  // name is declared non-null under this module's @NullMarked contract, but this is reached via
  // getColumnMetadata(String), a plain io.r2dbc.spi.RowMetadata override - external callers of
  // the public R2DBC SPI aren't bound by that static guarantee, so failing fast here beats a
  // confusing NPE deeper in the call chain.
  @SuppressWarnings("java:S2583")
  int indexOf(final String name) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    final Integer index = indexByName.get(canonicalize(name));
    if (index == null) {
      throw new NoSuchElementException("No column named '" + name + "'");
    }
    return index;
  }

  private static String canonicalize(final String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.Map;

/**
 * A decoded row, backed by {@code core}'s already-decoded {@code Map<String, Object>}.
 *
 * <p>{@link #get(int, Class)}/{@link #get(String, Class)} only cast the already-decoded value to
 * {@code type}; neither attempts any widening conversion beyond what {@code core}'s decoder already
 * produced (e.g. asking for {@code Long} when the decoded value is an {@code Integer} throws {@link
 * ClassCastException} rather than converting). Broader R2DBC type-conversion support is separately
 * scoped future work.
 */
final class ClickHouseRow implements Row {

  private final Map<String, Object> values;
  private final ClickHouseRowMetadata metadata;

  ClickHouseRow(final Map<String, Object> values, final ClickHouseRowMetadata metadata) {
    this.values = values;
    this.metadata = metadata;
  }

  @Override
  public RowMetadata getMetadata() {
    return metadata;
  }

  @Override
  public <T> T get(final int index, final Class<T> type) {
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    final String name = metadata.getColumnMetadata(index).getName();
    return type.cast(values.get(name));
  }

  @Override
  public <T> T get(final String name, final Class<T> type) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    final String canonicalName = metadata.getColumnMetadata(name).getName();
    return type.cast(values.get(canonicalName));
  }
}

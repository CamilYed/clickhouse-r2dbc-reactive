package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

/**
 * A decoded row, backed by {@code core}'s already-decoded, positional {@link DecodedRow}.
 *
 * <p>{@link #get(int, Class)}/{@link #get(String, Class)} only cast the already-decoded value to
 * {@code type}; neither attempts any widening conversion beyond what {@code core}'s decoder already
 * produced (e.g. asking for {@code Long} when the decoded value is an {@code Integer} throws {@link
 * ClassCastException} rather than converting). Broader R2DBC type-conversion support is separately
 * scoped future work.
 *
 * <p>{@link #get(String, Class)} resolves {@code name} to a wire index via {@code metadata} (a
 * lookup built once per result, not once per row) and reads {@code row.valueAt(index)} directly —
 * unlike the {@code Map<String, Object>}-backed row this type used to wrap, there is no per-call
 * name-keyed lookup against the row itself, only against the already-shared {@code metadata}.
 */
final class ClickHouseRow implements Row {

  private final DecodedRow row;
  private final ClickHouseRowMetadata metadata;

  ClickHouseRow(final DecodedRow row, final ClickHouseRowMetadata metadata) {
    this.row = row;
    this.metadata = metadata;
  }

  @Override
  public RowMetadata getMetadata() {
    return metadata;
  }

  // type is declared non-null under this module's @NullMarked contract, but this overrides a
  // plain io.r2dbc.spi.Row method - external callers of the public R2DBC SPI aren't bound by
  // that static guarantee, so failing fast here beats a confusing NPE deeper in the call chain.
  @SuppressWarnings("java:S2583")
  @Override
  public <T> T get(final int index, final Class<T> type) {
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    return type.cast(row.valueAt(index));
  }

  // See get(int, Class) above for why this defensive check is kept despite @NullMarked.
  @SuppressWarnings("java:S2583")
  @Override
  public <T> T get(final String name, final Class<T> type) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    return type.cast(row.valueAt(metadata.indexOf(name)));
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.jspecify.annotations.Nullable;

/**
 * A decoded row, backed by {@code core}'s already-decoded, positional {@link DecodedRow}.
 *
 * <p>{@link #get(int, Class)}/{@link #get(String, Class)} route the already-decoded value through
 * {@link ClickHouseValueConverter}: a direct match to {@code type} (or {@code null}) returns as-is,
 * a controlled numeric or {@code ZonedDateTime}-derived conversion is attempted for the fixed
 * matrices that class documents, and anything else throws {@link
 * ClickHouseValueConversionException} — see that class's Javadoc for the full, deliberately
 * limited conversion surface.
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
  @Override
  public <T> @Nullable T get(final int index, final Class<T> type) {
    if (type == null) { // NOSONAR - see defensive-null-check note above
      throw new IllegalArgumentException("type must not be null");
    }
    return ClickHouseValueConverter.convert(row.valueAt(index), type);
  }

  // See get(int, Class) above for why this defensive check is kept despite @NullMarked.
  @Override
  public <T> @Nullable T get(final String name, final Class<T> type) {
    if (name == null) { // NOSONAR - see get(int, Class) above
      throw new IllegalArgumentException("name must not be null");
    }
    if (type == null) { // NOSONAR - see get(int, Class) above
      throw new IllegalArgumentException("type must not be null");
    }
    return ClickHouseValueConverter.convert(row.valueAt(metadata.indexOf(name)), type);
  }
}

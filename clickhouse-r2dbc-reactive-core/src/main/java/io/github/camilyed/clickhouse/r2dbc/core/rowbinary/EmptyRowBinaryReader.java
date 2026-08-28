package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * The {@link RowBinaryReader} for a response body with no {@code RowBinaryWithNamesAndTypes} header
 * at all — a DDL statement (e.g. {@code CREATE TABLE}), which never sends one. {@link #hasNext()}
 * is always {@code false} and {@link #columns()} is always empty, matching {@link
 * RowBinaryHeader}'s {@code present() == false} case exactly.
 */
final class EmptyRowBinaryReader implements RowBinaryReader {

  private final InputStream in;

  EmptyRowBinaryReader(final InputStream in) {
    this.in = in;
  }

  @Override
  public List<ColumnDescriptor> columns() {
    return List.of();
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Object[] nextRowValues() {
    throw new IllegalStateException(
        "nextRowValues() called on an empty RowBinary body - hasNext() is always false here");
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}

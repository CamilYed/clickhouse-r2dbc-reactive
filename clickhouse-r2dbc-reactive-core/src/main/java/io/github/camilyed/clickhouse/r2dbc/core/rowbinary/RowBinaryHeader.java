package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import com.clickhouse.data.ClickHouseColumn;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The {@code RowBinaryWithNamesAndTypes} header — column count, names, and ClickHouse type strings
 * — read once off the wire, before {@link RowBinaryDecoder} decides whether to decode the following
 * rows natively or fall back to client-v2's own reader (see that class's Javadoc).
 *
 * <p>{@link #rawBytes()} is the exact byte sequence consumed while parsing — captured, not just
 * counted, so a fallback decision can hand client-v2's reader an {@link
 * java.io.SequenceInputStream} that replays these bytes followed by the rest of the response body,
 * letting it parse the header itself exactly as it always has, rather than requiring a schema-aware
 * entry point into client-v2 that this driver would then have to keep in sync with its own
 * header-parsing logic.
 *
 * <p>{@link #present()} distinguishes "the response body never sent a header at all" (a DDL
 * statement, which sends no {@code RowBinaryWithNamesAndTypes} data whatsoever) from "the header
 * was read and reports zero columns" — matches client-v2's own {@code
 * RowBinaryWithNamesAndTypesFormatReader#readSchema()}, which catches an {@link EOFException} on
 * the very first varint read and leaves its schema {@code null} rather than an empty one.
 *
 * <p>Overrides {@code equals}/{@code hashCode}/{@code toString} rather than using the record's
 * generated ones — {@code rawBytes} is a {@code byte[]}, and array fields don't get content-aware
 * behavior for free from a record (the generated methods would compare/print array identity, not
 * content).
 */
record RowBinaryHeader(List<ClickHouseColumn> columns, byte[] rawBytes, boolean present) {

  static RowBinaryHeader readFrom(final InputStream source) throws IOException {
    final CapturingInputStream capturing = new CapturingInputStream(source);
    final int columnCount;
    try {
      columnCount = (int) RowBinaryWireFormat.readVarUInt(capturing);
    } catch (final EOFException e) {
      return new RowBinaryHeader(List.of(), capturing.capturedBytes(), false);
    }
    final List<String> names = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      names.add(RowBinaryWireFormat.readString(capturing));
    }
    final List<ClickHouseColumn> columns = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      final String typeName = RowBinaryWireFormat.readString(capturing);
      columns.add(ClickHouseColumn.of(names.get(i), typeName));
    }
    return new RowBinaryHeader(columns, capturing.capturedBytes(), true);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof final RowBinaryHeader other)) {
      return false;
    }
    return present == other.present
        && columns.equals(other.columns)
        && Arrays.equals(rawBytes, other.rawBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns, Arrays.hashCode(rawBytes), present);
  }

  @Override
  public String toString() {
    return "RowBinaryHeader[columns="
        + columns
        + ", rawBytes="
        + Arrays.toString(rawBytes)
        + ", present="
        + present
        + "]";
  }

  /** Tees every byte read from the wrapped stream into a growable in-memory capture buffer. */
  private static final class CapturingInputStream extends FilterInputStream {

    private final ByteArrayOutputStream captured = new ByteArrayOutputStream();

    private CapturingInputStream(final InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      final int b = super.read();
      if (b != -1) {
        captured.write(b);
      }
      return b;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      final int n = super.read(b, off, len);
      if (n > 0) {
        captured.write(b, off, n);
      }
      return n;
    }

    byte[] capturedBytes() {
      return captured.toByteArray();
    }
  }
}

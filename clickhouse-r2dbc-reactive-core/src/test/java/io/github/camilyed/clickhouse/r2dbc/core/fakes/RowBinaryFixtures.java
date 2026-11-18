package io.github.camilyed.clickhouse.r2dbc.core.fakes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Hand-built {@code RowBinaryWithNamesAndTypes} wire bytes for {@code core}'s own decoder tests.
 *
 * <p>Mirrors {@code testkit.fakes.ClickHouseWireFixtures}, kept as a small module-local copy rather
 * than a shared dependency: {@code testkit} depends on {@code core}, so {@code core} cannot depend
 * back on {@code testkit} without a cycle.
 */
public final class RowBinaryFixtures {

  private RowBinaryFixtures() {}

  /** One column named {@code "1"} of type {@code UInt8}, one row with value {@code 1}. */
  public static byte[] selectOneRowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"1"}, new String[] {"UInt8"}, new byte[] {0x01});
  }

  /**
   * One column named {@code "1"} of type {@code UInt8}, two rows with values {@code 1} and {@code
   * 2} — both fully present in this single chunk, unlike {@link
   * #selectOneRowBinaryWithNamesAndTypes()}. Exists specifically so a test can hand a cancellation
   * scenario a source that never completes on its own (e.g. a {@code Flux.never()} tail) without
   * the very first row also hanging forever: client-v2's own reader eagerly reads one row ahead
   * internally (see {@code AbstractBinaryFormatReader#next()} swapping {@code nextRecord} into
   * {@code currentRecord} and immediately calling {@code readNextRecord()} again), so handing out
   * row 1 always requires row 2's bytes (or an end-of-stream signal) to already be available —
   * providing only one row's bytes followed by a source that never completes makes that look-ahead
   * block forever before row 1 is ever emitted, which looks like a hung test rather than the
   * cancellation behavior it was meant to exercise.
   */
  public static byte[] twoRowsOfUInt8RowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"1"}, new String[] {"UInt8"}, new byte[] {0x01, 0x02});
  }

  /** One point-query-shaped row: {@code label String, amount Decimal(18,4)}. */
  public static byte[] stringAndDecimalRowBinaryWithNamesAndTypes() {
    final ByteArrayOutputStream row = new ByteArrayOutputStream();
    try {
      writeString(row, "label-41");
      row.write(
          ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(421_234L).array());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return rowBinaryWithNamesAndTypes(
        new String[] {"label", "amount"},
        new String[] {"String", "Decimal(18, 4)"},
        row.toByteArray());
  }

  /** One scan-shaped row: {@code id UInt64, label String, amount Decimal(18,4)}. */
  public static byte[] uint64StringAndDecimalRowBinaryWithNamesAndTypes() {
    final ByteArrayOutputStream row = new ByteArrayOutputStream();
    try {
      row.write(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1});
      writeString(row, "label-max");
      row.write(
          ByteBuffer.allocate(Long.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .putLong(-123_456L)
              .array());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return rowBinaryWithNamesAndTypes(
        new String[] {"id", "label", "amount"},
        new String[] {"UInt64", "String", "Decimal(18, 4)"},
        row.toByteArray());
  }

  /**
   * One column named {@code "arr"} of type {@code Array(Int32)}, one row with values {@code [10,
   * 20, 30]}.
   */
  public static byte[] arrayOfInt32RowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"arr"},
        new String[] {"Array(Int32)"},
        new byte[] {
          0x03, // array length (varint)
          0x0A,
          0x00,
          0x00,
          0x00, // 10
          0x14,
          0x00,
          0x00,
          0x00, // 20
          0x1E,
          0x00,
          0x00,
          0x00 // 30
        });
  }

  /**
   * One column named {@code "embedding"} of type {@code QBit(Float32, 4)}, one row containing
   * {@code [1.25, -2.5, 0.0, 42.0]}. RowBinary transmits {@code QBit} as an array: a varint length
   * followed by little-endian elements.
   */
  public static byte[] qBitFloat32RowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"embedding"},
        new String[] {"QBit(Float32, 4)"},
        new byte[] {
          0x04, // vector length (varint)
          0x00,
          0x00,
          (byte) 0xA0,
          0x3F, // 1.25
          0x00,
          0x00,
          0x20,
          (byte) 0xC0, // -2.5
          0x00,
          0x00,
          0x00,
          0x00, // 0.0
          0x00,
          0x00,
          0x28,
          0x42 // 42.0
        });
  }

  /**
   * One column named {@code "status"} of type {@code Enum8('a' = 1, 'b' = 2)}, one row selecting
   * member {@code 'b'} (ordinal {@code 2}) — the wire encoding for an {@code Enum8} value is a
   * single signed byte equal to its ordinal, same as a plain {@code Int8}.
   */
  public static byte[] enum8SingleValueRowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"status"}, new String[] {"Enum8('a' = 1, 'b' = 2)"}, new byte[] {0x02});
  }

  /**
   * General-purpose builder for any column/type/row-bytes combination — used directly by tests that
   * need a wire fixture this class has no bespoke named method for yet (e.g. native scalar types,
   * {@code Nullable} wrapping, or a mix of supported/unsupported types to exercise a fallback
   * path), instead of growing a bespoke method per case.
   */
  public static byte[] rowBinaryWithNamesAndTypes(
      final String[] columnNames, final String[] columnTypes, final byte[] rowBytes) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      writeVarUInt(out, columnNames.length);
      for (final String name : columnNames) {
        writeString(out, name);
      }
      for (final String type : columnTypes) {
        writeString(out, type);
      }
      out.write(rowBytes);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  /**
   * Same header as {@link #rowBinaryWithNamesAndTypes(String[], String[], byte[])}, multiple rows.
   */
  public static byte[] rowBinaryWithNamesAndTypes(
      final String[] columnNames, final String[] columnTypes, final byte[]... rows) {
    final ByteArrayOutputStream allRows = new ByteArrayOutputStream();
    for (final byte[] row : rows) {
      allRows.writeBytes(row);
    }
    return rowBinaryWithNamesAndTypes(columnNames, columnTypes, allRows.toByteArray());
  }

  private static void writeString(final ByteArrayOutputStream out, final String value)
      throws IOException {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeVarUInt(out, bytes.length);
    out.write(bytes);
  }

  private static void writeVarUInt(final ByteArrayOutputStream out, final int value) {
    int remaining = value;
    while (true) {
      if ((remaining & ~0x7F) == 0) {
        out.write(remaining);
        return;
      }
      out.write((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }
  }
}

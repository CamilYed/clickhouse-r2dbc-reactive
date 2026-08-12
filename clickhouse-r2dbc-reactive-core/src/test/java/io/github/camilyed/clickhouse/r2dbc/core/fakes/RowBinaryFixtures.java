package io.github.camilyed.clickhouse.r2dbc.core.fakes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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

  private static byte[] rowBinaryWithNamesAndTypes(
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

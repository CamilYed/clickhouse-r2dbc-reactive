package io.github.camilyed.clickhouse.r2dbc.testkit.fakes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Hand-built ClickHouse wire bytes for controlled-server test scenarios.
 *
 * <p>Encodes the same {@code RowBinaryWithNamesAndTypes} shape that {@code
 * com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader} decodes: a {@code
 * VarUInt} column count, the column names as {@code String}s, the column types as {@code String}s,
 * then the row values with no further framing.
 */
public final class ClickHouseWireFixtures {

  private ClickHouseWireFixtures() {}

  /** One column named {@code "1"} of type {@code UInt8}, one row with value {@code 1}. */
  public static byte[] selectOneRowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"1"}, new String[] {"UInt8"}, new byte[] {0x01});
  }

  /**
   * One column named {@code "1"} of type {@code UInt8}, two rows with values {@code 1} and {@code
   * 2} — both fully present in this single chunk. Mirrors {@code
   * core.fakes.RowBinaryFixtures#twoRowsOfUInt8RowBinaryWithNamesAndTypes()} (kept as a small
   * module-local copy rather than a shared dependency for the same reason as that class's own
   * Javadoc explains: {@code testkit} depends on {@code core}, so {@code core} cannot depend back
   * on {@code testkit} without a cycle). Exists so a test consuming rows through a real network
   * response (not a hermetic {@code Flux.just(...)}) has more than one row to cancel after the
   * first.
   */
  public static byte[] twoRowsOfUInt8RowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"1"}, new String[] {"UInt8"}, new byte[] {0x01, 0x02});
  }

  /**
   * One column named {@code "n"} of type {@code Int32} (a 4-byte value), but the response body ends
   * after only 2 of those 4 bytes — a truncated/corrupt response, as opposed to a genuinely empty
   * result set. A clean end-of-stream exactly at a row boundary (no partial row started) is a
   * valid, zero-row result — see {@code core.fakes.RowBinaryFixtures}'s Javadoc on the reader's
   * one-row lookahead for why. Truncation strictly <em>inside</em> a value already being read is
   * what forces an actual decode failure rather than "no more rows."
   */
  public static byte[] truncatedInt32ValueRowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"n"}, new String[] {"Int32"}, new byte[] {0x01, 0x02});
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

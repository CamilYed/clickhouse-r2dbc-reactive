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
   * Two columns - {@code "a"} ({@code UInt8}, fully present) and {@code "b"} ({@code Int32}, a
   * 4-byte value truncated after only 2 of those bytes) - a response that ends abruptly mid-value,
   * forcing a genuine row-decode failure rather than a clean "no more rows" result.
   *
   * <p>A single truncated column is not enough to force this: client-v2's {@code
   * AbstractBinaryFormatReader#readRecord} explicitly swallows an {@code EOFException} that hits a
   * row's <em>first</em> column - {@code if (firstColumn) { endReached(); return false; }} - and
   * treats it exactly like a genuinely empty result set, indistinguishable from a clean
   * end-of-stream at a row boundary. It only rethrows (and the reader surfaces a real failure) when
   * the {@code EOFException} hits a <em>later</em> column in a row whose first column already read
   * successfully. So {@code "a"} has to fully succeed, and the truncation has to land on {@code
   * "b"}, for this fixture to actually exercise a decode failure instead of silently decoding as
   * zero rows.
   */
  public static byte[] truncatedInt32ValueRowBinaryWithNamesAndTypes() {
    return rowBinaryWithNamesAndTypes(
        new String[] {"a", "b"}, new String[] {"UInt8", "Int32"}, new byte[] {0x01, 0x02, 0x03});
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

package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Task following the closed-out latency-path-isolation ladder (docs/performance/latency-path-isolation.md):
 * that ladder tested copy-avoidance (Variant B), query/statement construction cost (task #309), and
 * admission-gate ordering (Variant C) — none explained {@code LatencyPathVariantABenchmark}'s ~2.6-4.9%
 * mean deficit vs. client-v2. One layer none of those four hypotheses ever isolated: client-v2's own
 * {@code RowBinaryWithNamesAndTypesFormatReader}/{@code AbstractBinaryFormatReader}/{@code
 * BinaryStreamReader} parsing machinery itself — its per-value type dispatch, buffering, and
 * allocation pattern while decoding varints/strings/decimals off the wire.
 *
 * <p>This class is a minimal, hand-rolled {@code RowBinaryWithNamesAndTypes} decoder covering only
 * the four ClickHouse wire types {@code TrivialQueryBenchmark}/{@code PointQueryBenchmark}/{@code
 * StreamingScanBenchmark} actually use — {@code UInt8}, {@code UInt64}, {@code String}, {@code
 * Decimal(P, S)} — reading directly off the {@link InputStream} it's given (in {@code
 * LatencyPathVariantDBenchmark}, that's {@code ZeroCopyByteBufInputStreamBridge}, holding the
 * copy-vs-zero-copy question Variant B already answered constant so this isolates the reader layer
 * alone). Deliberately narrow and unsupported outside these four types, and deliberately not wired
 * into {@code core} or {@code connector} — this pass stays "no production code changes" per this
 * repo's diagnostic-only convention for latency investigation work; see {@code
 * LatencyPathVariantDBenchmark}'s own Javadoc for how it's used.
 */
final class MinimalRowBinaryReader implements AutoCloseable {

  private static final Pattern DECIMAL_TYPE = Pattern.compile("Decimal\\((\\d+),\\s*(\\d+)\\)");

  private final PushbackInputStream in;
  private final ColumnType[] columns;
  private final int[] decimalScales;
  private final byte[] scratch8 = new byte[8];

  private MinimalRowBinaryReader(
      final PushbackInputStream in, final ColumnType[] columns, final int[] decimalScales) {
    this.in = in;
    this.columns = columns;
    this.decimalScales = decimalScales;
  }

  /**
   * Parses the {@code RowBinaryWithNamesAndTypes} header off {@code source} — column count, then
   * that many names (read and discarded; this class decodes positionally, matching how every
   * caller in this module already reads columns by index), then that many type names, resolved to
   * a {@link ColumnType} each.
   */
  static MinimalRowBinaryReader open(final InputStream source) throws IOException {
    final PushbackInputStream in = new PushbackInputStream(source, 1);
    final int columnCount = (int) readVarUInt(in);
    for (int i = 0; i < columnCount; i++) {
      readString(in); // column name - unused, decoding is positional
    }
    final ColumnType[] columns = new ColumnType[columnCount];
    final int[] scales = new int[columnCount];
    for (int i = 0; i < columnCount; i++) {
      final String typeName = readString(in);
      columns[i] = ColumnType.forWireName(typeName);
      scales[i] = columns[i] == ColumnType.DECIMAL ? decimalScaleOf(typeName) : 0;
    }
    return new MinimalRowBinaryReader(in, columns, scales);
  }

  /**
   * The next row's values, positionally matching the header's column order, or {@code null} once
   * the stream is exhausted — mirrors client-v2's own {@code reader.next() == null} end-of-stream
   * convention, checked via a one-byte peek/pushback rather than a dedicated row-count prefix
   * (this wire format has none; end of stream is the only terminator).
   */
  @Nullable Object[] nextRow() throws IOException {
    final int first = in.read();
    if (first == -1) {
      return null;
    }
    in.unread(first);
    final Object[] values = new Object[columns.length];
    for (int i = 0; i < columns.length; i++) {
      values[i] = decode(columns[i], decimalScales[i]);
    }
    return values;
  }

  private Object decode(final ColumnType type, final int decimalScale) throws IOException {
    return switch (type) {
      case UINT8 -> (long) readRequiredByte(in);
      case UINT64 -> readLongLE(in, scratch8);
      case STRING -> readString(in);
      case DECIMAL -> BigDecimal.valueOf(readLongLE(in, scratch8), decimalScale);
    };
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  private static int decimalScaleOf(final String typeName) {
    final Matcher matcher = DECIMAL_TYPE.matcher(typeName);
    if (!matcher.matches()) {
      throw new UnsupportedOperationException("Unsupported Decimal type shape: " + typeName);
    }
    return Integer.parseInt(matcher.group(2));
  }

  private static int readRequiredByte(final InputStream in) throws IOException {
    final int b = in.read();
    if (b == -1) {
      throw new EOFException("Unexpected end of stream while reading a value");
    }
    return b;
  }

  private static void readFully(final InputStream in, final byte[] destination) throws IOException {
    int offset = 0;
    while (offset < destination.length) {
      final int n = in.read(destination, offset, destination.length - offset);
      if (n == -1) {
        throw new EOFException("Unexpected end of stream while reading a value");
      }
      offset += n;
    }
  }

  /** LEB128-style unsigned varint, matching ClickHouse's own wire encoding for lengths/counts. */
  private static long readVarUInt(final InputStream in) throws IOException {
    long result = 0;
    int shift = 0;
    while (true) {
      final int b = readRequiredByte(in);
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
  }

  private static long readLongLE(final InputStream in, final byte[] scratch8) throws IOException {
    readFully(in, scratch8);
    long value = 0;
    for (int i = 7; i >= 0; i--) {
      value = (value << 8) | (scratch8[i] & 0xFFL);
    }
    return value;
  }

  private static String readString(final InputStream in) throws IOException {
    final int length = (int) readVarUInt(in);
    if (length == 0) {
      return "";
    }
    final byte[] bytes = new byte[length];
    readFully(in, bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /** The only four ClickHouse wire types this benchmark-local decoder understands — see class Javadoc. */
  private enum ColumnType {
    UINT8,
    UINT64,
    STRING,
    DECIMAL;

    static ColumnType forWireName(final String typeName) {
      if ("UInt8".equals(typeName)) {
        return UINT8;
      }
      if ("UInt64".equals(typeName)) {
        return UINT64;
      }
      if ("String".equals(typeName)) {
        return STRING;
      }
      if (typeName.startsWith("Decimal(")) {
        return DECIMAL;
      }
      throw new UnsupportedOperationException(
          "MinimalRowBinaryReader does not support ClickHouse type: " + typeName);
    }
  }
}

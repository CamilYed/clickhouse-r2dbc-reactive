package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Low-level {@code RowBinary} wire-format primitives — the unsigned varint length/count encoding,
 * little-endian fixed-width integer reads, and length-prefixed UTF-8 strings every {@code
 * RowBinary}/{@code RowBinaryWithNamesAndTypes} value is built from. Shared by {@link
 * NativeRowBinaryReader} (row decoding) and {@link RowBinaryHeader} (header parsing) so both read
 * the exact same encoding the exact same way.
 *
 * <p>Deliberately narrow: this is the wire format's own primitive alphabet, not a general "byte
 * utilities" grab-bag — every method here is either LEB128 varint decoding or little-endian
 * fixed-width decoding, both spelled out in ClickHouse's own {@code RowBinary} wire format
 * documentation. Semantics cross-checked against client-v2's own {@code BinaryStreamReader}
 * (package {@code com.clickhouse.client.api.data_formats.internal}) so a native and a client-v2
 * decode of the same bytes agree exactly.
 */
final class RowBinaryWireFormat {

  private RowBinaryWireFormat() {}

  /** Reads one byte, or throws {@link EOFException} if the stream is exhausted. */
  static int readRequiredByte(final InputStream in) throws IOException {
    final int b = in.read();
    if (b == -1) {
      throw new EOFException("Unexpected end of stream while reading a RowBinary value");
    }
    return b;
  }

  /** Fills {@code destination} completely from {@code in}, or throws {@link EOFException}. */
  static void readFully(final InputStream in, final byte[] destination) throws IOException {
    int offset = 0;
    while (offset < destination.length) {
      final int n = in.read(destination, offset, destination.length - offset);
      if (n == -1) {
        throw new EOFException("Unexpected end of stream while reading a RowBinary value");
      }
      offset += n;
    }
  }

  /**
   * LEB128-style unsigned varint (7 payload bits per byte, high bit = continuation flag) —
   * ClickHouse's own wire encoding for every length/count prefix in {@code RowBinary}.
   */
  static long readVarUInt(final InputStream in) throws IOException {
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

  /** A varint-length-prefixed UTF-8 string. */
  static String readString(final InputStream in) throws IOException {
    final int length = (int) readVarUInt(in);
    if (length == 0) {
      return "";
    }
    final byte[] bytes = new byte[length];
    readFully(in, bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /** A little-endian signed {@code short} (2 bytes). {@code scratch} must be at least 2 bytes. */
  static short readShortLE(final InputStream in, final byte[] scratch) throws IOException {
    readFully2(in, scratch);
    return (short) ((scratch[0] & 0xFF) | ((scratch[1] & 0xFF) << 8));
  }

  /** A little-endian signed {@code int} (4 bytes). {@code scratch} must be at least 4 bytes. */
  static int readIntLE(final InputStream in, final byte[] scratch) throws IOException {
    readFully4(in, scratch);
    return (scratch[0] & 0xFF)
        | ((scratch[1] & 0xFF) << 8)
        | ((scratch[2] & 0xFF) << 16)
        | ((scratch[3] & 0xFF) << 24);
  }

  /** A little-endian signed {@code long} (8 bytes). {@code scratch} must be at least 8 bytes. */
  static long readLongLE(final InputStream in, final byte[] scratch) throws IOException {
    readFully8(in, scratch);
    long value = 0;
    for (int i = 7; i >= 0; i--) {
      value = (value << 8) | (scratch[i] & 0xFFL);
    }
    return value;
  }

  /**
   * A little-endian integer of {@code byteWidth} bytes, widened to an unsigned {@link BigInteger}
   * (matches client-v2's {@code BinaryStreamReader.readBigIntegerLE(len, true)} for {@code UInt64}/
   * {@code UInt128}/{@code UInt256} and the wide {@code Decimal128}/{@code Decimal256} tiers).
   * {@code scratch} must be at least {@code byteWidth} bytes.
   */
  static BigInteger readUnsignedBigIntegerLE(
      final InputStream in, final byte[] scratch, final int byteWidth) throws IOException {
    return new BigInteger(1, readReversed(in, scratch, byteWidth));
  }

  /**
   * Same as {@link #readUnsignedBigIntegerLE} but signed (two's complement) — matches client-v2's
   * {@code readBigIntegerLE(len, false)}, used for {@code Decimal128}/{@code Decimal256} (which are
   * signed, unlike {@code UInt128}/{@code UInt256}).
   */
  static BigInteger readSignedBigIntegerLE(
      final InputStream in, final byte[] scratch, final int byteWidth) throws IOException {
    return new BigInteger(readReversed(in, scratch, byteWidth));
  }

  private static byte[] readReversed(final InputStream in, final byte[] scratch, final int length)
      throws IOException {
    readFully(in, scratch, length);
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = scratch[length - 1 - i];
    }
    return bytes;
  }

  private static void readFully(final InputStream in, final byte[] destination, final int length)
      throws IOException {
    int offset = 0;
    while (offset < length) {
      final int n = in.read(destination, offset, length - offset);
      if (n == -1) {
        throw new EOFException("Unexpected end of stream while reading a RowBinary value");
      }
      offset += n;
    }
  }

  private static void readFully2(final InputStream in, final byte[] destination)
      throws IOException {
    readFully(in, destination, 2);
  }

  private static void readFully4(final InputStream in, final byte[] destination)
      throws IOException {
    readFully(in, destination, 4);
  }

  private static void readFully8(final InputStream in, final byte[] destination)
      throws IOException {
    readFully(in, destination, 8);
  }
}

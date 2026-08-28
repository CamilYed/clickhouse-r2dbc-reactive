package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Decodes one {@code RowBinary} column value off the wire into the exact Java type client-v2's own
 * {@code BinaryStreamReader.readValue} produces for that ClickHouse type — see each {@link
 * ScalarColumnDecoder} constant's own comment for the specific mapping, cross-checked directly
 * against that class's source. {@code scratch} is a caller-owned, reused byte buffer (at least 32
 * bytes — the widest fixed-width value this hierarchy decodes, {@code Decimal256}) so decoding a
 * row allocates no fixed-size scratch arrays of its own, the same reuse strategy client-v2's own
 * reader uses internally.
 *
 * <p>A closed, exhaustively-known set of variants — every ClickHouse scalar type {@link
 * NativeColumnTypeResolver} currently resolves natively, nothing more. Adding a new
 * natively-decoded type means adding a case here <em>and</em> to {@link NativeColumnTypeResolver};
 * anything not covered here safely falls back to client-v2's own reader instead (see {@link
 * RowBinaryDecoder}), so this hierarchy never needs to be exhaustive over ClickHouse's full type
 * system.
 */
sealed interface ColumnDecoder
    permits ScalarColumnDecoder, DecimalColumnDecoder, FixedStringColumnDecoder {

  /** Reads one value of this decoder's type from {@code in}, using {@code scratch} as needed. */
  Object decode(InputStream in, byte[] scratch) throws IOException;
}

/**
 * The natively-decoded ClickHouse scalar types with no per-column parameters (unlike {@link
 * DecimalColumnDecoder}'s width/scale or {@link FixedStringColumnDecoder}'s length) — one stateless
 * decode strategy per constant, matching client-v2's {@code BinaryStreamReader.readValue} switch
 * exactly for these types.
 */
enum ScalarColumnDecoder implements ColumnDecoder {

  /** {@code Int8} → {@link Byte} — one signed byte. */
  INT8 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return (byte) RowBinaryWireFormat.readRequiredByte(in);
    }
  },

  /** {@code UInt8} → {@link Short} (widened, matches client-v2's {@code readUnsignedByte()}). */
  UINT8 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return (short) (RowBinaryWireFormat.readRequiredByte(in) & 0xFF);
    }
  },

  /** {@code Int16} → {@link Short} — little-endian 2 bytes, signed. */
  INT16 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readShortLE(in, scratch);
    }
  },

  /**
   * {@code UInt16} → {@link Integer} (widened, matches client-v2's {@code readUnsignedShortLE()}).
   */
  UINT16 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readShortLE(in, scratch) & 0xFFFF;
    }
  },

  /** {@code Int32} → {@link Integer} — little-endian 4 bytes, signed. */
  INT32 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readIntLE(in, scratch);
    }
  },

  /** {@code UInt32} → {@link Long} (widened, matches client-v2's {@code readUnsignedIntLE()}). */
  UINT32 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readIntLE(in, scratch) & 0xFFFFFFFFL;
    }
  },

  /** {@code Int64} → {@link Long} — little-endian 8 bytes, signed. */
  INT64 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readLongLE(in, scratch);
    }
  },

  /**
   * {@code UInt64} → {@link BigInteger} (unsigned) — matches client-v2's {@code readBigIntegerLE(8,
   * true)}; a {@code long} cannot represent the full {@code UInt64} range.
   */
  UINT64 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readUnsignedBigIntegerLE(in, scratch, 8);
    }
  },

  /** {@code Float32} → {@link Float} — IEEE-754 bit pattern, little-endian 4 bytes. */
  FLOAT32 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return Float.intBitsToFloat(RowBinaryWireFormat.readIntLE(in, scratch));
    }
  },

  /** {@code Float64} → {@link Double} — IEEE-754 bit pattern, little-endian 8 bytes. */
  FLOAT64 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return Double.longBitsToDouble(RowBinaryWireFormat.readLongLE(in, scratch));
    }
  },

  /** {@code Bool} → {@link Boolean} — one byte, {@code 1} is {@code true}. */
  BOOL {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readRequiredByte(in) == 1;
    }
  },

  /** {@code String} → {@link String} — varint length + UTF-8 bytes. */
  STRING {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readString(in);
    }
  },

  /**
   * {@code Int128} → {@link BigInteger} (signed) — little-endian 16 bytes; matches client-v2's
   * {@code readBigIntegerLE(16, false)}.
   */
  INT128 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readSignedBigIntegerLE(in, scratch, 16);
    }
  },

  /**
   * {@code UInt128} → {@link BigInteger} (unsigned) — little-endian 16 bytes; matches client-v2's
   * {@code readBigIntegerLE(16, true)}.
   */
  UINT128 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readUnsignedBigIntegerLE(in, scratch, 16);
    }
  },

  /**
   * {@code Int256} → {@link BigInteger} (signed) — little-endian 32 bytes; matches client-v2's
   * {@code readBigIntegerLE(32, false)}.
   */
  INT256 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readSignedBigIntegerLE(in, scratch, 32);
    }
  },

  /**
   * {@code UInt256} → {@link BigInteger} (unsigned) — little-endian 32 bytes; matches client-v2's
   * {@code readBigIntegerLE(32, true)}.
   */
  UINT256 {
    @Override
    public Object decode(final InputStream in, final byte[] scratch) throws IOException {
      return RowBinaryWireFormat.readUnsignedBigIntegerLE(in, scratch, 32);
    }
  }
}

/**
 * {@code Decimal(P, S)}/{@code Decimal32(S)}/{@code Decimal64(S)}/{@code Decimal128(S)}/{@code
 * Decimal256(S)} → {@link BigDecimal}. {@code byteWidth} (4/8/16/32) is resolved once by {@link
 * NativeColumnTypeResolver} from the column's precision (generic {@code Decimal(P,S)}) or its named
 * width ({@code Decimal32}/{@code 64}/{@code 128}/{@code 256}), matching client-v2's {@code
 * BinaryStreamReader.readDecimal} thresholds exactly — a {@code Decimal(P,S)} is stored as a plain
 * little-endian integer of that width representing {@code unscaledValue}; {@code scale}
 * reconstructs the decimal point.
 */
record DecimalColumnDecoder(int byteWidth, int scale) implements ColumnDecoder {

  @Override
  public Object decode(final InputStream in, final byte[] scratch) throws IOException {
    return switch (byteWidth) {
      case 4 -> BigDecimal.valueOf(RowBinaryWireFormat.readIntLE(in, scratch), scale);
      case 8 -> BigDecimal.valueOf(RowBinaryWireFormat.readLongLE(in, scratch), scale);
      default ->
          new BigDecimal(RowBinaryWireFormat.readSignedBigIntegerLE(in, scratch, byteWidth), scale);
    };
  }
}

/**
 * {@code FixedString(n)} → {@link String} — exactly {@code length} bytes, UTF-8 decoded (matches
 * client-v2's non-{@code binaryStringSupport} {@code FixedString} case; this driver never enables
 * that opt-in feature flag).
 */
record FixedStringColumnDecoder(int length) implements ColumnDecoder {

  @Override
  public Object decode(final InputStream in, final byte[] scratch) throws IOException {
    final byte[] bytes = new byte[length];
    RowBinaryWireFormat.readFully(in, bytes);
    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  }
}

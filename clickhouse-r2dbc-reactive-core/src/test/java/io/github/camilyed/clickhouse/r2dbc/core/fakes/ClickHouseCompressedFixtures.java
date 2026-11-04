package io.github.camilyed.clickhouse.r2dbc.core.fakes;

import com.clickhouse.data.ClickHouseCityHash;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.UncheckedIOException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * Builds real ClickHouse-wire-format LZ4- and ZSTD-compressed byte fixtures for {@code core}'s own
 * decompression tests, using the same compression and checksum primitives as the production
 * decoder.
 *
 * <p>Wire format (one block): 16 bytes CityHash128 checksum, 1 byte magic ({@code 0x82}), 4 bytes
 * little-endian {@code compressedSizeWithHeader} (9 + compressed data length), 4 bytes
 * little-endian {@code uncompressedSize}, then data compressed with the method named by the magic
 * byte. See {@code ClickHouseCompressedInputStream}'s Javadoc for the decode side.
 */
public final class ClickHouseCompressedFixtures {

  private static final byte LZ4_MAGIC = (byte) 0x82;
  private static final byte ZSTD_MAGIC = (byte) 0x90;
  private static final int BLOCK_HEADER_LENGTH = 9;

  private ClickHouseCompressedFixtures() {}

  /** {@code uncompressed} framed as a single ClickHouse LZ4 block. */
  public static byte[] compressAsSingleBlock(final byte[] uncompressed) {
    final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    final byte[] compressed = compressor.compress(uncompressed, 0, uncompressed.length);
    return frameBlock(LZ4_MAGIC, compressed, uncompressed.length);
  }

  /** {@code uncompressed} framed as a single ClickHouse ZSTD block. */
  public static byte[] compressAsSingleZstdBlock(final byte[] uncompressed) {
    return frameBlock(ZSTD_MAGIC, Zstd.compress(uncompressed), uncompressed.length);
  }

  private static byte[] frameBlock(
      final byte magic, final byte[] compressed, final int uncompressedSize) {
    final int compressedSizeWithHeader = BLOCK_HEADER_LENGTH + compressed.length;
    final byte[] block = new byte[compressedSizeWithHeader];
    block[0] = magic;
    writeInt32LE(block, 1, compressedSizeWithHeader);
    writeInt32LE(block, 5, uncompressedSize);
    System.arraycopy(compressed, 0, block, BLOCK_HEADER_LENGTH, compressed.length);
    final long[] checksum = ClickHouseCityHash.cityHash128(block, 0, compressedSizeWithHeader);
    final byte[] wire = new byte[16 + compressedSizeWithHeader];
    writeInt64LE(wire, 0, checksum[0]);
    writeInt64LE(wire, 8, checksum[1]);
    System.arraycopy(block, 0, wire, 16, compressedSizeWithHeader);
    return wire;
  }

  /**
   * {@code uncompressed} split into {@code blockSize}-byte chunks, each framed as its own
   * ClickHouse LZ4 block and concatenated — exercising a decoder's ability to read a response body
   * spanning more than one block, the normal shape of a real, non-trivial ClickHouse response.
   */
  public static byte[] compressAsBlocksOf(final byte[] uncompressed, final int blockSize) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      int offset = 0;
      while (offset < uncompressed.length) {
        final int length = Math.min(blockSize, uncompressed.length - offset);
        final byte[] chunk = new byte[length];
        System.arraycopy(uncompressed, offset, chunk, 0, length);
        out.write(compressAsSingleBlock(chunk));
        offset += length;
      }
    } catch (final java.io.IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  private static void writeInt32LE(final byte[] buffer, final int offset, final int value) {
    buffer[offset] = (byte) value;
    buffer[offset + 1] = (byte) (value >>> 8);
    buffer[offset + 2] = (byte) (value >>> 16);
    buffer[offset + 3] = (byte) (value >>> 24);
  }

  private static void writeInt64LE(final byte[] buffer, final int offset, final long value) {
    for (int i = 0; i < 8; i++) {
      buffer[offset + i] = (byte) (value >>> (8 * i));
    }
  }
}

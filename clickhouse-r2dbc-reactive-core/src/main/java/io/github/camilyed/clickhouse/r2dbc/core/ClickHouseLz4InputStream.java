package io.github.camilyed.clickhouse.r2dbc.core;

import com.clickhouse.data.ClickHouseCityHash;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Decompresses a ClickHouse HTTP response body sent with {@code compress=1} — ClickHouse's own
 * custom LZ4 block framing, not standard HTTP {@code Content-Encoding}. Wraps a raw, still-framed
 * {@code source} and presents the decompressed bytes to whatever reads from this stream instead.
 *
 * <p>Wire format, verified byte-for-byte against client-v2 0.9.8's own {@code
 * ClickHouseLZ4InputStream}/{@code ClickHouseLZ4OutputStream} (encode and decode sides
 * cross-checked against real source, not assumed): the body is one or more <em>blocks</em>
 * concatenated back to back, each shaped as:
 *
 * <ul>
 *   <li>bytes 0-15: a CityHash128 checksum of everything from byte 16 onward in this block (two
 *       little-endian {@code int64}s, low half then high half)
 *   <li>byte 16: a magic byte, always {@code 0x82}
 *   <li>bytes 17-20: {@code compressedSizeWithHeader} (little-endian {@code int32}) — {@code 9}
 *       (this "block header": magic + this field + the next field) plus the length of the
 *       LZ4-compressed data that follows
 *   <li>bytes 21-24: {@code uncompressedSize} (little-endian {@code int32}) — the exact
 *       decompressed byte count, required upfront because {@link LZ4FastDecompressor} is
 *       block-based, not a self-delimiting streaming format
 *   <li>then {@code compressedSizeWithHeader - 9} bytes of LZ4-compressed data
 * </ul>
 *
 * <p>Reused rather than reimplemented: {@link ClickHouseCityHash} (checksum) and {@link LZ4Factory}
 * (block decompression) are already transitively resolvable via this project's existing {@code
 * client-v2} dependency (see {@code gradle/verification-metadata.xml}), and hand-porting either
 * algorithm from scratch, without a compiler in the loop to catch a transcription bug, would be far
 * riskier than depending on the same primitives client-v2 itself uses for this exact purpose.
 *
 * <p>Reads the whole next block into memory before serving any of its bytes — simple and correct,
 * at the cost of one extra byte-array allocation per block; each block is bounded (ClickHouse caps
 * how much it buffers before flushing one), so this never holds more than a few tens of kilobytes
 * at a time. See ROADMAP.md's deferred benchmark item if this ever needs to avoid that allocation.
 */
final class ClickHouseLz4InputStream extends InputStream {

  private static final byte MAGIC = (byte) 0x82;
  private static final int HEADER_LENGTH = 25;
  private static final int BLOCK_HEADER_LENGTH = 9;

  private final InputStream source;
  private final LZ4FastDecompressor decompressor;
  private final byte[] headerBuffer = new byte[HEADER_LENGTH];
  private final byte[] singleByteBuffer = new byte[1];

  private byte[] currentBlock = new byte[0];
  private int position;
  private int limit;

  ClickHouseLz4InputStream(final InputStream source) {
    this.source = source;
    this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
  }

  @Override
  public int read() throws IOException {
    final int n = read(singleByteBuffer, 0, 1);
    return n == -1 ? -1 : singleByteBuffer[0] & 0xFF;
  }

  @Override
  public int read(final byte[] destination, final int offset, final int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    int copied = 0;
    while (copied < length) {
      if (position >= limit && !refillBlock()) {
        return copied == 0 ? -1 : copied;
      }
      final int toCopy = Math.min(length - copied, limit - position);
      System.arraycopy(currentBlock, position, destination, offset + copied, toCopy);
      position += toCopy;
      copied += toCopy;
    }
    return copied;
  }

  @Override
  public void close() throws IOException {
    source.close();
  }

  /**
   * Reads and decompresses the next block from {@code source} into {@link #currentBlock}, or
   * returns {@code false} if {@code source} ended cleanly on a block boundary — the normal end of a
   * ClickHouse response body, not an error.
   */
  private boolean refillBlock() throws IOException {
    if (!readFully(headerBuffer, 0, HEADER_LENGTH)) {
      return false;
    }
    if (headerBuffer[16] != MAGIC) {
      throw new IOException("Invalid ClickHouse LZ4 block magic byte: " + headerBuffer[16]);
    }
    final int compressedSizeWithHeader = readInt32LE(headerBuffer, 17);
    final int uncompressedSize = readInt32LE(headerBuffer, 21);
    final int compressedDataLength = compressedSizeWithHeader - BLOCK_HEADER_LENGTH;
    final byte[] block = new byte[compressedSizeWithHeader];
    System.arraycopy(headerBuffer, 16, block, 0, BLOCK_HEADER_LENGTH);
    if (!readFully(block, BLOCK_HEADER_LENGTH, compressedDataLength)) {
      throw new EOFException(
          "Truncated ClickHouse LZ4 stream: expected " + compressedDataLength + " more bytes");
    }
    verifyChecksum(block, compressedSizeWithHeader);
    final byte[] decompressed = new byte[uncompressedSize];
    decompressor.decompress(block, BLOCK_HEADER_LENGTH, decompressed, 0, uncompressedSize);
    currentBlock = decompressed;
    position = 0;
    limit = uncompressedSize;
    return true;
  }

  private void verifyChecksum(final byte[] block, final int compressedSizeWithHeader)
      throws IOException {
    final long[] checksum = ClickHouseCityHash.cityHash128(block, 0, compressedSizeWithHeader);
    final long expectedLow = readInt64LE(headerBuffer, 0);
    final long expectedHigh = readInt64LE(headerBuffer, 8);
    if (checksum[0] != expectedLow || checksum[1] != expectedHigh) {
      throw new IOException("Corrupted ClickHouse LZ4 response: block checksum mismatch");
    }
  }

  /**
   * Reads exactly {@code length} bytes into {@code buffer} starting at {@code offset}, blocking
   * across as many underlying {@link InputStream#read(byte[], int, int)} calls as needed — a single
   * call is never guaranteed to return everything requested. Returns {@code false} only when {@code
   * source} was already at end-of-stream before any byte of this call was read (a clean block
   * boundary); a partial read followed by end-of-stream throws instead, since that shape can only
   * mean a truncated/corrupted response.
   */
  private boolean readFully(final byte[] buffer, final int offset, final int length)
      throws IOException {
    int read = 0;
    while (read < length) {
      final int n = source.read(buffer, offset + read, length - read);
      if (n < 0) {
        if (read == 0) {
          return false;
        }
        throw new EOFException(
            "Truncated ClickHouse LZ4 stream: read " + read + " of " + length + " expected bytes");
      }
      read += n;
    }
    return true;
  }

  private static int readInt32LE(final byte[] buffer, final int offset) {
    return (buffer[offset] & 0xFF)
        | ((buffer[offset + 1] & 0xFF) << 8)
        | ((buffer[offset + 2] & 0xFF) << 16)
        | ((buffer[offset + 3] & 0xFF) << 24);
  }

  private static long readInt64LE(final byte[] buffer, final int offset) {
    long value = 0;
    for (int i = 7; i >= 0; i--) {
      value = (value << 8) | (buffer[offset + i] & 0xFF);
    }
    return value;
  }
}

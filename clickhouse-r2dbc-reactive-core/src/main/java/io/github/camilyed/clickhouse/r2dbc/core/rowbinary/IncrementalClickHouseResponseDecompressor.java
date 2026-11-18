package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import com.clickhouse.data.ClickHouseCityHash;
import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import java.util.Optional;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import reactor.core.publisher.Flux;

final class IncrementalClickHouseResponseDecompressor {

  private IncrementalClickHouseResponseDecompressor() {}

  static Flux<ByteBuffer> decompress(final Flux<ByteBuffer> source) {
    return Flux.defer(
        () -> {
          final BlockParser parser = new BlockParser();
          return source.concatMap(parser::decompress, 1).concatWith(Flux.defer(parser::finish));
        });
  }

  private enum Stage {
    HEADER,
    COMPRESSED_DATA
  }

  private static final class BlockParser {

    private static final byte LZ4_MAGIC = (byte) 0x82;
    private static final byte ZSTD_MAGIC = (byte) 0x90;
    private static final int CHECKSUM_LENGTH = 16;
    private static final int BLOCK_HEADER_LENGTH = 9;
    private static final int WIRE_HEADER_LENGTH = CHECKSUM_LENGTH + BLOCK_HEADER_LENGTH;
    private static final int MAX_BLOCK_BYTES = 64 * 1024 * 1024;

    private final LZ4FastDecompressor lz4 = LZ4Factory.fastestInstance().fastDecompressor();
    private final byte[] header = new byte[WIRE_HEADER_LENGTH];

    private Stage stage = Stage.HEADER;
    private int headerOffset;
    private byte[] block = new byte[0];
    private int blockOffset;
    private int uncompressedSize;

    private Flux<ByteBuffer> decompress(final ByteBuffer chunk) {
      return Flux.generate(
          sink -> {
            final Optional<ByteBuffer> decompressed = decompressNext(chunk);
            if (decompressed.isPresent()) {
              sink.next(decompressed.get());
            } else {
              sink.complete();
            }
          });
    }

    private Optional<ByteBuffer> decompressNext(final ByteBuffer source) {
      while (source.hasRemaining()) {
        switch (stage) {
          case HEADER -> readHeader(source);
          case COMPRESSED_DATA -> {
            final Optional<ByteBuffer> decompressed = readCompressedData(source);
            if (decompressed.isPresent()) {
              return decompressed;
            }
          }
        }
      }
      return Optional.empty();
    }

    private void readHeader(final ByteBuffer source) {
      final int copied = Math.min(source.remaining(), header.length - headerOffset);
      source.get(header, headerOffset, copied);
      headerOffset += copied;
      if (headerOffset != header.length) {
        return;
      }
      final byte compressionMethod = header[CHECKSUM_LENGTH];
      if (compressionMethod != LZ4_MAGIC && compressionMethod != ZSTD_MAGIC) {
        throw new IllegalArgumentException(
            "Unsupported ClickHouse compression method byte: " + compressionMethod);
      }
      final int compressedSizeWithHeader = readInt32LE(header, CHECKSUM_LENGTH + 1);
      uncompressedSize = readInt32LE(header, CHECKSUM_LENGTH + 5);
      validateBlockSizes(compressedSizeWithHeader, uncompressedSize);
      block = new byte[compressedSizeWithHeader];
      System.arraycopy(header, CHECKSUM_LENGTH, block, 0, BLOCK_HEADER_LENGTH);
      blockOffset = BLOCK_HEADER_LENGTH;
      stage = Stage.COMPRESSED_DATA;
    }

    private Optional<ByteBuffer> readCompressedData(final ByteBuffer source) {
      final int copied = Math.min(source.remaining(), block.length - blockOffset);
      source.get(block, blockOffset, copied);
      blockOffset += copied;
      if (blockOffset != block.length) {
        return Optional.empty();
      }
      verifyChecksum();
      final byte[] decompressed = new byte[uncompressedSize];
      final int compressedDataLength = block.length - BLOCK_HEADER_LENGTH;
      if (block[0] == LZ4_MAGIC) {
        lz4.decompress(block, BLOCK_HEADER_LENGTH, decompressed, 0, uncompressedSize);
      } else {
        final long result =
            Zstd.decompressByteArray(
                decompressed,
                0,
                uncompressedSize,
                block,
                BLOCK_HEADER_LENGTH,
                compressedDataLength);
        if (Zstd.isError(result) || result != uncompressedSize) {
          throw new IllegalArgumentException(
              "Invalid ClickHouse ZSTD block: " + Zstd.getErrorName(result));
        }
      }
      resetForNextBlock();
      return Optional.of(ByteBuffer.wrap(decompressed));
    }

    private void verifyChecksum() {
      final long[] actual = ClickHouseCityHash.cityHash128(block, 0, block.length);
      final long expectedLow = readInt64LE(header, 0);
      final long expectedHigh = readInt64LE(header, Long.BYTES);
      if (actual[0] != expectedLow || actual[1] != expectedHigh) {
        throw new IllegalArgumentException(
            "Corrupted ClickHouse compressed response: block checksum mismatch");
      }
    }

    private void resetForNextBlock() {
      stage = Stage.HEADER;
      headerOffset = 0;
      block = new byte[0];
      blockOffset = 0;
      uncompressedSize = 0;
    }

    private Flux<ByteBuffer> finish() {
      if (stage == Stage.HEADER && headerOffset == 0) {
        return Flux.empty();
      }
      return Flux.error(
          new IllegalArgumentException("Truncated ClickHouse compressed response block"));
    }

    private static void validateBlockSizes(
        final int compressedSizeWithHeader, final int uncompressedSize) {
      if (compressedSizeWithHeader < BLOCK_HEADER_LENGTH
          || compressedSizeWithHeader > MAX_BLOCK_BYTES) {
        throw new IllegalArgumentException(
            "Invalid ClickHouse compressed block size: " + compressedSizeWithHeader);
      }
      if (uncompressedSize < 0 || uncompressedSize > MAX_BLOCK_BYTES) {
        throw new IllegalArgumentException(
            "Invalid ClickHouse uncompressed block size: " + uncompressedSize);
      }
    }

    private static int readInt32LE(final byte[] buffer, final int offset) {
      return (buffer[offset] & 0xFF)
          | ((buffer[offset + 1] & 0xFF) << 8)
          | ((buffer[offset + 2] & 0xFF) << 16)
          | ((buffer[offset + 3] & 0xFF) << 24);
    }

    private static long readInt64LE(final byte[] buffer, final int offset) {
      long value = 0;
      for (int index = Long.BYTES - 1; index >= 0; index--) {
        value = (value << 8) | (buffer[offset + index] & 0xFFL);
      }
      return value;
    }
  }
}

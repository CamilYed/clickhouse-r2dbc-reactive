package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.core.fakes.ClickHouseCompressedFixtures;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ClickHouseCompressedInputStreamTest {

  @Test
  void shouldDecompressASingleBlock() throws IOException {
    // given
    final byte[] original = "hello, clickhouse".getBytes(StandardCharsets.UTF_8);
    final byte[] wire = ClickHouseCompressedFixtures.compressAsSingleBlock(original);

    // when
    final byte[] decompressed;
    try (InputStream stream = new ClickHouseCompressedInputStream(new ByteArrayInputStream(wire))) {
      decompressed = stream.readAllBytes();
    }

    // then
    assertThat(decompressed).isEqualTo(original);
  }

  @Test
  void shouldDecompressAZstdBlockProducedByNewClickHouseServers() throws IOException {
    // given
    final byte[] original = "hello from clickhouse 26.9".getBytes(StandardCharsets.UTF_8);
    final byte[] wire = ClickHouseCompressedFixtures.compressAsSingleZstdBlock(original);

    // when
    final byte[] decompressed;
    try (InputStream stream = new ClickHouseCompressedInputStream(new ByteArrayInputStream(wire))) {
      decompressed = stream.readAllBytes();
    }

    // then
    assertThat(decompressed).isEqualTo(original);
  }

  @Test
  void shouldDecompressAnEmptyBlock() throws IOException {
    // given
    final byte[] wire = ClickHouseCompressedFixtures.compressAsSingleBlock(new byte[0]);

    // when
    final byte[] decompressed;
    try (InputStream stream = new ClickHouseCompressedInputStream(new ByteArrayInputStream(wire))) {
      decompressed = stream.readAllBytes();
    }

    // then
    assertThat(decompressed).isEmpty();
  }

  @Test
  void shouldDecompressMultipleBlocksConcatenatedInOneStream() throws IOException {
    // given
    final byte[] original = "x".repeat(200_000).getBytes(StandardCharsets.UTF_8);
    final byte[] wire = ClickHouseCompressedFixtures.compressAsBlocksOf(original, 64 * 1024);

    // when
    final byte[] decompressed;
    try (InputStream stream = new ClickHouseCompressedInputStream(new ByteArrayInputStream(wire))) {
      decompressed = stream.readAllBytes();
    }

    // then
    assertThat(decompressed).isEqualTo(original);
  }

  @Test
  void shouldReadOneByteAtATimeViaTheSingleByteReadOverload() throws IOException {
    // given
    final byte[] wire = ClickHouseCompressedFixtures.compressAsSingleBlock(new byte[] {'h', 'i'});

    // when
    final int first;
    final int second;
    final int end;
    try (InputStream stream = new ClickHouseCompressedInputStream(new ByteArrayInputStream(wire))) {
      first = stream.read();
      second = stream.read();
      end = stream.read();
    }

    // then
    assertThat(first).isEqualTo('h');
    assertThat(second).isEqualTo('i');
    assertThat(end).isEqualTo(-1);
  }

  @Test
  void shouldRejectAnUnsupportedCompressionMethod() {
    // given
    final byte[] garbage = new byte[25];
    Arrays.fill(garbage, (byte) 0x00);

    // when
    final Throwable thrown =
        catchThrowable(
            () -> {
              try (InputStream stream =
                  new ClickHouseCompressedInputStream(new ByteArrayInputStream(garbage))) {
                stream.readAllBytes();
              }
            });

    // then
    assertThat(thrown).isInstanceOf(IOException.class).hasMessageContaining("compression method");
  }

  @Test
  void shouldRejectATruncatedBlock() {
    // given
    final byte[] wire =
        ClickHouseCompressedFixtures.compressAsSingleBlock(
            "hello".getBytes(StandardCharsets.UTF_8));
    final byte[] truncated = Arrays.copyOf(wire, wire.length - 1);

    // when
    final Throwable thrown =
        catchThrowable(
            () -> {
              try (InputStream stream =
                  new ClickHouseCompressedInputStream(new ByteArrayInputStream(truncated))) {
                stream.readAllBytes();
              }
            });

    // then
    assertThat(thrown).isInstanceOf(IOException.class);
  }

  @Test
  void shouldRejectACorruptedChecksum() {
    // given
    final byte[] wire =
        ClickHouseCompressedFixtures.compressAsSingleBlock(
            "hello".getBytes(StandardCharsets.UTF_8));
    wire[0] ^= 0x01; // flip a bit inside the checksum

    // when
    final Throwable thrown =
        catchThrowable(
            () -> {
              try (InputStream stream =
                  new ClickHouseCompressedInputStream(new ByteArrayInputStream(wire))) {
                stream.readAllBytes();
              }
            });

    // then
    assertThat(thrown).isInstanceOf(IOException.class).hasMessageContaining("checksum");
  }
}

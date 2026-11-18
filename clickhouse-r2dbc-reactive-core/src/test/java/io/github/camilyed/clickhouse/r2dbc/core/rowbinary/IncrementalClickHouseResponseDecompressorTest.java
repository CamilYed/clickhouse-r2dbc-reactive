package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.fakes.ClickHouseCompressedFixtures;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class IncrementalClickHouseResponseDecompressorTest {

  @Test
  void shouldDecompressAZstdBlockFragmentedAcrossEveryWireByte() {
    // given
    final byte[] original = "hello from clickhouse 26.9".getBytes(StandardCharsets.UTF_8);
    final byte[] compressed = ClickHouseCompressedFixtures.compressAsSingleZstdBlock(original);
    final Flux<ByteBuffer> source =
        Flux.range(0, compressed.length)
            .map(index -> ByteBuffer.wrap(new byte[] {compressed[index]}));

    // when / then
    StepVerifier.create(IncrementalClickHouseResponseDecompressor.decompress(source))
        .expectNext(ByteBuffer.wrap(original))
        .verifyComplete();
  }

  @Test
  void shouldKeepTheNextCompressedBlockUnreadWhenDownstreamCancels() {
    // given
    final AtomicBoolean sourceCancelled = new AtomicBoolean();
    final byte[] first =
        ClickHouseCompressedFixtures.compressAsSingleBlock(
            "first".getBytes(StandardCharsets.UTF_8));
    final byte[] second =
        ClickHouseCompressedFixtures.compressAsSingleBlock(
            "second".getBytes(StandardCharsets.UTF_8));
    final ByteBuffer body =
        ByteBuffer.allocate(first.length + second.length).put(first).put(second);
    body.flip();
    final Flux<ByteBuffer> source =
        Flux.just(body).concatWith(Flux.never()).doOnCancel(() -> sourceCancelled.set(true));

    // when
    StepVerifier.create(IncrementalClickHouseResponseDecompressor.decompress(source), 1)
        .expectNext(ByteBuffer.wrap("first".getBytes(StandardCharsets.UTF_8)))
        .thenCancel()
        .verify(Duration.ofSeconds(5));

    // then
    assertThat(sourceCancelled).isTrue();
    // and
    assertThat(body.remaining()).isEqualTo(second.length);
  }
}

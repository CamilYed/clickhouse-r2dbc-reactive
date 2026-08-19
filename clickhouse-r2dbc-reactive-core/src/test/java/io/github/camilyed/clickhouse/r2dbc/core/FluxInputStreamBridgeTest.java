package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class FluxInputStreamBridgeTest {

  @Test
  void shouldReadAllBytesFromASingleChunk() throws IOException {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));

    // when
    final byte[] readBytes;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      readBytes = bridge.readAllBytes();
    }

    // then
    assertThat(readBytes).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void shouldReadBytesAcrossMultipleChunks() throws IOException {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(
            ByteBuffer.wrap("hel".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("lo".getBytes(StandardCharsets.UTF_8)));

    // when
    final byte[] readBytes;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      readBytes = bridge.readAllBytes();
    }

    // then
    assertThat(readBytes).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void shouldReadOneByteAtATimeViaTheSingleByteReadOverload() throws IOException {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap("hi".getBytes(StandardCharsets.UTF_8)));

    // when
    final int first;
    final int second;
    final int end;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      first = bridge.read();
      second = bridge.read();
      end = bridge.read();
    }

    // then
    assertThat(first).isEqualTo('h');
    assertThat(second).isEqualTo('i');
    assertThat(end).isEqualTo(-1);
  }

  @Test
  void shouldThrowWhenTheUpstreamFluxSignalsAnError() throws IOException {
    // given
    final RuntimeException upstreamError = new RuntimeException("boom");
    final Flux<ByteBuffer> source = Flux.error(upstreamError);

    // when
    final Throwable thrown;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      thrown = catchThrowable(bridge::readAllBytes);
    }

    // then
    assertThat(thrown).isInstanceOf(IOException.class).hasCause(upstreamError);
  }

  @Test
  void shouldOnlyRequestAsManyItemsAsTheConfiguredDemand() {
    // given
    final AtomicLong requested = new AtomicLong();
    final Flux<ByteBuffer> source =
        Flux.just(
                ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)))
            .doOnRequest(requested::addAndGet);

    // when
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 2)) {

      // then
      assertThat(requested.get()).isEqualTo(2L);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void shouldCancelTheUpstreamSubscriptionWhenClosed() throws IOException {
    // given
    final AtomicBoolean cancelled = new AtomicBoolean();
    final Flux<ByteBuffer> source = Flux.<ByteBuffer>never().doOnCancel(() -> cancelled.set(true));

    // when
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      // otwórz i zamknij, nic więcej
    }

    // then
    assertThat(cancelled.get()).isTrue();
  }

  @Test
  void shouldReturnZeroWhenReadingZeroLengthEvenAfterTheStreamHasEnded() throws IOException {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap("hi".getBytes(StandardCharsets.UTF_8)));
    final byte[] destination = new byte[0];

    // when
    final int bytesRead;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      bridge.readAllBytes();
      bytesRead = bridge.read(destination, 0, 0);
    }

    // then
    assertThat(bytesRead).isZero();
  }

  @Test
  void shouldReturnZeroForAZeroLengthReadWithoutWaitingForMoreData() {
    // given
    final Flux<ByteBuffer> source = Flux.never();
    final byte[] destination = new byte[0];

    // when / then
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
            assertThat(bridge.read(destination, 0, 0)).isZero();
          }
        });
  }
}

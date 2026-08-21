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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
  void shouldMergeMultipleAlreadyBufferedChunksIntoASingleRead() throws IOException {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(
            ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("d".getBytes(StandardCharsets.UTF_8)));
    final byte[] destination = new byte[4];

    // when
    final int bytesReadInOneCall;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      bytesReadInOneCall = bridge.read(destination, 0, 4);
    }

    // then
    assertThat(bytesReadInOneCall).isEqualTo(4);
    assertThat(destination).isEqualTo("abcd".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void shouldStillReadEachChunkSeparatelyWhenTheyArriveOneAtATime() throws IOException {
    // given
    final AtomicReference<FluxSink<ByteBuffer>> sinkRef = new AtomicReference<>();
    final Flux<ByteBuffer> source = Flux.create(sinkRef::set);

    // when
    final int first;
    final int second;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      sinkRef.get().next(ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)));
      first = bridge.read();
      sinkRef.get().next(ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)));
      sinkRef.get().complete();
      second = bridge.read();
    }

    // then
    assertThat(first).isEqualTo('a');
    assertThat(second).isEqualTo('b');
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
  void shouldReachNaturalCompletionInsteadOfCancellingWhenTheRestOfTheResponseHasAlreadyArrived()
      throws IOException {
    // given - mirrors Flux#next(): only the first chunk is ever read, but by the time close() runs
    // the rest of a small response (one more chunk, then completion) has already arrived and is
    // sitting in the queue - the realistic case for an already-received small/point-query response.
    final AtomicBoolean cancelled = new AtomicBoolean();
    final AtomicReference<FluxSink<ByteBuffer>> sinkRef = new AtomicReference<>();
    final Flux<ByteBuffer> source =
        Flux.<ByteBuffer>create(sinkRef::set).doOnCancel(() -> cancelled.set(true));

    // when
    final int firstByte;
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      sinkRef.get().next(ByteBuffer.wrap("f".getBytes(StandardCharsets.UTF_8)));
      firstByte = bridge.read();
      sinkRef.get().next(ByteBuffer.wrap("second".getBytes(StandardCharsets.UTF_8)));
      sinkRef.get().complete();
    }

    // then
    assertThat(firstByte).isEqualTo('f');
    // and - closing must not have sent a fresh cancel to the upstream: it already reached Complete
    // naturally before close() ever ran, so a transport underneath this source (e.g. Reactor
    // Netty's HTTP client) would see a normally-finished response, eligible for connection reuse.
    assertThat(cancelled.get()).isFalse();
  }

  @Test
  void shouldFallBackToCancellingWhenMoreThanTheDrainBudgetIsStillUnread() throws IOException {
    // given - one small chunk is read, then far more than the drain byte budget (64 KiB) arrives
    // without ever completing - standing in for a caller abandoning a large, still-streaming
    // response early (e.g. cancelling a big scan after the first few rows). The bounded drain must
    // give up quickly here rather than reading the whole remaining response just to avoid a cancel.
    final AtomicBoolean cancelled = new AtomicBoolean();
    final AtomicReference<FluxSink<ByteBuffer>> sinkRef = new AtomicReference<>();
    final Flux<ByteBuffer> source =
        Flux.<ByteBuffer>create(sinkRef::set).doOnCancel(() -> cancelled.set(true));

    // when
    try (InputStream bridge = FluxInputStreamBridge.subscribeTo(source, 4)) {
      sinkRef.get().next(ByteBuffer.wrap("f".getBytes(StandardCharsets.UTF_8)));
      assertThat(bridge.read()).isEqualTo('f');
      sinkRef.get().next(ByteBuffer.wrap(new byte[20_000]));
      sinkRef.get().next(ByteBuffer.wrap(new byte[20_000]));
      sinkRef.get().next(ByteBuffer.wrap(new byte[20_000]));
      sinkRef.get().next(ByteBuffer.wrap(new byte[20_000]));
      // deliberately never completed - a still-active, still-arriving response
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

package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

class RowBinaryDecoderTest {

  private final RowDecodingScheduler decodingScheduler = RowDecodingScheduler.defaults();

  @AfterEach
  void disposeDecodingScheduler() {
    decodingScheduler.dispose();
  }

  @Test
  void shouldDecodeSelectOneIntoOneRow() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

    // when
    final DecodedRow row =
        RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE)
            .blockFirst(Duration.ofSeconds(5));

    // then
    assertThat(row.valueAt(0)).isEqualTo((short) 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldDecodeAnArrayColumnAsAPlainList() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.arrayOfInt32RowBinaryWithNamesAndTypes()));

    // when
    final DecodedRow row =
        RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE)
            .blockFirst(Duration.ofSeconds(5));

    // then
    assertThat((List<Integer>) row.valueAt(0)).containsExactly(10, 20, 30);
  }

  @Test
  void shouldDecodeAnEnum8ColumnAsAPlainString() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.enum8SingleValueRowBinaryWithNamesAndTypes()));

    // when
    final DecodedRow row =
        RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE)
            .blockFirst(Duration.ofSeconds(5));

    // then
    assertThat(row.valueAt(0)).isInstanceOf(String.class).isEqualTo("b");
  }

  @Test
  void shouldDecodeAMultiColumnRowThroughTheNativeReader() {
    // given - Int32 and UInt64 both resolve natively, so this exercises NativeRowBinaryReader
    // rather than the client-v2-backed fallback
    final byte[] wireBytes =
        RowBinaryFixtures.rowBinaryWithNamesAndTypes(
            new String[] {"a", "b"},
            new String[] {"Int32", "UInt64"},
            new byte[] {
              0x2A,
              0x00,
              0x00,
              0x00, // Int32 42
              (byte) 0xE8,
              0x03,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00 // UInt64 1000
            });
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(wireBytes));

    // when - RowBinaryDecoderMode.NATIVE explicitly, since the default (CLICKHOUSE) never
    // constructs NativeRowBinaryReader at all
    final DecodedRow row =
        RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE, RowBinaryDecoderMode.NATIVE)
            .blockFirst(Duration.ofSeconds(5));

    // then
    assertThat(row.valueAt(0)).isEqualTo(42);
    // and
    assertThat(row.valueAt(1)).isEqualTo(BigInteger.valueOf(1000));
  }

  @Test
  void shouldFallBackToListDecodingWhenAnyColumnInTheRowIsUnsupported() {
    // given - Int32 resolves natively, but Array(Int32) does not: NativeColumnTypeResolver has no
    // fallback for a single unsupported column, so RowBinaryDecoder must route the WHOLE row
    // through ListDecodingRowBinaryReader, including the otherwise-native Int32 column
    final byte[] wireBytes =
        RowBinaryFixtures.rowBinaryWithNamesAndTypes(
            new String[] {"a", "arr"},
            new String[] {"Int32", "Array(Int32)"},
            new byte[] {
              0x07,
              0x00,
              0x00,
              0x00, // Int32 7
              0x02, // array length 2
              0x0A,
              0x00,
              0x00,
              0x00, // 10
              0x14,
              0x00,
              0x00,
              0x00 // 20
            });
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(wireBytes));

    // when - RowBinaryDecoderMode.NATIVE explicitly: the default (CLICKHOUSE) would route this
    // through ListDecodingRowBinaryReader unconditionally, which would make this test pass for
    // the wrong reason (never exercising NativeColumnTypeResolver's all-or-nothing decision)
    final DecodedRow row =
        RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE, RowBinaryDecoderMode.NATIVE)
            .blockFirst(Duration.ofSeconds(5));

    // then - the native-eligible column still decodes correctly via the fallback path
    assertThat(row.valueAt(0)).isEqualTo(7);
    // and
    @SuppressWarnings("unchecked")
    final List<Integer> arrayValue = (List<Integer>) row.valueAt(1);
    assertThat(arrayValue).containsExactly(10, 20);
  }

  @Test
  void shouldDecodeANullableNativeColumnAcrossMultipleRows() {
    // given - Nullable(Int32): first row null, second row value 5
    final byte[] wireBytes =
        RowBinaryFixtures.rowBinaryWithNamesAndTypes(
            new String[] {"n"},
            new String[] {"Nullable(Int32)"},
            new byte[] {0x01}, // null
            new byte[] {0x00, 0x05, 0x00, 0x00, 0x00} // not null, value 5
            );
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(wireBytes));

    // when - RowBinaryDecoderMode.NATIVE explicitly, to exercise NativeRowBinaryReader's own
    // Nullable handling rather than client-v2's
    final List<DecodedRow> rows =
        RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE, RowBinaryDecoderMode.NATIVE)
            .collectList()
            .block(Duration.ofSeconds(5));

    // then
    assertThat(rows).extracting(r -> r.valueAt(0)).containsExactly(null, 5);
  }

  @Test
  void shouldEmitNoRowsForAResponseWithNoHeaderAtAll() {
    // given - an empty body, e.g. a DDL statement's response, never sends the
    // RowBinaryWithNamesAndTypes header at all
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(new byte[0]));

    // when / then
    StepVerifier.create(RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE))
        .verifyComplete();
  }

  @Test
  void shouldExposeEmptyColumnsForAResponseWithNoHeaderAtAll() {
    // given
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(new byte[0]));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler, ResponseCompression.NONE)
            .block(Duration.ofSeconds(5));

    // then
    assertThat(result.columns()).isEmpty();
    // and
    StepVerifier.create(result.rows()).verifyComplete();
  }

  @Test
  void shouldEmitNoRowsForAResponseWithNoHeaderAtAllInNativeMode() {
    // given - same DDL/empty-body case as shouldEmitNoRowsForAResponseWithNoHeaderAtAll, but
    // routed through EmptyRowBinaryReader (RowBinaryDecoderMode.NATIVE) instead of
    // ListDecodingRowBinaryReader's own null-schema handling (RowBinaryDecoderMode.CLICKHOUSE) -
    // both must agree on the observable outcome
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(new byte[0]));

    // when / then
    StepVerifier.create(
            RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE, RowBinaryDecoderMode.NATIVE))
        .verifyComplete();
  }

  @Test
  void shouldExposeEmptyColumnsForAResponseWithNoHeaderAtAllInNativeMode() {
    // given
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(new byte[0]));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(
                source, decodingScheduler, ResponseCompression.NONE, RowBinaryDecoderMode.NATIVE)
            .block(Duration.ofSeconds(5));

    // then
    assertThat(result.columns()).isEmpty();
    // and
    StepVerifier.create(result.rows()).verifyComplete();
  }

  @Test
  void shouldExposeColumnSchemaAlongsideDecodedRows() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler, ResponseCompression.NONE)
            .block(Duration.ofSeconds(5));

    // then
    assertThat(result.columns()).containsExactly(new ColumnDescriptor("1", "UInt8"));
    // and
    assertThat(result.rows().blockFirst(Duration.ofSeconds(5)).valueAt(0)).isEqualTo((short) 1);
  }

  @Test
  void shouldReadEveryRowOnTheGivenSchedulerRegardlessOfWhichThreadRequestsIt() {
    // given - a source that, on every request for its next element, records which thread made the
    // request; decode()'s row-generation must run on decodingScheduler's own worker even though
    // this test drives the whole chain from a thread decodingScheduler never created.
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler, ResponseCompression.NONE)
            .block(Duration.ofSeconds(5));
    final String rowThreadName =
        result
            .rows()
            .map(row -> Thread.currentThread().getName())
            .blockFirst(Duration.ofSeconds(5));

    // then
    assertThat(rowThreadName).startsWith("clickhouse-r2dbc-decoder");
  }

  @Test
  void shouldDecodeEveryRowInOrderThroughAVirtualThreadBackedSchedulerFromFragmentedChunks() {
    // given - the production decode path (RowBinaryDecoder.decode -> RowDecodingScheduler ->
    // FluxInputStreamBridge -> client-v2's reader) exercised through the virtual-thread scheduler
    // variant specifically, not just the platform-thread default every other test in this class
    // uses; the wire bytes arrive one byte at a time rather than as a single chunk, so the bridge
    // has to actually block/resume across many fragments, not just pass one buffer straight through
    final byte[] wireBytes = RowBinaryFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes();
    final Flux<ByteBuffer> fragmentedSource =
        Flux.range(0, wireBytes.length).map(i -> ByteBuffer.wrap(new byte[] {wireBytes[i]}));
    final RowDecodingScheduler virtualThreadScheduler = RowDecodingScheduler.virtualThreads(2);

    try {
      // when
      final DecodedResult result =
          RowBinaryDecoder.decode(
                  fragmentedSource, virtualThreadScheduler, ResponseCompression.NONE)
              .block(Duration.ofSeconds(5));
      final List<Short> rowValues =
          result
              .rows()
              .map(row -> (Short) row.valueAt(0))
              .collectList()
              .block(Duration.ofSeconds(5));

      // then
      assertThat(rowValues).containsExactly((short) 1, (short) 2);
    } finally {
      virtualThreadScheduler.dispose();
    }
  }

  @Test
  void shouldReadEveryRowOnAVirtualThreadWhenUsingTheVirtualThreadBackedScheduler() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));
    final RowDecodingScheduler virtualThreadScheduler = RowDecodingScheduler.virtualThreads(2);

    try {
      // when
      final DecodedResult result =
          RowBinaryDecoder.decode(source, virtualThreadScheduler, ResponseCompression.NONE)
              .block(Duration.ofSeconds(5));
      final String rowThreadName =
          result
              .rows()
              .map(row -> Thread.currentThread().getName())
              .blockFirst(Duration.ofSeconds(5));

      // then
      assertThat(rowThreadName).startsWith("clickhouse-r2dbc-decoder-vt-");
    } finally {
      virtualThreadScheduler.dispose();
    }
  }

  @Test
  void shouldCancelTheUnderlyingSourceWhenTheDecodedRowsAreCancelledBeforeCompletion() {
    // given - the source never completes on its own (a Flux.never() tail after both rows), so
    // cancelling downstream after the first row tests cancellation propagation through
    // RowBinaryDecoder's Flux.generate, not a race with the source's own natural completion. Two
    // rows, not one, because client-v2's reader reads one row ahead internally (see
    // RowBinaryFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes()'s Javadoc) - a single-row
    // fixture
    // would make even emitting row 1 block forever on that look-ahead. An explicit initial request
    // of
    // 1 (rather than StepVerifier's default unbounded demand) means Flux.generate's own request
    // loop
    // stops naturally after emitting exactly that one row and goes idle, rather than immediately
    // trying to generate a third row it would otherwise block forever reading - thenCancel() then
    // cancels a genuinely idle subscription, not a still-running generation loop.
    final AtomicBoolean sourceCancelled = new AtomicBoolean();
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes()))
            .concatWith(Flux.never())
            .doOnCancel(() -> sourceCancelled.set(true));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler, ResponseCompression.NONE)
            .block(Duration.ofSeconds(5));
    StepVerifier.create(result.rows(), 1)
        .expectNextCount(1)
        .thenCancel()
        .verify(Duration.ofSeconds(5));

    // then - decode() runs the whole sequence via .subscribeOn(reactorScheduler), so thenCancel()
    // dispatches the cancel signal to that scheduler's worker thread and verify() returns as soon
    // as
    // the signal has been sent, not once the worker has actually finished processing it; asserting
    // immediately races that worker, so this awaits the eventually-true condition instead of
    // checking it once (see CLAUDE.md's "no Thread.sleep, use Awaitility" rule).
    await().atMost(Duration.ofSeconds(5)).untilTrue(sourceCancelled);
  }

  @Test
  void shouldCancelTheUnderlyingSourceWhenDecodeRowsIsCancelledBeforeCompletion() {
    // given - same shape and same explicit-initial-request-of-1 reasoning as the decode() test
    // above, but for the raw decodeRows() entry point benchmarks/tests use directly
    final AtomicBoolean sourceCancelled = new AtomicBoolean();
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes()))
            .concatWith(Flux.never())
            .doOnCancel(() -> sourceCancelled.set(true));

    // when
    StepVerifier.create(RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE), 1)
        .expectNextCount(1)
        .thenCancel()
        .verify(Duration.ofSeconds(5));

    // then
    assertThat(sourceCancelled.get()).isTrue();
  }

  @Test
  void shouldRouteACloseFailureToReactorsErrorDroppedHookInsteadOfSwallowingItSilently() {
    // given - a source whose Subscription#cancel() itself throws, standing in for the underlying
    // stream failing to close during cancellation. RowBinaryDecoder#closeReader (Flux.generate's
    // disposal hook) wraps such a failure and, per its Javadoc, relies on FluxGenerate's own
    // cleanup() to route it to Reactor's Operators.onErrorDropped rather than propagating it back
    // to the cancelling subscriber - a temporary global Hooks.onErrorDropped is therefore the only
    // black-box way to observe that this path is actually reached, not silently discarded.
    final List<Throwable> droppedErrors = new CopyOnWriteArrayList<>();
    Hooks.onErrorDropped(droppedErrors::add);
    final AtomicBoolean emitted = new AtomicBoolean();
    final Flux<ByteBuffer> source =
        Flux.from(
            (Publisher<ByteBuffer>)
                downstream ->
                    downstream.onSubscribe(new FailingCancelSubscription(downstream, emitted)));

    try {
      // when
      StepVerifier.create(RowBinaryDecoder.decodeRows(source, ResponseCompression.NONE), 1)
          .expectNextCount(1)
          .thenCancel()
          .verify(Duration.ofSeconds(5));

      // then
      assertThat(droppedErrors).hasSize(1);
      // and
      assertThat(droppedErrors.get(0))
          .hasMessageContaining("Failed to close the row decoder's underlying stream")
          .cause()
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("cancel failed");
    } finally {
      Hooks.resetOnErrorDropped();
    }
  }

  /**
   * A {@link Subscription} that emits one two-row chunk on its first {@link #request(long)} and
   * never completes on its own, but throws on {@link #cancel()} - simulating the underlying stream
   * failing to close, for {@link
   * #shouldRouteACloseFailureToReactorsErrorDroppedHookInsteadOfSwallowingItSilently()}.
   */
  private static final class FailingCancelSubscription implements Subscription {

    private final Subscriber<? super ByteBuffer> downstream;
    private final AtomicBoolean emitted;

    private FailingCancelSubscription(
        final Subscriber<? super ByteBuffer> downstream, final AtomicBoolean emitted) {
      this.downstream = downstream;
      this.emitted = emitted;
    }

    @Override
    public void request(final long n) {
      if (emitted.compareAndSet(false, true)) {
        downstream.onNext(
            ByteBuffer.wrap(RowBinaryFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes()));
      }
    }

    @Override
    public void cancel() {
      throw new IllegalStateException("cancel failed");
    }
  }
}

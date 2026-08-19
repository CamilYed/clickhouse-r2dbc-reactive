package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
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
    final DecodedRow row = RowBinaryDecoder.decodeRows(source).blockFirst(Duration.ofSeconds(5));

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
    final DecodedRow row = RowBinaryDecoder.decodeRows(source).blockFirst(Duration.ofSeconds(5));

    // then
    assertThat((List<Integer>) row.valueAt(0)).containsExactly(10, 20, 30);
  }

  @Test
  void shouldExposeColumnSchemaAlongsideDecodedRows() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler).block(Duration.ofSeconds(5));

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
        RowBinaryDecoder.decode(source, decodingScheduler).block(Duration.ofSeconds(5));
    final String rowThreadName =
        result
            .rows()
            .map(row -> Thread.currentThread().getName())
            .blockFirst(Duration.ofSeconds(5));

    // then
    assertThat(rowThreadName).startsWith("clickhouse-r2dbc-decoder");
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
    // of 1 (rather than StepVerifier's default unbounded demand) means Flux.generate's own request
    // loop stops naturally after emitting exactly that one row and goes idle, rather than
    // immediately trying to generate a third row it would otherwise block forever reading -
    // thenCancel() then cancels a genuinely idle subscription, not a still-running generation loop.
    final AtomicBoolean sourceCancelled = new AtomicBoolean();
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes()))
            .concatWith(Flux.never())
            .doOnCancel(() -> sourceCancelled.set(true));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler).block(Duration.ofSeconds(5));
    StepVerifier.create(result.rows(), 1)
        .expectNextCount(1)
        .thenCancel()
        .verify(Duration.ofSeconds(5));

    // then
    assertThat(sourceCancelled.get()).isTrue();
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
    StepVerifier.create(RowBinaryDecoder.decodeRows(source), 1)
        .expectNextCount(1)
        .thenCancel()
        .verify(Duration.ofSeconds(5));

    // then
    assertThat(sourceCancelled.get()).isTrue();
  }
}

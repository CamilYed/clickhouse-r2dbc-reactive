package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class IncrementalRowBinaryDecoderTest {

  @Test
  void shouldDecodeSelectOneWhenEveryWireByteArrivesInItsOwnChunk() {
    // given
    final byte[] wireBytes = RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes();
    final Flux<ByteBuffer> source =
        Flux.range(0, wireBytes.length)
            .map(index -> ByteBuffer.wrap(new byte[] {wireBytes[index]}));

    // when
    final List<RowBinaryDecodeSignal> signals =
        IncrementalRowBinaryDecoder.decode(source).collectList().block(Duration.ofSeconds(5));

    // then
    assertThat(signals)
        .satisfiesExactly(
            signal ->
                assertThat(signal)
                    .isEqualTo(
                        new RowBinaryDecodeSignal.Schema(
                            List.of(new ColumnDescriptor("1", "UInt8")))),
            signal -> {
              assertThat(signal).isInstanceOf(RowBinaryDecodeSignal.Row.class);
              final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signal;
              assertThat(row.value().valueAt(0)).isEqualTo((short) 1);
            });
  }

  @Test
  void shouldStopParsingTheCurrentChunkAndCancelTheSourceWhenDemandIsCancelled() {
    // given
    final AtomicBoolean sourceCancelled = new AtomicBoolean();
    final ByteBuffer body =
        ByteBuffer.wrap(RowBinaryFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes());
    final Flux<ByteBuffer> source =
        Flux.just(body).concatWith(Flux.never()).doOnCancel(() -> sourceCancelled.set(true));

    // when
    StepVerifier.create(IncrementalRowBinaryDecoder.decode(source), 2)
        .assertNext(
            signal ->
                assertThat(signal)
                    .isEqualTo(
                        new RowBinaryDecodeSignal.Schema(
                            List.of(new ColumnDescriptor("1", "UInt8")))))
        .assertNext(
            signal -> {
              assertThat(signal).isInstanceOf(RowBinaryDecodeSignal.Row.class);
              final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signal;
              assertThat(row.value().valueAt(0)).isEqualTo((short) 1);
            })
        .thenCancel()
        .verify(Duration.ofSeconds(5));

    // then
    assertThat(sourceCancelled).isTrue();
    // and - the second row's byte remains unread in the current chunk
    assertThat(body.remaining()).isEqualTo(1);
  }
}

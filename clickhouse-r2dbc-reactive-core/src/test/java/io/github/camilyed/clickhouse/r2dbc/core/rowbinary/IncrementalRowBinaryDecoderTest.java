package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.fakes.ClickHouseCompressedFixtures;
import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.math.BigDecimal;
import java.math.BigInteger;
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

  @Test
  void shouldDecodeAFragmentedPointQueryRowWithoutAnInputStream() {
    // given
    final byte[] wireBytes = RowBinaryFixtures.stringAndDecimalRowBinaryWithNamesAndTypes();
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
                            List.of(
                                new ColumnDescriptor("label", "String"),
                                new ColumnDescriptor("amount", "Decimal(18, 4)")))),
            signal -> {
              assertThat(signal).isInstanceOf(RowBinaryDecodeSignal.Row.class);
              final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signal;
              assertThat(row.value().valueAt(0)).isEqualTo("label-41");
              assertThat(row.value().valueAt(1)).isEqualTo(new BigDecimal("42.1234"));
            });
  }

  @Test
  void shouldDecodeUnsigned64BitValuesWithoutNarrowingThemToSignedLong() {
    // given
    final byte[] wireBytes = RowBinaryFixtures.uint64StringAndDecimalRowBinaryWithNamesAndTypes();
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
                            List.of(
                                new ColumnDescriptor("id", "UInt64"),
                                new ColumnDescriptor("label", "String"),
                                new ColumnDescriptor("amount", "Decimal(18, 4)")))),
            signal -> {
              assertThat(signal).isInstanceOf(RowBinaryDecodeSignal.Row.class);
              final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signal;
              assertThat(row.value().valueAt(0)).isEqualTo(new BigInteger("18446744073709551615"));
              assertThat(row.value().valueAt(1)).isEqualTo("label-max");
              assertThat(row.value().valueAt(2)).isEqualTo(new BigDecimal("-12.3456"));
            });
  }

  @Test
  void shouldRejectAResponseTruncatedInsideItsFirstColumnValue() {
    // given
    final byte[] wireBytes =
        RowBinaryFixtures.rowBinaryWithNamesAndTypes(
            new String[] {"label"}, new String[] {"String"}, new byte[] {0x03, 'a'});

    // when / then
    StepVerifier.create(IncrementalRowBinaryDecoder.decode(Flux.just(ByteBuffer.wrap(wireBytes))))
        .expectNext(
            new RowBinaryDecodeSignal.Schema(List.of(new ColumnDescriptor("label", "String"))))
        .expectErrorMessage("Truncated RowBinaryWithNamesAndTypes response")
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void shouldDecodeACompressedPointQueryResponseFragmentedAcrossEveryWireByte() {
    // given
    final byte[] compressed =
        ClickHouseCompressedFixtures.compressAsSingleBlock(
            RowBinaryFixtures.stringAndDecimalRowBinaryWithNamesAndTypes());
    final Flux<ByteBuffer> source =
        Flux.range(0, compressed.length)
            .map(index -> ByteBuffer.wrap(new byte[] {compressed[index]}));

    // when
    final List<RowBinaryDecodeSignal> signals =
        IncrementalRowBinaryDecoder.decode(
                IncrementalClickHouseResponseDecompressor.decompress(source))
            .collectList()
            .block(Duration.ofSeconds(5));

    // then
    assertThat(signals)
        .satisfiesExactly(
            signal ->
                assertThat(signal)
                    .isEqualTo(
                        new RowBinaryDecodeSignal.Schema(
                            List.of(
                                new ColumnDescriptor("label", "String"),
                                new ColumnDescriptor("amount", "Decimal(18, 4)")))),
            signal -> {
              assertThat(signal).isInstanceOf(RowBinaryDecodeSignal.Row.class);
              final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signal;
              assertThat(row.value().valueAt(0)).isEqualTo("label-41");
              assertThat(row.value().valueAt(1)).isEqualTo(new BigDecimal("42.1234"));
            });
  }

  @Test
  void shouldDistinguishANullValueFromAValueThatNeedsMoreWireBytes() {
    // given
    final byte[] wireBytes =
        RowBinaryFixtures.rowBinaryWithNamesAndTypes(
            new String[] {"value"},
            new String[] {"Nullable(Int32)"},
            new byte[] {0x01},
            new byte[] {0x00, 0x07, 0x00, 0x00, 0x00});
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
                            List.of(new ColumnDescriptor("value", "Nullable(Int32)")))),
            signal -> {
              assertThat(signal).isInstanceOf(RowBinaryDecodeSignal.Row.class);
              final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signal;
              assertThat(row.value().valueAt(0)).isNull();
            },
            signal -> {
              assertThat(signal).isInstanceOf(RowBinaryDecodeSignal.Row.class);
              final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signal;
              assertThat(row.value().valueAt(0)).isEqualTo(7);
            });
  }
}

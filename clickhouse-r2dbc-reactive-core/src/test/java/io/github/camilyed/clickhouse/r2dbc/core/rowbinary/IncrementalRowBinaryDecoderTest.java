package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.fakes.ClickHouseCompressedFixtures;
import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  @ParameterizedTest(name = "{0}")
  @MethodSource("scalarValues")
  void shouldDecodeScalarValuesWithClientV2CompatibleJavaTypes(
      final String scenario, final String type, final byte[] rowBytes, final Object expected) {
    // given
    final byte[] wireBytes =
        RowBinaryFixtures.rowBinaryWithNamesAndTypes(
            new String[] {"value"}, new String[] {type}, rowBytes);
    final Flux<ByteBuffer> source =
        Flux.range(0, wireBytes.length)
            .map(index -> ByteBuffer.wrap(new byte[] {wireBytes[index]}));

    // when
    final List<RowBinaryDecodeSignal> signals =
        IncrementalRowBinaryDecoder.decode(source).collectList().block(Duration.ofSeconds(5));

    // then
    assertThat(signals).hasSize(2);
    final RowBinaryDecodeSignal.Row row = (RowBinaryDecodeSignal.Row) signals.get(1);
    assertThat(row.value().valueAt(0)).isEqualTo(expected);
  }

  private static Stream<Arguments> scalarValues() {
    return Stream.of(
        Arguments.of("Int8", "Int8", new byte[] {-1}, (byte) -1),
        Arguments.of("Int16", "Int16", new byte[] {-2, -1}, (short) -2),
        Arguments.of("UInt16", "UInt16", new byte[] {-1, -1}, 65_535),
        Arguments.of("Int32", "Int32", littleEndian(-3, Integer.BYTES), -3),
        Arguments.of("UInt32", "UInt32", new byte[] {-1, -1, -1, -1}, 4_294_967_295L),
        Arguments.of("Int64", "Int64", littleEndian(-4, Long.BYTES), -4L),
        Arguments.of("Float32", "Float32", littleEndian(Float.floatToIntBits(1.25F), 4), 1.25F),
        Arguments.of("Float64", "Float64", littleEndian(Double.doubleToLongBits(-2.5D), 8), -2.5D),
        Arguments.of("Bool", "Bool", new byte[] {1}, true),
        Arguments.of("Int128", "Int128", filled(16, (byte) -1), BigInteger.valueOf(-1)),
        Arguments.of(
            "UInt128",
            "UInt128",
            filled(16, (byte) -1),
            new BigInteger("340282366920938463463374607431768211455")),
        Arguments.of(
            "FixedString", "FixedString(4)", "test".getBytes(StandardCharsets.UTF_8), "test"),
        Arguments.of("Decimal32", "Decimal32(2)", littleEndian(1_234, 4), new BigDecimal("12.34")),
        Arguments.of(
            "Decimal128", "Decimal128(4)", littleEndian(123_456, 16), new BigDecimal("12.3456")));
  }

  private static byte[] littleEndian(final long value, final int width) {
    final byte[] bytes = new byte[width];
    final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    if (width >= Long.BYTES) {
      buffer.putLong(value);
    } else {
      buffer.putInt((int) value);
    }
    return bytes;
  }

  private static byte[] filled(final int size, final byte value) {
    final byte[] bytes = new byte[size];
    java.util.Arrays.fill(bytes, value);
    return bytes;
  }
}

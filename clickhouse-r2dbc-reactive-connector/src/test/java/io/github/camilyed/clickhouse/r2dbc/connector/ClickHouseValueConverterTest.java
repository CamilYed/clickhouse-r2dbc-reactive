package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ClickHouseValueConverterTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("successfulConversions")
  void shouldConvertBetweenSupportedTypes(
      final String description,
      final Object source,
      final Class<?> targetType,
      final Object expected) {
    // when
    final Object converted = ClickHouseValueConverter.convert(source, targetType);

    // then
    assertThat(converted).isEqualTo(expected);
  }

  private static Stream<Arguments> successfulConversions() {
    final ZonedDateTime zonedDateTime = ZonedDateTime.of(2024, 6, 15, 12, 30, 0, 0, ZoneOffset.UTC);
    return Stream.of(
        Arguments.of("Integer -> Long", 42, Long.class, 42L),
        Arguments.of("Integer -> Byte", 42, Byte.class, (byte) 42),
        Arguments.of("Short -> Integer", (short) 100, Integer.class, 100),
        Arguments.of("Long -> BigInteger", 100L, BigInteger.class, BigInteger.valueOf(100)),
        Arguments.of("BigInteger -> Long", BigInteger.valueOf(100), Long.class, 100L),
        Arguments.of("Float -> Double", 3.5f, Double.class, 3.5),
        Arguments.of("Double -> Float", 3.5, Float.class, 3.5f),
        Arguments.of("BigDecimal -> Double", new BigDecimal("12.50"), Double.class, 12.5),
        Arguments.of("BigDecimal -> Integer (exact)", new BigDecimal("100"), Integer.class, 100),
        Arguments.of("Integer -> BigDecimal", 100, BigDecimal.class, BigDecimal.valueOf(100)),
        Arguments.of("Byte -> BigDecimal", (byte) 5, BigDecimal.class, BigDecimal.valueOf(5)),
        Arguments.of(
            "ZonedDateTime -> LocalDateTime",
            zonedDateTime,
            LocalDateTime.class,
            zonedDateTime.toLocalDateTime()),
        Arguments.of(
            "ZonedDateTime -> Instant", zonedDateTime, Instant.class, zonedDateTime.toInstant()),
        Arguments.of(
            "ZonedDateTime -> OffsetDateTime",
            zonedDateTime,
            OffsetDateTime.class,
            zonedDateTime.toOffsetDateTime()));
  }

  @Test
  void shouldRejectAnOutOfRangeNarrowingConversion() {
    // given
    final int outOfByteRange = 300;

    // when / then
    assertThatThrownBy(() -> ClickHouseValueConverter.convert(outOfByteRange, Byte.class))
        .isInstanceOf(ClickHouseValueConversionException.class);
  }

  @Test
  void shouldRejectANonIntegralValueRequestedAsAnIntegerType() {
    // given
    final BigDecimal nonIntegral = new BigDecimal("1.5");

    // when / then
    assertThatThrownBy(() -> ClickHouseValueConverter.convert(nonIntegral, Integer.class))
        .isInstanceOf(ClickHouseValueConversionException.class);
  }

  @Test
  void shouldRejectALongThatOverflowsAnInteger() {
    // when / then
    assertThatThrownBy(() -> ClickHouseValueConverter.convert(Long.MAX_VALUE, Integer.class))
        .isInstanceOf(ClickHouseValueConversionException.class);
  }

  @Test
  void shouldRejectADoubleThatWouldOverflowToInfinityAsAFloat() {
    // given
    final double tooLargeForFloat = 1e300;

    // when / then
    assertThatThrownBy(() -> ClickHouseValueConverter.convert(tooLargeForFloat, Float.class))
        .isInstanceOf(ClickHouseValueConversionException.class);
  }

  @Test
  void shouldReturnAnAlreadyMatchingValueUnchanged() {
    // given
    final String value = "hello";

    // when
    final String converted = ClickHouseValueConverter.convert(value, String.class);

    // then
    assertThat(converted).isSameAs(value);
  }

  @Test
  void shouldReturnNullUnchangedRegardlessOfRequestedType() {
    // when
    final Integer converted = ClickHouseValueConverter.convert(null, Integer.class);

    // then
    assertThat(converted).isNull();
  }

  @Test
  void shouldRejectAConversionWithNoDefinedPath() {
    // given
    final String value = "not a number";

    // when / then
    assertThatThrownBy(() -> ClickHouseValueConverter.convert(value, Integer.class))
        .isInstanceOf(ClickHouseValueConversionException.class);
  }

  @Test
  void shouldRejectConvertingABooleanToANumericType() {
    // given
    // Bool decodes as Boolean; deliberately not part of the numeric matrix.
    final boolean value = true;

    // when / then
    assertThatThrownBy(() -> ClickHouseValueConverter.convert(value, Integer.class))
        .isInstanceOf(ClickHouseValueConversionException.class);
  }
}

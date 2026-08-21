package io.github.camilyed.clickhouse.r2dbc.connector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Converts a value {@code core}'s decoder already produced for a column to a caller-requested
 * {@code Class<T>}, for {@link ClickHouseRow#get(int, Class)}/{@link ClickHouseRow#get(String,
 * Class)}.
 *
 * <p>Deliberately not "convert anything to anything" - two fixed matrices:
 *
 * <ul>
 *   <li>Numeric: {@link Byte}/{@link Short}/{@link Integer}/{@link Long}/{@link Float}/{@link
 *       Double}/{@link BigInteger}/{@link BigDecimal}, in any direction, via an exact {@link
 *       BigDecimal} intermediate - range-checked (no silent overflow: an out-of-range or
 *       non-integral request throws rather than truncating).
 *   <li>{@link ZonedDateTime}-derived: {@code DateTime}/{@code DateTime64} columns decode as {@link
 *       ZonedDateTime} (see {@code RowBinaryDecoder}); this class additionally allows reading one
 *       back as a {@link LocalDateTime}, {@link Instant}, or {@link OffsetDateTime} - each an
 *       unambiguous, lossless-for-the-requested-shape view of the same instant.
 * </ul>
 *
 * <p>Every other requested type that already matches the decoded value's own runtime type (e.g.
 * {@code String}, {@link java.util.UUID}, {@code LocalDate} for {@code Date}/{@code Date32}, {@link
 * Boolean}) needs no conversion at all - the identity fast path below returns it directly - so this
 * class only ever has to handle the two matrices above. Every conversion failure throws {@link
 * ClickHouseValueConversionException}.
 *
 * <p><b>{@code List} elements (from an {@code Array}/{@code Nested} column) are deliberately not
 * covered by the numeric matrix above</b> - {@code convert} only ever inspects the {@code List}
 * itself against the identity fast path, never its elements, so a caller asking for {@code
 * List.class} always gets back exactly the element types {@code core}'s {@code
 * ListDecodingRowBinaryReader}/client-v2's {@code convertArray()} produced, with no
 * widening/narrowing applied. That element type is not a guess: client-v2 decodes each array
 * element through the identical per-{@code ClickHouseDataType} reader a scalar column of that same
 * type uses (see {@code ListDecodingRowBinaryReader}'s Javadoc), so {@code Array(T)} always decodes
 * to a {@code List} of exactly the Java type a scalar column of type {@code T} would - e.g. {@code
 * Array(Int32)} to {@code List<Integer>}, {@code Array(UInt32)} to {@code List<Long>} (widened, the
 * same as scalar {@code UInt32}), {@code Array(String)} to {@code List<String>}. The full
 * scalar-type table this mirrors is verified against a real server in {@code
 * RealWorldTableAgainstRealClickHouseTest#shouldDecodeNumericTypes()} (transport-http module).
 * Requesting element-level conversion (e.g. reading an {@code Array(UInt32)} column as {@code
 * List<Integer>}) is not supported - it would need to iterate and convert every element on every
 * row, and no caller has needed it yet.
 */
final class ClickHouseValueConverter {

  private static final Set<Class<?>> NUMERIC_TARGET_TYPES =
      Set.of(
          Byte.class,
          Short.class,
          Integer.class,
          Long.class,
          Float.class,
          Double.class,
          BigInteger.class,
          BigDecimal.class);

  private ClickHouseValueConverter() {}

  static <T> @Nullable T convert(final @Nullable Object value, final Class<T> targetType) {
    if (value == null) {
      return null;
    }
    if (targetType.isInstance(value)) {
      return targetType.cast(value);
    }
    if (value instanceof final Number number && NUMERIC_TARGET_TYPES.contains(targetType)) {
      return targetType.cast(convertNumber(number, targetType));
    }
    if (value instanceof final ZonedDateTime zonedDateTime) {
      final Object converted = convertZonedDateTime(zonedDateTime, targetType);
      if (converted != null) {
        return targetType.cast(converted);
      }
    }
    throw new ClickHouseValueConversionException(value, targetType);
  }

  private static Object convertNumber(final Number value, final Class<?> targetType) {
    try {
      final BigDecimal exact = toExactBigDecimal(value);
      if (targetType == BigDecimal.class) {
        return exact;
      }
      if (targetType == BigInteger.class) {
        return exact.toBigIntegerExact();
      }
      if (targetType == Byte.class) {
        return exact.byteValueExact();
      }
      if (targetType == Short.class) {
        return exact.shortValueExact();
      }
      if (targetType == Integer.class) {
        return exact.intValueExact();
      }
      if (targetType == Long.class) {
        return exact.longValueExact();
      }
      if (targetType == Float.class) {
        return checkedFloat(exact, value, targetType);
      }
      return checkedDouble(exact, value, targetType);
    } catch (final ArithmeticException e) {
      throw new ClickHouseValueConversionException(value, targetType, e);
    }
  }

  // BigDecimal.valueOf(double) - not `new BigDecimal(double)` - deliberately: it goes through
  // Double.toString(double) first, giving the canonical decimal a human/ClickHouse would expect
  // for that value, rather than the exact (and often huge, noisy) binary-floating-point expansion
  // `new BigDecimal(double)` would produce for values like 0.1.
  private static BigDecimal toExactBigDecimal(final Number value) {
    if (value instanceof final BigDecimal bigDecimal) {
      return bigDecimal;
    }
    if (value instanceof final BigInteger bigInteger) {
      return new BigDecimal(bigInteger);
    }
    if (value instanceof Float || value instanceof Double) {
      return BigDecimal.valueOf(value.doubleValue());
    }
    return BigDecimal.valueOf(value.longValue());
  }

  private static Float checkedFloat(
      final BigDecimal exact, final Object originalValue, final Class<?> targetType) {
    final float result = exact.floatValue();
    if (Float.isInfinite(result)) {
      throw new ClickHouseValueConversionException(originalValue, targetType);
    }
    return result;
  }

  private static Double checkedDouble(
      final BigDecimal exact, final Object originalValue, final Class<?> targetType) {
    final double result = exact.doubleValue();
    if (Double.isInfinite(result)) {
      throw new ClickHouseValueConversionException(originalValue, targetType);
    }
    return result;
  }

  private static @Nullable Object convertZonedDateTime(
      final ZonedDateTime value, final Class<?> targetType) {
    if (targetType == LocalDateTime.class) {
      return value.toLocalDateTime();
    }
    if (targetType == Instant.class) {
      return value.toInstant();
    }
    if (targetType == OffsetDateTime.class) {
      return value.toOffsetDateTime();
    }
    return null;
  }
}

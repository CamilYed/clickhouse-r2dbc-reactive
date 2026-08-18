package io.github.camilyed.clickhouse.r2dbc.connector;

/**
 * Thrown by {@link ClickHouseValueConverter} when a decoded column value cannot be converted to
 * the caller-requested target type - either the requested numeric conversion is out of range or
 * non-integral (e.g. {@code BigDecimal("1.5")} to {@code Integer}), or no conversion is defined at
 * all between the decoded value's type and the requested one. One predictable, documented
 * exception type for every conversion failure, replacing the inconsistent mix of {@link
 * ClassCastException}/{@link ArithmeticException} a caller would otherwise see depending on which
 * conversion happened to be attempted.
 */
final class ClickHouseValueConversionException extends IllegalArgumentException {

  ClickHouseValueConversionException(final Object value, final Class<?> targetType) {
    super(message(value, targetType));
  }

  ClickHouseValueConversionException(
      final Object value, final Class<?> targetType, final Throwable cause) {
    super(message(value, targetType), cause);
  }

  private static String message(final Object value, final Class<?> targetType) {
    return "Cannot convert value ["
        + value
        + "] of type "
        + value.getClass().getName()
        + " to requested type "
        + targetType.getName();
  }
}

package io.github.camilyed.clickhouse.r2dbc.core;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Resolves one {@link ClickHouseColumn} to a {@link ColumnPlan} — a {@link ColumnDecoder} plus
 * whether the column is {@code Nullable} — for every ClickHouse scalar type {@link ColumnDecoder}
 * natively decodes, or {@link Optional#empty()} for anything else.
 *
 * <p>{@link Optional#empty()} is not an error: {@link RowBinaryDecoder} treats it as the signal to
 * fall back to client-v2's own reader for the <em>whole</em> result (see that class's Javadoc) —
 * this resolver only ever needs to recognize the types it natively supports, not be exhaustive over
 * ClickHouse's full type system. {@code Nullable(T)} unwraps transparently: {@link
 * ClickHouseColumn#getDataType()} already reports {@code T} directly with {@link
 * ClickHouseColumn#isNullable()} as a separate flag, matching client-v2's own {@code
 * BinaryStreamReader.readValue} — see its {@code column.isNullable()} check before the type switch.
 */
final class NativeColumnTypeResolver {

  private NativeColumnTypeResolver() {}

  static Optional<ColumnPlan> resolve(final ClickHouseColumn column) {
    final ColumnDecoder decoder = decoderFor(column);
    if (decoder == null) {
      return Optional.empty();
    }
    return Optional.of(new ColumnPlan(column.isNullable(), decoder));
  }

  private static @Nullable ColumnDecoder decoderFor(final ClickHouseColumn column) {
    final ClickHouseDataType dataType = column.getDataType();
    return switch (dataType) {
      case Int8 -> ScalarColumnDecoder.INT8;
      case UInt8 -> ScalarColumnDecoder.UINT8;
      case Int16 -> ScalarColumnDecoder.INT16;
      case UInt16 -> ScalarColumnDecoder.UINT16;
      case Int32 -> ScalarColumnDecoder.INT32;
      case UInt32 -> ScalarColumnDecoder.UINT32;
      case Int64 -> ScalarColumnDecoder.INT64;
      case UInt64 -> ScalarColumnDecoder.UINT64;
      case Int128 -> ScalarColumnDecoder.INT128;
      case UInt128 -> ScalarColumnDecoder.UINT128;
      case Int256 -> ScalarColumnDecoder.INT256;
      case UInt256 -> ScalarColumnDecoder.UINT256;
      case Float32 -> ScalarColumnDecoder.FLOAT32;
      case Float64 -> ScalarColumnDecoder.FLOAT64;
      case Bool -> ScalarColumnDecoder.BOOL;
      case String -> ScalarColumnDecoder.STRING;
      case FixedString -> new FixedStringColumnDecoder(column.getPrecision());
      case Decimal ->
          new DecimalColumnDecoder(decimalByteWidth(column.getPrecision()), column.getScale());
      case Decimal32 -> new DecimalColumnDecoder(4, column.getScale());
      case Decimal64 -> new DecimalColumnDecoder(8, column.getScale());
      case Decimal128 -> new DecimalColumnDecoder(16, column.getScale());
      case Decimal256 -> new DecimalColumnDecoder(32, column.getScale());
      default -> null;
    };
  }

  /**
   * The little-endian storage width, in bytes, ClickHouse uses for a generic {@code Decimal(P, S)}
   * column of the given precision {@code P} — matches client-v2's own {@code
   * BinaryStreamReader.readDecimal} threshold cascade exactly (precision 1-9 → {@code Decimal32}'s
   * 4-byte width, 10-18 → {@code Decimal64}'s 8, 19-38 → {@code Decimal128}'s 16, 39-76 → {@code
   * Decimal256}'s 32).
   */
  private static int decimalByteWidth(final int precision) {
    if (precision <= 9) {
      return 4;
    }
    if (precision <= 18) {
      return 8;
    }
    if (precision <= 38) {
      return 16;
    }
    return 32;
  }
}

/**
 * One column's resolved native decode plan: its {@link ColumnDecoder} plus whether a {@code
 * Nullable} null-map byte precedes the value on the wire.
 */
record ColumnPlan(boolean nullable, ColumnDecoder decoder) {}

package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import com.clickhouse.data.ClickHouseColumn;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NativeColumnTypeResolverTest {

  @Test
  void shouldResolveAnInt32ColumnToAScalarDecoder() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Int32");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan).isPresent();
    assertThat(plan.get().nullable()).isFalse();
    assertThat(plan.get().decoder()).isEqualTo(ScalarColumnDecoder.INT32);
  }

  @Test
  void shouldResolveAUInt64ColumnToAScalarDecoder() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "UInt64");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(ScalarColumnDecoder.UINT64);
  }

  @Test
  void shouldResolveAnInt128ColumnToAScalarDecoder() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Int128");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(ScalarColumnDecoder.INT128);
  }

  @Test
  void shouldResolveAUInt128ColumnToAScalarDecoder() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "UInt128");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(ScalarColumnDecoder.UINT128);
  }

  @Test
  void shouldResolveAnInt256ColumnToAScalarDecoder() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Int256");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(ScalarColumnDecoder.INT256);
  }

  @Test
  void shouldResolveAUInt256ColumnToAScalarDecoder() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "UInt256");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(ScalarColumnDecoder.UINT256);
  }

  @Test
  void shouldMarkANullableColumnAsNullableWhileResolvingItsInnerType() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Nullable(Int32)");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().nullable()).isTrue();
    assertThat(plan.get().decoder()).isEqualTo(ScalarColumnDecoder.INT32);
  }

  @Test
  void shouldResolveAGenericDecimalToTheDecimal64FourByteWidthForPrecisionUpToNine() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Decimal(9, 2)");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(new DecimalColumnDecoder(4, 2));
  }

  @Test
  void shouldResolveAGenericDecimalToTheEightByteWidthForPrecisionUpToEighteen() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Decimal(18, 4)");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(new DecimalColumnDecoder(8, 4));
  }

  @Test
  void shouldResolveAGenericDecimalToTheSixteenByteWidthForPrecisionUpToThirtyEight() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Decimal(38, 10)");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(new DecimalColumnDecoder(16, 10));
  }

  @Test
  void shouldResolveNamedDecimalWidthsToTheirFixedByteWidthRegardlessOfPrecision() {
    // given
    final ClickHouseColumn decimal32 = ClickHouseColumn.of("value", "Decimal32(4)");
    final ClickHouseColumn decimal64 = ClickHouseColumn.of("value", "Decimal64(4)");
    final ClickHouseColumn decimal128 = ClickHouseColumn.of("value", "Decimal128(4)");
    final ClickHouseColumn decimal256 = ClickHouseColumn.of("value", "Decimal256(4)");

    // when / then
    assertThat(NativeColumnTypeResolver.resolve(decimal32).get().decoder())
        .isEqualTo(new DecimalColumnDecoder(4, 4));
    assertThat(NativeColumnTypeResolver.resolve(decimal64).get().decoder())
        .isEqualTo(new DecimalColumnDecoder(8, 4));
    assertThat(NativeColumnTypeResolver.resolve(decimal128).get().decoder())
        .isEqualTo(new DecimalColumnDecoder(16, 4));
    assertThat(NativeColumnTypeResolver.resolve(decimal256).get().decoder())
        .isEqualTo(new DecimalColumnDecoder(32, 4));
  }

  @Test
  void shouldResolveAFixedStringToItsDeclaredLength() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "FixedString(10)");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan.get().decoder()).isEqualTo(new FixedStringColumnDecoder(10));
  }

  @Test
  void shouldNotResolveAnArrayColumn() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "Array(Int32)");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan).isEmpty();
  }

  @Test
  void shouldNotResolveAUuidColumn() {
    // given
    final ClickHouseColumn column = ClickHouseColumn.of("value", "UUID");

    // when
    final Optional<ColumnPlan> plan = NativeColumnTypeResolver.resolve(column);

    // then
    assertThat(plan).isEmpty();
  }
}

package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import java.io.ByteArrayInputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

class NativeRowBinaryReaderTest {

  @Test
  void shouldDecodeASingleRowOfNativeScalarColumns() {
    // given - one Int32 column, one row with value 42 (little-endian)
    final byte[] rowBytes = {0x2A, 0x00, 0x00, 0x00};
    final var in = new ByteArrayInputStream(rowBytes);
    final var reader =
        new NativeRowBinaryReader(
            in,
            List.of(new ColumnDescriptor("n", "Int32")),
            new ColumnPlan[] {new ColumnPlan(false, ScalarColumnDecoder.INT32)});

    // when
    final boolean hasNext = reader.hasNext();
    final Object[] values = reader.nextRowValues();

    // then
    assertThat(hasNext).isTrue();
    assertThat(values).containsExactly(42);
  }

  @Test
  void shouldReportNoMoreRowsAtEndOfStream() {
    // given
    final var in = new ByteArrayInputStream(new byte[0]);
    final var reader =
        new NativeRowBinaryReader(
            in,
            List.of(new ColumnDescriptor("n", "Int32")),
            new ColumnPlan[] {new ColumnPlan(false, ScalarColumnDecoder.INT32)});

    // when
    final boolean hasNext = reader.hasNext();

    // then
    assertThat(hasNext).isFalse();
  }

  @Test
  void shouldDecodeMultipleRowsInWireOrder() {
    // given - two Int32 rows: 1, then 2
    final byte[] rowBytes = {0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00};
    final var in = new ByteArrayInputStream(rowBytes);
    final var reader =
        new NativeRowBinaryReader(
            in,
            List.of(new ColumnDescriptor("n", "Int32")),
            new ColumnPlan[] {new ColumnPlan(false, ScalarColumnDecoder.INT32)});

    // when
    final Object[] firstRow = reader.nextRowValues();
    final Object[] secondRow = reader.nextRowValues();
    final boolean hasMore = reader.hasNext();

    // then
    assertThat(firstRow).containsExactly(1);
    assertThat(secondRow).containsExactly(2);
    assertThat(hasMore).isFalse();
  }

  @Test
  void shouldDecodeMultipleColumnsInASingleRow() {
    // given - Int32 value 1 followed by a UInt8 value 2 in the same row
    final byte[] rowBytes = {0x01, 0x00, 0x00, 0x00, 0x02};
    final var in = new ByteArrayInputStream(rowBytes);
    final var reader =
        new NativeRowBinaryReader(
            in,
            List.of(new ColumnDescriptor("a", "Int32"), new ColumnDescriptor("b", "UInt8")),
            new ColumnPlan[] {
              new ColumnPlan(false, ScalarColumnDecoder.INT32),
              new ColumnPlan(false, ScalarColumnDecoder.UINT8)
            });

    // when
    final Object[] values = reader.nextRowValues();

    // then
    assertThat(values).containsExactly(1, (short) 2);
  }

  @Test
  void shouldDecodeANullableColumnsNullValue() {
    // given - Nullable(Int32) null-map byte 1, no value bytes follow
    final byte[] rowBytes = {0x01};
    final var in = new ByteArrayInputStream(rowBytes);
    final var reader =
        new NativeRowBinaryReader(
            in,
            List.of(new ColumnDescriptor("n", "Nullable(Int32)")),
            new ColumnPlan[] {new ColumnPlan(true, ScalarColumnDecoder.INT32)});

    // when
    final Object[] values = reader.nextRowValues();

    // then
    assertThat(values).containsExactly((Object) null);
  }

  @Test
  void shouldDecodeANullableColumnsNonNullValue() {
    // given - Nullable(Int32) null-map byte 0, then the Int32 value 7
    final byte[] rowBytes = {0x00, 0x07, 0x00, 0x00, 0x00};
    final var in = new ByteArrayInputStream(rowBytes);
    final var reader =
        new NativeRowBinaryReader(
            in,
            List.of(new ColumnDescriptor("n", "Nullable(Int32)")),
            new ColumnPlan[] {new ColumnPlan(true, ScalarColumnDecoder.INT32)});

    // when
    final Object[] values = reader.nextRowValues();

    // then
    assertThat(values).containsExactly(7);
  }

  @Test
  void shouldExposeTheSuppliedColumnSchema() {
    // given
    final var in = new ByteArrayInputStream(new byte[0]);
    final var schema = List.of(new ColumnDescriptor("n", "Int32"));
    final var reader =
        new NativeRowBinaryReader(
            in, schema, new ColumnPlan[] {new ColumnPlan(false, ScalarColumnDecoder.INT32)});

    // when
    final List<ColumnDescriptor> columns = reader.columns();

    // then
    assertThat(columns).isEqualTo(schema);
  }
}

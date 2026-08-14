package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class ClickHouseRowTest {

  private final ClickHouseRowMetadata metadata =
      new ClickHouseRowMetadata(
          List.of(new ColumnDescriptor("id", "UInt32"), new ColumnDescriptor("name", "String")));

  private final ClickHouseRow row = rowOf(7, "Ada");

  @Test
  void shouldReadAValueByIndex() {
    // when
    final Object value = row.get(0, Object.class);

    // then
    assertThat(value).isEqualTo(7);
  }

  @Test
  void shouldReadAValueByNameCaseInsensitively() {
    // when
    final String value = row.get("NAME", String.class);

    // then
    assertThat(value).isEqualTo("Ada");
  }

  @Test
  void shouldRejectAReadWithoutAType() {
    // when / then
    assertThatThrownBy(() -> row.get(0, null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectAReadByNameWithoutAName() {
    // when / then
    assertThatThrownBy(() -> row.get(null, Object.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectAReadByNameWithoutAType() {
    // when / then
    assertThatThrownBy(() -> row.get("name", null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectAReadForAnUnknownColumnName() {
    // when / then
    assertThatThrownBy(() -> row.get("missing", Object.class))
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void shouldExposeItsOwnRowMetadata() {
    // when / then
    assertThat(row.getMetadata()).isSameAs(metadata);
  }

  private ClickHouseRow rowOf(final Object id, final Object name) {
    return new ClickHouseRow(new DecodedRow(new Object[] {id, name}), metadata);
  }
}

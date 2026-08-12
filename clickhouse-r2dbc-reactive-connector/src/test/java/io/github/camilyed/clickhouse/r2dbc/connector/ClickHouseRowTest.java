package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class ClickHouseRowTest {

  private final ClickHouseRowMetadata metadata =
      new ClickHouseRowMetadata(
          List.of(new ColumnDescriptor("id", "UInt32"), new ColumnDescriptor("name", "String")));

  private final ClickHouseRow row = rowOf(Map.of("id", 7, "name", "Ada"));

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

  private ClickHouseRow rowOf(final Map<String, Object> values) {
    return new ClickHouseRow(new LinkedHashMap<>(values), metadata);
  }
}

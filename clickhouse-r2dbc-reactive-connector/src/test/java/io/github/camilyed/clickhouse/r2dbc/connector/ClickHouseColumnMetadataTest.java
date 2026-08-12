package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import org.junit.jupiter.api.Test;

class ClickHouseColumnMetadataTest {

  private final ClickHouseColumnMetadata column =
      new ClickHouseColumnMetadata(new ColumnDescriptor("id", "Nullable(Int32)"));

  @Test
  void shouldExposeItsWireColumnName() {
    // when / then
    assertThat(column.getName()).isEqualTo("id");
  }

  @Test
  void shouldExposeClickHousesOwnTypeNameAsTheTypeDescriptor() {
    // when / then
    assertThat(column.getType().getName()).isEqualTo("Nullable(Int32)");
  }

  @Test
  void shouldLeaveJavaTypeUnavailableRatherThanGuessIt() {
    // when / then
    assertThat(column.getJavaType()).isNull();
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.r2dbc.spi.ColumnMetadata;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class ClickHouseRowMetadataTest {

  private final ClickHouseRowMetadata metadata =
      new ClickHouseRowMetadata(
          List.of(new ColumnDescriptor("id", "UInt32"), new ColumnDescriptor("name", "String")));

  @Test
  void shouldLookUpColumnMetadataByIndex() {
    // when
    final var column = metadata.getColumnMetadata(1);

    // then
    assertThat(column.getName()).isEqualTo("name");
    assertThat(column.getType().getName()).isEqualTo("String");
  }

  @Test
  void shouldLookUpColumnMetadataByNameCaseInsensitively() {
    // when
    final var column = metadata.getColumnMetadata("ID");

    // then
    assertThat(column.getName()).isEqualTo("id");
  }

  @Test
  void shouldRejectLookingUpAnUnknownColumnName() {
    // when / then
    assertThatThrownBy(() -> metadata.getColumnMetadata("missing"))
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void shouldRejectALookupWithoutAName() {
    // when / then
    assertThatThrownBy(() -> metadata.getColumnMetadata((String) null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldListEveryColumnInWireOrder() {
    // when
    final var columns = metadata.getColumnMetadatas();

    // then
    assertThat(columns).extracting(ColumnMetadata::getName).containsExactly("id", "name");
  }
}

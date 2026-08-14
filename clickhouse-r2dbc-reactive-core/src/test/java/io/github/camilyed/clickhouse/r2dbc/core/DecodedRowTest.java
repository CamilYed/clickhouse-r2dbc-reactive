package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DecodedRowTest {

  @Test
  void shouldExposeAValueByItsWireIndex() {
    // given
    final DecodedRow row = new DecodedRow(new Object[] {1, "Ada", true});

    // when / then
    assertThat(row.valueAt(1)).isEqualTo("Ada");
  }

  @Test
  void shouldBeEqualToAnotherRowWithTheSameValues() {
    // given
    final DecodedRow row = new DecodedRow(new Object[] {1, "Ada"});
    final DecodedRow sameValuesDifferentArray = new DecodedRow(new Object[] {1, "Ada"});

    // when / then
    assertThat(row).isEqualTo(sameValuesDifferentArray).hasSameHashCodeAs(sameValuesDifferentArray);
  }

  @Test
  void shouldNotBeEqualToARowWithDifferentValues() {
    // given
    final DecodedRow row = new DecodedRow(new Object[] {1, "Ada"});
    final DecodedRow differentValues = new DecodedRow(new Object[] {2, "Ada"});

    // when / then
    assertThat(row).isNotEqualTo(differentValues);
  }
}

package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClickHouseQueryTest {

  @Test
  void shouldGenerateAQueryIdWhenNoneIsProvided() {
    // when
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT 1");

    // then
    assertThat(query.queryId()).isNotBlank();
  }

  @Test
  void shouldUseTheProvidedQueryIdWhenGiven() {
    // when
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT 1", "my-query-id");

    // then
    assertThat(query.queryId()).isEqualTo("my-query-id");
  }

  @Test
  void shouldGenerateADifferentQueryIdForEachQuery() {
    // when
    final ClickHouseQuery first = ClickHouseQuery.of("SELECT 1");
    final ClickHouseQuery second = ClickHouseQuery.of("SELECT 1");

    // then
    assertThat(first.queryId()).isNotEqualTo(second.queryId());
  }

  @Test
  void shouldKeepTheSqlItWasGiven() {
    // when
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT 1");

    // then
    assertThat(query.sql()).isEqualTo("SELECT 1");
  }

  @Test
  void shouldHaveNoParametersByDefault() {
    // when
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT 1");

    // then
    assertThat(query.parameters()).isEmpty();
  }

  @Test
  void shouldFindNoParameterNamesInAPlainQuery() {
    // when
    final var names = ClickHouseQuery.parameterNamesIn("SELECT 1");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldFindParameterNamesInFirstOccurrenceOrderWithoutDuplicates() {
    // when
    final var names =
        ClickHouseQuery.parameterNamesIn(
            "SELECT * FROM t WHERE b = {b:String} AND a = {a:UInt32} AND b = {b:String}");

    // then
    assertThat(names).containsExactly("b", "a");
  }

  @Test
  void shouldEncodeBoundValuesAsClickHousesParamWireFormat() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {n:Nullable(String)}, {a:UInt32}");
    final Map<String, Object> boundValues = new LinkedHashMap<>();
    boundValues.put("n", null);
    boundValues.put("a", 42);

    // when
    final ClickHouseQuery parameterized = query.withParameters(boundValues);

    // then
    assertThat(parameterized.parameters())
        .containsExactly(Map.entry("n", "\\N"), Map.entry("a", "42"));
  }

  @Test
  void shouldEscapeBackslashesTabsAndNewlinesInStringParameters() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {s:String}");

    // when
    final ClickHouseQuery parameterized = query.withParameters(Map.of("s", "a\\b\tc\nd\re"));

    // then
    assertThat(parameterized.parameters()).containsEntry("s", "a\\\\b\\tc\\nd\\re");
  }
}

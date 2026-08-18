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
        .containsExactlyInAnyOrderEntriesOf(Map.of("n", "\\N", "a", "42"));
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

  @Test
  void shouldHaveNoSettingsByDefault() {
    // when
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT 1");

    // then
    assertThat(query.settings()).isEmpty();
  }

  @Test
  void shouldCarryServerSettingsUnchangedWhenAttached() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT 1");

    // when
    final ClickHouseQuery withSettings = query.withSettings(Map.of("max_execution_time", "5.000"));

    // then
    assertThat(withSettings.settings()).containsExactly(Map.entry("max_execution_time", "5.000"));
  }

  @Test
  void shouldKeepAlreadyBoundParametersWhenSettingsAreAttached() {
    // given
    final ClickHouseQuery query =
        ClickHouseQuery.of("SELECT {a:UInt32}").withParameters(Map.of("a", 42));

    // when
    final ClickHouseQuery withSettings = query.withSettings(Map.of("max_execution_time", "5.000"));

    // then
    assertThat(withSettings.parameters()).containsExactly(Map.entry("a", "42"));
  }

  @Test
  void shouldKeepAlreadyAttachedSettingsWhenParametersAreBound() {
    // given
    final ClickHouseQuery query =
        ClickHouseQuery.of("SELECT {a:UInt32}").withSettings(Map.of("max_execution_time", "5.000"));

    // when
    final ClickHouseQuery parameterized = query.withParameters(Map.of("a", 42));

    // then
    assertThat(parameterized.settings()).containsExactly(Map.entry("max_execution_time", "5.000"));
  }
}

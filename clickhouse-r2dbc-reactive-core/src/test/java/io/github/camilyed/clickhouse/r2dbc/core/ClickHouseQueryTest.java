package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
  void shouldEncodeALocalDateTimeWithASpaceSeparatorNotJavasTIsoSeparator() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {d:DateTime}");

    // when
    final ClickHouseQuery parameterized =
        query.withParameters(Map.of("d", LocalDateTime.of(2024, 1, 15, 10, 30, 15)));

    // then
    assertThat(parameterized.parameters()).containsEntry("d", "2024-01-15 10:30:15");
  }

  @Test
  void shouldEncodeAnInstantNormalizedToUtc() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {d:DateTime}");
    final Instant instant =
        OffsetDateTime.of(2024, 1, 15, 12, 30, 15, 0, ZoneOffset.ofHours(2)).toInstant();

    // when
    final ClickHouseQuery parameterized = query.withParameters(Map.of("d", instant));

    // then
    assertThat(parameterized.parameters()).containsEntry("d", "2024-01-15 10:30:15");
  }

  @Test
  void shouldEncodeAnOffsetDateTimeNormalizedToUtc() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {d:DateTime}");
    final OffsetDateTime offsetDateTime =
        OffsetDateTime.of(2024, 1, 15, 12, 30, 15, 0, ZoneOffset.ofHours(2));

    // when
    final ClickHouseQuery parameterized = query.withParameters(Map.of("d", offsetDateTime));

    // then
    assertThat(parameterized.parameters()).containsEntry("d", "2024-01-15 10:30:15");
  }

  @Test
  void shouldEncodeAZonedDateTimeNormalizedToUtc() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {d:DateTime}");
    final ZonedDateTime zonedDateTime =
        ZonedDateTime.of(2024, 1, 15, 12, 30, 15, 0, ZoneOffset.ofHours(2));

    // when
    final ClickHouseQuery parameterized = query.withParameters(Map.of("d", zonedDateTime));

    // then
    assertThat(parameterized.parameters()).containsEntry("d", "2024-01-15 10:30:15");
  }

  @Test
  void shouldEncodeANumericListAsAClickHouseArrayLiteral() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {a:Array(UInt32)}");

    // when
    final ClickHouseQuery parameterized = query.withParameters(Map.of("a", List.of(1, 2, 3)));

    // then
    assertThat(parameterized.parameters()).containsEntry("a", "[1,2,3]");
  }

  @Test
  void shouldEncodeAStringListAsAQuotedAndEscapedClickHouseArrayLiteral() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {a:Array(String)}");

    // when
    final ClickHouseQuery parameterized =
        query.withParameters(Map.of("a", List.of("it's", "plain")));

    // then
    assertThat(parameterized.parameters()).containsEntry("a", "['it\\'s','plain']");
  }

  @Test
  void shouldEncodeNullElementsInsideAnArrayAsTheArrayLiteralNullKeyword() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {a:Array(Nullable(UInt32))}");
    final List<Integer> withNull = new ArrayList<>();
    withNull.add(1);
    withNull.add(null);

    // when
    final ClickHouseQuery parameterized = query.withParameters(Map.of("a", withNull));

    // then
    assertThat(parameterized.parameters()).containsEntry("a", "[1,NULL]");
  }

  @Test
  void shouldRejectANestedArrayAsABoundParameterValue() {
    // given
    final ClickHouseQuery query = ClickHouseQuery.of("SELECT {a:Array(Array(UInt32))}");

    // when / then
    assertThatThrownBy(() -> query.withParameters(Map.of("a", List.of(List.of(1, 2)))))
        .isInstanceOf(UnsupportedOperationException.class);
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

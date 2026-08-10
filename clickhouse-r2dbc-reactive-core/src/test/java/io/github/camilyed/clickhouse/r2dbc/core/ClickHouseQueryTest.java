package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

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
}

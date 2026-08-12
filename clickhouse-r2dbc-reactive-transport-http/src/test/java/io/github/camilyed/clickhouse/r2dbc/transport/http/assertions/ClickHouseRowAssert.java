package io.github.camilyed.clickhouse.r2dbc.transport.http.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Map;
import org.assertj.core.api.AbstractAssert;

/** Custom assertion over one decoded ClickHouse row ({@code column name -> value}). */
public final class ClickHouseRowAssert extends AbstractAssert<ClickHouseRowAssert, Map<String, Object>> {

    private ClickHouseRowAssert(final Map<String, Object> actual) {
        super(actual, ClickHouseRowAssert.class);
    }

    public static ClickHouseRowAssert assertThatRow(final Map<String, Object> actual) {
        return new ClickHouseRowAssert(actual);
    }

    /** Asserts {@code column} equals {@code expected} exactly ({@link Object#equals}). */
    public ClickHouseRowAssert hasValue(final String column, final Object expected) {
        isNotNull();
        assertThat(actual.get(column)).as("column '%s'", column).isEqualTo(expected);
        return this;
    }

    /** Asserts {@code column} is a {@link BigDecimal} numerically equal to {@code expected}, ignoring scale. */
    public ClickHouseRowAssert hasDecimal(final String column, final String expected) {
        isNotNull();
        assertThat((BigDecimal) actual.get(column)).as("column '%s'", column).isEqualByComparingTo(expected);
        return this;
    }

    /** Asserts {@code column} is present and {@code null}. */
    public ClickHouseRowAssert hasNullAt(final String column) {
        isNotNull();
        assertThat(actual.get(column)).as("column '%s'", column).isNull();
        return this;
    }

    /** Asserts {@code column}'s decoded value is an instance of {@code type}, without checking its value. */
    public ClickHouseRowAssert hasTypeAt(final String column, final Class<?> type) {
        isNotNull();
        assertThat(actual.get(column)).as("column '%s'", column).isInstanceOf(type);
        return this;
    }
}

package io.github.camilyed.clickhouse.r2dbc.testkit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.data.Offset;

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

    /** Asserts {@code column} is a decoded {@code Array}/{@code Nested} value, equal to {@code expected} in order. */
    @SuppressWarnings("unchecked")
    public ClickHouseRowAssert hasList(final String column, final Object... expected) {
        isNotNull();
        assertThat((List<Object>) actual.get(column)).as("column '%s'", column).containsExactly(expected);
        return this;
    }

    /** Asserts {@code column} is a decoded {@code Tuple} value, equal to {@code expected} in order. */
    public ClickHouseRowAssert hasTuple(final String column, final Object... expected) {
        isNotNull();
        assertThat((Object[]) actual.get(column)).as("column '%s'", column).containsExactly(expected);
        return this;
    }

    /** Asserts {@code column} is a decoded {@code Map} value, containing exactly {@code expected} entries. */
    @SuppressWarnings("unchecked")
    public ClickHouseRowAssert hasMap(final String column, final Map.Entry<?, ?>... expected) {
        isNotNull();
        assertThat((Map<Object, Object>) actual.get(column)).as("column '%s'", column).containsExactly(expected);
        return this;
    }

    /**
     * Asserts {@code column} is a decoded {@code Enum8}/{@code Enum16} value whose member name is
     * {@code expected}. The decoded value is client-v2's own internal {@code EnumValue}, read here
     * only via its public {@link Object#toString()}, not by name — see {@code
     * RealWorldTableAgainstRealClickHouseTest}'s Javadoc for why that's the deliberate, minimal way
     * to read it without depending on the internal type itself.
     */
    public ClickHouseRowAssert hasEnumName(final String column, final String expected) {
        isNotNull();
        assertThat(actual.get(column)).as("column '%s'", column).hasToString(expected);
        return this;
    }

    /** Asserts {@code column} is a {@link BigInteger} equal to {@code expected} (for {@code Int128}..{@code UInt256}). */
    public ClickHouseRowAssert hasBigInteger(final String column, final String expected) {
        isNotNull();
        assertThat((BigInteger) actual.get(column)).as("column '%s'", column).isEqualTo(new BigInteger(expected));
        return this;
    }

    /** Asserts {@code column} is a {@link Float} within {@code offset} of {@code expected} (for {@code Float32}). */
    public ClickHouseRowAssert hasFloatCloseTo(final String column, final float expected, final Offset<Float> offset) {
        isNotNull();
        assertThat((Float) actual.get(column)).as("column '%s'", column).isCloseTo(expected, offset);
        return this;
    }

    /** Asserts {@code column} is an {@link InetAddress} equal to {@code expected} (for {@code IPv4}/{@code IPv6}). */
    public ClickHouseRowAssert hasInetAddress(final String column, final InetAddress expected) {
        isNotNull();
        assertThat((InetAddress) actual.get(column)).as("column '%s'", column).isEqualTo(expected);
        return this;
    }

    /** Asserts {@code column} is a {@link UUID} equal to {@code expected}. */
    public ClickHouseRowAssert hasUuid(final String column, final UUID expected) {
        isNotNull();
        assertThat((UUID) actual.get(column)).as("column '%s'", column).isEqualTo(expected);
        return this;
    }
}

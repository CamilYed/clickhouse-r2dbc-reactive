package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static io.github.camilyed.clickhouse.r2dbc.transport.http.assertions.ClickHouseRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.github.camilyed.clickhouse.r2dbc.transport.http.abilities.RealClickHouseQueryAbility;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end proof against real ClickHouse, organized by the type categories ClickHouse's own docs
 * use (<a href="https://clickhouse.com/docs/reference/data-types">clickhouse.com/docs/reference/data-types</a>),
 * decoded through the full pipeline (transport → bridge → {@code RowBinaryDecoder}) — not one row,
 * one table, or one type at a time.
 *
 * <p>Status per category, checked directly against that taxonomy, not assumed:
 *
 * <ul>
 *   <li><b>Numeric</b> — covered ({@link #shouldDecodeNumericTypes()}): all {@code Int8..256}/{@code
 *       UInt8..256} widths, {@code Float32}/{@code Float64}, {@code Decimal}, {@code Bool}.
 *   <li><b>String</b> — covered ({@link #shouldDecodeStringTypes()}): {@code String}, {@code
 *       FixedString}.
 *   <li><b>Date and time</b> — partially covered ({@link #shouldDecodeDateAndTimeTypes()}): {@code
 *       Date}/{@code Date32}/{@code DateTime}/{@code DateTime64}. {@code Time}/{@code Time64} are
 *       <b>not</b> just untested — checked client-v2's {@code BinaryStreamReader} {@code readValue}
 *       switch directly and confirmed there is no case for them at all in our pinned version; a
 *       column of that type would throw {@code IllegalArgumentException("Unsupported data type")}.
 *       A real gap, not a gap in our tests.
 *   <li><b>Network</b> — covered ({@link #shouldDecodeNetworkTypes()}): {@code IPv4}, {@code IPv6}.
 *   <li><b>Composite</b> ({@code Array}/{@code Tuple}/{@code Map}/{@code Nested}) — {@code Map} and
 *       {@code Tuple} covered ({@link #shouldDecodeMapType()}, {@link #shouldDecodeTupleType()}):
 *       checked client-v2's {@code BinaryStreamReader.readMap()}/{@code readTuple()} directly — they
 *       already return a plain {@code Map}/{@code Object[]}, not a {@code .internal} type, for
 *       element types that aren't themselves {@code Array}/{@code Nested}. {@code Array} and {@code
 *       Nested} are genuinely still blocked: both go through {@code convertArray()}, which only
 *       avoids returning the {@code .internal} {@code ArrayValue} when a {@code List.class} type hint
 *       is supplied — and the public {@code next()}/{@code readRecord()} path we use never passes
 *       one. Unblocking them means either a small dedicated row-reading loop that calls {@code
 *       BinaryStreamReader.readValue(column, List.class)} directly (a deliberate, documented
 *       dependency on that one {@code .internal} class, same shape as the Phase 0 {@code InputStream}
 *       bridge decision) or leaving them unsupported — an open design decision, not silently skipped.
 *   <li><b>Semi-structured</b> ({@code JSON}/{@code Dynamic}/{@code Variant}) — not attempted; all
 *       still experimental/evolving in ClickHouse itself.
 *   <li><b>Nullable and optional</b> — {@code Nullable} covered ({@link
 *       #shouldDecodeAMultiTypeMultiRowTable()}, both a present and an actually-{@code NULL} value
 *       across multiple rows); {@code LowCardinality} not attempted.
 *   <li><b>Specialized</b> — {@code UUID} covered ({@link #shouldDecodeSpecializedTypes()}); {@code
 *       Enum8}/{@code Enum16} covered ({@link #shouldDecodeEnumTypes()}): the returned value is still
 *       an {@code .internal} {@code EnumValue} instance, but it publicly overrides {@code toString()}
 *       to return the member name, so calling {@code toString()} on the opaque {@code Object} reads
 *       the value without ever importing or casting to the internal type — the tradeoff being a
 *       dependency on {@code toString()}'s current behavior rather than a documented contract, since
 *       the whole class is {@code .internal}. Geo types, vector-search types ({@code QBit}), and
 *       domains not attempted yet (next likely quick win: {@code readGeoPoint()}/{@code
 *       readGeoRing()} etc. also return plain arrays, same shape as {@code Map}/{@code Tuple} above).
 *   <li><b>Aggregate function</b> ({@code AggregateFunction}/{@code SimpleAggregateFunction}) — not
 *       attempted; these store intermediate aggregation state, not plain literal-insertable values,
 *       so proving them needs a different test shape (insert via an aggregate query, not a literal).
 *   <li><b>Special Data Types</b> ({@code Expression}/{@code Interval}/{@code Nothing}/{@code Set})
 *       — not applicable here: these are query-intermediate constructs, not column/storage types a
 *       {@code CREATE TABLE} can hold.
 * </ul>
 */
@Testcontainers
class RealWorldTableAgainstRealClickHouseTest implements RealClickHouseQueryAbility {

    @Container
    private final ClickHouseContainer clickHouse = new ClickHouseContainer("clickhouse/clickhouse-server:latest");

    private ClickHouseHttpTransport transport;

    @Override
    public ClickHouseHttpTransport transport() {
        if (transport == null) {
            transport = new ClickHouseHttpTransport(clickHouse.getHttpUrl(), clickHouse.getUsername(), clickHouse.getPassword());
        }
        return transport;
    }

    @Test
    void shouldDecodeAMultiTypeMultiRowTable() {
        // given
        execute("CREATE TABLE people ("
                + "id UInt32, "
                + "name String, "
                + "email Nullable(String), "
                + "age UInt8, "
                + "balance Decimal(10,2), "
                + "created_at DateTime, "
                + "is_active Bool"
                + ") ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO people VALUES "
                + "(1, 'Alice', 'alice@example.com', 30, 1234.56, '2024-01-15 10:00:00', 1), "
                + "(2, 'Bob', NULL, 45, 0.00, '2023-06-01 08:30:00', 0), "
                + "(3, 'Carol', 'carol@example.com', 22, 999999.99, '2025-03-20 23:59:59', 1)");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM people ORDER BY id");

        // then
        assertThat(rows).hasSize(3);
        assertThatRow(rows.get(0))
                .hasValue("id", 1L)
                .hasValue("name", "Alice")
                .hasValue("email", "alice@example.com")
                .hasValue("age", (short) 30)
                .hasDecimal("balance", "1234.56")
                .hasTypeAt("created_at", ZonedDateTime.class)
                .hasValue("is_active", true);
        assertThatRow(rows.get(1)).hasValue("id", 2L).hasNullAt("email").hasValue("is_active", false);
        assertThatRow(rows.get(2)).hasValue("id", 3L).hasDecimal("balance", "999999.99");
    }

    @Test
    void shouldDecodeNumericTypes() {
        // given
        execute("CREATE TABLE numeric_types ("
                + "id UInt32, "
                + "int8_val Int8, int16_val Int16, int32_val Int32, int64_val Int64, "
                + "int128_val Int128, int256_val Int256, "
                + "uint8_val UInt8, uint16_val UInt16, uint32_val UInt32, uint64_val UInt64, "
                + "uint128_val UInt128, uint256_val UInt256, "
                + "float32_val Float32, float64_val Float64, "
                + "decimal_val Decimal(18,4), "
                + "bool_val Bool"
                + ") ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO numeric_types VALUES (1, "
                + "-100, -20000, -2000000000, -9000000000000000000, "
                + "-123456789012345678901234567890, -12345678901234567890123456789012345678901234567890123456789012345678, "
                + "250, 60000, 4000000000, 18000000000000000000, "
                + "123456789012345678901234567890, 12345678901234567890123456789012345678901234567890123456789012345678, "
                + "3.14, 2.718281828459, "
                + "12345.6789, "
                + "true)");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM numeric_types");

        // then
        assertThat(rows).hasSize(1);
        final Map<String, Object> row = rows.get(0);
        assertThatRow(row)
                .hasValue("int8_val", (byte) -100)
                .hasValue("int16_val", (short) -20000)
                .hasValue("int32_val", -2000000000)
                .hasValue("int64_val", -9000000000000000000L)
                .hasValue("uint8_val", (short) 250)
                .hasValue("uint16_val", 60000)
                .hasValue("uint32_val", 4000000000L)
                .hasValue("float64_val", 2.718281828459)
                .hasDecimal("decimal_val", "12345.6789")
                .hasValue("bool_val", true);
        assertThat((BigInteger) row.get("int128_val")).isEqualTo(new BigInteger("-123456789012345678901234567890"));
        assertThat((BigInteger) row.get("int256_val"))
                .isEqualTo(new BigInteger("-12345678901234567890123456789012345678901234567890123456789012345678"));
        assertThat((BigInteger) row.get("uint64_val")).isEqualTo(new BigInteger("18000000000000000000"));
        assertThat((BigInteger) row.get("uint128_val")).isEqualTo(new BigInteger("123456789012345678901234567890"));
        assertThat((BigInteger) row.get("uint256_val"))
                .isEqualTo(new BigInteger("12345678901234567890123456789012345678901234567890123456789012345678"));
        assertThat((Float) row.get("float32_val")).isCloseTo(3.14f, Offset.offset(0.001f));
    }

    @Test
    void shouldDecodeStringTypes() {
        // given
        execute("CREATE TABLE string_types (id UInt32, string_val String, fixedstring_val FixedString(5)) "
                + "ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO string_types VALUES (1, 'hello world', 'abcde')");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM string_types");

        // then
        assertThat(rows).hasSize(1);
        assertThatRow(rows.get(0)).hasValue("string_val", "hello world").hasValue("fixedstring_val", "abcde");
    }

    @Test
    void shouldDecodeDateAndTimeTypes() {
        // given
        execute("CREATE TABLE date_time_types ("
                + "id UInt32, date_val Date, date32_val Date32, datetime_val DateTime, datetime64_val DateTime64(3)"
                + ") ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO date_time_types VALUES "
                + "(1, '2024-06-15', '2024-06-15', '2024-06-15 12:30:00', '2024-06-15 12:30:00.123')");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM date_time_types");

        // then
        assertThat(rows).hasSize(1);
        assertThatRow(rows.get(0))
                .hasTypeAt("date_val", ZonedDateTime.class)
                .hasTypeAt("date32_val", ZonedDateTime.class)
                .hasTypeAt("datetime_val", ZonedDateTime.class)
                .hasTypeAt("datetime64_val", ZonedDateTime.class);
    }

    @Test
    void shouldDecodeNetworkTypes() throws UnknownHostException {
        // given
        execute("CREATE TABLE network_types (id UInt32, ipv4_val IPv4, ipv6_val IPv6) ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO network_types VALUES (1, '192.168.1.1', '2001:db8::1')");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM network_types");

        // then
        assertThat(rows).hasSize(1);
        assertThat((InetAddress) rows.get(0).get("ipv4_val")).isEqualTo(InetAddress.getByName("192.168.1.1"));
        assertThat((InetAddress) rows.get(0).get("ipv6_val")).isEqualTo(InetAddress.getByName("2001:db8::1"));
    }

    @Test
    void shouldDecodeSpecializedTypes() {
        // given
        execute("CREATE TABLE specialized_types (id UInt32, uuid_val UUID) ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO specialized_types VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0')");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM specialized_types");

        // then
        assertThat(rows).hasSize(1);
        assertThat((UUID) rows.get(0).get("uuid_val")).isEqualTo(UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0"));
    }

    @Test
    void shouldDecodeMapType() {
        // given
        execute("CREATE TABLE map_types (id UInt32, map_val Map(String, Int32)) ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO map_types VALUES (1, {'a': 1, 'b': 2})");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM map_types");

        // then
        assertThat(rows).hasSize(1);
        assertThat((Map<?, ?>) rows.get(0).get("map_val")).containsExactly(entry("a", 1), entry("b", 2));
    }

    @Test
    void shouldDecodeTupleType() {
        // given
        execute("CREATE TABLE tuple_types (id UInt32, tuple_val Tuple(String, Int32)) ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO tuple_types VALUES (1, ('hello', 42))");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM tuple_types");

        // then
        assertThat(rows).hasSize(1);
        assertThat((Object[]) rows.get(0).get("tuple_val")).containsExactly("hello", 42);
    }

    @Test
    void shouldDecodeEnumTypes() {
        // given
        execute("CREATE TABLE enum_types (id UInt32, enum8_val Enum8('a' = 1, 'b' = 2), enum16_val Enum16('x' = 1, 'y' = 2)) "
                + "ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO enum_types VALUES (1, 'a', 'y')");

        // when
        final List<Map<String, Object>> rows = queryRows("SELECT * FROM enum_types");

        // then
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("enum8_val")).hasToString("a");
        assertThat(rows.get(0).get("enum16_val")).hasToString("y");
    }
}

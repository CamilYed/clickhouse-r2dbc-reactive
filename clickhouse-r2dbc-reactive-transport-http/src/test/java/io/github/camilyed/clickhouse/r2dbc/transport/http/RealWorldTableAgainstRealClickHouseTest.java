package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static io.github.camilyed.clickhouse.r2dbc.testkit.assertions.ClickHouseRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.github.camilyed.clickhouse.r2dbc.transport.http.abilities.RealClickHouseQueryAbility;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof against real ClickHouse, organized by the type categories ClickHouse's own docs
 * use (<a
 * href="https://clickhouse.com/docs/reference/data-types">clickhouse.com/docs/reference/data-types</a>),
 * decoded through the full pipeline (transport → bridge → {@code RowBinaryDecoder}) — not one row,
 * one table, or one type at a time.
 *
 * <p>Extends {@link BaseClickHouseIntegrationTest}: the ClickHouse container is shared across every
 * test class that extends it (started once per JVM), and every table created by a test method here
 * is dropped automatically before the next one runs, so test order never matters.
 *
 * <p>Status per category, checked directly against that taxonomy, not assumed:
 *
 * <ul>
 *   <li><b>Numeric</b> — covered ({@link #shouldDecodeNumericTypes()}): all {@code
 *       Int8..256}/{@code UInt8..256} widths, {@code Float32}/{@code Float64}, {@code Decimal},
 *       {@code Bool}. {@code Decimal128}/{@code Decimal256} (the {@code BigInteger}-backed wide
 *       tiers, precision above 18) additionally covered by {@link #shouldDecodeLargeDecimalTypes()}
 *       — see that test's Javadoc for why.
 *   <li><b>String</b> — covered ({@link #shouldDecodeStringTypes()}): {@code String}, {@code
 *       FixedString}.
 *   <li><b>Date and time</b> — partially covered ({@link #shouldDecodeDateAndTimeTypes()}): {@code
 *       Date}/{@code Date32} (decode as {@link java.time.LocalDate} as of client-v2 0.9.8 — see the
 *       version catalog and that test's own comment) and {@code DateTime}/{@code DateTime64}
 *       (decode as {@link java.time.ZonedDateTime}). {@code Time}/{@code Time64} gained a real case
 *       in client-v2's {@code BinaryStreamReader} {@code readValue} switch as of 0.9.8 (absent at
 *       0.9.0, where a column of that type would throw {@code IllegalArgumentException("Unsupported
 *       data type")}) but are still untested here — now a genuine test gap, not a version gap.
 *   <li><b>Network</b> — covered ({@link #shouldDecodeNetworkTypes()}): {@code IPv4}, {@code IPv6}.
 *   <li><b>Composite</b> ({@code Array}/{@code Tuple}/{@code Map}/{@code Nested}) — {@code Map},
 *       {@code Tuple}, and {@code Array} covered ({@link #shouldDecodeMapType()}, {@link
 *       #shouldDecodeTupleType()}, {@link #shouldDecodeArrayType()}). {@code Map}/{@code Tuple}
 *       already returned a plain {@code Map}/{@code Object[]} from client-v2 with no changes
 *       needed. {@code Array} needed one deliberate addition: {@code core}'s {@code
 *       ListDecodingRowBinaryReader} overrides the reader's {@code protected readRecord(Object[])}
 *       hook to supply a {@code List.class} type hint per column, which {@code
 *       BinaryStreamReader.convertArray()} already knows how to honor — avoiding the {@code
 *       .internal} {@code ArrayValue} client-v2 returns otherwise. Same shape of compromise as the
 *       Phase 0 {@code InputStream} bridge: one narrow, documented, tested dependency on
 *       client-v2's {@code .internal} package, not a general one. {@code Nested} now also covered
 *       ({@link #shouldDecodeNestedType()}): ClickHouse flattens {@code Nested(...)} into one
 *       {@code Array(...)} column per sub-field by default ({@code flatten_nested=1}), so on the
 *       wire it's indistinguishable from ordinary {@code Array} columns — same mechanism, no new
 *       code needed, just confirmed directly rather than assumed.
 *   <li><b>Semi-structured</b> ({@code JSON}/{@code Dynamic}/{@code Variant}) — {@code JSON}
 *       covered ({@link #shouldDecodeJsonTypeAsAPlainString()}): GA since ClickHouse 25.3, no
 *       {@code allow_experimental_json_type} needed. Decoded as a plain {@code String} — {@link
 *       ClickHouseHttpTransport} appends {@code output_format_binary_write_json_as_string=1} to
 *       every query unconditionally (harmless when a table has no JSON column), and {@code core}'s
 *       {@code RowBinaryDecoder#newReader} sets the matching local {@code QuerySettings} flag so
 *       client-v2's reader decodes it the same way instead of into its complex {@code .internal}
 *       JSON object representation. {@code Dynamic}/{@code Variant} still not attempted — newer,
 *       less settled experimental types.
 *   <li><b>Nullable and optional</b> — {@code Nullable} covered ({@link
 *       #shouldDecodeAMultiTypeMultiRowTable()}, both a present and an actually-{@code NULL} value
 *       across multiple rows); {@code LowCardinality} now covered too ({@link
 *       #shouldDecodeLowCardinalityType()}) — confirmed against client-v2's own test suite that
 *       it's a "virtual type" there (the wrapper is stripped, decoding dispatches to the underlying
 *       type), the same shape as {@code Nullable}, so this proves the unwrapping works end to end
 *       rather than assuming client-v2's own tests are enough.
 *   <li><b>Specialized</b> — {@code UUID} covered ({@link #shouldDecodeSpecializedTypes()}); {@code
 *       Enum8}/{@code Enum16} covered ({@link #shouldDecodeEnumTypes()}): decodes as a plain {@link
 *       String} of the member name (e.g. {@code "PLACED"}), not client-v2's {@code .internal}
 *       {@code EnumValue} — {@code core}'s {@code ListDecodingRowBinaryReader} normalizes it away
 *       at the driver boundary the same way it already does for {@code Array}/{@code Nested} (see
 *       that class's Javadoc), so a caller can ask for {@code String.class} directly instead of
 *       {@code Object.class} plus a {@code toString()} call. Geo types, vector-search types ({@code
 *       QBit}), and domains not attempted yet (next likely quick win: {@code readGeoPoint()}/{@code
 *       readGeoRing()} etc. also return plain arrays, same shape as {@code Map}/{@code Tuple}
 *       above).
 *   <li><b>Aggregate function</b> ({@code AggregateFunction}/{@code SimpleAggregateFunction}) — not
 *       attempted; these store intermediate aggregation state, not plain literal-insertable values,
 *       so proving them needs a different test shape (insert via an aggregate query, not a
 *       literal).
 *   <li><b>Special Data Types</b> ({@code Expression}/{@code Interval}/{@code Nothing}/{@code Set})
 *       — not applicable here: these are query-intermediate constructs, not column/storage types a
 *       {@code CREATE TABLE} can hold.
 * </ul>
 */
class RealWorldTableAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest
    implements RealClickHouseQueryAbility {

  private ClickHouseHttpTransport transport;

  @Override
  public ClickHouseHttpTransport transport() {
    if (transport == null) {
      transport =
          new ClickHouseHttpTransport(
              clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    }
    return transport;
  }

  @Test
  void shouldDecodeAMultiTypeMultiRowTable() {
    // given
    execute(
        "CREATE TABLE people ("
            + "id UInt32, "
            + "name String, "
            + "email Nullable(String), "
            + "age UInt8, "
            + "balance Decimal(10,2), "
            + "created_at DateTime, "
            + "is_active Bool"
            + ") ENGINE = MergeTree ORDER BY id");
    execute(
        "INSERT INTO people VALUES "
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
    execute(
        "CREATE TABLE numeric_types ("
            + "id UInt32, "
            + "int8_val Int8, int16_val Int16, int32_val Int32, int64_val Int64, "
            + "int128_val Int128, int256_val Int256, "
            + "uint8_val UInt8, uint16_val UInt16, uint32_val UInt32, uint64_val UInt64, "
            + "uint128_val UInt128, uint256_val UInt256, "
            + "float32_val Float32, float64_val Float64, "
            + "decimal_val Decimal(18,4), "
            + "bool_val Bool"
            + ") ENGINE = MergeTree ORDER BY id");
    execute(
        "INSERT INTO numeric_types VALUES (1, "
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
    assertThatRow(rows.get(0))
        .hasValue("int8_val", (byte) -100)
        .hasValue("int16_val", (short) -20000)
        .hasValue("int32_val", -2000000000)
        .hasValue("int64_val", -9000000000000000000L)
        .hasValue("uint8_val", (short) 250)
        .hasValue("uint16_val", 60000)
        .hasValue("uint32_val", 4000000000L)
        .hasValue("float64_val", 2.718281828459)
        .hasDecimal("decimal_val", "12345.6789")
        .hasValue("bool_val", true)
        .hasBigInteger("int128_val", "-123456789012345678901234567890")
        .hasBigInteger(
            "int256_val", "-12345678901234567890123456789012345678901234567890123456789012345678")
        .hasBigInteger("uint64_val", "18000000000000000000")
        .hasBigInteger("uint128_val", "123456789012345678901234567890")
        .hasBigInteger(
            "uint256_val", "12345678901234567890123456789012345678901234567890123456789012345678")
        .hasFloatCloseTo("float32_val", 3.14f, Offset.offset(0.001f));
  }

  /**
   * {@code Decimal(P,S)} with {@code P} above 18 significant digits switches client-v2's {@code
   * BinaryStreamReader.readDecimal} from a plain {@code long}/{@code int} read to a {@code
   * BigInteger}-backed one ({@code Decimal128} for 19–38 digits, {@code Decimal256} for 39–76) —
   * {@link #shouldDecodeNumericTypes()} above only exercises {@code Decimal(18,4)}, which stays on
   * the {@code long} path. Added while checking client-v2 0.10.0's changelog for a reported {@code
   * getBigDecimal} truncation fix: that fix lives in {@code NumberConverter}/{@code
   * SerializerUtils}/{@code ValueConverters} (the typed-getter path on {@code
   * AbstractBinaryFormatReader}), which this driver's decode path never calls — {@code
   * ListDecodingRowBinaryReader} goes through {@code BinaryStreamReader.readValue} directly — so
   * that specific fix doesn't apply here. But the investigation surfaced that the BigInteger-backed
   * width tiers had no coverage at all in this repo, positive or negative value, which is the
   * actual gap worth closing before trusting a version bump.
   */
  @Test
  void shouldDecodeLargeDecimalTypes() {
    // given
    execute(
        "CREATE TABLE large_decimals ("
            + "id UInt32, "
            + "decimal128_val Decimal(38,10), "
            + "decimal256_val Decimal(76,20)"
            + ") ENGINE = MergeTree ORDER BY id");
    execute(
        "INSERT INTO large_decimals VALUES "
            + "(1, 9123456789012345678901234567.1234567890, "
            + "91234567890123456789012345678901234567890123456789012345.12345678901234567890), "
            + "(2, -9123456789012345678901234567.1234567890, "
            + "-91234567890123456789012345678901234567890123456789012345.12345678901234567890)");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM large_decimals ORDER BY id");

    // then
    assertThat(rows).hasSize(2);
    assertThatRow(rows.get(0))
        .hasDecimal("decimal128_val", "9123456789012345678901234567.1234567890")
        .hasDecimal(
            "decimal256_val",
            "91234567890123456789012345678901234567890123456789012345.12345678901234567890");
    assertThatRow(rows.get(1))
        .hasDecimal("decimal128_val", "-9123456789012345678901234567.1234567890")
        .hasDecimal(
            "decimal256_val",
            "-91234567890123456789012345678901234567890123456789012345.12345678901234567890");
  }

  @Test
  void shouldDecodeStringTypes() {
    // given
    execute(
        "CREATE TABLE string_types (id UInt32, string_val String, fixedstring_val FixedString(5)) "
            + "ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO string_types VALUES (1, 'hello world', 'abcde')");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM string_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0))
        .hasValue("string_val", "hello world")
        .hasValue("fixedstring_val", "abcde");
  }

  @Test
  void shouldDecodeDateAndTimeTypes() {
    // given
    execute(
        "CREATE TABLE date_time_types ("
            + "id UInt32, date_val Date, date32_val Date32, datetime_val DateTime, datetime64_val DateTime64(3)"
            + ") ENGINE = MergeTree ORDER BY id");
    execute(
        "INSERT INTO date_time_types VALUES "
            + "(1, '2024-06-15', '2024-06-15', '2024-06-15 12:30:00', '2024-06-15 12:30:00.123')");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM date_time_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0))
        // Date/Date32 decode as LocalDate as of client-v2 0.9.8 (readDateAsLocalDate/
        // readDate32AsLocalDate, unconditional - no typeHint involved) - a client-v2 change, not
        // ours: at 0.9.0 both went through convertDateTime(..., typeHint) the same way
        // DateTime/DateTime32/DateTime64 still do today, always producing a ZonedDateTime
        // regardless of hint. LocalDate is the more correct fit for a type with no time-of-day or
        // timezone component, so this is a real fix upstream, not a regression - see the version
        // catalog for the pinned client-v2 version.
        .hasTypeAt("date_val", LocalDate.class)
        .hasTypeAt("date32_val", LocalDate.class)
        .hasTypeAt("datetime_val", ZonedDateTime.class)
        .hasTypeAt("datetime64_val", ZonedDateTime.class);
  }

  @Test
  void shouldDecodeNetworkTypes() throws UnknownHostException {
    // given
    execute(
        "CREATE TABLE network_types (id UInt32, ipv4_val IPv4, ipv6_val IPv6) ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO network_types VALUES (1, '192.168.1.1', '2001:db8::1')");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM network_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0))
        .hasInetAddress("ipv4_val", InetAddress.getByName("192.168.1.1"))
        .hasInetAddress("ipv6_val", InetAddress.getByName("2001:db8::1"));
  }

  @Test
  void shouldDecodeSpecializedTypes() {
    // given
    execute(
        "CREATE TABLE specialized_types (id UInt32, uuid_val UUID) ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO specialized_types VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0')");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM specialized_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0))
        .hasUuid("uuid_val", UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0"));
  }

  @Test
  void shouldDecodeJsonTypeAsAPlainString() {
    // given
    execute("CREATE TABLE json_types (id UInt32, payload_val JSON) ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO json_types VALUES (1, '{\"a\":\"1\"}')");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM json_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0)).hasValue("payload_val", "{\"a\":\"1\"}");
  }

  @Test
  void shouldDecodeMapType() {
    // given
    execute(
        "CREATE TABLE map_types (id UInt32, map_val Map(String, Int32)) ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO map_types VALUES (1, {'a': 1, 'b': 2})");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM map_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0)).hasMap("map_val", entry("a", 1), entry("b", 2));
  }

  @Test
  void shouldDecodeTupleType() {
    // given
    execute(
        "CREATE TABLE tuple_types (id UInt32, tuple_val Tuple(String, Int32)) ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO tuple_types VALUES (1, ('hello', 42))");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM tuple_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0)).hasTuple("tuple_val", "hello", 42);
  }

  @Test
  void shouldDecodeEnumTypes() {
    // given
    execute(
        "CREATE TABLE enum_types (id UInt32, enum8_val Enum8('a' = 1, 'b' = 2), enum16_val Enum16('x' = 1, 'y' = 2)) "
            + "ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO enum_types VALUES (1, 'a', 'y')");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM enum_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0)).hasEnumName("enum8_val", "a").hasEnumName("enum16_val", "y");
  }

  @Test
  void shouldDecodeArrayType() {
    // given
    execute(
        "CREATE TABLE array_types (id UInt32, array_val Array(Int32)) ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO array_types VALUES (1, [10, 20, 30])");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM array_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0)).hasList("array_val", 10, 20, 30);
  }

  @Test
  void shouldDecodeNestedType() {
    // given - ClickHouse flattens Nested(...) into one Array(...) column per sub-field by default
    // (flatten_nested=1), addressed as "<nested_col>.<sub_field>" in both INSERT and SELECT * -
    // so on the wire this is indistinguishable from two ordinary Array columns, exercising the
    // exact same ListDecodingRowBinaryReader/convertArray() path as shouldDecodeArrayType().
    execute(
        "CREATE TABLE nested_types (id UInt32, items Nested(name String, quantity Int32)) "
            + "ENGINE = MergeTree ORDER BY id");
    execute(
        "INSERT INTO nested_types (id, items.name, items.quantity) VALUES "
            + "(1, ['apple', 'banana'], [3, 5])");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM nested_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0))
        .hasList("items.name", "apple", "banana")
        .hasList("items.quantity", 3, 5);
  }

  @Test
  void shouldDecodeLowCardinalityType() {
    // given - confirmed directly against client-v2's own test suite (DataTypeTests, our pinned
    // v0.9.0) rather than assumed: LowCardinality is a "virtual type" there, meaning
    // ClickHouseColumn strips the wrapper and dispatches to the underlying type's reader, the same
    // way Nullable(...) already does - so no new decode path is exercised here, just confirming
    // that unwrapping actually happens end to end through our own pipeline.
    execute(
        "CREATE TABLE low_cardinality_types (id UInt32, category LowCardinality(String)) "
            + "ENGINE = MergeTree ORDER BY id");
    execute("INSERT INTO low_cardinality_types VALUES (1, 'electronics')");

    // when
    final List<Map<String, Object>> rows = queryRows("SELECT * FROM low_cardinality_types");

    // then
    assertThat(rows).hasSize(1);
    assertThatRow(rows.get(0)).hasValue("category", "electronics");
  }
}

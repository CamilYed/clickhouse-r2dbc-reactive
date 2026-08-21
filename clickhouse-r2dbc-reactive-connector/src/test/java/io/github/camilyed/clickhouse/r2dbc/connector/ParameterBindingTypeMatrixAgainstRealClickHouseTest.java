package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves {@link ClickHouseQuery#withParameters(java.util.Map)}'s wire encoding for the types
 * flagged as untested-and-likely-wrong in ROADMAP.md's Phase 8, item 6 — everything beyond the
 * already-covered {@code null}/integer/{@code String}-escaping cases — end to end through {@link
 * ClickHouseStatement#bind}, a real ClickHouse server, and back through {@link ClickHouseRow#get}.
 * Each test binds a value, round-trips it through a real table column of the matching ClickHouse
 * type, and asserts the decoded value equals what was bound — proving the wire format actually
 * round-trips rather than just inspecting the encoded string in isolation (that string-level
 * coverage already exists in {@code core}'s {@code ClickHouseQueryTest}, which this class
 * deliberately doesn't duplicate).
 *
 * <p>Deliberately out of scope here, left as an explicit follow-up rather than silently untested:
 * {@code Map} and {@code Tuple} bound-parameter values — neither has an unambiguous single Java
 * type to bind (unlike {@code List} for {@code Array}), and ClickHouse's own "Queries with
 * Parameters" support for composite {@code Map}/{@code Tuple} literals as parameter values is less
 * settled than the scalar/array cases covered here.
 */
class ParameterBindingTypeMatrixAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  private enum Status {
    ACTIVE,
    INACTIVE
  }

  private ClickHouseHttpTransport transport;

  private ClickHouseHttpTransport transport() {
    if (transport == null) {
      transport =
          new ClickHouseHttpTransport(
              clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    }
    return transport;
  }

  private final RowDecodingScheduler decodingScheduler = RowDecodingScheduler.defaults();

  private ClickHouseConnection connection() {
    return new ClickHouseConnection(transport(), decodingScheduler);
  }

  @Test
  void shouldRoundTripAUuidParameter() {
    // given
    execute("CREATE TABLE uuid_param_test (id UUID) ENGINE = Memory");
    final UUID bound = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");

    // when
    final UUID decoded =
        Flux.from(
                connection()
                    .createStatement("INSERT INTO uuid_param_test VALUES ({id:UUID})")
                    .bind("id", bound)
                    .execute())
            .thenMany(select("SELECT id FROM uuid_param_test", "id", UUID.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isEqualTo(bound);
  }

  @Test
  void shouldRoundTripABigDecimalParameter() {
    // given
    execute("CREATE TABLE decimal_param_test (amount Decimal(18,4)) ENGINE = Memory");
    final BigDecimal bound = new BigDecimal("12345.6789");

    // when
    final BigDecimal decoded =
        Flux.from(
                connection()
                    .createStatement(
                        "INSERT INTO decimal_param_test VALUES ({amount:Decimal(18,4)})")
                    .bind("amount", bound)
                    .execute())
            .thenMany(select("SELECT amount FROM decimal_param_test", "amount", BigDecimal.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isEqualByComparingTo(bound);
  }

  @Test
  void shouldRoundTripALocalDateParameter() {
    // given
    execute("CREATE TABLE date_param_test (day Date) ENGINE = Memory");
    final LocalDate bound = LocalDate.of(2024, 6, 15);

    // when
    final LocalDate decoded =
        Flux.from(
                connection()
                    .createStatement("INSERT INTO date_param_test VALUES ({day:Date})")
                    .bind("day", bound)
                    .execute())
            .thenMany(select("SELECT day FROM date_param_test", "day", LocalDate.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isEqualTo(bound);
  }

  @Test
  void shouldRoundTripALocalDateTimeParameter() {
    // given
    execute("CREATE TABLE local_date_time_param_test (at DateTime) ENGINE = Memory");
    final LocalDateTime bound = LocalDateTime.of(2024, 1, 15, 10, 30, 15);

    // when
    final LocalDateTime decoded =
        Flux.from(
                connection()
                    .createStatement(
                        "INSERT INTO local_date_time_param_test VALUES ({at:DateTime})")
                    .bind("at", bound)
                    .execute())
            .thenMany(
                select("SELECT at FROM local_date_time_param_test", "at", LocalDateTime.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isEqualTo(bound);
  }

  @Test
  void shouldRoundTripAnInstantParameterNormalizedToUtc() {
    // given - an Instant built from a non-UTC offset, to prove the UTC normalization actually
    // happens rather than accidentally sending the wall-clock local time
    execute("CREATE TABLE instant_param_test (at DateTime) ENGINE = Memory");
    final Instant bound =
        OffsetDateTime.of(2024, 1, 15, 12, 30, 15, 0, ZoneOffset.ofHours(2)).toInstant();

    // when
    final Instant decoded =
        Flux.from(
                connection()
                    .createStatement("INSERT INTO instant_param_test VALUES ({at:DateTime})")
                    .bind("at", bound)
                    .execute())
            .thenMany(select("SELECT at FROM instant_param_test", "at", Instant.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isEqualTo(bound);
  }

  @Test
  void shouldRoundTripABooleanParameter() {
    // given
    execute("CREATE TABLE bool_param_test (flag Bool) ENGINE = Memory");

    // when
    final Boolean decoded =
        Flux.from(
                connection()
                    .createStatement("INSERT INTO bool_param_test VALUES ({flag:Bool})")
                    .bind("flag", true)
                    .execute())
            .thenMany(select("SELECT flag FROM bool_param_test", "flag", Boolean.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isTrue();
  }

  @Test
  void shouldRoundTripAJavaEnumConstantAsItsName() {
    // given - proves a plain java.lang.Enum bound value encodes as its constant name (the generic
    // toString() fallback), not any ClickHouse Enum column specifically - see class Javadoc
    execute("CREATE TABLE enum_param_test (status String) ENGINE = Memory");

    // when
    final String decoded =
        Flux.from(
                connection()
                    .createStatement("INSERT INTO enum_param_test VALUES ({status:String})")
                    .bind("status", Status.ACTIVE)
                    .execute())
            .thenMany(select("SELECT status FROM enum_param_test", "status", String.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isEqualTo("ACTIVE");
  }

  @Test
  void shouldRoundTripAnIpv4AddressParameter() throws UnknownHostException {
    // given
    execute("CREATE TABLE ipv4_param_test (address IPv4) ENGINE = Memory");

    // when
    final InetAddress decoded =
        Flux.from(
                connection()
                    .createStatement("INSERT INTO ipv4_param_test VALUES ({address:IPv4})")
                    .bind("address", "192.168.1.1")
                    .execute())
            .thenMany(select("SELECT address FROM ipv4_param_test", "address", InetAddress.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).isEqualTo(InetAddress.getByName("192.168.1.1"));
  }

  @Test
  void shouldRoundTripANumericArrayParameter() {
    // given - UInt32 doesn't fit in a signed Java int, so the driver decodes each element as
    // Long (same widening already established for scalar UInt32 columns elsewhere), not Integer
    execute("CREATE TABLE numeric_array_param_test (nums Array(UInt32)) ENGINE = Memory");

    // when
    final List<?> decoded =
        Flux.from(
                connection()
                    .createStatement(
                        "INSERT INTO numeric_array_param_test VALUES ({nums:Array(UInt32)})")
                    .bind("nums", List.of(1, 2, 3))
                    .execute())
            .thenMany(select("SELECT nums FROM numeric_array_param_test", "nums", List.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).asInstanceOf(LIST).containsExactly(1L, 2L, 3L);
  }

  @Test
  void shouldRoundTripAStringArrayParameter() {
    // given
    execute("CREATE TABLE string_array_param_test (labels Array(String)) ENGINE = Memory");

    // when
    final List<?> decoded =
        Flux.from(
                connection()
                    .createStatement(
                        "INSERT INTO string_array_param_test VALUES ({labels:Array(String)})")
                    .bind("labels", List.of("it's", "plain"))
                    .execute())
            .thenMany(select("SELECT labels FROM string_array_param_test", "labels", List.class))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(decoded).asInstanceOf(LIST).containsExactly("it's", "plain");
  }

  private <T> Flux<T> select(final String sql, final String column, final Class<T> type) {
    return Flux.from(connection().createStatement(sql).execute())
        .flatMap(result -> result.map((row, rowMetadata) -> row.get(column, type)));
  }

  private void execute(final String sql) {
    transport()
        .query(ClickHouseQuery.of(sql))
        .aggregate()
        .asByteArray()
        .block(Duration.ofSeconds(10));
  }
}

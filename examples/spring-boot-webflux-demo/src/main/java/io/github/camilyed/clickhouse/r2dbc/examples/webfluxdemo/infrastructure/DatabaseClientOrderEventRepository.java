package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.infrastructure;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEvent;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEventRepository;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderStatus;
import io.r2dbc.spi.Readable;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link OrderEventRepository} implemented against Spring's {@link DatabaseClient}, backed by the
 * {@code ConnectionFactory}/{@code DatabaseClient} beans {@code R2dbcConfiguration} builds.
 *
 * <p>Every method here issues fully pre-formed SQL with values embedded directly as escaped
 * literals — deliberately never {@code DatabaseClient.bind(...)} or {@code R2dbcEntityTemplate}'s
 * object-mapped insert/query. See {@code R2dbcConfiguration}'s Javadoc for the full reasoning:
 * Spring's {@code :name}/{@code .bind(...)} machinery rewrites placeholders using a {@link
 * org.springframework.r2dbc.core.binding.BindMarkersFactory} that has no way to carry the
 * ClickHouse-specific type this driver's own {@code {name:Type}} parameter syntax requires inline,
 * so it doesn't actually work against this driver yet — confirmed the hard way while building this
 * adapter, not assumed.
 *
 * <p>Rows are mapped back via {@code Row.get(name, Class)} with hand-picked target types rather
 * than Spring Data's converters — two real, driver-wide gotchas surfaced doing that, both
 * documented here rather than hidden behind a silently-"working" helper:
 *
 * <ul>
 *   <li>{@code count()}'s {@code UInt64} result is explicitly cast to {@code UInt32} server-side
 *       before decoding: this driver decodes ClickHouse {@code UInt64} as {@link
 *       java.math.BigInteger} (client-v2's own RowBinary reader does the same — verified against
 *       its pinned source, not assumed), not {@code Long}; {@code Row.get} only casts the
 *       already-decoded value rather than widening it, so asking for {@code Long.class} against a
 *       raw {@code UInt64} column throws {@link ClassCastException}. A real application counting
 *       rows that could exceed {@code UInt32}'s range would request {@link java.math.BigInteger}
 *       instead.
 *   <li>{@code status} ({@code Enum8}) decodes as client-v2's own internal {@code EnumValue}, not a
 *       plain {@link String} — asking {@code Row.get(name, String.class)} for it throws {@link
 *       ClassCastException} the same way. Read here via {@code Object.class} and {@code
 *       toString()}, then parsed into {@link OrderStatus} — see that enum's own Javadoc. Worth
 *       flagging as a possible future driver improvement (decoding {@code Enum8}/{@code Enum16} as
 *       plain {@link String} directly, rather than leaking an internal client-v2 type through the
 *       public R2DBC {@code Row} surface), not something this demo tries to fix itself.
 * </ul>
 */
@Repository
class DatabaseClientOrderEventRepository implements OrderEventRepository {

  private static final DateTimeFormatter CLICKHOUSE_DATETIME64 =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

  private final DatabaseClient databaseClient;

  DatabaseClientOrderEventRepository(final DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  @Override
  public Mono<Void> save(final OrderEvent event) {
    final String sql =
        "INSERT INTO order_events "
            + "(id, customer_id, category, tags, amount, discount, status, client_ip, occurred_at) "
            + "VALUES ('"
            + event.id()
            + "', '"
            + event.customerId()
            + "', '"
            + escapeForClickHouseLiteral(event.category())
            + "', "
            + toArrayLiteral(event.tags())
            + ", "
            + event.amount().toPlainString()
            + ", "
            + toDecimalLiteral(event.discount())
            + ", '"
            + event.status()
            + "', '"
            + escapeForClickHouseLiteral(event.clientIp())
            + "', '"
            + CLICKHOUSE_DATETIME64.format(event.occurredAt())
            + "')";
    return databaseClient.sql(sql).fetch().rowsUpdated().then();
  }

  @Override
  public Flux<OrderEvent> findAll() {
    return databaseClient
        .sql(
            "SELECT id, customer_id, category, tags, amount, discount, status, client_ip, "
                + "occurred_at FROM order_events ORDER BY occurred_at")
        .map(DatabaseClientOrderEventRepository::toOrderEvent)
        .all();
  }

  @Override
  public Mono<Long> count() {
    return databaseClient
        .sql("SELECT toUInt32(count()) AS total FROM order_events")
        .map(row -> row.get("total", Long.class))
        .one();
  }

  @Override
  public Flux<CategoryTotal> totalAmountByCategory() {
    return databaseClient
        .sql(
            "SELECT category, sum(amount) AS total_amount FROM order_events "
                + "GROUP BY category ORDER BY total_amount DESC")
        .map(
            row ->
                new CategoryTotal(
                    row.get("category", String.class), row.get("total_amount", BigDecimal.class)))
        .all();
  }

  @SuppressWarnings("unchecked")
  private static OrderEvent toOrderEvent(final Readable row) {
    return new OrderEvent(
        row.get("id", UUID.class),
        row.get("customer_id", UUID.class),
        row.get("category", String.class),
        (List<String>) (List<?>) row.get("tags", List.class),
        row.get("amount", BigDecimal.class),
        Optional.ofNullable(row.get("discount", BigDecimal.class)),
        OrderStatus.valueOf(row.get("status", Object.class).toString()),
        row.get("client_ip", InetAddress.class).getHostAddress(),
        row.get("occurred_at", ZonedDateTime.class).toInstant());
  }

  private static String toArrayLiteral(final List<String> values) {
    return values.stream()
        .map(value -> "'" + escapeForClickHouseLiteral(value) + "'")
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private static String toDecimalLiteral(final Optional<BigDecimal> amount) {
    return amount.map(BigDecimal::toPlainString).orElse("NULL");
  }

  /**
   * Doubles embedded single quotes, matching the same literal-embedding convention {@code
   * transport-http}'s {@code ClickHouseHttpTransport.escapeForSingleQuotedSql} already uses (and
   * has proven against real ClickHouse) for its own {@code KILL QUERY WHERE query_id = '...'}
   * construction — not invented fresh for this demo.
   */
  private static String escapeForClickHouseLiteral(final String value) {
    return value.replace("'", "''");
  }
}

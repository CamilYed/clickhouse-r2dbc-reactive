package io.github.camilyed.clickhouse.r2dbc.core;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

/**
 * A query to send to ClickHouse: the SQL text, its {@code query_id}, any bound values for
 * ClickHouse's own {@code {name:Type}} parameterized-query placeholders, and any server-side {@code
 * settings} to apply while running it (e.g. {@code max_execution_time}).
 *
 * <p>{@code query_id} is ClickHouse's own request correlator — sent as a request header, echoed
 * back by the server in responses/errors, and required for server-side {@code KILL QUERY}
 * cancellation semantics (see {@code docs/CLIENT_V2_HTTP_REFERENCE.md}). A caller may supply one
 * explicitly, e.g. to correlate a query with their own application logs, or let one be generated —
 * either way every {@code ClickHouseQuery} has one; there is no "no query_id" state to null-check
 * for downstream.
 *
 * <p>{@code parameters} carries values already encoded into ClickHouse's own {@code param_<name>}
 * wire representation (its "Escaped" text format) — see {@link #withParameters(Map)}. Checked
 * directly against clickhouse.com/docs/interfaces/http's "Queries with parameters" section, not
 * assumed: the SQL text itself declares each placeholder as {@code {name:Type}}; the matching value
 * travels as a separate {@code param_name=<value>} request parameter, never substituted into the
 * SQL text itself.
 *
 * <p>{@code settings} is a distinct, unrelated mechanism: raw ClickHouse server settings (e.g.
 * {@code max_execution_time}), sent as plain {@code <name>=<value>} request parameters — no {@code
 * param_} prefix, and no corresponding {@code {name:Type}} placeholder in the SQL text — see {@link
 * #withSettings(Map)}.
 *
 * <p>{@code serverErrorRetryEnabled} is {@code false} for every query unless a caller opts in via
 * {@link #withServerErrorRetryEnabled()} — see that method's Javadoc for exactly what it changes
 * and why this is a per-query opt-in rather than a connection-wide setting.
 */
public record ClickHouseQuery(
    String sql,
    String queryId,
    Map<String, String> parameters,
    Map<String, String> settings,
    boolean serverErrorRetryEnabled) {

  /** A query with a freshly generated {@code query_id}, no bound parameters, no settings. */
  public static ClickHouseQuery of(final String sql) {
    return new ClickHouseQuery(sql, UUID.randomUUID().toString(), Map.of(), Map.of(), false);
  }

  /**
   * A query correlated with a caller-supplied {@code query_id}, no bound parameters, no settings.
   */
  public static ClickHouseQuery of(final String sql, final String queryId) {
    return new ClickHouseQuery(sql, queryId, Map.of(), Map.of(), false);
  }

  /**
   * The distinct {@code {name:Type}} parameter names {@code sql} declares, in first-occurrence
   * order. Skips single-quoted string literals, double-quoted/backtick-quoted identifiers, and
   * every comment form ClickHouse's own lexer accepts (including nested block comments) while
   * scanning, so placeholder-shaped text sitting inside any of those is never mistaken for a real
   * bind parameter — see {@link ClickHouseSqlPlaceholderScanner}'s Javadoc for the full reasoning
   * and what this deliberately still doesn't attempt (it's a scanner for exactly these spans, not a
   * SQL parser).
   */
  public static List<String> parameterNamesIn(final String sql) {
    return ClickHouseSqlPlaceholderScanner.parameterNamesIn(sql);
  }

  /**
   * This query with {@code boundValues} encoded into ClickHouse's own {@code param_<name>} wire
   * representation and attached as {@link #parameters()}. A {@code null} value encodes to
   * ClickHouse's own null marker, {@code \N}.
   *
   * <p><b>Type-specific encoding, beyond the {@code String}/{@code null} cases above:</b>
   *
   * <ul>
   *   <li>{@link java.time.LocalDate}, {@link UUID}, {@link java.math.BigDecimal}, {@link Boolean},
   *       and any {@code enum} constant already round-trip correctly through Java's own {@code
   *       toString()} — {@code LocalDate} matches ClickHouse's {@code Date} literal format
   *       (ISO-8601, {@code yyyy-MM-dd}) exactly, and ClickHouse's {@code Bool} type explicitly
   *       accepts the lowercase {@code true}/{@code false} text {@code Boolean#toString()} produces
   *       (verified against ClickHouse's own Bool-type documentation, not assumed) — so no
   *       special-casing is needed for them.
   *   <li>{@link LocalDateTime}, {@link Instant}, {@link OffsetDateTime}, and {@link ZonedDateTime}
   *       are formatted as {@code yyyy-MM-dd HH:mm:ss} — a space, not Java's {@code toString()}
   *       {@code T} separator, since ClickHouse's own "Queries with Parameters" documentation gives
   *       {@code 2024-01-15 10:30:15} as the {@code DateTime} literal form. {@code Instant}/{@code
   *       OffsetDateTime}/{@code ZonedDateTime} are normalized to UTC first ({@code
   *       withZoneSameInstant}/{@code toInstant}), since a {@code DateTime} parameter carries no
   *       timezone of its own on the wire. Sub-second precision is deliberately truncated to whole
   *       seconds — this always parses cleanly against a plain {@code DateTime} column; a caller
   *       binding against a {@code DateTime64} column that needs fractional-second precision should
   *       bind an already-formatted {@code String} instead, since this driver has no way to know a
   *       placeholder's declared ClickHouse type from the Java value alone.
   *   <li>{@link List} is encoded as a ClickHouse {@code Array} literal, {@code [e1,e2,...]} —
   *       {@code String} elements are single-quoted and escaped (backslash/quote/tab/newline/CR);
   *       every other element type is encoded the same way a top-level scalar of that type would
   *       be. Nested {@code List}s (arrays of arrays) are not yet supported and fail fast with
   *       {@link UnsupportedOperationException} rather than silently producing a wrong literal.
   * </ul>
   */
  public ClickHouseQuery withParameters(final Map<String, Object> boundValues) {
    final Map<String, String> encoded = new LinkedHashMap<>();
    boundValues.forEach((name, value) -> encoded.put(name, encodeParameterValue(value)));
    return new ClickHouseQuery(
        sql, queryId, Map.copyOf(encoded), settings, serverErrorRetryEnabled);
  }

  /**
   * This query with {@code newSettings} attached as {@link #settings()}, replacing any previously
   * attached settings — e.g. {@code max_execution_time}, ClickHouse's own server-side query
   * execution time limit (see {@code ClickHouseConnection#setStatementTimeout}). Unlike {@link
   * #withParameters(Map)}, values here are sent to ClickHouse exactly as given, with no escaping or
   * wire-format encoding: they are raw {@code <name>=<value>} request parameters, not tied to any
   * {@code {name:Type}} placeholder declared in {@code sql}.
   */
  public ClickHouseQuery withSettings(final Map<String, String> newSettings) {
    return new ClickHouseQuery(
        sql, queryId, parameters, Map.copyOf(newSettings), serverErrorRetryEnabled);
  }

  /**
   * This query with {@link #serverErrorRetryEnabled()} set to {@code true} — an explicit, per-query
   * opt-in to retrying it after a ClickHouse server error that {@code
   * ServerException.isRetryable()} classifies as retryable, on top of the transport's existing
   * pre-send-only retry (see {@code RetryPolicy}'s Javadoc for that default behavior, unaffected by
   * this flag).
   *
   * <p>Deliberately a per-query opt-in rather than a connection-wide setting: a single connection
   * is routinely used for both reads and writes, and this transport has no reliable way to tell a
   * {@code SELECT} from a literal-SQL {@code INSERT} by inspecting {@code sql} (CTEs,
   * multi-statement text, and an {@code INSERT ... SELECT} would all defeat a naive check) — only
   * the caller who wrote the query actually knows whether retrying it after a partial server-side
   * attempt is safe. Call this only for queries that are safe to fully re-execute, i.e. read-only
   * {@code SELECT}s; never for an {@code INSERT} or any other statement with a side effect, since a
   * retryable-looking server error offers no guarantee the statement didn't already partially or
   * fully apply server-side.
   *
   * <p>Even with this enabled, the transport still never retries once any response bytes have
   * already been delivered downstream — this only widens <em>when</em> a retry may be attempted
   * (also after the request was fully sent, not just before), never removes the "nothing was
   * delivered yet" safety condition that makes a retry harmless to the caller.
   */
  public ClickHouseQuery withServerErrorRetryEnabled() {
    return new ClickHouseQuery(sql, queryId, parameters, settings, true);
  }

  private static final DateTimeFormatter CLICKHOUSE_DATE_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);

  // value is genuinely nullable here - bindNull(...) stores a null entry in boundValues, and this
  // method's job is precisely to turn that into ClickHouse's own null marker below.
  private static String encodeParameterValue(final @Nullable Object value) {
    if (value == null) {
      return "\\N";
    }
    if (value instanceof String string) {
      return escapeForClickHouseParam(string);
    }
    if (value instanceof LocalDateTime localDateTime) {
      return CLICKHOUSE_DATE_TIME_FORMAT.format(localDateTime);
    }
    if (value instanceof Instant instant) {
      return CLICKHOUSE_DATE_TIME_FORMAT.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return CLICKHOUSE_DATE_TIME_FORMAT.format(
          offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
    }
    if (value instanceof ZonedDateTime zonedDateTime) {
      return CLICKHOUSE_DATE_TIME_FORMAT.format(
          zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
    }
    if (value instanceof List<?> list) {
      return encodeArrayParameterValue(list);
    }
    return value.toString();
  }

  private static String escapeForClickHouseParam(final String value) {
    final StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      switch (c) {
        case '\\' -> escaped.append("\\\\");
        case '\t' -> escaped.append("\\t");
        case '\n' -> escaped.append("\\n");
        case '\r' -> escaped.append("\\r");
        default -> escaped.append(c);
      }
    }
    return escaped.toString();
  }

  private static String encodeArrayParameterValue(final List<?> values) {
    return values.stream()
        .map(ClickHouseQuery::encodeArrayElement)
        .collect(Collectors.joining(",", "[", "]"));
  }

  // element is genuinely nullable here - a caller may legitimately bind a List containing null
  // entries (e.g. Array(Nullable(String))), same reasoning as encodeParameterValue above.
  private static String encodeArrayElement(final @Nullable Object element) {
    if (element == null) {
      return "NULL";
    }
    if (element instanceof String string) {
      return "'" + escapeForClickHouseArrayString(string) + "'";
    }
    if (element instanceof List<?>) {
      throw new UnsupportedOperationException(
          "Nested arrays are not yet supported as a bound parameter value");
    }
    return encodeParameterValue(element);
  }

  private static String escapeForClickHouseArrayString(final String value) {
    final StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      switch (c) {
        case '\\' -> escaped.append("\\\\");
        case '\'' -> escaped.append("\\'");
        case '\t' -> escaped.append("\\t");
        case '\n' -> escaped.append("\\n");
        case '\r' -> escaped.append("\\r");
        default -> escaped.append(c);
      }
    }
    return escaped.toString();
  }
}

package io.github.camilyed.clickhouse.r2dbc.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 */
public record ClickHouseQuery(
    String sql, String queryId, Map<String, String> parameters, Map<String, String> settings) {

  /** A query with a freshly generated {@code query_id}, no bound parameters, no settings. */
  public static ClickHouseQuery of(final String sql) {
    return new ClickHouseQuery(sql, UUID.randomUUID().toString(), Map.of(), Map.of());
  }

  /**
   * A query correlated with a caller-supplied {@code query_id}, no bound parameters, no settings.
   */
  public static ClickHouseQuery of(final String sql, final String queryId) {
    return new ClickHouseQuery(sql, queryId, Map.of(), Map.of());
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
   */
  public ClickHouseQuery withParameters(final Map<String, Object> boundValues) {
    final Map<String, String> encoded = new LinkedHashMap<>();
    boundValues.forEach((name, value) -> encoded.put(name, encodeParameterValue(value)));
    return new ClickHouseQuery(sql, queryId, Map.copyOf(encoded), settings);
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
    return new ClickHouseQuery(sql, queryId, parameters, Map.copyOf(newSettings));
  }

  // value is genuinely nullable here - bindNull(...) stores a null entry in boundValues, and this
  // method's job is precisely to turn that into ClickHouse's own null marker below.
  private static String encodeParameterValue(final @Nullable Object value) {
    if (value == null) {
      return "\\N";
    }
    if (value instanceof String string) {
      return escapeForClickHouseParam(string);
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
}

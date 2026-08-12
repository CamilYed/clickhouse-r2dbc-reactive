package io.github.camilyed.clickhouse.r2dbc.core;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A query to send to ClickHouse: the SQL text, its {@code query_id}, and any bound values for
 * ClickHouse's own {@code {name:Type}} parameterized-query placeholders.
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
 */
public record ClickHouseQuery(String sql, String queryId, Map<String, String> parameters) {

  private static final Pattern PARAMETER_PLACEHOLDER =
      Pattern.compile("\\{([a-zA-Z_][a-zA-Z0-9_]*):[^}]+}");

  /** A query with a freshly generated {@code query_id} and no bound parameters. */
  public static ClickHouseQuery of(final String sql) {
    return new ClickHouseQuery(sql, UUID.randomUUID().toString(), Map.of());
  }

  /** A query correlated with a caller-supplied {@code query_id} and no bound parameters. */
  public static ClickHouseQuery of(final String sql, final String queryId) {
    return new ClickHouseQuery(sql, queryId, Map.of());
  }

  /**
   * The distinct {@code {name:Type}} parameter names {@code sql} declares, in first-occurrence
   * order. Does not attempt to distinguish a real placeholder from incidental {@code {...:...}}
   * text inside a string literal or comment — a known, narrow limitation of this regex-based scan.
   */
  public static List<String> parameterNamesIn(final String sql) {
    final Set<String> names = new LinkedHashSet<>();
    final Matcher matcher = PARAMETER_PLACEHOLDER.matcher(sql);
    while (matcher.find()) {
      names.add(matcher.group(1));
    }
    return List.copyOf(names);
  }

  /**
   * This query with {@code boundValues} encoded into ClickHouse's own {@code param_<name>} wire
   * representation and attached as {@link #parameters()}. A {@code null} value encodes to
   * ClickHouse's own null marker, {@code \N}.
   */
  public ClickHouseQuery withParameters(final Map<String, Object> boundValues) {
    final Map<String, String> encoded = new LinkedHashMap<>();
    boundValues.forEach((name, value) -> encoded.put(name, encodeParameterValue(value)));
    return new ClickHouseQuery(sql, queryId, Map.copyOf(encoded));
  }

  private static String encodeParameterValue(final Object value) {
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

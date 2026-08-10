package io.github.camilyed.clickhouse.r2dbc.core;

import java.util.UUID;

/**
 * A query to send to ClickHouse: the SQL text plus its {@code query_id}.
 *
 * <p>{@code query_id} is ClickHouse's own request correlator — sent as a request header, echoed
 * back by the server in responses/errors, and required for server-side {@code KILL QUERY}
 * cancellation semantics (see {@code docs/CLIENT_V2_HTTP_REFERENCE.md}). A caller may supply one
 * explicitly, e.g. to correlate a query with their own application logs, or let one be generated —
 * either way every {@code ClickHouseQuery} has one; there is no "no query_id" state to null-check
 * for downstream.
 */
public record ClickHouseQuery(String sql, String queryId) {

    /** A query with a freshly generated {@code query_id}. */
    public static ClickHouseQuery of(final String sql) {
        return new ClickHouseQuery(sql, UUID.randomUUID().toString());
    }

    /** A query correlated with a caller-supplied {@code query_id}. */
    public static ClickHouseQuery of(final String sql, final String queryId) {
        return new ClickHouseQuery(sql, queryId);
    }
}

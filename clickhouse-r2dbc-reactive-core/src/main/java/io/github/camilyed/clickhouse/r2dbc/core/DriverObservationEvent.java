package io.github.camilyed.clickhouse.r2dbc.core;

/**
 * The closed set of lifecycle events a {@link DriverObservationListener} can receive for one query
 * attempt — exactly one of {@link QueryCompletedEvent}, {@link QueryFailedEvent}, or {@link
 * QueryCancelledEvent} eventually follows a {@link QueryStartedEvent}, or none of the three does at
 * all if the caller never subscribes to the returned {@code Result}'s rows (see {@link
 * QueryCompletedEvent}'s Javadoc).
 */
public sealed interface DriverObservationEvent
    permits QueryStartedEvent, QueryCompletedEvent, QueryFailedEvent, QueryCancelledEvent {

  /** ClickHouse's own {@code query_id} for the request this event reports on. */
  String queryId();

  /** Whether this event reports on a {@code SELECT}/DDL/DML statement or a streamed insert. */
  OperationKind operationKind();

  /** A short, non-reversible identifier for the SQL text — never the SQL text itself. */
  SqlFingerprint sqlFingerprint();
}

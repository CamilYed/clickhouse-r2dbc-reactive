package io.github.camilyed.clickhouse.r2dbc.core;

import java.time.Duration;

/**
 * Fired by {@link DriverObservationListener#queryCompleted} once a query's outcome is fully known.
 *
 * <p>For {@link OperationKind#QUERY}, this only fires once the returned {@code Result}'s rows have
 * actually been consumed — via {@code Result.map}/{@code Result.flatMap}, never merely {@code
 * Result.getRowsUpdated()} alone. This driver never eagerly drains a response body a caller hasn't
 * asked for (see {@code ClickHouseHttpTransport}'s own backpressure-first design), so there is no
 * well-defined row/byte count to report if rows are never subscribed to; a caller that only reads
 * {@code getRowsUpdated()} never triggers this event at all for that query. This is a deliberate,
 * documented v1 limitation, not an oversight — richer coverage of that path is separately scoped
 * future work.
 *
 * <p>For {@link OperationKind#INSERT}, {@code rowCount} is the written-row count ClickHouse itself
 * reports and {@code byteCount} is the number of request bytes sent — both known as soon as the
 * request completes, with no row-by-row streaming involved. {@code timeToFirstRow} is always {@link
 * Duration#ZERO} for an insert: there is no discrete "first row" to time, since nothing is streamed
 * back.
 */
public record QueryCompletedEvent(
    String queryId,
    OperationKind operationKind,
    SqlFingerprint sqlFingerprint,
    Duration totalTime,
    Duration timeToFirstRow,
    long rowCount,
    long byteCount)
    implements DriverObservationEvent {}

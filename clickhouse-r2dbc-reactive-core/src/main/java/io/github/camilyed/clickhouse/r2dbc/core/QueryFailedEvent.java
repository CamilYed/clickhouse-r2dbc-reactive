package io.github.camilyed.clickhouse.r2dbc.core;

import java.time.Duration;

/**
 * Fired by {@link DriverObservationListener#queryFailed} when a query fails — a transport failure,
 * a ClickHouse server error, or a local decode bug — before or while its {@code Result} is being
 * produced or consumed.
 */
public record QueryFailedEvent(
    String queryId,
    OperationKind operationKind,
    SqlFingerprint sqlFingerprint,
    Duration totalTime,
    Throwable cause)
    implements DriverObservationEvent {}

package io.github.camilyed.clickhouse.r2dbc.core;

import java.time.Duration;

/**
 * Fired by {@link DriverObservationListener#queryCancelled} when a query is cancelled — the
 * subscriber cancels its subscription — before it completes or fails.
 */
public record QueryCancelledEvent(
    String queryId, OperationKind operationKind, SqlFingerprint sqlFingerprint, Duration totalTime)
    implements DriverObservationEvent {}

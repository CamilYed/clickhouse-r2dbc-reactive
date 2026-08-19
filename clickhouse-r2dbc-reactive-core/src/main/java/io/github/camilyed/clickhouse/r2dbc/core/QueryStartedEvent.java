package io.github.camilyed.clickhouse.r2dbc.core;

/** Fired by {@link DriverObservationListener#queryStarted} when a query begins. */
public record QueryStartedEvent(
    String queryId, OperationKind operationKind, SqlFingerprint sqlFingerprint)
    implements DriverObservationEvent {}

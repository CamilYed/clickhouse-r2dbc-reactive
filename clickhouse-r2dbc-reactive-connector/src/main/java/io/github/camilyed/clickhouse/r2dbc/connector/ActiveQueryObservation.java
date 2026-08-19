package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCancelledEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCompletedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryFailedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryStartedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.SqlFingerprint;
import java.time.Duration;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * The real {@link QueryObservation}: fingerprints {@code sql}, timestamps the attempt, and forwards
 * every lifecycle event to a {@code listener} that has already reported {@link
 * DriverObservationListener#isEnabled()} as {@code true} — see {@link QueryObservation#start}, the
 * only place this class is constructed from.
 */
final class ActiveQueryObservation implements QueryObservation {

  private final DriverObservationListener listener;
  private final String queryId;
  private final OperationKind operationKind;
  private final SqlFingerprint sqlFingerprint;
  private final Instant startedAt;
  private @Nullable Instant firstRowAt;

  private ActiveQueryObservation(
      final DriverObservationListener listener,
      final String queryId,
      final OperationKind operationKind,
      final SqlFingerprint sqlFingerprint,
      final Instant startedAt) {
    this.listener = listener;
    this.queryId = queryId;
    this.operationKind = operationKind;
    this.sqlFingerprint = sqlFingerprint;
    this.startedAt = startedAt;
  }

  static ActiveQueryObservation start(
      final DriverObservationListener listener,
      final String queryId,
      final OperationKind operationKind,
      final String sql) {
    final SqlFingerprint fingerprint = SqlFingerprint.of(sql);
    final ActiveQueryObservation observation =
        new ActiveQueryObservation(listener, queryId, operationKind, fingerprint, Instant.now());
    listener.queryStarted(new QueryStartedEvent(queryId, operationKind, fingerprint));
    return observation;
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void firstRowReceived() {
    if (firstRowAt == null) {
      firstRowAt = Instant.now();
    }
  }

  @Override
  public void completed(final long rowCount, final long byteCount) {
    final Duration timeToFirstRow =
        firstRowAt == null ? Duration.ZERO : Duration.between(startedAt, firstRowAt);
    listener.queryCompleted(
        new QueryCompletedEvent(
            queryId,
            operationKind,
            sqlFingerprint,
            elapsedSinceStart(),
            timeToFirstRow,
            rowCount,
            byteCount));
  }

  @Override
  public void failed(final Throwable cause) {
    listener.queryFailed(
        new QueryFailedEvent(queryId, operationKind, sqlFingerprint, elapsedSinceStart(), cause));
  }

  @Override
  public void cancelled() {
    listener.queryCancelled(
        new QueryCancelledEvent(queryId, operationKind, sqlFingerprint, elapsedSinceStart()));
  }

  private Duration elapsedSinceStart() {
    return Duration.between(startedAt, Instant.now());
  }
}

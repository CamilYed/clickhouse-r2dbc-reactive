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
 * Tracks one query attempt's lifecycle against a {@link DriverObservationListener}, from {@link
 * #start} to exactly one of {@link #completed}/{@link #failed}/{@link #cancelled} — the single-use,
 * per-attempt collaborator {@link ClickHouseResult#decode}, {@link ClickHouseStatement}, {@link
 * ClickHouseBatch}, and {@link ClickHouseConnection#insertStreaming} each build one of per query,
 * rather than each hand-rolling event construction and elapsed-time bookkeeping inline — the same
 * "small, focused, stateful collaborator" shape as {@link ResultConsumptionGuard}.
 *
 * <p><b>Not thread-safe</b>, deliberately: reactive streams guarantee serial (non-concurrent)
 * signal delivery to a single subscriber, and every method here is only ever called from within
 * that one subscriber's own callbacks for the query attempt this instance was created for.
 */
final class QueryObservation {

  private final DriverObservationListener listener;
  private final String queryId;
  private final OperationKind operationKind;
  private final SqlFingerprint sqlFingerprint;
  private final Instant startedAt;
  private @Nullable Instant firstRowAt;

  private QueryObservation(
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

  /**
   * Fires {@link DriverObservationListener#queryStarted} and returns a tracker for this attempt.
   * {@code sql}'s fingerprint (see {@link SqlFingerprint}) is computed once, here, and reused for
   * every subsequent event this attempt fires.
   */
  static QueryObservation start(
      final DriverObservationListener listener,
      final String queryId,
      final OperationKind operationKind,
      final String sql) {
    final SqlFingerprint fingerprint = SqlFingerprint.of(sql);
    final QueryObservation observation =
        new QueryObservation(listener, queryId, operationKind, fingerprint, Instant.now());
    listener.queryStarted(new QueryStartedEvent(queryId, operationKind, fingerprint));
    return observation;
  }

  /** Records that the first row has been received — every call after the first is a no-op. */
  void firstRowReceived() {
    if (firstRowAt == null) {
      firstRowAt = Instant.now();
    }
  }

  /**
   * Fires {@link DriverObservationListener#queryCompleted}. {@code timeToFirstRow} is {@link
   * Duration#ZERO} if {@link #firstRowReceived()} was never called for this attempt — see {@link
   * QueryCompletedEvent}'s Javadoc for why that's the documented, correct value for an {@link
   * OperationKind#INSERT}, and simply means "no rows were ever consumed" for a {@link
   * OperationKind#QUERY}.
   */
  void completed(final long rowCount, final long byteCount) {
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

  /** Fires {@link DriverObservationListener#queryFailed}. */
  void failed(final Throwable cause) {
    listener.queryFailed(
        new QueryFailedEvent(queryId, operationKind, sqlFingerprint, elapsedSinceStart(), cause));
  }

  /** Fires {@link DriverObservationListener#queryCancelled}. */
  void cancelled() {
    listener.queryCancelled(
        new QueryCancelledEvent(queryId, operationKind, sqlFingerprint, elapsedSinceStart()));
  }

  private Duration elapsedSinceStart() {
    return Duration.between(startedAt, Instant.now());
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;

/**
 * Tracks one query attempt's lifecycle against a {@link DriverObservationListener}, from {@link
 * #start} to exactly one of {@link #completed}/{@link #failed}/{@link #cancelled} — the single-use,
 * per-attempt collaborator {@link ClickHouseResult#decode}, {@link ClickHouseStatement}, {@link
 * ClickHouseBatch}, and {@link ClickHouseConnection#insertStreaming} each build one of per query,
 * rather than each hand-rolling event construction and elapsed-time bookkeeping inline — the same
 * "small, focused, stateful collaborator" shape as {@link ResultConsumptionGuard}.
 *
 * <p>A closed pair of variants rather than one class with an internal on/off flag: {@link
 * ActiveQueryObservation} does the real work {@link DriverObservationListener}'s Javadoc describes
 * (fingerprinting, timestamping, event construction/dispatch); {@link NoopQueryObservation} is a
 * stateless singleton that does none of it. {@link #start} picks between them once, based on {@link
 * DriverObservationListener#isEnabled()} — every caller downstream only ever sees the {@link
 * QueryObservation} interface, never which variant it got, and {@link #isEnabled()} lets {@link
 * ClickHouseResult#decode} and {@link ClickHouseConnection#insertStreaming} skip wiring their own
 * per-chunk/per-row counters entirely when observation was never going to consume them.
 *
 * <p><b>Not thread-safe</b>, deliberately: reactive streams guarantee serial (non-concurrent)
 * signal delivery to a single subscriber, and every method here is only ever called from within
 * that one subscriber's own callbacks for the query attempt this instance was created for.
 */
sealed interface QueryObservation permits ActiveQueryObservation, NoopQueryObservation {

  /**
   * Fires {@link DriverObservationListener#queryStarted} and returns a tracker for this attempt —
   * or, when {@code listener.isEnabled()} is {@code false}, skips fingerprinting/timestamping/event
   * construction entirely and returns the shared {@link NoopQueryObservation} instance instead. See
   * {@link DriverObservationListener#isEnabled()}'s Javadoc for why that's a hard "don't bother", not
   * just a filter applied after the fact.
   */
  static QueryObservation start(
      final DriverObservationListener listener,
      final String queryId,
      final OperationKind operationKind,
      final String sql) {
    return listener.isEnabled()
        ? ActiveQueryObservation.start(listener, queryId, operationKind, sql)
        : NoopQueryObservation.INSTANCE;
  }

  /**
   * Whether this instance actually forwards to a real listener — {@code false} exactly when {@link
   * #start} was given a disabled listener, letting a caller skip wiring counters that would only
   * ever have fed a call this instance is never going to make.
   */
  boolean isEnabled();

  /** Records that the first row has been received — every call after the first is a no-op. */
  void firstRowReceived();

  /**
   * Fires {@link DriverObservationListener#queryCompleted}. {@code timeToFirstRow} is {@link
   * java.time.Duration#ZERO} if {@link #firstRowReceived()} was never called for this attempt — see
   * {@link io.github.camilyed.clickhouse.r2dbc.core.QueryCompletedEvent}'s Javadoc for why that's the
   * documented, correct value for an {@link OperationKind#INSERT}, and simply means "no rows were
   * ever consumed" for a {@link OperationKind#QUERY}.
   */
  void completed(long rowCount, long byteCount);

  /** Fires {@link DriverObservationListener#queryFailed}. */
  void failed(Throwable cause);

  /** Fires {@link DriverObservationListener#queryCancelled}. */
  void cancelled();
}

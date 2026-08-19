package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCancelledEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCompletedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryFailedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryStartedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DriverObservationListener} reference implementation that logs every query lifecycle
 * event, keyed on {@code query_id} so every log line for one query attempt can be correlated by
 * grepping that id — {@link #queryFailed} logs at WARN, every other event at INFO. Registered the
 * same way as any other {@link DriverObservationListener}; see {@code
 * ClickHouseConnectionFactoryProvider#OBSERVATION_LISTENER}.
 *
 * <p>Never logs the SQL text itself — only its {@link
 * io.github.camilyed.clickhouse.r2dbc.core.SqlFingerprint} — same restriction {@link
 * DriverObservationListener}'s own Javadoc documents for every implementation.
 */
public final class Slf4jDriverObservationListener implements DriverObservationListener {

  private static final Logger LOG = LoggerFactory.getLogger(Slf4jDriverObservationListener.class);

  @Override
  public void queryStarted(final QueryStartedEvent event) {
    LOG.info(
        "query_id={} operation={} sql={} started",
        event.queryId(),
        event.operationKind(),
        event.sqlFingerprint());
  }

  @Override
  public void queryCompleted(final QueryCompletedEvent event) {
    LOG.info(
        "query_id={} operation={} sql={} completed totalTime={} timeToFirstRow={} rowCount={}"
            + " byteCount={}",
        event.queryId(),
        event.operationKind(),
        event.sqlFingerprint(),
        event.totalTime(),
        event.timeToFirstRow(),
        event.rowCount(),
        event.byteCount());
  }

  @Override
  public void queryFailed(final QueryFailedEvent event) {
    LOG.warn(
        "query_id={} operation={} sql={} failed after totalTime={}",
        event.queryId(),
        event.operationKind(),
        event.sqlFingerprint(),
        event.totalTime(),
        event.cause());
  }

  @Override
  public void queryCancelled(final QueryCancelledEvent event) {
    LOG.info(
        "query_id={} operation={} sql={} cancelled after totalTime={}",
        event.queryId(),
        event.operationKind(),
        event.sqlFingerprint(),
        event.totalTime());
  }
}

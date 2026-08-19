package io.github.camilyed.clickhouse.r2dbc.connector.fakes;

import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCancelledEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCompletedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryFailedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryStartedEvent;
import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory {@link DriverObservationListener} that records every event it receives, in receipt
 * order — used by tests to assert which events fired and with what data, instead of mocking the
 * listener interface.
 */
public final class RecordingDriverObservationListener implements DriverObservationListener {

  private final List<QueryStartedEvent> started = new ArrayList<>();
  private final List<QueryCompletedEvent> completed = new ArrayList<>();
  private final List<QueryFailedEvent> failed = new ArrayList<>();
  private final List<QueryCancelledEvent> cancelled = new ArrayList<>();
  private final boolean enabled;

  public RecordingDriverObservationListener() {
    this(true);
  }

  private RecordingDriverObservationListener(final boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * A variant reporting {@link #isEnabled()} as {@code false} — still records anything it's called
   * with, so a test can assert the connector actually stopped calling it, rather than merely
   * trusting {@link #isEnabled()}'s own return value.
   */
  public static RecordingDriverObservationListener disabled() {
    return new RecordingDriverObservationListener(false);
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void queryStarted(final QueryStartedEvent event) {
    started.add(event);
  }

  @Override
  public void queryCompleted(final QueryCompletedEvent event) {
    completed.add(event);
  }

  @Override
  public void queryFailed(final QueryFailedEvent event) {
    failed.add(event);
  }

  @Override
  public void queryCancelled(final QueryCancelledEvent event) {
    cancelled.add(event);
  }

  public List<QueryStartedEvent> startedEvents() {
    return started;
  }

  public List<QueryCompletedEvent> completedEvents() {
    return completed;
  }

  public List<QueryFailedEvent> failedEvents() {
    return failed;
  }

  public List<QueryCancelledEvent> cancelledEvents() {
    return cancelled;
  }
}

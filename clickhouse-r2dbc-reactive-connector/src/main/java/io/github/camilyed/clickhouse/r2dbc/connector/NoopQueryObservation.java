package io.github.camilyed.clickhouse.r2dbc.connector;

/**
 * The {@link QueryObservation} used for every query attempt when the configured {@code
 * DriverObservationListener} reports itself disabled — see {@link QueryObservation#start}. A
 * stateless singleton: every method is an immediate no-op, so a caller pays nothing beyond the
 * field reads/branches needed to reach it.
 */
final class NoopQueryObservation implements QueryObservation {

  static final NoopQueryObservation INSTANCE = new NoopQueryObservation();

  private NoopQueryObservation() {}

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public void firstRowReceived() {}

  @Override
  public void completed(final long rowCount, final long byteCount) {}

  @Override
  public void failed(final Throwable cause) {}

  @Override
  public void cancelled() {}
}

package io.github.camilyed.clickhouse.r2dbc.core;

/**
 * A neutral, framework-agnostic hook for observing query lifecycle events — no metrics or tracing
 * library dependency anywhere in this module; an implementation is free to forward these calls into
 * whichever metrics/tracing/logging system a consumer already uses. Every method defaults to a
 * no-op, so an implementation only overrides the events it actually cares about.
 *
 * <p>Registered via {@code ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER} — see that
 * option's Javadoc for how it's wired to a {@code ClickHouseConnectionFactory}. Never the SQL text,
 * bind values, or credentials are passed to any of these methods — see {@link SqlFingerprint}'s
 * Javadoc for why a hash stands in for the SQL text.
 */
public interface DriverObservationListener {

  /** A listener that does nothing — the default when no listener is configured. */
  DriverObservationListener NOOP = new DriverObservationListener() {};

  /** A query has started. */
  default void queryStarted(final QueryStartedEvent event) {}

  /**
   * A query's outcome is fully known and successful — see {@link QueryCompletedEvent}'s Javadoc
   * for exactly when this does and doesn't fire.
   */
  default void queryCompleted(final QueryCompletedEvent event) {}

  /** A query failed. */
  default void queryFailed(final QueryFailedEvent event) {}

  /** A query was cancelled before completing or failing. */
  default void queryCancelled(final QueryCancelledEvent event) {}
}

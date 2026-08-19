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
 *
 * <p><b>{@link #isEnabled()} is a hard "don't bother" switch, not just a filter applied after the
 * fact.</b> {@code NOOP}'s own lifecycle methods are already empty, so overriding them was never
 * the expensive part — computing what would have been passed to them was: a {@link SqlFingerprint}
 * (SHA-256 over the SQL text) plus {@link java.time.Instant#now()} timestamps get built for every
 * query attempt whether or not anything ever reads them, and the connector additionally wires
 * per-row/per-chunk counters purely to feed the eventual {@code queryCompleted} call. A caller
 * whose {@link #isEnabled()} returns {@code false} is promising it wants none of that work done,
 * not just that it won't be called with the result — so an implementation must not return {@code
 * false} while still expecting its overridden lifecycle methods to fire; that combination is a
 * contradiction this interface doesn't try to detect, only to document.
 */
public interface DriverObservationListener {

  /** A listener that does nothing — the default when no listener is configured. */
  DriverObservationListener NOOP =
      new DriverObservationListener() {
        @Override
        public boolean isEnabled() {
          return false;
        }
      };

  /**
   * Whether this listener actually wants to observe queries — {@code true} by default, so an
   * implementation that only overrides the lifecycle methods below behaves exactly as before this
   * method existed. Returning {@code false} lets the connector skip building event data entirely
   * (see this interface's own Javadoc) rather than just skipping the call.
   */
  default boolean isEnabled() {
    return true;
  }

  /** A query has started. */
  default void queryStarted(final QueryStartedEvent event) {}

  /**
   * A query's outcome is fully known and successful — see {@link QueryCompletedEvent}'s Javadoc for
   * exactly when this does and doesn't fire.
   */
  default void queryCompleted(final QueryCompletedEvent event) {}

  /** A query failed. */
  default void queryFailed(final QueryFailedEvent event) {}

  /** A query was cancelled before completing or failing. */
  default void queryCancelled(final QueryCancelledEvent event) {}
}

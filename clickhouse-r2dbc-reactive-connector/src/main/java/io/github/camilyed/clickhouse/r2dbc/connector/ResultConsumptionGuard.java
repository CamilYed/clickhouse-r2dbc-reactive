package io.github.camilyed.clickhouse.r2dbc.connector;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enforces R2DBC's single-consumption contract for a {@link ClickHouseResult} and every {@link
 * ClickHouseResult#filter} view derived from it. A {@link ClickHouseResult} and its {@code
 * filter()}-derived views share one instance of this guard (see {@link ClickHouseResult#filter}),
 * rather than each getting its own, so consuming any one of them marks every other one as consumed
 * too - exactly the property {@link ClickHouseResult}'s own Javadoc previously stated as an
 * unenforced misuse.
 */
final class ResultConsumptionGuard {

  private final AtomicBoolean consumed = new AtomicBoolean(false);

  /** Marks this guard consumed, or throws {@link IllegalStateException} if already consumed. */
  void markConsumedOrFail() {
    if (!consumed.compareAndSet(false, true)) {
      throw new IllegalStateException("This result has already been consumed");
    }
  }

  /**
   * Throws {@link IllegalStateException} if already consumed, without itself marking this guard
   * consumed - {@link ClickHouseResult#filter} is a lazy view, not a terminal operation, so
   * deriving a filtered view must not itself count as consumption.
   */
  void failIfAlreadyConsumed() {
    if (consumed.get()) {
      throw new IllegalStateException("This result has already been consumed");
    }
  }
}

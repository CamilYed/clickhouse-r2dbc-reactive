package io.github.camilyed.clickhouse.r2dbc.core;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The dedicated worker pool {@link RowBinaryDecoder#decode} runs every blocking client-v2 call on —
 * reader construction <b>and</b> every subsequent row read alike — instead of the process-wide
 * {@link Schedulers#boundedElastic()} every other library in the same JVM also reaches for by
 * default.
 *
 * <p>Bounded and named ({@code clickhouse-r2dbc-decoder-*}) so a thread dump immediately identifies
 * these threads as this driver's own decode workers, and so this driver's row-decoding load can
 * neither starve, nor be starved by, unrelated {@code boundedElastic()} work sharing the same
 * process. Sized smaller than {@link Schedulers#boundedElastic()}'s own default on purpose: row
 * decoding is one blocking worker per in-flight query, not the kind of arbitrary, bursty blocking
 * call {@code boundedElastic()} is tuned for.
 *
 * <p><b>Ownership is explicit, not incidental.</b> Whoever constructs an instance owns its
 * lifecycle and is responsible for calling {@link #dispose()} once it's done with it — nothing in
 * this class disposes itself automatically. See {@code
 * io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory} for the shipped
 * connector's ownership: one instance per factory, shared by every {@code Connection} it produces,
 * mirroring how the same factory's {@link Scheduler}-free transport connection pool is already
 * shared rather than recreated per connection.
 */
public final class RowDecodingScheduler {

  private static final String THREAD_NAME_PREFIX = "clickhouse-r2dbc-decoder";

  /**
   * One worker per available processor by default — row decoding is a blocking unit of work per
   * in-flight query, not something that benefits from the 10x-per-core sizing {@link
   * Schedulers#boundedElastic()} uses for arbitrary, unrelated blocking calls. An unbenchmarked
   * default, like {@link RowBinaryDecoder}'s own {@code RESPONSE_CHUNK_DEMAND} — revisit with
   * measurements once this project's performance phase starts (see README.md's "Performance and
   * dependency impact" section).
   */
  private static final int DEFAULT_WORKER_COUNT = Runtime.getRuntime().availableProcessors();

  /** Same unbenchmarked-placeholder status as {@link #DEFAULT_WORKER_COUNT} above. */
  private static final int DEFAULT_QUEUED_TASK_CAPACITY = 10_000;

  private final Scheduler scheduler;

  private RowDecodingScheduler(final Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * A scheduler sized for typical single-connection-factory use — see {@link #DEFAULT_WORKER_COUNT}
   * / {@link #DEFAULT_QUEUED_TASK_CAPACITY}'s Javadoc for the exact defaults and why they're
   * deliberately smaller than {@link Schedulers#boundedElastic()}'s own.
   */
  public static RowDecodingScheduler defaults() {
    return create(DEFAULT_WORKER_COUNT, DEFAULT_QUEUED_TASK_CAPACITY);
  }

  /**
   * @param workerCount the maximum number of backing threads this scheduler ever creates; must be
   *     positive
   * @param queuedTaskCapacity the maximum number of decode tasks queued per worker before further
   *     submissions are rejected; must be positive
   */
  public static RowDecodingScheduler create(final int workerCount, final int queuedTaskCapacity) {
    if (workerCount <= 0) {
      throw new IllegalArgumentException("workerCount must be positive, got: " + workerCount);
    }
    if (queuedTaskCapacity <= 0) {
      throw new IllegalArgumentException(
          "queuedTaskCapacity must be positive, got: " + queuedTaskCapacity);
    }
    return new RowDecodingScheduler(
        Schedulers.newBoundedElastic(workerCount, queuedTaskCapacity, THREAD_NAME_PREFIX));
  }

  /**
   * The underlying Reactor {@link Scheduler} — package-private: only {@link RowBinaryDecoder} needs
   * it.
   */
  Scheduler asReactorScheduler() {
    return scheduler;
  }

  /**
   * Shuts down every backing thread this scheduler owns. Idempotent — safe to call more than once.
   */
  public void dispose() {
    scheduler.dispose();
  }

  /** Whether {@link #dispose()} has already been called. */
  public boolean isDisposed() {
    return scheduler.isDisposed();
  }
}

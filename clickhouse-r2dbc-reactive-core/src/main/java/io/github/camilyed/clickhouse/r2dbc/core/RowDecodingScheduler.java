package io.github.camilyed.clickhouse.r2dbc.core;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
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

  private static final String VIRTUAL_THREAD_NAME_PREFIX = THREAD_NAME_PREFIX + "-vt-";

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
  private final int workerCount;
  private final @Nullable ExecutorService ownedExecutor;

  private RowDecodingScheduler(
      final Scheduler scheduler,
      final int workerCount,
      final @Nullable ExecutorService ownedExecutor) {
    this.scheduler = scheduler;
    this.workerCount = workerCount;
    this.ownedExecutor = ownedExecutor;
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
   * A scheduler sized to {@code workerCount} workers with this class's own default queued task
   * capacity ({@link #DEFAULT_QUEUED_TASK_CAPACITY}) — the shape a caller that already knows the
   * right worker count for its own situation (e.g. {@code
   * io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory} sizing this to its
   * resolved connection-pool size) reaches for instead of repeating the queued-task-capacity
   * default itself.
   *
   * @param workerCount the maximum number of backing threads this scheduler ever creates; must be
   *     positive
   */
  public static RowDecodingScheduler withWorkerCount(final int workerCount) {
    return create(workerCount, DEFAULT_QUEUED_TASK_CAPACITY);
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
        Schedulers.newBoundedElastic(workerCount, queuedTaskCapacity, THREAD_NAME_PREFIX),
        workerCount,
        null);
  }

  /**
   * An alternative to {@link #withWorkerCount(int)} that runs decode tasks on JDK 21 virtual
   * threads instead of a bounded platform-thread pool, while preserving the exact same "at most
   * {@code maxConcurrency} decode tasks in flight at once" contract — see {@link
   * RowBinaryDecoder#decode}'s Javadoc for why that contract incidentally doubles as this driver's
   * real admission-control gate on the connection pool, and why {@code maxConcurrency} must still
   * be sized the same way {@code workerCount} is (typically the resolved {@code
   * transportMaxConnections}), not left unbounded. Precise wording matters: this bounds how many
   * decode tasks may actively run {@code command} at once, not how many virtual threads this
   * scheduler ever creates — a caller that submits far more tasks than {@code maxConcurrency} at
   * once gets that many additional parked (not running) virtual threads, cheap but not zero-cost.
   * See {@link AdmissionGatedExecutorService}'s own Javadoc for the exact mechanism, including how
   * cancellation/interruption of a still-waiting task is handled.
   *
   * <p>Motivated by JFR evidence (ROADMAP.md, Phase 11 PR5 follow-up, 2026-08-24) that decode is
   * I/O-wait-dominated — a worker spends most of its time blocked reading network bytes, not doing
   * CPU work — which is exactly the shape virtual threads are for: cheap park/unpark, no platform
   * thread held while parked. This does not raise the throughput ceiling, still capped by the
   * physical connection pool; it targets the resource cost {@link #withWorkerCount(int)} pays for
   * every decode worker whether it's parked or not.
   *
   * <p><b>Pinning risk</b>: a virtual thread blocked inside a {@code synchronized} block pins to
   * its carrier platform thread, cancelling out the benefit above for that duration. Checked
   * against client-v2's actual decode-path source (upstream {@code BinaryStreamReader}/{@code
   * RowBinaryWithNamesAndTypesFormatReader}, 2026-08-24): the only {@code synchronized} method on
   * that path, {@code BinaryStreamReader.ArrayValue#asList()}, is a pure in-memory list-view cache
   * over an already-fully-read array — it never blocks on I/O while holding the monitor. The actual
   * blocking reads (client-v2's {@code InputStream.read(...)} calls) run directly against the
   * caller-supplied stream with no {@code BufferedInputStream}/{@code DataInputStream} wrapping in
   * between, and this driver's own {@code FluxInputStreamBridge} has no {@code synchronized} usage
   * either — so this driver's decode path is Loom-friendly today. Run with {@code
   * -Djdk.tracePinnedThreads=full} in any benchmark comparing this against {@link
   * #withWorkerCount(int)} to catch pinning empirically rather than relying on this analysis alone.
   *
   * @param maxConcurrency the maximum number of decode tasks this scheduler ever runs at once; must
   *     be positive
   */
  public static RowDecodingScheduler virtualThreads(final int maxConcurrency) {
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be positive, got: " + maxConcurrency);
    }
    final ExecutorService executorService = admissionGatedVirtualThreadExecutor(maxConcurrency);
    return new RowDecodingScheduler(
        Schedulers.fromExecutorService(executorService, VIRTUAL_THREAD_NAME_PREFIX),
        maxConcurrency,
        executorService);
  }

  private static ExecutorService admissionGatedVirtualThreadExecutor(final int maxConcurrency) {
    final ExecutorService virtualThreadPerTask =
        Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name(VIRTUAL_THREAD_NAME_PREFIX, 0).factory());
    return new AdmissionGatedExecutorService(virtualThreadPerTask, new Semaphore(maxConcurrency));
  }

  /**
   * The maximum number of decode tasks this scheduler ever runs at once — what it was constructed
   * with via {@link #create(int, int)}/{@link #withWorkerCount(int)}/{@link #defaults()}. A real
   * capacity guarantee callers can check against their own concurrency ceiling (e.g. a connection
   * pool size), not an implementation detail.
   */
  public int workerCount() {
    return workerCount;
  }

  /**
   * Whether this scheduler runs decode tasks on JDK 21 virtual threads ({@link #virtualThreads})
   * rather than a bounded platform-thread pool ({@link #create}/{@link #withWorkerCount}/{@link
   * #defaults}) — see {@link #virtualThreads(int)}'s Javadoc for why a caller would choose one over
   * the other.
   */
  public boolean isVirtualThreadBacked() {
    return ownedExecutor != null;
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
    if (ownedExecutor != null) {
      ownedExecutor.shutdownNow();
    }
  }

  /** Whether {@link #dispose()} has already been called. */
  public boolean isDisposed() {
    return scheduler.isDisposed();
  }

  /**
   * Runs every submitted task on its own virtual thread — the number of virtual threads this
   * creates is <b>not</b> itself bounded — but only lets {@code maxConcurrency} of them actually
   * run {@code command} at once — the rest park on a {@link Semaphore} inside their own (cheap)
   * virtual thread instead of occupying a platform thread while queued. This is the mechanism that
   * lets {@link #virtualThreads(int)} preserve the same "at most N decode tasks in flight"
   * admission-control contract {@link #withWorkerCount(int)} provides via a bounded platform-thread
   * pool — see that method's Javadoc for why that contract matters beyond decode throughput itself.
   * Precise wording matters here: this is a virtual-thread-per-task executor with bounded
   * <em>active</em> concurrency, not a bounded pool of virtual threads — a caller that submits far
   * more tasks than {@code maxConcurrency} at once gets that many additional parked virtual
   * threads, not a rejection or an unbounded platform-thread queue.
   *
   * <p>Waiting on the admission permit is interruptible ({@link Semaphore#acquire()}, not {@link
   * Semaphore#acquireUninterruptibly()}): a task cancelled or interrupted while still waiting for a
   * permit must never run {@code command} once one later frees up. {@link
   * ExecutorService#shutdownNow()} interrupts every running/queued task on the delegate executor,
   * which is exactly how {@link #dispose()} unblocks every virtual thread parked here instead of
   * leaving it stuck forever.
   */
  private static final class AdmissionGatedExecutorService extends AbstractExecutorService {

    private final ExecutorService delegate;
    private final Semaphore admission;

    /**
     * Closes a real race window that interrupt-based cancellation alone cannot: {@link
     * #shutdownNow()} interrupts every task's thread to unblock whichever one currently holds the
     * admission permit, but that thread's {@code finally}-block {@link Semaphore#release()} can
     * wake a <em>different</em>, already-waiting task's {@link Semaphore#acquire()} before that
     * second task's own interrupt has been delivered/observed — {@code Thread.interrupt()} and
     * {@code Semaphore.release()} on two different threads give no ordering guarantee between
     * "this thread's interrupt flag is set" and "that thread's acquire() unblocks". Without this
     * flag, the newly-unblocked task can slip through and run {@code command} after the executor
     * was already told to shut down. Setting this <em>before</em> delegating to {@code
     * delegate.shutdownNow()} (which is what actually interrupts the permit holder and triggers its
     * release) guarantees the flag is visible to every task by the time any permit released as part
     * of shutdown could possibly be re-acquired — a real happens-before chain, not a best-effort
     * reduction of the race's probability.
     */
    private volatile boolean disposed;

    private AdmissionGatedExecutorService(
        final ExecutorService delegate, final Semaphore admission) {
      this.delegate = delegate;
      this.admission = admission;
    }

    @Override
    public void execute(final Runnable command) {
      delegate.execute(
          () -> {
            try {
              admission.acquire();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            try {
              if (!disposed) {
                command.run();
              }
            } finally {
              admission.release();
            }
          });
    }

    @Override
    public void shutdown() {
      disposed = true;
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      disposed = true;
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit)
        throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }
  }
}

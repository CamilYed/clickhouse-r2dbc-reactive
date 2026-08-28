package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Runs every submitted task on its own virtual thread — the number of virtual threads this creates
 * is <b>not</b> itself bounded — but only lets {@code maxConcurrency} of them actually run {@code
 * command} at once — the rest park on a {@link Semaphore} inside their own (cheap) virtual thread
 * instead of occupying a platform thread while queued. This is the mechanism that lets {@link
 * RowDecodingScheduler#virtualThreads(int)} preserve the same "at most N decode tasks in flight"
 * admission-control contract {@link RowDecodingScheduler#withWorkerCount(int)} provides via a
 * bounded platform-thread pool — see that method's Javadoc for why that contract matters beyond
 * decode throughput itself. Precise wording matters here: this is a virtual-thread-per-task
 * executor with bounded <em>active</em> concurrency, not a bounded pool of virtual threads — a
 * caller that submits far more tasks than {@code maxConcurrency} at once gets that many additional
 * parked virtual threads, not a rejection or an unbounded platform-thread queue.
 *
 * <p>Waiting on the admission permit is interruptible ({@link Semaphore#acquire()}, not {@link
 * Semaphore#acquireUninterruptibly()}): a task cancelled or interrupted while still waiting for a
 * permit must never run {@code command} once one later frees up. {@link
 * ExecutorService#shutdownNow()} interrupts every running/queued task on the delegate executor,
 * which is exactly how {@link RowDecodingScheduler#dispose()} unblocks every virtual thread parked
 * here instead of leaving it stuck forever.
 */
final class AdmissionGatedExecutorService extends AbstractExecutorService {

  private final ExecutorService delegate;
  private final Semaphore admission;

  /**
   * Closes a real race window that interrupt-based cancellation alone cannot: {@link
   * #shutdownNow()} interrupts every task's thread to unblock whichever one currently holds the
   * admission permit, but that thread's {@code finally}-block {@link Semaphore#release()} can wake
   * a <em>different</em>, already-waiting task's {@link Semaphore#acquire()} before that second
   * task's own interrupt has been delivered/observed — {@code Thread.interrupt()} and {@code
   * Semaphore.release()} on two different threads give no ordering guarantee between "this thread's
   * interrupt flag is set" and "that thread's acquire() unblocks". Without this flag, the
   * newly-unblocked task can slip through and run {@code command} after the executor was already
   * told to shut down. Setting this <em>before</em> delegating to {@code delegate.shutdownNow()}
   * (which is what actually interrupts the permit holder and triggers its release) guarantees the
   * flag is visible to every task by the time any permit released as part of shutdown could
   * possibly be re-acquired — a real happens-before chain, not a best-effort reduction of the
   * race's probability.
   */
  private volatile boolean disposed;

  AdmissionGatedExecutorService(final ExecutorService delegate, final Semaphore admission) {
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

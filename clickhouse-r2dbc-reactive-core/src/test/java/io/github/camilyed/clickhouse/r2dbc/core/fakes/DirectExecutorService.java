package io.github.camilyed.clickhouse.r2dbc.core.fakes;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An in-memory {@link java.util.concurrent.ExecutorService} that runs every submitted task
 * synchronously on the calling thread instead of handing it to a real worker thread. Lets a test
 * drive an {@code Executor}-based collaborator (e.g. {@code AdmissionGatedExecutorService}) through
 * an exact, deterministic sequence of events, rather than racing real threads and interrupts to hit
 * a specific interleaving.
 */
public final class DirectExecutorService extends AbstractExecutorService {

  private boolean shutdown;

  @Override
  public void execute(final Runnable command) {
    command.run();
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    return List.of();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return shutdown;
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit) {
    return shutdown;
  }
}

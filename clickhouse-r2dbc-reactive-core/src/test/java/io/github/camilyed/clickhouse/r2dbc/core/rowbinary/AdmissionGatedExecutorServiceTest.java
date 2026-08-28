package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.fakes.DirectExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AdmissionGatedExecutorService} in isolation from the real virtual-thread
 * pool and Reactor scheduler it normally backs (see {@link RowDecodingSchedulerTest}'s own
 * dispose()-race tests for that end-to-end shape). Using {@link DirectExecutorService} as the
 * delegate lets every scenario here run its command synchronously on the test thread, so the "task
 * disposed while waiting for its admission permit" scenario can be driven through an exact,
 * deterministic sequence of events instead of racing a real interrupt against a real {@link
 * Semaphore#release()} the way the equivalent scheduler-level test necessarily does.
 */
class AdmissionGatedExecutorServiceTest {

  @Test
  void shouldRunACommandWhenAnAdmissionPermitIsAvailableAndNotDisposed() {
    // given
    final AdmissionGatedExecutorService executor =
        new AdmissionGatedExecutorService(new DirectExecutorService(), new Semaphore(1));
    final AtomicBoolean commandRan = new AtomicBoolean(false);

    // when
    executor.execute(() -> commandRan.set(true));

    // then
    assertThat(commandRan.get()).isTrue();
  }

  @Test
  void shouldNotRunACommandWhenAlreadyDisposedByTheTimeItsAdmissionPermitBecomesAvailable() {
    // given - the single permit is only released after shutdownNow() has already run, simulating
    // the exact race shutdownNow()'s disposed flag exists to close: a task's admission.acquire()
    // succeeding only after the executor was already told to shut down
    final Semaphore admission = new Semaphore(1);
    admission.acquireUninterruptibly();
    final AdmissionGatedExecutorService executor =
        new AdmissionGatedExecutorService(new DirectExecutorService(), admission);
    final AtomicBoolean commandRan = new AtomicBoolean(false);

    // when
    executor.shutdownNow();
    admission.release();
    executor.execute(() -> commandRan.set(true));

    // then
    assertThat(commandRan.get()).isFalse();
  }

  @Test
  void shouldStillReleaseTheAdmissionPermitForACommandSkippedBecauseOfDisposal() {
    // given
    final Semaphore admission = new Semaphore(1);
    admission.acquireUninterruptibly();
    final AdmissionGatedExecutorService executor =
        new AdmissionGatedExecutorService(new DirectExecutorService(), admission);
    executor.shutdownNow();
    admission.release();

    // when
    executor.execute(() -> {});

    // then - the permit released above was re-acquired and released again by the skipped
    // command's own finally block, so a fresh acquire() must still succeed immediately
    assertThat(admission.tryAcquire()).isTrue();
  }

  @Test
  void shouldDelegateShutdownWithoutMarkingItselfDisposed() {
    // given
    final DirectExecutorService delegate = new DirectExecutorService();
    final AdmissionGatedExecutorService executor =
        new AdmissionGatedExecutorService(delegate, new Semaphore(1));

    // when
    executor.shutdown();

    // then
    assertThat(delegate.isShutdown()).isTrue();
  }
}

package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class RowDecodingSchedulerTest {

  @Test
  void shouldNotBeDisposedRightAfterCreation() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.defaults();

    try {
      // when / then
      assertThat(scheduler.isDisposed()).isFalse();
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldReportDisposedAfterDispose() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.defaults();

    // when
    scheduler.dispose();

    // then
    assertThat(scheduler.isDisposed()).isTrue();
  }

  @Test
  void shouldAllowDisposingMoreThanOnce() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.defaults();
    scheduler.dispose();

    // when / then
    assertThatCode(scheduler::dispose).doesNotThrowAnyException();
  }

  @Test
  void shouldRejectANonPositiveWorkerCount() {
    // when / then
    assertThatThrownBy(() -> RowDecodingScheduler.create(0, 100))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectANonPositiveQueuedTaskCapacity() {
    // when / then
    assertThatThrownBy(() -> RowDecodingScheduler.create(4, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldCreateAUsableSchedulerWithExplicitSizing() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.create(2, 10);

    try {
      // when / then
      assertThat(scheduler.isDisposed()).isFalse();
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldReportTheWorkerCountItWasExplicitlyCreatedWith() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.create(6, 100);

    try {
      // when / then
      assertThat(scheduler.workerCount()).isEqualTo(6);
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldReportAvailableProcessorsAsTheDefaultWorkerCount() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.defaults();

    try {
      // when / then
      assertThat(scheduler.workerCount()).isEqualTo(Runtime.getRuntime().availableProcessors());
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldCreateASchedulerSizedToAnExplicitWorkerCountWithTheDefaultQueuedTaskCapacity() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.withWorkerCount(12);

    try {
      // when / then
      assertThat(scheduler.workerCount()).isEqualTo(12);
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldRejectANonPositiveWorkerCountViaWithWorkerCount() {
    // when / then
    assertThatThrownBy(() -> RowDecodingScheduler.withWorkerCount(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldReportNotVirtualThreadBackedForThePlatformThreadDefaults() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.defaults();

    try {
      // when / then
      assertThat(scheduler.isVirtualThreadBacked()).isFalse();
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldReportVirtualThreadBackedForTheVirtualThreadsFactory() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(4);

    try {
      // when / then
      assertThat(scheduler.isVirtualThreadBacked()).isTrue();
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldRejectANonPositiveMaxConcurrencyForVirtualThreads() {
    // when / then
    assertThatThrownBy(() -> RowDecodingScheduler.virtualThreads(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldReportTheProvidedMaxConcurrencyAsWorkerCountForVirtualThreads() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(5);

    try {
      // when / then
      assertThat(scheduler.workerCount()).isEqualTo(5);
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldNotBeDisposedRightAfterCreationForVirtualThreads() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(4);

    try {
      // when / then
      assertThat(scheduler.isDisposed()).isFalse();
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldReportDisposedAfterDisposeForVirtualThreads() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(4);

    // when
    scheduler.dispose();

    // then
    assertThat(scheduler.isDisposed()).isTrue();
  }

  @Test
  void shouldAllowDisposingMoreThanOnceForVirtualThreads() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(4);
    scheduler.dispose();

    // when / then
    assertThatCode(scheduler::dispose).doesNotThrowAnyException();
  }

  @Test
  void shouldCapConcurrentDecodeTasksAtTheProvidedMaxConcurrencyEvenOnVirtualThreads() {
    // given
    final int maxConcurrency = 3;
    final int taskCount = 10;
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(maxConcurrency);
    final AtomicInteger currentlyRunning = new AtomicInteger();
    final AtomicInteger maxObservedConcurrency = new AtomicInteger();
    final CountDownLatch releaseGate = new CountDownLatch(1);

    try {
      // when
      Flux.range(0, taskCount)
          .flatMap(
              i ->
                  Mono.fromRunnable(
                          () -> {
                            final int running = currentlyRunning.incrementAndGet();
                            maxObservedConcurrency.updateAndGet(
                                previous -> Math.max(previous, running));
                            awaitUninterruptibly(releaseGate);
                            currentlyRunning.decrementAndGet();
                          })
                      .subscribeOn(scheduler.asReactorScheduler()),
              taskCount)
          .subscribe();

      // then
      await().atMost(Duration.ofSeconds(5)).until(() -> currentlyRunning.get() == maxConcurrency);
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(5))
          .until(() -> currentlyRunning.get() == maxConcurrency);
      assertThat(maxObservedConcurrency.get()).isEqualTo(maxConcurrency);
    } finally {
      releaseGate.countDown();
      scheduler.dispose();
    }
  }

  @Test
  void shouldNeverRunACommandThatWasCancelledWhileWaitingForAnAdmissionPermit() {
    // given - only one admission permit; task A holds it and blocks, task B is scheduled behind it
    // and then cancelled before task A ever releases the permit task B was waiting for
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(1);
    final CountDownLatch taskAStarted = new CountDownLatch(1);
    final CountDownLatch releaseTaskA = new CountDownLatch(1);
    final AtomicBoolean taskBRan = new AtomicBoolean(false);

    try {
      // when
      scheduler
          .asReactorScheduler()
          .schedule(
              () -> {
                taskAStarted.countDown();
                awaitUninterruptibly(releaseTaskA);
              });
      await().atMost(Duration.ofSeconds(5)).until(() -> taskAStarted.getCount() == 0);
      scheduler.asReactorScheduler().schedule(() -> taskBRan.set(true)).dispose();
      releaseTaskA.countDown();

      // then - task B must never run, even once its permit later becomes available
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(5))
          .until(() -> !taskBRan.get());
    } finally {
      releaseTaskA.countDown();
      scheduler.dispose();
    }
  }

  @Test
  void shouldTerminateAWaitingTaskWhenTheSchedulerIsDisposedBeforeAPermitFreesUp() {
    // given
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(1);

    final CountDownLatch taskAStarted = new CountDownLatch(1);
    final CountDownLatch taskATerminated = new CountDownLatch(1);
    final CountDownLatch neverReleased = new CountDownLatch(1);

    final AtomicBoolean taskBRan = new AtomicBoolean(false);

    try {
      // when - task A acquires the only permit and blocks interruptibly
      scheduler
          .asReactorScheduler()
          .schedule(
              () -> {
                taskAStarted.countDown();

                try {
                  neverReleased.await();
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  taskATerminated.countDown();
                }
              });

      await().atMost(Duration.ofSeconds(5)).until(() -> taskAStarted.getCount() == 0);

      // task B is submitted while A owns the only admission permit
      scheduler.asReactorScheduler().schedule(() -> taskBRan.set(true));

      scheduler.dispose();

      // then
      await().atMost(Duration.ofSeconds(5)).until(() -> taskATerminated.getCount() == 0);

      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(5))
          .until(() -> !taskBRan.get());
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void shouldStillReleaseTheAdmissionPermitWhenACommandThrows() {
    // given - only one admission permit; task A holds it and throws instead of completing normally.
    // Scheduling a raw, throwing Runnable directly (rather than through a Mono/Flux operator like
    // production code does via Mono.fromCallable(...).subscribeOn(...), which catches a thrown
    // exception internally and converts it to an onError signal before it ever reaches the raw
    // executor) means the exception genuinely becomes an uncaught exception on the scheduler's own
    // background thread - the default JVM-wide handler is swapped out for the scope of this test so
    // that expected exception doesn't fail the test process itself, then restored. The restore must
    // wait for uncaughtExceptionHandled (not just for task B to have run): task B's permit is
    // released inside AdmissionGatedExecutorService's own finally block, which runs *before* the
    // exception finishes unwinding out to the thread's uncaught-exception dispatch - restoring the
    // real handler as soon as task B ran raced that dispatch and lost under load (a real build
    // failure), since nothing otherwise guarantees task A's own uncaught-exception handling has
    // already happened by the time task B's flag flips.
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(1);
    final AtomicBoolean taskBRan = new AtomicBoolean(false);
    final CountDownLatch uncaughtExceptionHandled = new CountDownLatch(1);
    final Thread.UncaughtExceptionHandler previousHandler =
        Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(
        (thread, throwable) -> uncaughtExceptionHandled.countDown());

    try {
      // when
      scheduler
          .asReactorScheduler()
          .schedule(
              () -> {
                throw new IllegalStateException("boom");
              });
      scheduler.asReactorScheduler().schedule(() -> taskBRan.set(true));

      // then - the permit still gets released, so task B (submitted after) can still run
      await().atMost(Duration.ofSeconds(5)).untilTrue(taskBRan);
      // and - only now is it safe to restore the real handler
      await().atMost(Duration.ofSeconds(5)).until(() -> uncaughtExceptionHandled.getCount() == 0);
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
      scheduler.dispose();
    }
  }

  private static void awaitUninterruptibly(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for release gate", e);
    }
  }
}

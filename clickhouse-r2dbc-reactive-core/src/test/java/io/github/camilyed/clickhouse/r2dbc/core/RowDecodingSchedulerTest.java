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
    // given - only one admission permit; task A holds it and blocks forever (never releases
    // voluntarily), task B is scheduled behind it and never gets a chance to run before the
    // scheduler itself is disposed
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(1);
    final CountDownLatch taskAStarted = new CountDownLatch(1);
    final AtomicBoolean taskBRan = new AtomicBoolean(false);

    try {
      // when
      scheduler
          .asReactorScheduler()
          .schedule(
              () -> {
                taskAStarted.countDown();
                awaitUninterruptibly(new CountDownLatch(1));
              });
      await().atMost(Duration.ofSeconds(5)).until(() -> taskAStarted.getCount() == 0);
      scheduler.asReactorScheduler().schedule(() -> taskBRan.set(true));
      scheduler.dispose();

      // then - task B never runs, and disposal itself doesn't hang waiting on task A or task B
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
    // given - only one admission permit; task A holds it and throws instead of completing normally
    final RowDecodingScheduler scheduler = RowDecodingScheduler.virtualThreads(1);
    final AtomicBoolean taskBRan = new AtomicBoolean(false);

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
    } finally {
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

package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

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
}

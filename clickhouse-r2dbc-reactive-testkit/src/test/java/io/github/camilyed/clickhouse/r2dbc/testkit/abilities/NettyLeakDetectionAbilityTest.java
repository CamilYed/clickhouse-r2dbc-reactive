package io.github.camilyed.clickhouse.r2dbc.testkit.abilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Proves the leak-detection harness itself actually detects a leak, rather than trusting it
 * silently - a harness nobody has seen fail is not proven to work. Run with {@code
 * -Dio.netty.leakDetection.level=paranoid} (see this module's {@code build.gradle.kts}), same as
 * every other test relying on {@link NettyLeakDetectionAbility}.
 *
 * <p>Polling for a leak report is not just "call {@code System.gc()} and check" - Netty only drains
 * and reports its leak-tracking {@code ReferenceQueue} as a side effect of tracking the
 * <em>next</em> resource, not automatically in the background the moment the GC actually collects
 * an abandoned buffer. Every poll attempt here therefore also allocates and releases a fresh
 * throwaway buffer to force that drain to happen - without it, this test passed by coincidence on a
 * machine where something else nearby happened to allocate a tracked buffer during the polling
 * window (confirmed: green locally, red on a quieter CI runner with nothing else running).
 */
class NettyLeakDetectionAbilityTest implements NettyLeakDetectionAbility {

  @Test
  void shouldDetectAnUnreleasedByteBufAsALeak() {
    // given
    thereAreNoRecordedByteBufLeaksYet();
    allocateAndForgetToRelease();

    // when / then
    awaitUntilALeakIsRecorded();
  }

  @Test
  void shouldReportNoLeaksWhenEveryByteBufIsProperlyReleased() {
    // given
    thereAreNoRecordedByteBufLeaksYet();
    final ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeBytes(new byte[] {1, 2, 3});
    buffer.release();

    // when / then
    assertNoByteBufLeaksWereDetected();
  }

  @Test
  void shouldFailTheAssertionWhenALeakWasRecorded() {
    // given
    thereAreNoRecordedByteBufLeaksYet();
    allocateAndForgetToRelease();
    awaitUntilALeakIsRecorded();

    // when / then
    assertThatThrownBy(this::assertNoByteBufLeaksWereDetected).isInstanceOf(AssertionError.class);
  }

  private void awaitUntilALeakIsRecorded() {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              System.gc();
              nudgeTheLeakDetectorToDrainItsQueue();
              assertThat(LeakRecordingResourceLeakDetector.recordedLeaks()).isNotEmpty();
            });
  }

  private void nudgeTheLeakDetectorToDrainItsQueue() {
    Unpooled.buffer(16).release();
  }

  private void allocateAndForgetToRelease() {
    // deliberately not released, and no reference kept beyond this method - that is the leak
    final ByteBuf leaked = Unpooled.buffer(16);
    leaked.writeBytes(new byte[] {1, 2, 3});
  }
}

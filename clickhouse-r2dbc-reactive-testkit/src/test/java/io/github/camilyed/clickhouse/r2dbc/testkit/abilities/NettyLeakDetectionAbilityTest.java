package io.github.camilyed.clickhouse.r2dbc.testkit.abilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Proves the leak-detection harness itself actually detects a leak, rather than trusting it
 * silently - a harness nobody has seen fail is not proven to work. Run with {@code
 * -Dio.netty.leakDetection.level=paranoid} (see this module's {@code build.gradle.kts}), same as
 * every other test relying on {@link NettyLeakDetectionAbility}.
 */
class NettyLeakDetectionAbilityTest implements NettyLeakDetectionAbility {

  /**
   * TEMPORARY DIAGNOSTIC - not a real regression test, just printing/asserting the installation
   * state directly so a build log answers "is the custom detector actually installed?" without
   * guessing through GC/queue-drain timing. Remove once shouldDetectAnUnreleasedByteBufAsALeak
   * passes reliably.
   */
  @Test
  void diagnosticCustomDetectorInstallationState() {
    System.out.println(
        "DIAGNOSTIC io.netty.customResourceLeakDetector system property = "
            + System.getProperty("io.netty.customResourceLeakDetector"));
    System.out.println(
        "DIAGNOSTIC io.netty.leakDetection.level system property = "
            + System.getProperty("io.netty.leakDetection.level"));
    System.out.println(
        "DIAGNOSTIC ResourceLeakDetector.getLevel() = " + ResourceLeakDetector.getLevel());

    final ResourceLeakDetector<Object> detector =
        ResourceLeakDetectorFactory.instance().newResourceLeakDetector(Object.class);
    System.out.println(
        "DIAGNOSTIC newResourceLeakDetector(Object.class) returned class = "
            + detector.getClass().getName());

    assertThat(detector).isInstanceOf(LeakRecordingResourceLeakDetector.class);
  }

  /**
   * TEMPORARY DIAGNOSTIC - isolates whether the break is in Netty's {@code ByteBuf}
   * allocation/tracking path specifically, or in the GC/reference-queue-drain plumbing itself, by
   * tracking a plain {@link Object} directly through {@link ResourceLeakDetector#track}, bypassing
   * {@code Unpooled}/{@code ByteBuf} entirely. If this also fails, the bug is in GC/queue-drain
   * timing or our override chain, not in how {@code Unpooled.buffer()} allocates. If this passes,
   * the bug is specific to the {@code ByteBuf} allocation path not being tracked as expected. Remove
   * once shouldDetectAnUnreleasedByteBufAsALeak passes reliably.
   */
  @Test
  void diagnosticTrackingAPlainObjectDirectlyIsDetectedAsALeak() {
    // given
    thereAreNoRecordedByteBufLeaksYet();
    final ResourceLeakDetector<Object> detector =
        ResourceLeakDetectorFactory.instance().newResourceLeakDetector(Object.class, 1);
    trackAndDeliberatelyForgetToClose(detector);

    // when / then
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              System.gc();
              trackAndDeliberatelyForgetToClose(detector);
              assertThat(LeakRecordingResourceLeakDetector.recordedLeaks()).isNotEmpty();
            });
  }

  private void trackAndDeliberatelyForgetToClose(final ResourceLeakDetector<Object> detector) {
    // deliberately never closing the returned tracker, and no reference kept beyond this method -
    // that is the leak
    final Object target = new Object();
    detector.track(target);
  }

  @Test
  void shouldDetectAnUnreleasedByteBufAsALeak() {
    // given
    thereAreNoRecordedByteBufLeaksYet();
    allocateAndForgetToRelease();

    // when / then - leaks are only reported once the tracked object is unreachable and collected,
    // which System.gc() only requests, never guarantees, so this polls rather than asserting once
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              System.gc();
              // Nudges Netty's ResourceLeakDetector into draining its reference queue - see
              // NettyLeakDetectionAbility#assertNoByteBufLeaksWereDetected()'s Javadoc for why a
              // fresh tracked allocation, not just System.gc(), is required to actually surface an
              // already-collected leak.
              Unpooled.buffer(1).release();
              assertThat(LeakRecordingResourceLeakDetector.recordedLeaks()).isNotEmpty();
            });
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
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              System.gc();
              // Nudges Netty's ResourceLeakDetector into draining its reference queue - see
              // NettyLeakDetectionAbility#assertNoByteBufLeaksWereDetected()'s Javadoc for why a
              // fresh tracked allocation, not just System.gc(), is required to actually surface an
              // already-collected leak.
              Unpooled.buffer(1).release();
              assertThat(LeakRecordingResourceLeakDetector.recordedLeaks()).isNotEmpty();
            });

    // when / then
    assertThatThrownBy(this::assertNoByteBufLeaksWereDetected).isInstanceOf(AssertionError.class);
  }

  private void allocateAndForgetToRelease() {
    // deliberately not released, and no reference kept beyond this method - that is the leak
    final ByteBuf leaked = Unpooled.buffer(16);
    leaked.writeBytes(new byte[] {1, 2, 3});
  }
}

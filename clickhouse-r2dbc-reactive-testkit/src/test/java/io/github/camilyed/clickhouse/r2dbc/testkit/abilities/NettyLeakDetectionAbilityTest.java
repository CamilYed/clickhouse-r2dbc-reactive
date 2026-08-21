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
 */
class NettyLeakDetectionAbilityTest implements NettyLeakDetectionAbility {

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

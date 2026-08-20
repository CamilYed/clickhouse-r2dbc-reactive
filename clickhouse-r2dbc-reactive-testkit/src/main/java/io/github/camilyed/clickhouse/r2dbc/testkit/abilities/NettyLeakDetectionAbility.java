package io.github.camilyed.clickhouse.r2dbc.testkit.abilities;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector;

/**
 * Test DSL: asserts no Netty {@code ByteBuf} (or other pooled/reference-counted resource) leaked
 * during a test - see ROADMAP.md's "Netty leak-detection test lane" (Phase 7 item 6). Covers the
 * shapes most likely to strand a buffer: cancellation, disconnect mid-response, timeout, retry, and
 * downstream cancellation after a few records - each scenario test that already exercises one of
 * these shapes (see {@code ClickHouseHttpTransportTest}) can add a leak assertion for free by
 * implementing this ability, no new scenario needed.
 *
 * <p>Requires the JVM to run with {@code -Dio.netty.leakDetection.level=paranoid} and {@code
 * -Dio.netty.customResourceLeakDetector=io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector}
 * (both already wired into this module's and {@code transport-http}'s Gradle {@code Test} task) -
 * see {@link LeakRecordingResourceLeakDetector}'s Javadoc for why the custom detector must be
 * installed via that JVM property rather than programmatically.
 */
public interface NettyLeakDetectionAbility {

  /** Call at the start of a test to start from a clean slate, independent of any earlier test. */
  default void thereAreNoRecordedByteBufLeaksYet() {
    LeakRecordingResourceLeakDetector.clearRecordedLeaks();
  }

  /**
   * Best-effort: a leak is only reported once the tracked object becomes unreachable and is garbage
   * collected, and {@link System#gc()} only requests a collection, never guarantees one - so this
   * can under-detect (a real leak that just hasn't been GC'd yet slips through) but will never
   * over-report (nothing is invented that didn't actually happen). Good enough to catch a genuinely
   * forgotten {@code .release()} in CI over many runs, not a hard real-time guarantee.
   */
  default void assertNoByteBufLeaksWereDetected() {
    for (int attempt = 0; attempt < 5; attempt++) {
      System.gc();
    }
    assertThat(LeakRecordingResourceLeakDetector.recordedLeaks())
        .describedAs("Netty resource leaks recorded during this test")
        .isEmpty();
  }
}

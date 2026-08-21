package io.github.camilyed.clickhouse.r2dbc.testkit.abilities;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector;
import io.netty.buffer.Unpooled;

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
   *
   * <p>Each attempt also allocates and immediately releases a throwaway <em>direct</em> {@code
   * ByteBuf} after the {@code System.gc()} call - not just requesting a collection, and not a heap
   * buffer. Two things have to both be true for this nudge to matter: Netty's {@code
   * ResourceLeakDetector} only drains its internal reference queue for already-GC'd-but-unreleased
   * trackers as a side effect of tracking a <em>new</em> allocation ({@code
   * ResourceLeakDetector#track}), never spontaneously in the background - and {@code Unpooled}'s
   * heap buffers are never tracked at all ({@code UnpooledByteBufAllocator}'s heap path skips
   * {@code toLeakAwareBuffer} entirely; only direct and composite buffers go through it), so the
   * nudge has to allocate a direct buffer specifically or it wouldn't call {@code track()} in the
   * first place and would drain nothing. Without this nudge, a leaked buffer from earlier in the
   * test could already be garbage-collected and sitting in that queue, unreported forever, simply
   * because nothing in the test happened to track another allocation afterward - confirmed by
   * {@code NettyLeakDetectionAbilityTest} actually failing to detect its own deliberately-leaked
   * buffer before this nudge (and the direct-vs-heap fix) was in place.
   */
  default void assertNoByteBufLeaksWereDetected() {
    for (int attempt = 0; attempt < 5; attempt++) {
      System.gc();
      Unpooled.directBuffer(1).release();
    }
    assertThat(LeakRecordingResourceLeakDetector.recordedLeaks())
        .describedAs("Netty resource leaks recorded during this test")
        .isEmpty();
  }
}

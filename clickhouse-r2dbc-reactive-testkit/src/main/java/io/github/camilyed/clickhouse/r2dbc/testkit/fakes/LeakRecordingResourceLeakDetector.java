package io.github.camilyed.clickhouse.r2dbc.testkit.fakes;

import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A {@link ResourceLeakDetector} that records every leak it reports into an in-memory queue, in
 * addition to Netty's own default logging - so a test can assert "nothing leaked" directly instead
 * of scraping log output for {@code "LEAK:"}.
 *
 * <p>Installed via the {@code -Dio.netty.customResourceLeakDetector=<this class's fully-qualified
 * name>} JVM property (already wired into this module's and {@code transport-http}'s Gradle {@code
 * Test} task) - <strong>not</strong> programmatically. An earlier version of this class tried
 * installing itself at runtime via {@code
 * ResourceLeakDetectorFactory.setResourceLeakDetectorFactory (...)}, called from a test method;
 * that never actually took effect, because Netty's {@code AbstractByteBuf} binds its {@code
 * ResourceLeakDetector} into a {@code static final} field the very first time any {@code ByteBuf}
 * is allocated anywhere in the JVM - which, in a real multi-class test suite, has always already
 * happened (some earlier test touched {@code ByteBuf}/{@code Unpooled}) long before any one test
 * method runs. The JVM property is read once, when {@code DefaultResourceLeakDetectorFactory}
 * constructs that very first detector via reflection, so it is the only ordering-safe way to
 * install a custom detector - confirmed by that earlier attempt failing in exactly this way (leaks
 * allocated and dropped, but never recorded).
 *
 * <p>Netty reflectively tries a {@code (Class, int, long)} constructor first, falling back to
 * {@code (Class, int)} - both are provided here so either Netty version's lookup succeeds.
 */
public final class LeakRecordingResourceLeakDetector<T> extends ResourceLeakDetector<T> {

  private static final Queue<String> RECORDED_LEAKS = new ConcurrentLinkedQueue<>();

  public LeakRecordingResourceLeakDetector(
      final Class<T> resourceType, final int samplingInterval) {
    super(resourceType, samplingInterval);
  }

  public LeakRecordingResourceLeakDetector(
      final Class<T> resourceType, final int samplingInterval, final long maxActive) {
    super(resourceType, samplingInterval, maxActive);
  }

  /** A snapshot of every leak recorded so far in this JVM fork. */
  public static List<String> recordedLeaks() {
    return List.copyOf(RECORDED_LEAKS);
  }

  /**
   * Clears recorded leaks - call before a scenario whose result you want to assert in isolation.
   */
  public static void clearRecordedLeaks() {
    RECORDED_LEAKS.clear();
  }

  @Override
  protected void reportTracedLeak(final String resourceType, final String records) {
    RECORDED_LEAKS.add(resourceType + " leaked:" + records);
    super.reportTracedLeak(resourceType, records);
  }

  @Override
  protected void reportUntracedLeak(final String resourceType) {
    RECORDED_LEAKS.add(
        resourceType
            + " leaked (untraced - run with -Dio.netty.leakDetection.level=paranoid for a stack"
            + " trace of the allocation site)");
    super.reportUntracedLeak(resourceType);
  }
}

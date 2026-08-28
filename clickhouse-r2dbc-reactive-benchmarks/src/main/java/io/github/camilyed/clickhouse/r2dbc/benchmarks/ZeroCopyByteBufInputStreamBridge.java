package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * Variant B of the latency-path-isolation ladder (docs/performance/latency-path-isolation.md): the
 * same push-to-pull {@link InputStream} shape {@code core.FluxInputStreamBridge} provides, but
 * operating directly on the live {@code Flux<ByteBuf>} Reactor Netty hands back instead of a
 * pre-copied {@code Flux<ByteBuffer>} — avoiding the {@code ByteBuf}-&gt;{@code byte[]} copy {@code
 * ClickHouseResult.decodePlain}'s {@code asByteArray()} call performs on the production path.
 *
 * <p><b>Retain/release contract.</b> Reactor Netty releases each {@link ByteBuf} automatically once
 * a subscriber's {@code onNext} returns, unless the subscriber retains it first — the standard
 * Netty "retain to hold past onNext" pattern. Every buffer this class receives is {@link
 * ByteBuf#retain() retained} exactly once on arrival ({@link QueueingSubscriber#hookOnNext}) and
 * {@link ByteBuf#release() released} exactly once, on whichever path first fully consumes or
 * discards it: normal read-to-exhaustion ({@link #read(byte[], int, int)}), the bounded
 * drain-then-natural-completion path ({@link #drainQuickly()}), or the final best-effort sweep of
 * anything still queued/pending at {@link #close()} time. Verify with {@code
 * -Dio.netty.leakDetection.level=paranoid} plus {@code
 * io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector} before
 * trusting any number produced against this class — see this module's benchmark for how it's wired
 * in.
 *
 * <p><b>Known, disclosed gap.</b> {@link #close()}'s cleanup covers full natural consumption and
 * the bounded-drain-then-hard-cancel path, draining both the internal {@link #pending} deque and
 * anything still sitting in the raw {@link #queue}. It does not close a narrow race where Reactor
 * delivers a further {@code onNext} concurrently with (and just after) that sweep, during a hard
 * cancel — an inherent race in any externally-triggered-cancellation cleanup, harmless for {@code
 * core.FluxInputStreamBridge}'s plain {@code ByteBuffer}s but a real (if rare) leak risk here given
 * these are pooled, ref-counted buffers. Not exercised by this benchmark's own scenarios (SELECT 1
 * / point lookups always read to natural completion, never early-cancel), disclosed here rather
 * than silently ignored. This is a diagnostic prototype for isolating one latency question, not a
 * production candidate — closing this gap would be required before it ever could be.
 *
 * <p>Benchmark-local: deliberately not touching {@code core.FluxInputStreamBridge} or any other
 * production class, per the latency-path-isolation plan's "no production code changes in this pass"
 * scope.
 */
final class ZeroCopyByteBufInputStreamBridge extends InputStream {

  private static final Duration DRAIN_TIME_BUDGET = Duration.ofMillis(50);
  private static final int DRAIN_BYTE_BUDGET = 64 * 1024;
  private static final int MAX_COALESCE_BYTES = 64 * 1024;

  private final BlockingQueue<StreamSignal> queue;
  private final QueueingSubscriber subscriber;
  private final Deque<ByteBuf> pending = new ArrayDeque<>();
  private final byte[] singleByteReadBuffer = new byte[1];

  private ByteBuf current = Unpooled.EMPTY_BUFFER;
  private boolean finished;
  private Optional<StreamSignal> stashedSignal = Optional.empty();

  private ZeroCopyByteBufInputStreamBridge(final Flux<ByteBuf> source, final int demand) {
    this.queue = new ArrayBlockingQueue<>(demand + 1);
    this.subscriber = new QueueingSubscriber(queue, demand);
    source.subscribe(subscriber);
  }

  /** Subscribes to {@code source}, requesting {@code demand} chunks up front. */
  static ZeroCopyByteBufInputStreamBridge subscribeTo(
      final Flux<ByteBuf> source, final int demand) {
    return new ZeroCopyByteBufInputStreamBridge(source, demand);
  }

  @Override
  public int read() throws IOException {
    final int bytesRead = read(singleByteReadBuffer, 0, 1);
    return bytesRead == -1 ? -1 : singleByteReadBuffer[0] & 0xFF;
  }

  @Override
  public int read(final byte[] destination, final int offset, final int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    if (finished && !current.isReadable()) {
      return -1;
    }
    if (!current.isReadable() && !fillCurrent()) {
      return -1;
    }
    final int toCopy = Math.min(length, current.readableBytes());
    current.readBytes(destination, offset, toCopy);
    if (!current.isReadable()) {
      current.release();
      current = Unpooled.EMPTY_BUFFER;
    }
    return toCopy;
  }

  private boolean fillCurrent() throws IOException {
    if (!pending.isEmpty()) {
      current = pending.pollFirst();
      return true;
    }
    final StreamSignal signal = takeNextSignal();
    return switch (signal) {
      case StreamSignal.Data(ByteBuf buffer) -> {
        current = buffer;
        drainOpportunistically();
        yield true;
      }
      case StreamSignal.Complete ignored -> {
        finished = true;
        yield false;
      }
      case StreamSignal.Error(Throwable cause) -> {
        finished = true;
        throw new IOException("Upstream Flux signalled an error", cause);
      }
    };
  }

  /**
   * Mirrors {@code core.FluxInputStreamBridge#coalesce}'s opportunistic drain, but without its
   * merge-copy: already-queued {@code Data} signals are moved into {@link #pending} un-merged, one
   * {@link ByteBuf} per entry, up to {@link #MAX_COALESCE_BYTES} total.
   */
  private void drainOpportunistically() {
    int totalBytes = current.readableBytes();
    while (totalBytes < MAX_COALESCE_BYTES) {
      final StreamSignal signal = queue.poll();
      if (signal == null) {
        return;
      }
      if (!(signal instanceof StreamSignal.Data(ByteBuf buffer))) {
        stashedSignal = Optional.of(signal);
        return;
      }
      subscriber.requestOne();
      pending.addLast(buffer);
      totalBytes += buffer.readableBytes();
    }
  }

  private StreamSignal takeNextSignal() throws IOException {
    if (stashedSignal.isPresent()) {
      final StreamSignal signal = stashedSignal.get();
      stashedSignal = Optional.empty();
      return signal;
    }
    try {
      final StreamSignal signal = queue.take();
      if (signal instanceof StreamSignal.Data) {
        subscriber.requestOne();
      }
      return signal;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for data", e);
    }
  }

  @Override
  public void close() {
    if (!finished && !drainQuickly()) {
      subscriber.dispose();
    }
    releaseEverythingStillHeld();
  }

  /**
   * Bounded attempt to reach natural stream completion within {@link #DRAIN_TIME_BUDGET}/{@link
   * #DRAIN_BYTE_BUDGET} — mirrors {@code core.FluxInputStreamBridge#drainQuickly}'s reason for
   * existing: let a caller that only wanted the first row (e.g. {@code Flux.next()}-style
   * consumption) still hand the connection back to the pool instead of forcing a hard cancel.
   */
  private boolean drainQuickly() {
    final long deadlineNanos = System.nanoTime() + DRAIN_TIME_BUDGET.toNanos();
    int bytesDrained = 0;
    while (bytesDrained < DRAIN_BYTE_BUDGET) {
      final long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        return false;
      }
      final Optional<StreamSignal> signal = pollWithinBudget(remainingNanos);
      if (signal.isEmpty()) {
        return false;
      }
      final Optional<Integer> dataBytesConsumed = applyDrainedSignal(signal.get());
      if (dataBytesConsumed.isEmpty()) {
        return true;
      }
      bytesDrained += dataBytesConsumed.get();
    }
    return false;
  }

  private Optional<Integer> applyDrainedSignal(final StreamSignal signal) {
    return switch (signal) {
      case StreamSignal.Data(ByteBuf buffer) -> {
        subscriber.requestOne();
        final int bytes = buffer.readableBytes();
        buffer.release();
        yield Optional.of(bytes);
      }
      case StreamSignal.Complete ignored -> {
        finished = true;
        yield Optional.empty();
      }
      case StreamSignal.Error ignored -> {
        finished = true;
        yield Optional.empty();
      }
    };
  }

  private Optional<StreamSignal> pollWithinBudget(final long timeoutNanos) {
    if (stashedSignal.isPresent()) {
      final StreamSignal signal = stashedSignal.get();
      stashedSignal = Optional.empty();
      return Optional.of(signal);
    }
    try {
      return Optional.ofNullable(queue.poll(timeoutNanos, TimeUnit.NANOSECONDS));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    }
  }

  /**
   * Final release sweep: {@link #pending}, {@link #current} if still holding unread bytes, and
   * anything left sitting in the raw {@link #queue} (reached when a hard cancel via {@link
   * QueueingSubscriber#dispose()} left already-delivered chunks undrained) — see this class's
   * Javadoc for the one race this sweep does not close.
   */
  private void releaseEverythingStillHeld() {
    ByteBuf buf;
    while ((buf = pending.pollFirst()) != null) {
      buf.release();
    }
    if (current.isReadable()) {
      current.release();
    }
    current = Unpooled.EMPTY_BUFFER;
    StreamSignal signal;
    while ((signal = queue.poll()) != null) {
      if (signal instanceof StreamSignal.Data(ByteBuf buffer)) {
        buffer.release();
      }
    }
  }

  private sealed interface StreamSignal {
    record Data(ByteBuf buffer) implements StreamSignal {}

    record Complete() implements StreamSignal {}

    record Error(Throwable cause) implements StreamSignal {}
  }

  private static final class QueueingSubscriber extends BaseSubscriber<ByteBuf> {

    private final BlockingQueue<StreamSignal> queue;
    private final int demand;

    private QueueingSubscriber(final BlockingQueue<StreamSignal> queue, final int demand) {
      this.queue = queue;
      this.demand = demand;
    }

    @Override
    protected void hookOnSubscribe(final Subscription subscription) {
      request(demand);
    }

    @Override
    protected void hookOnNext(final ByteBuf value) {
      queue.add(new StreamSignal.Data(value.retain()));
    }

    @Override
    protected void hookOnComplete() {
      queue.add(new StreamSignal.Complete());
    }

    @Override
    protected void hookOnError(final Throwable throwable) {
      queue.add(new StreamSignal.Error(throwable));
    }

    private void requestOne() {
      request(1);
    }
  }
}

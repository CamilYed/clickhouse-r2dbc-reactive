package io.github.camilyed.clickhouse.r2dbc.core;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * Adapts a push-based {@code Flux<ByteBuffer>} (how Reactor delivers HTTP response chunks) into a
 * pull-based, blocking {@link InputStream} (what client-v2's row decoder expects to read from).
 *
 * <h2>Why this exists</h2>
 *
 * Reactor Netty hands us chunks by <em>pushing</em> them to a subscriber as they arrive off the
 * socket — {@code onNext} is called whenever data shows up, on Netty's own thread. client-v2's
 * {@code RowBinaryWithNamesAndTypesFormatReader} expects the opposite shape: a plain {@link
 * InputStream} it can <em>pull</em> bytes from, one blocking {@code read()} call at a time. This
 * class is the adapter between those two shapes — it must never let the pull side block the push
 * side, or every other query sharing that Netty event loop stalls too.
 *
 * <h2>How backpressure works here (the "credit" model)</h2>
 *
 * On subscription, {@link QueueingSubscriber} asks the source for exactly {@code demand} items up
 * front (its {@code hookOnSubscribe}) — no more. Each arriving {@code ByteBuffer} lands in {@link
 * #queue}, sized {@code demand + 1} (one spare slot reserved for the final {@code Complete}/{@code
 * Error} signal, so the producer is never blocked trying to enqueue the terminal signal even when
 * the queue is otherwise full). Every time {@link #read(byte[], int, int)} fully drains one buffer,
 * it asks for exactly one more ({@link QueueingSubscriber#requestOne()}) — one "credit" is spent
 * and immediately replenished, 1-for-1. The queue therefore never holds more than {@code demand}
 * in-flight items, which is the whole point: the thread delivering {@code onNext} only ever does a
 * bounded, non-blocking {@code queue.add(...)} and returns immediately. It is never the thread that
 * waits for a slow consumer — if the consumer falls behind, Reactor simply stops calling {@code
 * onNext} until the next {@code request(1)} arrives (real backpressure, not buffering without
 * limit).
 *
 * <h2>Where the blocking actually happens</h2>
 *
 * Only the thread calling {@link #read()}/{@link #read(byte[], int, int)} ever blocks, inside
 * {@link #fillCurrent()}'s {@code queue.take()} — waiting for the next signal to arrive. That
 * thread must be a dedicated worker (e.g. whatever thread is running client-v2's decoder loop),
 * never the Netty event loop itself. {@link StreamSignal} is what travels through the queue —
 * {@code Data} for a chunk, {@code Complete}/{@code Error} to end the stream — modeled as a sealed
 * type specifically so {@link #fillCurrent()}'s {@code switch} is exhaustive: adding a new signal
 * variant without handling it here would be a compile error, not a silent bug.
 *
 * <h2>Cancellation</h2>
 *
 * {@link #close()} disposes the subscriber, which cancels the upstream subscription — whether
 * nothing has been read yet or a consumer stops early, the source {@code Flux} is torn down instead
 * of left running to completion unread.
 */
public final class FluxInputStreamBridge extends InputStream {

  private final BlockingQueue<StreamSignal> queue;
  private final QueueingSubscriber subscriber;

  private ByteBuffer current = ByteBuffer.allocate(0);
  private boolean finished;

  /**
   * Reused across every {@link #read()} call rather than allocated fresh each time — this class is
   * read by exactly one dedicated worker thread per its own class-level Javadoc, so a single
   * mutable field is safe here. Measured, not assumed: {@code DecoderOnlyBenchmark}'s {@code -prof
   * gc} run (see docs/PERFORMANCE.md's Phase 5 "Optimization phase" section) found this driver
   * allocating roughly 3x more bytes/row than client-v2 while decoding the same payload, and every
   * {@code String} column's length-prefix varint is read one byte at a time via this exact overload
   * — a real, confirmed, per-row allocation this field removes entirely.
   */
  private final byte[] singleByteReadBuffer = new byte[1];

  private FluxInputStreamBridge(final Flux<ByteBuffer> source, final int demand) {
    // ArrayBlockingQueue over LinkedBlockingQueue: the queue's capacity is fixed for this
    // instance's whole lifetime (demand + 1, see this class's Javadoc), so there is no growth
    // scenario an array-backed queue would handle worse - and it avoids a per-node allocation on
    // every add()/take() that a linked queue pays for, at the cost of a single upfront array
    // allocation instead. Not yet benchmarked in isolation - see docs/PERFORMANCE.md's task #197.
    this.queue = new ArrayBlockingQueue<>(demand + 1);
    this.subscriber = new QueueingSubscriber(queue, demand);
    source.subscribe(subscriber);
  }

  /** Subscribes to {@code source} and returns an {@link InputStream} reading its bytes. */
  public static FluxInputStreamBridge subscribeTo(final Flux<ByteBuffer> source, final int demand) {
    return new FluxInputStreamBridge(source, demand);
  }

  @Override
  public int read() throws IOException {
    final int bytesRead = read(singleByteReadBuffer, 0, 1);
    return bytesRead == -1 ? -1 : singleByteReadBuffer[0] & 0xFF;
  }

  @Override
  public int read(final byte[] destination, final int offset, final int length) throws IOException {
    // InputStream#read(byte[], int, int)'s own contract requires 0 for a zero-length request
    // regardless of stream state - checked first, before finished/fillCurrent(), so a zero-length
    // read neither reports false end-of-stream nor blocks in queue.take() waiting for a chunk it
    // was never going to copy any bytes from.
    if (length == 0) {
      return 0;
    }
    if (finished) {
      return -1;
    }
    if (!current.hasRemaining() && !fillCurrent()) {
      return -1;
    }
    final int toCopy = Math.min(length, current.remaining());
    current.get(destination, offset, toCopy);
    if (!current.hasRemaining()) {
      subscriber.requestOne();
    }
    return toCopy;
  }

  private boolean fillCurrent() throws IOException {
    try {
      final StreamSignal signal = queue.take();
      return switch (signal) {
        case StreamSignal.Data(ByteBuffer buffer) -> {
          current = buffer;
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
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for data", e);
    }
  }

  @Override
  public void close() {
    subscriber.dispose();
  }

  private static final class QueueingSubscriber extends BaseSubscriber<ByteBuffer> {

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
    protected void hookOnNext(final ByteBuffer value) {
      queue.add(new StreamSignal.Data(value));
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

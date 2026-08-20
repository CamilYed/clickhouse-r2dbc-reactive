package io.github.camilyed.clickhouse.r2dbc.core;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
 * <h2>Chunk coalescing</h2>
 *
 * {@code queue.take()} is a real cross-thread synchronization point (Reactor Netty's event loop on
 * one side, this class's dedicated worker thread on the other) — measured directly via
 * docs/PERFORMANCE.md's {@code StreamingScanBenchmark} investigation: chunk count scales linearly
 * with row count (~285-293 rows per network chunk, confirmed by instrumentation), and the estimated
 * per-chunk cost (~8-12µs) accounts for the latency gap that grows with result size. {@link
 * #fillCurrent()} therefore does one blocking {@code take()} for the first available signal, then
 * opportunistically drains any <em>already-queued</em> {@code Data} signals with non-blocking
 * {@code queue.poll()} calls, merging them into a single larger {@link #current} buffer (up to
 * {@link #MAX_COALESCE_BYTES}) before returning. This never blocks waiting for more data to arrive
 * — a {@code poll()} that finds the queue empty just means nothing was ready yet, so this trades
 * away some handoffs only when the producer is already ahead, never at the cost of added latency
 * when it isn't. A {@code Complete}/{@code Error} encountered mid-merge is stashed in {@link
 * #stashedSignal} rather than discarded, so the next {@link #fillCurrent()} call picks it up
 * without another {@code take()}. Each individual {@code Data} signal dequeued — whether via the
 * initial {@code take()} or an opportunistic {@code poll()} — still triggers exactly one {@link
 * QueueingSubscriber#requestOne()}, preserving the 1-for-1 credit balance described above. Only
 * *when* that call happens moved — now at dequeue time, inside {@link #fillCurrent()}/{@link
 * #coalesce(ByteBuffer)}, rather than at buffer-exhaustion time inside {@link #read(byte[], int,
 * int)} — not the invariant itself.
 *
 * <h2>Cancellation</h2>
 *
 * {@link #close()} disposes the subscriber, which cancels the upstream subscription — whether
 * nothing has been read yet or a consumer stops early, the source {@code Flux} is torn down instead
 * of left running to completion unread.
 */
public final class FluxInputStreamBridge extends InputStream {

  /**
   * Soft cap on how many bytes {@link #fillCurrent()} will opportunistically merge from
   * already-queued chunks into one {@link #current} buffer — see this class's "Chunk coalescing"
   * Javadoc section. Bounded so a producer that's built up a very deep backlog doesn't force one
   * huge allocation; in practice, rarely binding since the queue itself never holds more than
   * {@code demand + 1} items at once (see {@link #FluxInputStreamBridge}'s constructor). A
   * self-documented placeholder like {@code RowBinaryDecoder.RESPONSE_CHUNK_DEMAND} — not yet tuned
   * against a range of values, just chosen large enough not to bind in practice today.
   */
  private static final int MAX_COALESCE_BYTES = 64 * 1024;

  private final BlockingQueue<StreamSignal> queue;
  private final QueueingSubscriber subscriber;

  private ByteBuffer current = ByteBuffer.allocate(0);
  private boolean finished;

  /**
   * A {@code Complete}/{@code Error} signal found while {@link #fillCurrent()} was
   * opportunistically draining additional already-queued {@code Data} chunks — stashed here instead
   * of discarded, so the next {@link #fillCurrent()} call consumes it directly without another
   * {@code queue.take()}.
   */
  private StreamSignal stashedSignal;

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
    return toCopy;
  }

  /**
   * Fetches the next signal — from {@link #stashedSignal} if {@link #fillCurrent()} previously
   * parked one there, otherwise by blocking on {@link #queue}. Every {@code Data} signal dequeued
   * here triggers exactly one {@link QueueingSubscriber#requestOne()} — the one point in this class
   * where a physical chunk is consumed and credit must be replenished; see this class's "Chunk
   * coalescing" Javadoc section for why that moved here from {@link #read(byte[], int, int)}.
   */
  private StreamSignal takeNextSignal() throws IOException {
    if (stashedSignal != null) {
      final StreamSignal signal = stashedSignal;
      stashedSignal = null;
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

  private boolean fillCurrent() throws IOException {
    final StreamSignal signal = takeNextSignal();
    return switch (signal) {
      case StreamSignal.Data(ByteBuffer buffer) -> {
        current = coalesce(buffer);
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
   * Opportunistically merges any additional {@code Data} chunks already sitting in {@link #queue}
   * onto {@code first} via non-blocking {@link BlockingQueue#poll()} calls, up to {@link
   * #MAX_COALESCE_BYTES} total — see this class's "Chunk coalescing" Javadoc section. A {@code
   * Complete}/{@code Error} found mid-drain is parked in {@link #stashedSignal} rather than
   * discarded. Returns {@code first} unchanged (no extra allocation) when nothing more was
   * immediately available — the common case once the producer isn't running ahead.
   */
  private ByteBuffer coalesce(final ByteBuffer first) {
    List<ByteBuffer> extra = null;
    int totalBytes = first.remaining();
    while (totalBytes < MAX_COALESCE_BYTES) {
      final StreamSignal next = queue.poll();
      if (next == null) {
        break;
      }
      if (!(next instanceof StreamSignal.Data(ByteBuffer buffer))) {
        stashedSignal = next;
        break;
      }
      subscriber.requestOne();
      if (extra == null) {
        extra = new ArrayList<>();
      }
      extra.add(buffer);
      totalBytes += buffer.remaining();
    }
    if (extra == null) {
      return first;
    }
    final ByteBuffer merged = ByteBuffer.allocate(totalBytes);
    merged.put(first);
    for (final ByteBuffer buffer : extra) {
      merged.put(buffer);
    }
    merged.flip();
    return merged;
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

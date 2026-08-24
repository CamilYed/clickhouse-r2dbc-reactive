package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.github.camilyed.clickhouse.r2dbc.core.FluxInputStreamBridge;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Isolates {@link FluxInputStreamBridge}'s own queue/coalescing/copy cost from network wait,
 * network transport, and row decoding — no ClickHouse container, no HTTP, no client-v2 reader
 * involved. Built as step 1 of a reviewed external hints doc's optimization plan (external
 * LLM-generated implementation notes, cross-checked against the actual {@code
 * FluxInputStreamBridge} source before acting on them — same due-diligence pattern as the
 * virtual-thread decoder experiment): the doc's central warning is that JFR showing time inside
 * {@code ArrayBlockingQueue.take()} does not by itself distinguish real synchronization/copy
 * overhead (a cost this bridge could remove) from legitimate waiting for the next network chunk (a
 * cost no queue implementation can remove) — see this bridge's own "Chunk coalescing" Javadoc
 * section for the production numbers ({@code ~285-293} rows/chunk, {@code ~8-12}us/chunk) this
 * benchmark exists to decompose further. This class measures the <em>current</em>, production
 * implementation only under both extremes the doc calls out as needing separate measurement:
 *
 * <p><b>Result (2026-08-24, see ROADMAP.md's own entry for the full write-up): decisive negative.
 * {@link #producerAhead} measured a consistent ~40-50 ns/chunk pure CPU cost for the current
 * queue/coalescing/copy path — roughly 200x smaller than the ~8-12us/chunk production figure above.
 * At 1M rows (~3545 chunks), that's ~150-180us of removable cost out of a ~94,000-131,000us total
 * operation. The doc's proposed zero-copy/SPSC-queue candidate was never built</b> — a ~200x gap
 * between the removable cost and the actual bottleneck is decisive on its own, matching the doc's
 * own "Possible result C" (queue overhead was not the real bottleneck; focus elsewhere) without
 * needing to implement and correctness-test a replacement to find that out.
 *
 * <p>The two scenarios:
 *
 * <ul>
 *   <li>{@link #producerAhead} — every chunk is handed to {@link FluxInputStreamBridge} before the
 *       first {@code read()} call even happens ({@code Flux.fromIterable} emits synchronously, up
 *       to the bridge's initial demand, during {@code hookOnSubscribe}). The queue is rarely if
 *       ever empty here, so this scenario exposes queue/coalescing/copy overhead with network wait
 *       driven as close to zero as this harness can get it.
 *   <li>{@link #consumerAheadNetworkDelayed} — a dedicated producer thread trickles chunks out with
 *       an intended {@link #NETWORK_DELAY_NANOS} gap between them, so {@code
 *       FluxInputStreamBridge.read}'s {@code queue.take()} genuinely blocks waiting between most
 *       chunks. Qualitatively confirms {@code queue.take()} dominates once a producer is slower
 *       than the consumer, but its absolute numbers are a methodology caveat, not a precise
 *       measurement: the actual measured gap on a GitHub-hosted CI runner came out to {@code
 *       ~75-80}us/chunk, not the requested 10us — {@code LockSupport.parkNanos}'s real resolution
 *       on that (shared, virtualized) hardware is coarser than requested. Not worth chasing further
 *       given {@link #producerAhead}'s result already answers the question this class exists to
 *       answer.
 * </ul>
 *
 * <p>{@link #chunkSizeBytes} and {@link #totalResponseBytes} are swept independently so both "many
 * small chunks" and "few large chunks" shapes are covered, including the degenerate single-chunk
 * case ({@code chunkSizeBytes == totalResponseBytes}) where {@link FluxInputStreamBridge}'s
 * coalescing path never has a second buffer to merge at all.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FluxInputStreamBridgeMicrobenchmark {

  /**
   * Mirrors {@code RowBinaryDecoder.RESPONSE_CHUNK_DEMAND} (a private constant there) so this
   * benchmark drives {@link FluxInputStreamBridge} with the same demand production actually uses —
   * kept in sync manually since there's no shared constant to reference across modules, same
   * approach {@link DecoderOnlyBenchmark} already takes for the same reason.
   */
  private static final int DEMAND = 16;

  /**
   * Per-chunk delay {@link #consumerAheadNetworkDelayed}'s producer thread waits before handing
   * over the next chunk — chosen in the same order of magnitude as the {@code ~8-12}us/chunk figure
   * {@link FluxInputStreamBridge}'s own Javadoc cites from real {@code StreamingScanBenchmark}
   * instrumentation, so this scenario's "queue is usually empty on read" property is representative
   * rather than arbitrary.
   */
  private static final long NETWORK_DELAY_NANOS = TimeUnit.MICROSECONDS.toNanos(10);

  /** Chunk size sweep — mirrors the hints doc's own suggested tiers (1 KB through 64 KB). */
  @Param({"1024", "4096", "16384", "65536"})
  public int chunkSizeBytes;

  /**
   * Total response size sweep — deliberately excludes the doc's 16 MB tier to keep CI runtime
   * bounded; 64 KB and 1 MB already span "single/few chunks" through "hundreds of chunks".
   */
  @Param({"65536", "1048576"})
  public int totalResponseBytes;

  private List<ByteBuffer> chunks;
  private byte[] readBuffer;

  /**
   * Slices one deterministic, non-zero payload into {@link #chunkSizeBytes}-sized {@link
   * ByteBuffer} views once per trial — the byte content itself is never asserted on, only the total
   * count read, so a simple repeating pattern is enough.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    final byte[] payload = new byte[totalResponseBytes];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    chunks = sliceIntoChunks(payload, chunkSizeBytes);
    readBuffer = new byte[8192];
  }

  private static List<ByteBuffer> sliceIntoChunks(final byte[] payload, final int chunkSize) {
    final List<ByteBuffer> result = new ArrayList<>();
    int offset = 0;
    while (offset < payload.length) {
      final int length = Math.min(chunkSize, payload.length - offset);
      result.add(ByteBuffer.wrap(payload, offset, length).slice());
      offset += length;
    }
    return result;
  }

  /** See this class's "producer-ahead" bullet in its own Javadoc. */
  @Benchmark
  public void producerAhead(final Blackhole blackhole) {
    final long bytesRead =
        readFully(FluxInputStreamBridge.subscribeTo(Flux.fromIterable(chunks), DEMAND));
    blackhole.consume(bytesRead);
  }

  /** See this class's "consumer-ahead/network-delayed" bullet in its own Javadoc. */
  @Benchmark
  public void consumerAheadNetworkDelayed(final Blackhole blackhole) {
    final long bytesRead =
        readFully(FluxInputStreamBridge.subscribeTo(networkDelayedSource(chunks), DEMAND));
    blackhole.consume(bytesRead);
  }

  /**
   * A dedicated background thread — not a Reactor {@code Scheduler} worker, deliberately, since
   * {@code Flux.delayElements}' timer resolution is coarser than {@link #NETWORK_DELAY_NANOS} —
   * that parks for {@link #NETWORK_DELAY_NANOS} before handing each chunk to the sink, simulating
   * network chunks arriving one at a time rather than all being immediately available.
   */
  private static Flux<ByteBuffer> networkDelayedSource(final List<ByteBuffer> chunks) {
    return Flux.create(
        (final FluxSink<ByteBuffer> sink) -> {
          final Thread producer =
              new Thread(
                  () -> {
                    for (final ByteBuffer chunk : chunks) {
                      LockSupport.parkNanos(NETWORK_DELAY_NANOS);
                      sink.next(chunk.duplicate());
                    }
                    sink.complete();
                  },
                  "bridge-microbenchmark-network-delayed-producer");
          producer.setDaemon(true);
          producer.start();
        });
  }

  private long readFully(final FluxInputStreamBridge bridge) {
    long total = 0;
    try {
      int read = bridge.read(readBuffer, 0, readBuffer.length);
      while (read != -1) {
        total += read;
        read = bridge.read(readBuffer, 0, readBuffer.length);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      bridge.close();
    }
    return total;
  }
}

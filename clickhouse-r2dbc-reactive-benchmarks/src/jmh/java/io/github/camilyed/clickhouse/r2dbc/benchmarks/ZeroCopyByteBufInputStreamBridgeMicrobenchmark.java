package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.github.camilyed.clickhouse.r2dbc.core.FluxInputStreamBridge;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
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

/**
 * Answers a question {@code LatencyPathVariantBBenchmark}'s real-network run
 * (docs/performance/latency-path-isolation.md) couldn't cleanly settle: does the {@code
 * ByteBuf}-&gt;{@code byte[]} copy {@code ClickHouseResult.decodePlain}'s {@code asByteArray()}
 * call performs cost anything measurable, in isolation from HTTP/Docker/network noise? That class's
 * trusted 3-fork run showed a ≤1.3% difference that flipped sign between scenarios (`point` slower
 * with the copy removed, `SELECT 1` faster) — too small and inconsistent to separate "the copy
 * genuinely doesn't matter" from "the effect is real but buried under this machine's
 * few-hundred-microsecond network/Docker noise floor", a distinction that matters whether it's run
 * on a MacBook or on CI, since both carry their own (different) noise floors far larger than a
 * single small-buffer {@code memcpy}.
 *
 * <p>This class removes that noise floor entirely: no ClickHouse container, no HTTP, no client-v2
 * reader, no Reactor scheduler hand-off — just the copy step itself, isolated the same way {@link
 * FluxInputStreamBridgeMicrobenchmark} isolated that bridge's own queue/coalescing cost from
 * network wait (see that class's own decisive-negative result: ~40-50ns/chunk, ~200x smaller than
 * the production per-chunk figure it was checking against — the same style of verdict this class
 * exists to reach for the copy question specifically).
 *
 * <p>Both scenarios build a fresh {@link ByteBuf} per invocation via {@link
 * ByteBufAllocator#buffer(int)} (matching what Reactor Netty actually hands the transport layer —
 * pooled, ref-counted), so allocation cost is common to both sides and cancels out of the
 * comparison:
 *
 * <ul>
 *   <li>{@link #copyPath} performs the same steps {@code ByteBufFlux.asByteArray()} does — copy the
 *       readable bytes into a fresh {@code byte[]}, release the {@link ByteBuf} — then feeds the
 *       result through {@link FluxInputStreamBridge}, exactly as {@code
 *       ClickHouseResult.decodePlain} does today.
 *   <li>{@link #zeroCopyPath} feeds the freshly-allocated {@link ByteBuf} directly into {@link
 *       ZeroCopyByteBufInputStreamBridge} — no intermediate copy.
 * </ul>
 *
 * <p>{@link #responseBytes} sweeps from real {@code SELECT 1}/point response sizes (tens to low
 * hundreds of bytes — the exact scenarios {@code LatencyPathVariantBBenchmark} measured) up through
 * the chunk-size tiers {@link FluxInputStreamBridgeMicrobenchmark} already covers, for context.
 * Each response here is always a single chunk (no coalescing involved) — matching the real shape of
 * a {@code SELECT 1}/point response, which this class exists to explain.
 *
 * <p>Before trusting any run, verify no leak with the same {@code
 * -Dio.netty.leakDetection.level=paranoid} / {@code LeakRecordingResourceLeakDetector} wiring
 * {@code LatencyPathVariantBBenchmark} uses (see docs/performance/latency-path-isolation.md).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ZeroCopyByteBufInputStreamBridgeMicrobenchmark {

  /**
   * Mirrors {@code RowBinaryDecoder.RESPONSE_CHUNK_DEMAND} — see {@link
   * LatencyPathVariantBBenchmark}.
   */
  private static final int DEMAND = 16;

  /**
   * Smallest two tiers ({@code 64}/{@code 256}) bracket the real {@code SELECT 1}/point response
   * sizes {@code LatencyPathVariantBBenchmark} measured; the rest mirror {@link
   * FluxInputStreamBridgeMicrobenchmark}'s own chunk-size sweep for context.
   */
  @Param({"64", "256", "1024", "4096", "16384", "65536"})
  public int responseBytes;

  private byte[] payload;
  private byte[] readBuffer;

  /**
   * Deterministic, non-zero payload of {@link #responseBytes} bytes — content is never asserted on.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    payload = new byte[responseBytes];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    readBuffer = new byte[Math.max(8192, responseBytes)];
  }

  /**
   * {@code asByteArray()}-equivalent copy, then {@link FluxInputStreamBridge} — the production
   * path.
   */
  @Benchmark
  public void copyPath(final Blackhole blackhole) {
    final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(payload.length);
    buf.writeBytes(payload);
    final byte[] copied = new byte[buf.readableBytes()];
    buf.readBytes(copied);
    buf.release();
    final Flux<ByteBuffer> source = Flux.just(ByteBuffer.wrap(copied));
    blackhole.consume(readFully(FluxInputStreamBridge.subscribeTo(source, DEMAND)));
  }

  /**
   * The same freshly-allocated {@link ByteBuf}, fed directly into {@link
   * ZeroCopyByteBufInputStreamBridge} — no intermediate copy.
   *
   * <p>{@code Flux.just(buf)} delivers {@code onNext} synchronously inside {@code subscribeTo}'s
   * constructor call (confirmed by this benchmark's own first, leak-detected run) but — unlike real
   * Reactor Netty {@code ByteBufFlux} sources — does not itself release the buffer once {@code
   * onNext} returns. {@link ZeroCopyByteBufInputStreamBridge}'s {@code hookOnNext} retains on the
   * assumption that release will happen (the documented Netty "retain past onNext" contract real
   * Reactor Netty sources honor), so this benchmark must perform that release itself, once, right
   * after {@code subscribeTo} returns — simulating the production auto-release rather than skipping
   * it. Confirmed missing by the ~10GB direct-memory exhaustion this class's first run hit at the
   * larger {@link #responseBytes} tiers: without this release, every invocation leaked one buffer.
   */
  @Benchmark
  public void zeroCopyPath(final Blackhole blackhole) {
    final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(payload.length);
    buf.writeBytes(payload);
    final ZeroCopyByteBufInputStreamBridge bridge =
        ZeroCopyByteBufInputStreamBridge.subscribeTo(Flux.just(buf), DEMAND);
    buf.release(); // simulates Reactor Netty's own auto-release-after-onNext (see Javadoc above)
    blackhole.consume(readFully(bridge));
  }

  private long readFully(final InputStream source) {
    long total = 0;
    try {
      int read = source.read(readBuffer, 0, readBuffer.length);
      while (read != -1) {
        total += read;
        read = source.read(readBuffer, 0, readBuffer.length);
      }
      return total;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      closeQuietly(source);
    }
  }

  private static void closeQuietly(final InputStream source) {
    try {
      source.close();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

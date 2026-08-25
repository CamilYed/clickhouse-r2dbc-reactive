package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.FluxInputStreamBridge;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Variant C of the latency-path-isolation ladder (docs/performance/latency-path-isolation.md,
 * ROADMAP.md Phase 12) — the last remaining hypothesis after GC (ruled out via {@code -prof gc}),
 * the {@code asByteArray()} copy (Variant B, ruled out), and fixed query/statement construction
 * cost (task #309, ruled out) all failed to explain {@link LatencyPathVariantABenchmark}'s trusted
 * ~2.6-4.9% mean deficit on {@code SELECT 1}/point at both concurrency 1 and 8.
 *
 * <p><b>What this isolates.</b> Today, per this doc's own "Exact pipeline" section, {@code
 * RowBinaryDecoder.decode} subscribes to the transport's response {@code Flux} — the act that
 * actually sends the HTTP request, per this project's own documented "request not sent before
 * subscription" boundary — <em>inside</em> {@code
 * Mono.fromCallable(...).subscribeOn(decoderScheduler)}, i.e. only once a decoder-scheduler worker
 * permit has already been granted. With {@code decodeScheduler}/pool both pinned to {@link
 * #POOL_SIZE}, this means a completed or in-flight HTTP response cannot even begin being consumed
 * until a decoder-worker slot frees up — two independently-sized resources (an 8-connection HTTP
 * pool and an 8-worker decoder scheduler) end up serialized behind each other rather than
 * overlapping, on every single query, not just under overload. This is the ordering Variant C
 * targets, prototyped here without touching production code (this pass's "no production code
 * changes" scope), without buffering the whole response, without blocking the event loop, and
 * without changing cancellation, connection reuse, pool size, decoder identity, or compression
 * versus {@link LatencyPathVariantABenchmark}/{@link LatencyPathVariantBBenchmark}.
 *
 * <p><b>Scenarios</b> (mirrors {@link LatencyPathVariantBBenchmark}'s self-contained-pair shape —
 * same transport instance, same client-v2 reader, same {@link ResponseCompression#NONE}
 * simplification for the same package-private-{@code ClickHouseLz4InputStream} reason documented
 * there):
 *
 * <ul>
 *   <li>{@link #currentOrderingSelect1}/{@link #currentOrderingPoint} — today's production
 *       ordering: {@link FluxInputStreamBridge#subscribeTo} happens inside the {@code
 *       subscribeOn(decodeScheduler)}-scheduled callable, so subscription (and HTTP send) waits for
 *       decoder-worker admission.
 *   <li>{@link #earlyAcquisitionSelect1}/{@link #earlyAcquisitionPoint} — the prototype: {@link
 *       FluxInputStreamBridge#subscribeTo} is called eagerly, on the calling (JMH benchmark)
 *       thread, <em>before</em> {@code subscribeOn(decodeScheduler)} is even reached — subscription
 *       and HTTP send happen immediately, independent of decoder-worker availability. Only the
 *       blocking work downstream of that — client-v2's header-reading constructor and the first row
 *       read — is gated behind decoder-scheduler admission, matching that work's actual nature
 *       (CPU/blocking-read bound, not something that benefits from more concurrent workers than
 *       {@link #POOL_SIZE} once network wait is decoupled from it). Safe to call from any thread:
 *       {@link FluxInputStreamBridge#subscribeTo}'s constructor only calls {@code
 *       source.subscribe(subscriber)}, a synchronous, non-blocking call per Reactor's own subscribe
 *       contract — actual bytes arrive later, asynchronously, on Reactor Netty's event loop,
 *       exactly as they already do in the current-ordering scenario.
 * </ul>
 *
 * <p><b>Expected shape of a real effect, if there is one:</b> negligible difference at concurrency
 * 1 (no admission queueing to decouple from when nothing is contending for either resource), and a
 * shrinking gap at concurrency 8 (HTTP-response consumption starts as soon as the network has
 * something, rather than queueing again behind decoder-worker admission after already having queued
 * behind pool/event-loop capacity). Run at both {@code -Pjmh.threads=1} and {@code -Pjmh.threads=8}
 * (same flags {@link LatencyPathVariantABenchmark} uses) — a difference only at {@code -t8} would
 * be the signature of an admission-gate-ordering effect; a difference at both would point
 * elsewhere; no difference at either would reject this hypothesis, the same way Variant B and task
 * #309 were each rejected.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LatencyPathVariantCBenchmark {

  private static final String SELECT_1_SQL = "SELECT 1";

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private static final int POOL_SIZE = 8;

  // Mirrors RowBinaryDecoder.RESPONSE_CHUNK_DEMAND (private in core) - same value, redeclared here.
  private static final int RESPONSE_CHUNK_DEMAND = 16;

  // Mirrors RowDecodingScheduler's own queued-task-capacity default (private in core).
  private static final int QUEUED_TASK_CAPACITY = 10_000;

  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;
  private static final long ROW_POOL_SIZE = 10_000;

  private ClickHouseHttpTransport ourTransport;
  private Scheduler decodeScheduler;
  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and configures {@link
   * #ourTransport} with {@link ResponseCompression#NONE} and a {@link #POOL_SIZE}-connection pool
   * matching {@link LatencyPathVariantABenchmark}/{@link LatencyPathVariantBBenchmark}'s.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(ROW_POOL_SIZE);
    ids = PointQueryTable.deterministicIds(ROW_POOL_SIZE, ID_POOL_SIZE, ID_SEED);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            TransportOptions.defaults()
                .withAuthentication(
                    Authentication.basic(
                        BenchmarkEnvironment.username(), BenchmarkEnvironment.password()))
                .withMaxConnections(POOL_SIZE)
                .withResponseCompression(ResponseCompression.NONE));
    decodeScheduler =
        Schedulers.newBoundedElastic(POOL_SIZE, QUEUED_TASK_CAPACITY, "variant-c-decoder");
  }

  /** Releases the decode scheduler and this driver's transport/connection pool. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    decodeScheduler.dispose();
    ourTransport.dispose();
  }

  /** Today's production ordering, {@code SELECT 1}. */
  @Benchmark
  public void currentOrderingSelect1(final Blackhole blackhole) {
    blackhole.consume(decodeViaCurrentOrdering(ClickHouseQuery.of(SELECT_1_SQL)));
  }

  /** Variant C prototype: subscribe/send before decoder-scheduler admission, {@code SELECT 1}. */
  @Benchmark
  public void earlyAcquisitionSelect1(final Blackhole blackhole) {
    blackhole.consume(decodeViaEarlyAcquisition(ClickHouseQuery.of(SELECT_1_SQL)));
  }

  /** Today's production ordering, point lookup. */
  @Benchmark
  public void currentOrderingPoint(final Blackhole blackhole) {
    blackhole.consume(decodeViaCurrentOrdering(pointQuery()));
  }

  /** Variant C prototype: subscribe/send before decoder-scheduler admission, point lookup. */
  @Benchmark
  public void earlyAcquisitionPoint(final Blackhole blackhole) {
    blackhole.consume(decodeViaEarlyAcquisition(pointQuery()));
  }

  private ClickHouseQuery pointQuery() {
    final long id = nextId();
    return ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
  }

  /**
   * Today's ordering: {@code FluxInputStreamBridge.subscribeTo} runs inside the {@code
   * subscribeOn(decodeScheduler)}-scheduled callable, so both the subscription (HTTP send) and the
   * blocking read wait behind decoder-worker admission.
   */
  private String decodeViaCurrentOrdering(final ClickHouseQuery query) {
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    return Mono.fromCallable(
            () -> readFirstRow(FluxInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND)))
        .subscribeOn(decodeScheduler)
        .block(Duration.ofSeconds(10));
  }

  /**
   * Variant C prototype: {@code FluxInputStreamBridge.subscribeTo} runs eagerly, on the calling
   * thread, before {@code subscribeOn(decodeScheduler)} is reached — subscription (HTTP send)
   * happens immediately, decoupled from decoder-worker availability. Only the blocking read (header
   * parse + first row) is gated behind decoder-scheduler admission.
   */
  private String decodeViaEarlyAcquisition(final ClickHouseQuery query) {
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    final InputStream input = FluxInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND);
    return Mono.fromCallable(() -> readFirstRow(input))
        .subscribeOn(decodeScheduler)
        .block(Duration.ofSeconds(10));
  }

  /**
   * Reads the schema header plus the single row both scenarios here produce, via client-v2's public
   * {@link RowBinaryWithNamesAndTypesFormatReader} directly — same shape as {@link
   * LatencyPathVariantABenchmark}/{@link LatencyPathVariantBBenchmark}'s equivalent methods, so
   * decode cost is comparable across all three classes.
   */
  private static String readFirstRow(final InputStream source) throws IOException {
    try (InputStream input = source) {
      final RowBinaryWithNamesAndTypesFormatReader reader =
          new RowBinaryWithNamesAndTypesFormatReader(
              input,
              // setUseTimeZone("UTC") mirrors core's own RowBinaryDecoder.newReader - without it
              // AbstractBinaryFormatReader's constructor throws ClientException("Time zone is not
              // set."), first discovered by LatencyPathVariantBBenchmark's own first real run.
              new QuerySettings().setUseTimeZone("UTC"),
              new BinaryStreamReader.DefaultByteBufferAllocator());
      if (reader.next() == null) {
        return "";
      }
      return reader.getString(1);
    }
  }

  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

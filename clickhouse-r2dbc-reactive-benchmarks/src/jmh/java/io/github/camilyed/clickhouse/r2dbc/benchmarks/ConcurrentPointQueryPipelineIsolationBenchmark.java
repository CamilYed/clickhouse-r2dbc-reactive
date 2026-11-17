package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import org.HdrHistogram.Histogram;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Isolates the sustained-concurrency point-query tail before and after this driver's decode/SPI
 * boundary.
 *
 * <p>{@link PointQueryPipelineIsolationBenchmark} established raw-transport parity for one live
 * request at a time. The public matched-pool benchmark subsequently reproduced a stable p90-p99
 * deficit only under sustained logical concurrency, including after reversing benchmark execution
 * order. This class applies that exact sustained shape ({@value #REQUESTS_PER_INVOCATION} requests,
 * pool size {@value #POOL_SIZE}, concurrency 8/32/128) to the earlier raw-response boundary.
 *
 * <p>The comparisons answer one question at a time:
 *
 * <ol>
 *   <li>{@link #aOurTransportRawResponse(OurTransportRawResponseState)} vs {@link
 *       #bClientV2RawResponse(ClientV2RawResponseState)}: does the tail gap already exist while
 *       acquiring matched-pool connections and fully draining raw response bodies, before this
 *       driver's row decoder and R2DBC SPI are involved?
 *   <li>{@link #aOurTransportRawResponse(OurTransportRawResponseState)} vs {@link
 *       #cScheduledDecode(ScheduledDecodeState)}: how much tail appears when the production
 *       scheduler, bridge, decompressor, and row decoder are added to the same transport?
 *   <li>{@link #cScheduledDecode(ScheduledDecodeState)} vs {@link #dFullR2dbc(FullR2dbcState)}: how
 *       much tail remains attributable to the connector and R2DBC Statement/Result/Row path after
 *       transport and decoding are already accounted for?
 * </ol>
 *
 * <p>Every benchmark method has its own state, client, pool, ID cursor, and latency recorder. JMH
 * therefore never keeps client-v2 resources alive in a fork measuring this driver, or vice versa.
 * The prefixed method names deliberately keep this driver's raw path first, matching the reversed
 * ordering control that ruled out run order as the public benchmark's tail-latency explanation.
 *
 * <p>client-v2's response {@link InputStream} is drained by a continuation of its real asynchronous
 * query future, on the client-owned operation executor. It is not moved onto Reactor's event loop,
 * wrapped in a new artificial future, or offloaded via {@code boundedElastic}. This driver's raw
 * path remains end-to-end Reactor Netty and fully consumes each response through {@code
 * ByteBufFlux#asByteArray()} so pooled connections can be reused normally.
 *
 * <p>The raw transport explicitly uses a bounded pending-acquire capacity of {@value
 * #RAW_TRANSPORT_PENDING_ACQUIRE_MAX_COUNT}. Without the full driver's decode scheduler, there is
 * no upstream eight-request admission gate; Reactor Netty's default pending queue ({@code 2 ×
 * maxConnections} = 16) would therefore reject concurrency 32/128 instead of measuring it. The
 * explicit capacity is only large enough to hold the benchmark's worst case (128 logical requests
 * minus 8 live connections = 120 pending) with a small margin. It prevents rejection without
 * introducing unbounded buffering and is part of this raw-boundary experiment, not a proposed
 * production default.
 *
 * <p>Per-query histograms are reset after every warmup iteration but only measurement iterations
 * are merged into the trial summary. This distinction is explicit because JMH invokes {@link
 * TearDown} at {@link Level#Iteration} for warmup and measurement alike.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ConcurrentPointQueryPipelineIsolationBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConcurrentPointQueryPipelineIsolationBenchmark.class);

  private static final long ROWS = 10_000;
  private static final int POOL_SIZE = 8;
  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;
  private static final int REQUESTS_PER_INVOCATION = 4096;
  private static final int PREWARM_CALLS = 64;
  private static final int CLIENT_V2_READ_BUFFER_SIZE = 8 * 1024;
  private static final int RAW_TRANSPORT_PENDING_ACQUIRE_MAX_COUNT = 128;
  private static final Duration TIMEOUT = Duration.ofMinutes(1);
  private static final long LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  /** This driver's Reactor Netty transport, stopped after fully draining the raw response body. */
  @State(Scope.Benchmark)
  public static class OurTransportRawResponseState {

    @Param({"8", "32", "128"})
    public int concurrency;

    private ClickHouseHttpTransport transport;
    private Measurement measurement;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      transport = newRawTransport();
      prewarmRaw(id -> queryOurTransport(transport, id), "our transport");
      measurement = new Measurement(concurrency);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration(final IterationParams iterationParams) {
      finishIteration("ourTransportRawResponse", measurement, iterationParams);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("ourTransportRawResponse", measurement.mergedMeasurementHistogram());
      transport.disposeLater().block(Duration.ofSeconds(10));
    }
  }

  /** client-v2's public asynchronous query path, stopped after draining its raw response stream. */
  @State(Scope.Benchmark)
  public static class ClientV2RawResponseState {

    @Param({"8", "32", "128"})
    public int concurrency;

    private Client client;
    private Measurement measurement;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      client =
          new Client.Builder()
              .addEndpoint(BenchmarkEnvironment.httpUrl())
              .setUsername(BenchmarkEnvironment.username())
              .setPassword(BenchmarkEnvironment.password())
              .setDefaultDatabase("default")
              .useAsyncRequests(true)
              .enableConnectionPool(true)
              .setMaxConnections(POOL_SIZE)
              .build();
      prewarmRaw(id -> queryClientV2RawResponse(client, id), "client-v2");
      measurement = new Measurement(concurrency);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration(final IterationParams iterationParams) {
      finishIteration("clientV2RawResponse", measurement, iterationParams);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("clientV2RawResponse", measurement.mergedMeasurementHistogram());
      client.close();
    }
  }

  /** This driver's transport plus its production off-event-loop row-decoding boundary. */
  @State(Scope.Benchmark)
  public static class ScheduledDecodeState {

    @Param({"8", "32", "128"})
    public int concurrency;

    private ClickHouseHttpTransport transport;
    private RowDecodingScheduler scheduler;
    private Measurement measurement;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      transport = newRawTransport();
      scheduler = RowDecodingScheduler.withWorkerCount(POOL_SIZE);
      prewarm(id -> queryScheduledDecode(transport, scheduler, id), "scheduled decoder");
      measurement = new Measurement(concurrency);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration(final IterationParams iterationParams) {
      finishIteration("scheduledDecode", measurement, iterationParams);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("scheduledDecode", measurement.mergedMeasurementHistogram());
      scheduler.dispose();
      transport.disposeLater().block(Duration.ofSeconds(10));
    }
  }

  /** Full public R2DBC path, retained as the concurrent end-to-end control. */
  @State(Scope.Benchmark)
  public static class FullR2dbcState {

    @Param({"8", "32", "128"})
    public int concurrency;

    private OurDriverPointQueryClient client;
    private Measurement measurement;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      client = new OurDriverPointQueryClient(POOL_SIZE);
      prewarm(id -> client.query(id).map(PointResult::checksum), "full R2DBC");
      measurement = new Measurement(concurrency);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration(final IterationParams iterationParams) {
      finishIteration("fullR2dbc", measurement, iterationParams);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      logMergedLatencySummary("fullR2dbc", measurement.mergedMeasurementHistogram());
      client.close();
    }
  }

  /** Measures Reactor Netty connection acquisition, request/response, and complete body drain. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long aOurTransportRawResponse(final OurTransportRawResponseState state) {
    return run(state.measurement, id -> queryOurTransport(state.transport, id));
  }

  /** Measures client-v2 connection acquisition, request/response, and complete body drain. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long bClientV2RawResponse(final ClientV2RawResponseState state) {
    return run(state.measurement, id -> queryClientV2RawResponse(state.client, id));
  }

  /** Measures transport plus production scheduler, bridge, decompression, and row decoding. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long cScheduledDecode(final ScheduledDecodeState state) {
    return run(state.measurement, id -> queryScheduledDecode(state.transport, state.scheduler, id));
  }

  /** Measures the complete public R2DBC point-query path as the end-to-end control. */
  @Benchmark
  @OperationsPerInvocation(REQUESTS_PER_INVOCATION)
  public long dFullR2dbc(final FullR2dbcState state) {
    return run(state.measurement, id -> state.client.query(id).map(PointResult::checksum));
  }

  private static long run(final Measurement measurement, final LongFunction<Mono<Long>> query) {
    final Long checksum = measurement.execute(query).block(TIMEOUT);
    if (checksum == null) {
      throw new IllegalStateException("Concurrent point-query workload returned null");
    }
    return checksum;
  }

  private static ClickHouseHttpTransport newRawTransport() {
    return new ClickHouseHttpTransport(
        BenchmarkEnvironment.httpUrl(),
        TransportOptions.defaults()
            .withAuthentication(
                Authentication.basic(
                    BenchmarkEnvironment.username(), BenchmarkEnvironment.password()))
            .withMaxConnections(POOL_SIZE)
            .withPendingAcquireMaxCount(RAW_TRANSPORT_PENDING_ACQUIRE_MAX_COUNT));
  }

  private static void finishIteration(
      final String label, final Measurement measurement, final IterationParams iterationParams) {
    final Histogram interval =
        iterationParams.getType() == IterationType.MEASUREMENT
            ? measurement.finishMeasurementIteration()
            : measurement.finishWarmupIteration();
    logLatencySummary(label, interval);
  }

  private static Mono<Long> queryOurTransport(
      final ClickHouseHttpTransport transport, final long id) {
    final ClickHouseQuery query =
        ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
    return transport.query(query).asByteArray().reduce(0L, (total, chunk) -> total + chunk.length);
  }

  private static Mono<Long> queryScheduledDecode(
      final ClickHouseHttpTransport transport,
      final RowDecodingScheduler scheduler,
      final long id) {
    final ClickHouseQuery query =
        ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
    return transport
        .queryWithSummary(query)
        .flatMap(
            response ->
                RowBinaryDecoder.decode(
                    response.body().asByteArray().map(ByteBuffer::wrap),
                    scheduler,
                    transport.responseCompression()))
        .flatMap(decoded -> decoded.rows().single())
        .map(ConcurrentPointQueryPipelineIsolationBenchmark::checksum);
  }

  private static Mono<Long> queryClientV2RawResponse(final Client client, final long id) {
    return Mono.defer(
        () ->
            Mono.fromFuture(
                client
                    .query(SELECT_BY_ID_SQL, Map.of("id", id))
                    .thenApply(ConcurrentPointQueryPipelineIsolationBenchmark::drainClientV2)));
  }

  private static long drainClientV2(final QueryResponse response) {
    try (QueryResponse closeable = response;
        InputStream body = closeable.getInputStream()) {
      final byte[] buffer = new byte[CLIENT_V2_READ_BUFFER_SIZE];
      long totalBytes = 0;
      int bytesRead;
      while ((bytesRead = body.read(buffer)) != -1) {
        totalBytes += bytesRead;
      }
      return totalBytes;
    } catch (final Exception e) {
      throw new CompletionException(e);
    }
  }

  private static long checksum(final DecodedRow row) {
    final String label = (String) row.valueAt(0);
    final BigDecimal amount = (BigDecimal) row.valueAt(1);
    return 31L * label.hashCode() + amount.unscaledValue().longValue();
  }

  private static void prewarmRaw(final LongFunction<Mono<Long>> query, final String description) {
    final long value = prewarm(query, description);
    if (value <= 0) {
      throw new IllegalStateException(description + " returned an empty response body");
    }
  }

  private static long prewarm(final LongFunction<Mono<Long>> query, final String description) {
    final long[] ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
    long checksum = 0;
    for (int i = 0; i < PREWARM_CALLS; i++) {
      final Long value = query.apply(ids[i % ids.length]).block(TIMEOUT);
      if (value == null) {
        throw new IllegalStateException(description + " prewarm returned null");
      }
      checksum += value;
    }
    return checksum;
  }

  private static void logLatencySummary(final String label, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    LOG.info(
        "{} per-query latency (µs, n={}): mean={}, p50={}, p90={}, p95={}, p99={}, p99.9={}, max={}",
        label,
        histogram.getTotalCount(),
        String.format("%.1f", histogram.getMean()),
        histogram.getValueAtPercentile(50),
        histogram.getValueAtPercentile(90),
        histogram.getValueAtPercentile(95),
        histogram.getValueAtPercentile(99),
        histogram.getValueAtPercentile(99.9),
        histogram.getMaxValue());
  }

  private static void logMergedLatencySummary(final String label, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    LOG.info(
        "{} TRUE MERGED MEASUREMENT-ONLY per-query latency (µs, n={}): mean={}, p50={}, p90={},"
            + " p95={}, p99={}, p99.9={}, max={}",
        label,
        histogram.getTotalCount(),
        String.format("%.1f", histogram.getMean()),
        histogram.getValueAtPercentile(50),
        histogram.getValueAtPercentile(90),
        histogram.getValueAtPercentile(95),
        histogram.getValueAtPercentile(99),
        histogram.getValueAtPercentile(99.9),
        histogram.getMaxValue());
  }

  private static final class Measurement {

    private final long[] ids = PointQueryTable.deterministicIds(ROWS, ID_POOL_SIZE, ID_SEED);
    private final AtomicLong idCursor = new AtomicLong();
    private final IterationLatencyRecorder latencyRecorder =
        new IterationLatencyRecorder(LATENCY_HIGHEST_TRACKABLE_VALUE_MICROS);
    private final SustainedPointQueryWorkload workload;

    private Measurement(final int concurrency) {
      workload = new SustainedPointQueryWorkload(REQUESTS_PER_INVOCATION, concurrency);
    }

    private Mono<Long> execute(final LongFunction<Mono<Long>> query) {
      return workload.execute(this::nextId, query, latencyRecorder::record);
    }

    private Histogram finishWarmupIteration() {
      return latencyRecorder.finishWarmupIteration();
    }

    private Histogram finishMeasurementIteration() {
      return latencyRecorder.finishMeasurementIteration();
    }

    private Histogram mergedMeasurementHistogram() {
      return latencyRecorder.mergedMeasurementHistogram();
    }

    private long nextId() {
      final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
      return ids[index];
    }
  }
}

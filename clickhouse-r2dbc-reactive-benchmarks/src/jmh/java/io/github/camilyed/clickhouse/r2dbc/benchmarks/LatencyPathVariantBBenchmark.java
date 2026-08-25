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
import reactor.netty.ByteBufFlux;

/**
 * Variant B of the latency-path-isolation ladder (docs/performance/latency-path-isolation.md,
 * ROADMAP.md Phase 12): isolates the single question {@link LatencyPathVariantABenchmark}'s
 * trusted 3-fork numbers pointed at as the best-supported next step — does {@code
 * ClickHouseResult.decodePlain}'s {@code response.asByteArray().map(ByteBuffer::wrap)} copy explain
 * the flat ~4-5% mean deficit seen on {@code SELECT 1}/point at both concurrency 1 and 8, after GC
 * was ruled out by a {@code -prof gc} run.
 *
 * <p><b>Deliberately a self-contained A/B pair, not a cross-comparison against {@link
 * LatencyPathVariantABenchmark}.</b> Both benchmark methods below run inside <em>this</em> class,
 * against the <em>same</em> {@link ClickHouseHttpTransport} instance and the <em>same</em>
 * client-v2 reader class, differing only in which {@link InputStream} bridge feeds the reader —
 * {@code core.FluxInputStreamBridge} (the production copy path) vs {@link
 * ZeroCopyByteBufInputStreamBridge} (the zero-copy prototype). This is the correct way to isolate
 * one variable; comparing this class's numbers against Variant A's separately-run class would
 * conflate the copy question with whatever run-to-run noise exists between two independent JMH
 * invocations.
 *
 * <p><b>Response compression is disabled ({@link ResponseCompression#NONE}), unlike Variant A's
 * production-default {@link ResponseCompression#LZ4}.</b> This is a deliberate, disclosed
 * simplification, not an oversight: {@code core.ClickHouseLz4InputStream} (the production
 * LZ4-unwrapping class) is package-private in {@code core} and out of reach from this module
 * without widening core's visibility purely for a benchmark, which this pass's "no production code
 * changes" scope rules out. Decompression is itself a copy — a separate question from the one this
 * variant isolates — so running both paths uncompressed keeps the comparison clean rather than
 * conflating the two. Both {@code copyPath*} and {@code zeroCopyPath*} methods below use the same
 * {@link #ourTransport} instance, so this simplification applies identically to both sides.
 *
 * <p><b>Scenarios</b>: {@code SELECT 1} and the same point lookup {@link LatencyPathVariantABenchmark}
 * uses, against {@link PointQueryTable}. Streaming is deliberately out of scope for this first cut —
 * Variant A's trusted numbers already showed streaming favoring this driver, not the scenario the
 * flat deficit showed up in, and it's the scenario most likely to exercise {@link
 * ZeroCopyByteBufInputStreamBridge}'s disclosed hard-cancel race (see that class's Javadoc), which
 * this narrower first cut avoids needing to reason about at all.
 *
 * <p><b>Decoding</b> uses client-v2's own public {@code
 * RowBinaryWithNamesAndTypesFormatReader}/{@code AbstractBinaryFormatReader} directly (not this
 * project's package-private {@code ListDecodingRowBinaryReader}) and a benchmark-local {@link
 * Schedulers#newBoundedElastic(int, int, String) newBoundedElastic} scheduler (not {@code
 * RowDecodingScheduler}'s package-private internals) — both faithful substitutes for what {@code
 * core} does internally, chosen because the only thing {@code ListDecodingRowBinaryReader} adds
 * over its client-v2 base class is List/Enum column normalization, irrelevant to the {@code SELECT
 * 1}/point scenarios here (no Array/Enum columns), and {@code RowDecodingScheduler.create()} builds
 * exactly this kind of bounded-elastic scheduler internally. See ROADMAP.md's Phase 12 section and
 * this class's own investigation notes in the latency-path-isolation doc.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LatencyPathVariantBBenchmark {

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
   * #ourTransport} with {@link ResponseCompression#NONE} (see class Javadoc for why) and a
   * {@link #POOL_SIZE}-connection pool matching {@link LatencyPathVariantABenchmark}'s.
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
        Schedulers.newBoundedElastic(POOL_SIZE, QUEUED_TASK_CAPACITY, "variant-b-decoder");
  }

  /** Releases the decode scheduler and this driver's transport/connection pool. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    decodeScheduler.dispose();
    ourTransport.dispose();
  }

  /** Production copy path, {@code SELECT 1}: {@code asByteArray()} into {@code FluxInputStreamBridge}. */
  @Benchmark
  public void copyPathSelect1(final Blackhole blackhole) {
    blackhole.consume(decodeViaCopyPath(ClickHouseQuery.of(SELECT_1_SQL)));
  }

  /** Zero-copy prototype, {@code SELECT 1}: raw {@code ByteBuf} into {@link ZeroCopyByteBufInputStreamBridge}. */
  @Benchmark
  public void zeroCopyPathSelect1(final Blackhole blackhole) {
    blackhole.consume(decodeViaZeroCopyPath(ClickHouseQuery.of(SELECT_1_SQL)));
  }

  /** Production copy path, point lookup. */
  @Benchmark
  public void copyPathPoint(final Blackhole blackhole) {
    blackhole.consume(decodeViaCopyPath(pointQuery()));
  }

  /** Zero-copy prototype, point lookup. */
  @Benchmark
  public void zeroCopyPathPoint(final Blackhole blackhole) {
    blackhole.consume(decodeViaZeroCopyPath(pointQuery()));
  }

  private ClickHouseQuery pointQuery() {
    final long id = nextId();
    return ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
  }

  private String decodeViaCopyPath(final ClickHouseQuery query) {
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    return Mono.fromCallable(
            () -> readFirstRow(FluxInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND)))
        .subscribeOn(decodeScheduler)
        .block(Duration.ofSeconds(10));
  }

  private String decodeViaZeroCopyPath(final ClickHouseQuery query) {
    final ByteBufFlux body = ourTransport.query(query);
    return Mono.fromCallable(
            () -> readFirstRow(ZeroCopyByteBufInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND)))
        .subscribeOn(decodeScheduler)
        .block(Duration.ofSeconds(10));
  }

  /**
   * Reads the schema header plus the single row both scenarios here produce, via client-v2's
   * public {@link RowBinaryWithNamesAndTypesFormatReader} directly — same shape as {@link
   * LatencyPathVariantABenchmark}'s {@code clientV2*} methods, so decode cost (typed getter
   * conversion, not just raw bytes) is comparable across both benchmark classes.
   */
  private static String readFirstRow(final InputStream source) throws IOException {
    try (InputStream input = source) {
      final RowBinaryWithNamesAndTypesFormatReader reader =
          new RowBinaryWithNamesAndTypesFormatReader(
              input, new QuerySettings(), new BinaryStreamReader.DefaultByteBufferAllocator());
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

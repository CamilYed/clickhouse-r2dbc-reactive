package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import java.io.IOException;
import java.io.InputStream;
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
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ByteBufFlux;

/**
 * Variant D of the latency-path-isolation ladder (docs/performance/latency-path-isolation.md,
 * ROADMAP.md Phase 12) — the one layer none of A/B/C/task #309 ever isolated: client-v2's own
 * {@code RowBinaryWithNamesAndTypesFormatReader}/{@code AbstractBinaryFormatReader}/{@code
 * BinaryStreamReader} parsing machinery itself. Every prior variant treated that reader as a black
 * box and varied something *around* it (the copy feeding it, the admission ordering before it, the
 * query object built before calling it) — this variant swaps the reader itself for {@link
 * MinimalRowBinaryReader}, a minimal hand-rolled decoder covering only the four ClickHouse wire
 * types {@code TrivialQueryBenchmark}/{@code PointQueryBenchmark} actually use.
 *
 * <p><b>Deliberately holds the copy-vs-zero-copy question constant.</b> Both scenarios here feed
 * from {@link ZeroCopyByteBufInputStreamBridge} — Variant B already measured that choice at 15-35ns
 * a call, negligible either way, so using it for both sides here isolates the one remaining
 * untested variable (the reader/parsing layer) instead of re-mixing a question this ladder already
 * answered.
 *
 * <p>Same self-contained-pair shape as Variant B/C: one class, same transport instance, {@link
 * ResponseCompression#NONE} (same package-private-{@code ClickHouseLz4InputStream} reason as
 * those), {@code POOL_SIZE=8}. Scenarios: {@code SELECT 1} (single {@code UInt8} column) and the
 * same point lookup ({@code label String}, {@code amount Decimal(18,4)}) Variant B/C use.
 *
 * <p><b>Scope.</b> Trusted t1/t8 results on the SELECT 1 / point pair showed a scenario-dependent
 * effect (noise/flip on 1-column SELECT 1, a real and reproducible ~5-7% gap on the 2-column point
 * lookup) — consistent with a per-column decode cost rather than a fixed per-request one. The
 * {@code *Stream10k} pair added after that result reuses {@link PointQueryTable}'s existing
 * 3-column shape at the "10000" row tier (same shape as {@link StreamingScanBenchmark}, single tier
 * only) to check whether that per-column cost compounds over many rows or stays flat.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LatencyPathVariantDBenchmark {

  private static final String SELECT_1_SQL = "SELECT 1";

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  // Same shape as StreamingScanBenchmark's SELECT_ALL_SQL, at the "10000" tier only - this pair
  // exists to tell fixed-per-request cost apart from per-row cost (see trusted-t1/t8 verdict in
  // docs/performance/latency-path-isolation.md: the reader-layer effect showed up for the
  // 2-column point scenario but not the 1-column SELECT 1 scenario, consistent with a per-column
  // decode cost - this pair checks whether that cost compounds over many rows).
  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;

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
   * matching Variant A/B/C's.
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
        Schedulers.newBoundedElastic(POOL_SIZE, QUEUED_TASK_CAPACITY, "variant-d-decoder");
  }

  /** Releases the decode scheduler and this driver's transport/connection pool. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    decodeScheduler.dispose();
    ourTransport.dispose();
  }

  /** client-v2's reader, {@code SELECT 1}. */
  @Benchmark
  public void clientV2ReaderSelect1(final Blackhole blackhole) {
    blackhole.consume(decodeViaClientV2Reader(ClickHouseQuery.of(SELECT_1_SQL)));
  }

  /** {@link MinimalRowBinaryReader}, {@code SELECT 1}. */
  @Benchmark
  public void minimalReaderSelect1(final Blackhole blackhole) {
    blackhole.consume(decodeViaMinimalReader(ClickHouseQuery.of(SELECT_1_SQL)));
  }

  /** client-v2's reader, point lookup. */
  @Benchmark
  public void clientV2ReaderPoint(final Blackhole blackhole) {
    blackhole.consume(decodeViaClientV2Reader(pointQuery()));
  }

  /** {@link MinimalRowBinaryReader}, point lookup. */
  @Benchmark
  public void minimalReaderPoint(final Blackhole blackhole) {
    blackhole.consume(decodeViaMinimalReader(pointQuery()));
  }

  /** client-v2's reader, full 10k-row scan - same shape as {@link StreamingScanBenchmark}. */
  @Benchmark
  public void clientV2ReaderStream10k(final Blackhole blackhole) {
    decodeStreamViaClientV2Reader(blackhole);
  }

  /** {@link MinimalRowBinaryReader}, full 10k-row scan. */
  @Benchmark
  public void minimalReaderStream10k(final Blackhole blackhole) {
    decodeStreamViaMinimalReader(blackhole);
  }

  private ClickHouseQuery pointQuery() {
    final long id = nextId();
    return ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
  }

  private String decodeViaClientV2Reader(final ClickHouseQuery query) {
    final ByteBufFlux body = ourTransport.query(query);
    return Mono.fromCallable(
            () ->
                readFirstRowViaClientV2(
                    ZeroCopyByteBufInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND)))
        .subscribeOn(decodeScheduler)
        .block(Duration.ofSeconds(10));
  }

  private String decodeViaMinimalReader(final ClickHouseQuery query) {
    final ByteBufFlux body = ourTransport.query(query);
    return Mono.fromCallable(
            () ->
                readFirstRowViaMinimalReader(
                    ZeroCopyByteBufInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND)))
        .subscribeOn(decodeScheduler)
        .block(Duration.ofSeconds(10));
  }

  /** Same shape as Variant B/C's {@code readFirstRow} — client-v2's public reader, directly. */
  private static String readFirstRowViaClientV2(final InputStream source) throws IOException {
    try (InputStream input = source) {
      final RowBinaryWithNamesAndTypesFormatReader reader =
          new RowBinaryWithNamesAndTypesFormatReader(
              input,
              new QuerySettings().setUseTimeZone("UTC"),
              new BinaryStreamReader.DefaultByteBufferAllocator());
      if (reader.next() == null) {
        return "";
      }
      return reader.getString(1);
    }
  }

  private void decodeStreamViaClientV2Reader(final Blackhole blackhole) {
    final ByteBufFlux body = ourTransport.query(ClickHouseQuery.of(SELECT_ALL_SQL));
    final long rowCount =
        Mono.fromCallable(
                () ->
                    readAllRowsViaClientV2(
                        ZeroCopyByteBufInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND),
                        blackhole))
            .subscribeOn(decodeScheduler)
            .block(Duration.ofSeconds(10));
    blackhole.consume(rowCount);
  }

  private void decodeStreamViaMinimalReader(final Blackhole blackhole) {
    final ByteBufFlux body = ourTransport.query(ClickHouseQuery.of(SELECT_ALL_SQL));
    final long rowCount =
        Mono.fromCallable(
                () ->
                    readAllRowsViaMinimalReader(
                        ZeroCopyByteBufInputStreamBridge.subscribeTo(body, RESPONSE_CHUNK_DEMAND),
                        blackhole))
            .subscribeOn(decodeScheduler)
            .block(Duration.ofSeconds(10));
    blackhole.consume(rowCount);
  }

  /**
   * Drains every row of the {@code SELECT_ALL_SQL} scan via client-v2's reader, consuming all three
   * columns (matching {@link StreamingScanBenchmark#clientV2}'s own per-row reads) so the decode of
   * every column is on the hot path, not just the first.
   */
  private static long readAllRowsViaClientV2(final InputStream source, final Blackhole blackhole)
      throws IOException {
    try (InputStream input = source) {
      final RowBinaryWithNamesAndTypesFormatReader reader =
          new RowBinaryWithNamesAndTypesFormatReader(
              input,
              new QuerySettings().setUseTimeZone("UTC"),
              new BinaryStreamReader.DefaultByteBufferAllocator());
      long rowCount = 0;
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
        blackhole.consume(reader.getString(2));
        blackhole.consume(reader.getBigDecimal(3));
        rowCount++;
      }
      return rowCount;
    }
  }

  /** Same shape as {@link #readAllRowsViaClientV2}, via {@link MinimalRowBinaryReader}. */
  private static long readAllRowsViaMinimalReader(
      final InputStream source, final Blackhole blackhole) throws IOException {
    try (MinimalRowBinaryReader reader = MinimalRowBinaryReader.open(source)) {
      long rowCount = 0;
      Object[] row;
      while ((row = reader.nextRow()) != null) {
        blackhole.consume(row[0]);
        blackhole.consume(row[1]);
        blackhole.consume(row[2]);
        rowCount++;
      }
      return rowCount;
    }
  }

  /**
   * Same first-column-as-string convention as {@link #readFirstRowViaClientV2} — {@code UInt8} for
   * {@code SELECT 1} widens to its {@code Long} boxed form's {@code toString()} (matches
   * client-v2's own {@code getString} widening for that scenario), {@code label} for the point
   * lookup.
   */
  private static String readFirstRowViaMinimalReader(final InputStream source) throws IOException {
    try (MinimalRowBinaryReader reader = MinimalRowBinaryReader.open(source)) {
      final Object[] row = reader.nextRow();
      if (row == null) {
        return "";
      }
      return String.valueOf(row[0]);
    }
  }

  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

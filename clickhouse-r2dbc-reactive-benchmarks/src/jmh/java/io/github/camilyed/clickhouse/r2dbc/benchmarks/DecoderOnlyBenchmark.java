package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QuerySettings;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.FluxInputStreamBridge;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
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
import reactor.core.publisher.SynchronousSink;

/**
 * Diagnostic isolation benchmark for {@link StreamingScanBenchmark}'s confirmed, growing regression
 * (see docs/PERFORMANCE.md's Phase 5 "Optimization phase" section — hypotheses H0/H1): decodes the
 * exact same bytes from memory, no network involved on either side, isolating pure
 * row-decode/materialization cost from transport/bridge cost (which {@link
 * TransportOnlyStreamingBenchmark} isolates the other way). Together the two answer "where does the
 * gap actually live" instead of guessing from {@code StreamingScanBenchmark}'s combined number
 * alone.
 *
 * <p>The response body for {@code rows} is captured once in {@code @Setup(Level.Trial)} — a real
 * query against {@link PointQueryTable}, fully aggregated into one {@code byte[]} via {@code
 * .aggregate().asByteArray()} (the same operator this transport's own {@code killQueryBestEffort}
 * uses) — outside the measured region. Every {@code @Benchmark} method then decodes that same
 * in-memory payload repeatedly. Deliberately handed to each decoder as a single chunk rather than
 * replaying original network fragmentation: this isolates decode cost from chunk-boundary effects
 * on purpose, since chunking is exactly what {@link TransportOnlyStreamingBenchmark} exists to
 * cover.
 *
 * <p>Once H1 (the {@code LinkedHashMap} copy) was confirmed dominant and removed as a variable
 * ({@link #thisDriverCompactRow}), a real but smaller residual gap against {@link #clientV2}
 * remained. {@link #compactRowDirectLoop} and {@link #compactRowFluxNoBridge} are a small factorial
 * matrix isolating where that residual actually lives — bridge vs. Reactor vs. the row object
 * itself — one dimension changed at a time, rather than lumping it all under "bridge overhead": see
 * docs/PERFORMANCE.md's Phase 5 "Optimization phase" section for the full H2a–H2d breakdown and the
 * numbers once this matrix has been run.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class DecoderOnlyBenchmark {

  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;

  /**
   * Mirrors {@code RowBinaryDecoder.RESPONSE_CHUNK_DEMAND} (a private constant there) so {@link
   * #thisDriverWithoutMapCopy} feeds {@link FluxInputStreamBridge} identically to production — kept
   * in sync manually since there's no shared constant to reference across modules; revisit if that
   * value ever changes. Currently {@code 16} — see that constant's Javadoc for the chunk-coalescing
   * follow-up that raised it from {@code 4}.
   */
  private static final int RESPONSE_CHUNK_DEMAND = 16;

  /**
   * Row-count tiers — same shape as {@link StreamingScanBenchmark}'s, for a like-for-like split.
   */
  @Param({"10000", "100000", "1000000"})
  public long rows;

  private byte[] capturedResponseBytes;

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and captures one full response body
   * for {@code rows} — the only network call this benchmark class ever makes, run once per trial,
   * outside the measured window. Captured with {@link ResponseCompression#NONE} explicitly (not
   * {@link ClickHouseHttpTransport}'s {@link TransportOptions#defaults()} LZ4 default): every
   * {@code @Benchmark} method below decodes {@link #capturedResponseBytes} either via {@link
   * RowBinaryDecoder#decodeRows} with an explicit {@link ResponseCompression} argument, or by
   * feeding {@link FluxInputStreamBridge} the raw bytes directly with no decompression step at all
   * ({@link #thisDriverWithoutMapCopy}/{@link #thisDriverCompactRow}/{@link #compactRowDirectLoop}/
   * {@link #compactRowFluxNoBridge}/{@link #clientV2}) — capturing plain bytes up front keeps every
   * one of those consistent without threading compression through each consumption site.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            TransportOptions.defaults()
                .withAuthentication(
                    Authentication.basic(
                        BenchmarkEnvironment.username(), BenchmarkEnvironment.password()))
                .withResponseCompression(ResponseCompression.NONE));
    capturedResponseBytes =
        transport
            .query(ClickHouseQuery.of(SELECT_ALL_SQL))
            .aggregate()
            .asByteArray()
            .block(Duration.ofSeconds(60));
  }

  /**
   * This driver's production decode path, {@link RowBinaryDecoder#decodeRows}, over captured bytes.
   */
  @Benchmark
  public void thisDriver(final Blackhole blackhole) {
    final Flux<ByteBuffer> body = Flux.just(ByteBuffer.wrap(capturedResponseBytes));
    final long rowCount =
        RowBinaryDecoder.decodeRows(body, ResponseCompression.NONE)
            .count()
            .block(Duration.ofSeconds(60));
    blackhole.consume(rowCount);
  }

  /**
   * Diagnostic-only — never used in production. Identical to {@link #thisDriver} in every respect
   * (same {@link FluxInputStreamBridge}, same reader settings, same {@code Flux.generate} per-row
   * emission shape) except the final {@code new LinkedHashMap<>(reader.next())} copy is skipped:
   * {@code reader.next()} is still called (client-v2's own per-row cost is still paid, unchanged),
   * but nothing is copied out of it — a constant is emitted downstream instead. A single-variable
   * change against {@link #thisDriver}, added specifically so H1's cost can be measured directly
   * rather than inferred by subtracting client-v2's baseline from this driver's total, which folds
   * in every other difference between the two paths (the bridge's own queue/{@code StreamSignal}
   * machinery, {@code Flux.generate}'s state, the reader subclass) along with the map copy — see
   * docs/PERFORMANCE.md's Phase 5 "Optimization phase" section for why that subtraction wasn't
   * rigorous enough to attribute a bytes/row number to H1.
   */
  @Benchmark
  public void thisDriverWithoutMapCopy(final Blackhole blackhole) {
    final RowBinaryWithNamesAndTypesFormatReader reader =
        new RowBinaryWithNamesAndTypesFormatReader(
            FluxInputStreamBridge.subscribeTo(
                Flux.just(ByteBuffer.wrap(capturedResponseBytes)), RESPONSE_CHUNK_DEMAND),
            new QuerySettings()
                .setUseTimeZone("UTC")
                .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1"),
            new BinaryStreamReader.DefaultByteBufferAllocator());
    final long rowCount =
        Flux.generate(
                () -> reader,
                (final RowBinaryWithNamesAndTypesFormatReader r,
                    final SynchronousSink<Boolean> sink) -> {
                  if (r.hasNext()) {
                    r.next();
                    sink.next(Boolean.TRUE);
                  } else {
                    sink.complete();
                  }
                  return r;
                })
            .count()
            .block(Duration.ofSeconds(60));
    blackhole.consume(rowCount);
  }

  /**
   * The candidate that validated the compact-row redesign now used by production (see {@code
   * ListDecodingRowBinaryReader#nextRowValues} / {@link RowBinaryDecoder}): a compact {@code
   * Object[]} snapshot per row, built here from the same three typed getters {@link #clientV2}
   * calls ({@code getLong}/{@code getString}/{@code getBigDecimal}), never touching {@code
   * RecordWrapper.entrySet()} at all. Unlike {@link #thisDriverWithoutMapCopy} (which discards each
   * row untouched — useful for isolating H1's cost, but not a fair comparison against {@link
   * #clientV2}, which does real per-value work), this method does the same per-column extraction
   * {@link #clientV2} does and retains a real per-row object (an {@code Object[]}, blackholed same
   * as the row count) — the shape a production row needs to hand back to an R2DBC caller.
   *
   * <p>Production itself takes a related but slightly different, likely cheaper path: instead of
   * calling three typed getters per row, {@code ListDecodingRowBinaryReader#nextRowValues} clones
   * the reader's own already-decoded {@code currentRecord} array directly (bypassing per-column
   * getter dispatch entirely, not just the {@code Map} copy) — see that method's own Javadoc. This
   * benchmark's numbers are therefore an upper bound on the redesign's actual cost, not an exact
   * prediction; a dedicated benchmark exercising {@code nextRowValues} itself would measure that
   * path directly. Not yet built — see docs/PERFORMANCE.md's Phase 5 section for the open
   * follow-up.
   */
  @Benchmark
  public void thisDriverCompactRow(final Blackhole blackhole) {
    final RowBinaryWithNamesAndTypesFormatReader reader =
        new RowBinaryWithNamesAndTypesFormatReader(
            FluxInputStreamBridge.subscribeTo(
                Flux.just(ByteBuffer.wrap(capturedResponseBytes)), RESPONSE_CHUNK_DEMAND),
            new QuerySettings()
                .setUseTimeZone("UTC")
                .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1"),
            new BinaryStreamReader.DefaultByteBufferAllocator());
    final long rowCount =
        Flux.generate(
                () -> reader,
                (final RowBinaryWithNamesAndTypesFormatReader r,
                    final SynchronousSink<Object[]> sink) -> {
                  if (r.hasNext()) {
                    r.next();
                    final Object[] row = {r.getLong(1), r.getString(2), r.getBigDecimal(3)};
                    blackhole.consume(row);
                    sink.next(row);
                  } else {
                    sink.complete();
                  }
                  return r;
                })
            .count()
            .block(Duration.ofSeconds(60));
    blackhole.consume(rowCount);
  }

  /**
   * H2 isolation matrix, step A: same compact-row value extraction as {@link
   * #thisDriverCompactRow}, but over a plain {@code ByteArrayInputStream} with a direct {@code
   * while} loop — no {@link FluxInputStreamBridge}, no {@code Flux.generate}, no Reactor at all.
   * This is exactly {@link #clientV2}'s own transport shape, but building the same retained {@code
   * Object[]} row {@link #thisDriverCompactRow} does. Comparing this against {@link #clientV2}
   * isolates the cost of building/blackholing the row object itself, independent of any Reactor or
   * bridge machinery — see docs/PERFORMANCE.md's Phase 5 "Optimization phase" section, the H2
   * factorial breakdown (H2a–H2d).
   */
  @Benchmark
  public void compactRowDirectLoop(final Blackhole blackhole) throws Exception {
    long rowCount = 0;
    try (ByteArrayInputStream input = new ByteArrayInputStream(capturedResponseBytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader =
          new RowBinaryWithNamesAndTypesFormatReader(
              input,
              new QuerySettings()
                  .setUseTimeZone("UTC")
                  .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1"),
              new BinaryStreamReader.DefaultByteBufferAllocator());
      while (reader.next() != null) {
        final Object[] row = {reader.getLong(1), reader.getString(2), reader.getBigDecimal(3)};
        blackhole.consume(row);
        rowCount++;
      }
    }
    blackhole.consume(rowCount);
  }

  /**
   * H2 isolation matrix, step B: same compact-row value extraction and the same {@code
   * ByteArrayInputStream} as {@link #compactRowDirectLoop} (still no {@link
   * FluxInputStreamBridge}), but driven through {@code Flux.generate} instead of a plain {@code
   * while} loop — isolating Reactor's own per-row emission machinery ({@code SynchronousSink},
   * generator state) from the bridge. {@link #compactRowDirectLoop} vs this method isolates H2b
   * (Reactor/{@code Flux.generate} overhead); this method vs {@link #thisDriverCompactRow} isolates
   * H2a ({@link FluxInputStreamBridge} overhead) — see docs/PERFORMANCE.md's Phase 5 "Optimization
   * phase" section for the full H2a–H2d breakdown this factorial matrix is designed to answer.
   */
  @Benchmark
  public void compactRowFluxNoBridge(final Blackhole blackhole) {
    final RowBinaryWithNamesAndTypesFormatReader reader =
        new RowBinaryWithNamesAndTypesFormatReader(
            new ByteArrayInputStream(capturedResponseBytes),
            new QuerySettings()
                .setUseTimeZone("UTC")
                .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1"),
            new BinaryStreamReader.DefaultByteBufferAllocator());
    final long rowCount =
        Flux.generate(
                () -> reader,
                (final RowBinaryWithNamesAndTypesFormatReader r,
                    final SynchronousSink<Object[]> sink) -> {
                  if (r.hasNext()) {
                    r.next();
                    final Object[] row = {r.getLong(1), r.getString(2), r.getBigDecimal(3)};
                    blackhole.consume(row);
                    sink.next(row);
                  } else {
                    sink.complete();
                  }
                  return r;
                })
            .count()
            .block(Duration.ofSeconds(60));
    blackhole.consume(rowCount);
  }

  /**
   * client-v2's own reader, constructed directly over the captured bytes (no {@code Client}/{@code
   * QueryResponse} involved) — the same {@code RowBinaryWithNamesAndTypesFormatReader} class and
   * settings {@link RowBinaryDecoder#decodeRows} itself wraps, so this measures client-v2's decode
   * cost in isolation exactly as directly as this driver's own decode path is measured above.
   */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    long rowCount = 0;
    try (ByteArrayInputStream input = new ByteArrayInputStream(capturedResponseBytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader =
          new RowBinaryWithNamesAndTypesFormatReader(
              input,
              new QuerySettings()
                  .setUseTimeZone("UTC")
                  .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1"),
              new BinaryStreamReader.DefaultByteBufferAllocator());
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
        blackhole.consume(reader.getString(2));
        blackhole.consume(reader.getBigDecimal(3));
        rowCount++;
      }
    }
    blackhole.consume(rowCount);
  }
}

package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Network-free follow-up to {@link LatencyPathVariantDBenchmark}'s trusted t1/t8 result (see
 * docs/performance/latency-path-isolation.md's Variant D section): that pair showed a
 * scenario-dependent effect — noise/direction-flip on the 1-column {@code SELECT 1} scenario, a
 * real and reproducible ~5-7% gap on the 2-column point lookup, reproduced at both concurrency
 * levels — consistent with a per-column decode cost rather than a fixed per-request one, but still
 * measured with real HTTP/transport/scheduler/pool noise in the loop.
 *
 * <p>This class removes every one of those variables. Each scenario's exact {@code
 * RowBinaryWithNamesAndTypes} response bytes are captured once, over a real connection, in {@link
 * #setUpTrial()} — then every {@code @Benchmark} method decodes from a fresh {@link
 * ByteArrayInputStream} over the same in-memory {@code byte[]}, on the calling thread, with no
 * network, no {@code Scheduler}, no connection pool, and no {@link
 * ZeroCopyByteBufInputStreamBridge} in the loop. What's left to measure is only header parsing,
 * per-value type dispatch, and allocation inside the reader itself — client-v2's {@code
 * RowBinaryWithNamesAndTypesFormatReader} vs. {@link MinimalRowBinaryReader}.
 *
 * <p>Three scenarios, matching {@link LatencyPathVariantDBenchmark} exactly so results are
 * comparable: {@code select1} (1 row, 1 {@code UInt8} column), {@code point} (1 row, {@code label
 * String} + {@code amount Decimal(18,4)}), {@code stream10k} (10,000 rows, {@code id UInt64} +
 * {@code label String} + {@code amount Decimal(18,4)} — same shape as {@link
 * StreamingScanBenchmark}).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ReaderOnlyMicrobenchmark {

  private static final String SELECT_1_SQL = "SELECT 1";
  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";
  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;
  private static final long ROW_POOL_SIZE = 10_000;
  private static final long FIXED_POINT_ID = 1L;

  private byte[] select1Bytes;
  private byte[] pointBytes;
  private byte[] stream10kBytes;

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, captures each scenario's exact wire
   * bytes once over a real connection, then disposes that connection — no network use happens
   * inside any {@code @Benchmark} method below.
   */
  @Setup(Level.Trial)
  public void setUpTrial() throws IOException {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(ROW_POOL_SIZE);
    final ClickHouseHttpTransport captureTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            TransportOptions.defaults()
                .withAuthentication(
                    Authentication.basic(
                        BenchmarkEnvironment.username(), BenchmarkEnvironment.password()))
                .withResponseCompression(ResponseCompression.NONE));
    try {
      select1Bytes = captureBytes(captureTransport, ClickHouseQuery.of(SELECT_1_SQL));
      pointBytes =
          captureBytes(
              captureTransport,
              ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", FIXED_POINT_ID)));
      stream10kBytes = captureBytes(captureTransport, ClickHouseQuery.of(SELECT_ALL_SQL));
    } finally {
      captureTransport.dispose();
    }
  }

  private static byte[] captureBytes(
      final ClickHouseHttpTransport transport, final ClickHouseQuery query) throws IOException {
    final List<byte[]> chunks =
        transport.query(query).asByteArray().collectList().block(Duration.ofSeconds(10));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (final byte[] chunk : chunks) {
      out.write(chunk);
    }
    return out.toByteArray();
  }

  /** client-v2's reader, {@code SELECT 1}, from in-memory bytes. */
  @Benchmark
  public void clientV2ReaderSelect1(final Blackhole blackhole) throws IOException {
    try (InputStream input = new ByteArrayInputStream(select1Bytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader = openClientV2Reader(input);
      if (reader.next() == null) {
        return;
      }
      blackhole.consume(reader.getString(1));
    }
  }

  /** {@link MinimalRowBinaryReader}, {@code SELECT 1}, from in-memory bytes. */
  @Benchmark
  public void minimalReaderSelect1(final Blackhole blackhole) throws IOException {
    try (MinimalRowBinaryReader reader = openMinimalReader(select1Bytes)) {
      consumeRow(reader.nextRow(), blackhole);
    }
  }

  /** client-v2's reader, point lookup ({@code label String}, {@code amount Decimal(18,4)}). */
  @Benchmark
  public void clientV2ReaderPoint(final Blackhole blackhole) throws IOException {
    try (InputStream input = new ByteArrayInputStream(pointBytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader = openClientV2Reader(input);
      if (reader.next() == null) {
        return;
      }
      blackhole.consume(reader.getString(1));
      blackhole.consume(reader.getBigDecimal(2));
    }
  }

  /** {@link MinimalRowBinaryReader}, point lookup. */
  @Benchmark
  public void minimalReaderPoint(final Blackhole blackhole) throws IOException {
    try (MinimalRowBinaryReader reader = openMinimalReader(pointBytes)) {
      consumeRow(reader.nextRow(), blackhole);
    }
  }

  /** client-v2's reader, full 10k-row scan ({@code id}, {@code label}, {@code amount}). */
  @Benchmark
  public void clientV2ReaderStream10k(final Blackhole blackhole) throws IOException {
    try (InputStream input = new ByteArrayInputStream(stream10kBytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader = openClientV2Reader(input);
      while (reader.next() != null) {
        blackhole.consume(reader.getLong(1));
        blackhole.consume(reader.getString(2));
        blackhole.consume(reader.getBigDecimal(3));
      }
    }
  }

  /** {@link MinimalRowBinaryReader}, full 10k-row scan. */
  @Benchmark
  public void minimalReaderStream10k(final Blackhole blackhole) throws IOException {
    try (MinimalRowBinaryReader reader = openMinimalReader(stream10kBytes)) {
      Object[] row;
      while ((row = reader.nextRow()) != null) {
        consumeRow(row, blackhole);
      }
    }
  }

  private static RowBinaryWithNamesAndTypesFormatReader openClientV2Reader(final InputStream input)
      throws IOException {
    return new RowBinaryWithNamesAndTypesFormatReader(
        input,
        new QuerySettings().setUseTimeZone("UTC"),
        new BinaryStreamReader.DefaultByteBufferAllocator());
  }

  private static MinimalRowBinaryReader openMinimalReader(final byte[] bytes) throws IOException {
    return MinimalRowBinaryReader.open(new ByteArrayInputStream(bytes));
  }

  private static void consumeRow(final Object[] row, final Blackhole blackhole) {
    if (row == null) {
      return;
    }
    for (final Object value : row) {
      blackhole.consume(value);
    }
  }
}

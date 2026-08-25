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
import java.math.BigDecimal;
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
 * Follow-up to {@link ReaderOnlyMicrobenchmark} (see docs/performance/latency-path-isolation.md's
 * Variant D section): that class showed the reader-layer effect is real for the 2-column {@code
 * point} shape but not established for the 1-column {@code SELECT 1} shape. This class narrows
 * further — down to single ClickHouse types — to find which specific type(s) or which multi-column
 * machinery the effect actually comes from, instead of guessing from whole-scenario shapes.
 *
 * <p>Same network-free design as {@link ReaderOnlyMicrobenchmark}: every payload is captured once
 * over a real connection in {@link #setUpTrial()}, then every {@code @Benchmark} method decodes
 * from a fresh {@link ByteArrayInputStream} over the same in-memory bytes, on the calling thread —
 * no HTTP, no {@code Scheduler}, no connection pool, no {@code ZeroCopyByteBufInputStreamBridge}.
 *
 * <p><b>Correctness check, not a formal test suite.</b> This module has no {@code src/test/java}
 * source set (it's a diagnostic-only JMH module, not part of {@code check}/{@code build} — see this
 * module's {@code build.gradle.kts}), so rather than wiring up JUnit/AssertJ for one benchmark
 * class, {@link #setUpTrial()} decodes every captured payload with both readers once during setup
 * and throws {@link IllegalStateException} on any mismatch — a payload both readers disagree on
 * would silently invalidate every timing number below, so this fails fast instead of trusting
 * unverified decode parity.
 *
 * <p>Six shapes, matching {@link PointQueryTable}'s columns (`id UInt64`, `label String`, `amount
 * Decimal(18,4)`) individually and in the combinations Variant D already measured: {@code uint8}
 * (bare {@code SELECT 1}), {@code uint64} (`id` alone), {@code string} (`label` alone), {@code
 * decimal} (`amount` alone), {@code stringDecimal} (`label, amount` — Variant D's {@code point}
 * shape), {@code fullRow} (`id, label, amount` — Variant D's {@code stream10k} row shape).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RowBinaryReaderTypeMatrixBenchmark {

  private static final String SELECT_UINT8_SQL = "SELECT 1";
  private static final String SELECT_UINT64_SQL =
      "SELECT id FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";
  private static final String SELECT_STRING_SQL =
      "SELECT label FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";
  private static final String SELECT_DECIMAL_SQL =
      "SELECT amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";
  private static final String SELECT_STRING_DECIMAL_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";
  private static final String SELECT_FULL_ROW_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private static final long ROW_POOL_SIZE = 100;
  private static final long FIXED_ID = 1L;

  private byte[] uint8Bytes;
  private byte[] uint64Bytes;
  private byte[] stringBytes;
  private byte[] decimalBytes;
  private byte[] stringDecimalBytes;
  private byte[] fullRowBytes;

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, captures each shape's exact wire
   * bytes once over a real connection, verifies both readers decode identical values for every
   * shape (see class Javadoc), then disposes that connection — no network use inside any {@code
   * @Benchmark} method below.
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
      uint8Bytes = captureBytes(captureTransport, ClickHouseQuery.of(SELECT_UINT8_SQL));
      uint64Bytes = captureBytes(captureTransport, withFixedId(SELECT_UINT64_SQL));
      stringBytes = captureBytes(captureTransport, withFixedId(SELECT_STRING_SQL));
      decimalBytes = captureBytes(captureTransport, withFixedId(SELECT_DECIMAL_SQL));
      stringDecimalBytes = captureBytes(captureTransport, withFixedId(SELECT_STRING_DECIMAL_SQL));
      fullRowBytes = captureBytes(captureTransport, withFixedId(SELECT_FULL_ROW_SQL));
    } finally {
      captureTransport.dispose();
    }
    verifyReadersAgree("uint8", uint8Bytes, 1);
    verifyReadersAgree("uint64", uint64Bytes, 1);
    verifyReadersAgree("string", stringBytes, 1);
    verifyReadersAgree("decimal", decimalBytes, 1);
    verifyReadersAgree("stringDecimal", stringDecimalBytes, 2);
    verifyReadersAgree("fullRow", fullRowBytes, 3);
  }

  private static ClickHouseQuery withFixedId(final String sql) {
    return ClickHouseQuery.of(sql).withParameters(Map.of("id", FIXED_ID));
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

  /**
   * Decodes {@code bytes} with both readers and fails the whole benchmark run if they disagree on
   * any column's value — see class Javadoc for why this stands in for a formal correctness test
   * here.
   */
  private static void verifyReadersAgree(
      final String shapeName, final byte[] bytes, final int columnCount) throws IOException {
    final Object[] viaClientV2 = decodeRowViaClientV2(bytes, columnCount);
    final Object[] viaMinimal = decodeRowViaMinimal(bytes);
    for (int column = 0; column < columnCount; column++) {
      if (!valuesMatch(viaClientV2[column], viaMinimal[column])) {
        throw new IllegalStateException(
            "Reader mismatch for shape '"
                + shapeName
                + "' column "
                + column
                + ": clientV2="
                + viaClientV2[column]
                + " minimal="
                + viaMinimal[column]);
      }
    }
  }

  private static boolean valuesMatch(final Object clientV2Value, final Object minimalValue) {
    if (clientV2Value instanceof BigDecimal a && minimalValue instanceof BigDecimal b) {
      return a.compareTo(b) == 0;
    }
    return String.valueOf(clientV2Value).equals(String.valueOf(minimalValue));
  }

  private static Object[] decodeRowViaClientV2(final byte[] bytes, final int columnCount)
      throws IOException {
    try (InputStream input = new ByteArrayInputStream(bytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader = openClientV2Reader(input);
      if (reader.next() == null) {
        throw new IllegalStateException("Expected exactly one row, got none");
      }
      final Object[] values = new Object[columnCount];
      for (int column = 0; column < columnCount; column++) {
        values[column] = reader.getString(column + 1);
      }
      return values;
    }
  }

  private static Object[] decodeRowViaMinimal(final byte[] bytes) throws IOException {
    try (MinimalRowBinaryReader reader = openMinimalReader(bytes)) {
      final Object[] row = reader.nextRow();
      if (row == null) {
        throw new IllegalStateException("Expected exactly one row, got none");
      }
      return row;
    }
  }

  /** client-v2's reader, single {@code UInt8} column. */
  @Benchmark
  public void clientV2UInt8(final Blackhole blackhole) throws IOException {
    decodeOneColumnViaClientV2(uint8Bytes, blackhole);
  }

  /** {@link MinimalRowBinaryReader}, single {@code UInt8} column. */
  @Benchmark
  public void minimalUInt8(final Blackhole blackhole) throws IOException {
    decodeViaMinimal(uint8Bytes, blackhole);
  }

  /** client-v2's reader, single {@code UInt64} column. */
  @Benchmark
  public void clientV2UInt64(final Blackhole blackhole) throws IOException {
    decodeOneColumnViaClientV2(uint64Bytes, blackhole);
  }

  /** {@link MinimalRowBinaryReader}, single {@code UInt64} column. */
  @Benchmark
  public void minimalUInt64(final Blackhole blackhole) throws IOException {
    decodeViaMinimal(uint64Bytes, blackhole);
  }

  /** client-v2's reader, single {@code String} column. */
  @Benchmark
  public void clientV2String(final Blackhole blackhole) throws IOException {
    decodeOneColumnViaClientV2(stringBytes, blackhole);
  }

  /** {@link MinimalRowBinaryReader}, single {@code String} column. */
  @Benchmark
  public void minimalString(final Blackhole blackhole) throws IOException {
    decodeViaMinimal(stringBytes, blackhole);
  }

  /** client-v2's reader, single {@code Decimal(18,4)} column. */
  @Benchmark
  public void clientV2Decimal(final Blackhole blackhole) throws IOException {
    decodeOneColumnViaClientV2(decimalBytes, blackhole);
  }

  /** {@link MinimalRowBinaryReader}, single {@code Decimal(18,4)} column. */
  @Benchmark
  public void minimalDecimal(final Blackhole blackhole) throws IOException {
    decodeViaMinimal(decimalBytes, blackhole);
  }

  /** client-v2's reader, {@code String} + {@code Decimal} — Variant D's {@code point} shape. */
  @Benchmark
  public void clientV2StringDecimal(final Blackhole blackhole) throws IOException {
    decodeTwoColumnsViaClientV2(stringDecimalBytes, blackhole);
  }

  /** {@link MinimalRowBinaryReader}, {@code String} + {@code Decimal}. */
  @Benchmark
  public void minimalStringDecimal(final Blackhole blackhole) throws IOException {
    decodeViaMinimal(stringDecimalBytes, blackhole);
  }

  /**
   * client-v2's reader, {@code UInt64} + {@code String} + {@code Decimal} — Variant D's {@code
   * stream10k} row shape.
   */
  @Benchmark
  public void clientV2FullRow(final Blackhole blackhole) throws IOException {
    try (InputStream input = new ByteArrayInputStream(fullRowBytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader = openClientV2Reader(input);
      if (reader.next() == null) {
        return;
      }
      blackhole.consume(reader.getLong(1));
      blackhole.consume(reader.getString(2));
      blackhole.consume(reader.getBigDecimal(3));
    }
  }

  /** {@link MinimalRowBinaryReader}, {@code UInt64} + {@code String} + {@code Decimal}. */
  @Benchmark
  public void minimalFullRow(final Blackhole blackhole) throws IOException {
    decodeViaMinimal(fullRowBytes, blackhole);
  }

  private static void decodeOneColumnViaClientV2(final byte[] bytes, final Blackhole blackhole)
      throws IOException {
    try (InputStream input = new ByteArrayInputStream(bytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader = openClientV2Reader(input);
      if (reader.next() == null) {
        return;
      }
      blackhole.consume(reader.getString(1));
    }
  }

  private static void decodeTwoColumnsViaClientV2(final byte[] bytes, final Blackhole blackhole)
      throws IOException {
    try (InputStream input = new ByteArrayInputStream(bytes)) {
      final RowBinaryWithNamesAndTypesFormatReader reader = openClientV2Reader(input);
      if (reader.next() == null) {
        return;
      }
      blackhole.consume(reader.getString(1));
      blackhole.consume(reader.getBigDecimal(2));
    }
  }

  private static void decodeViaMinimal(final byte[] bytes, final Blackhole blackhole)
      throws IOException {
    try (MinimalRowBinaryReader reader = openMinimalReader(bytes)) {
      final Object[] row = reader.nextRow();
      if (row == null) {
        return;
      }
      for (final Object value : row) {
        blackhole.consume(value);
      }
    }
  }

  private static RowBinaryWithNamesAndTypesFormatReader openClientV2Reader(
      final InputStream input) throws IOException {
    return new RowBinaryWithNamesAndTypesFormatReader(
        input,
        new QuerySettings().setUseTimeZone("UTC"),
        new BinaryStreamReader.DefaultByteBufferAllocator());
  }

  private static MinimalRowBinaryReader openMinimalReader(final byte[] bytes) throws IOException {
    return MinimalRowBinaryReader.open(new ByteArrayInputStream(bytes));
  }
}

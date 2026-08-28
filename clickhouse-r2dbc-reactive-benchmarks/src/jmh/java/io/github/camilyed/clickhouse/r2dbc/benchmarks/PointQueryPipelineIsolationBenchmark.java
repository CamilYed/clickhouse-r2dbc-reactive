package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoderMode;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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

/**
 * Diagnostic benchmark for the fixed cost of a one-row point query.
 *
 * <p>This class deliberately does not try to turn timings from different layers into an additive
 * model. Instead it answers three narrower questions with one variable changed at a time:
 *
 * <ol>
 *   <li>{@link #clientV2RawResponse(ClientV2RawResponseState, Blackhole)} vs {@link
 *       #ourTransportRawResponse(OurTransportRawResponseState, Blackhole)}: is a meaningful gap
 *       already present before row decoding?
 *   <li>{@link #nativeDecodeRaw(CapturedResponseState, Blackhole)} vs {@link
 *       #nativeDecodeScheduled(CapturedResponseState, Blackhole)}: what fixed cost does the
 *       production decoder scheduling boundary add for a tiny in-memory response?
 *   <li>{@link #nativeDecodeScheduled(CapturedResponseState, Blackhole)} vs {@link
 *       #fullR2dbcNative(FullR2dbcNativeState, Blackhole)}: after accounting for the fact that one
 *       case is in-memory and the other is a real request, is there enough remaining evidence to
 *       justify drilling into the public R2DBC connector/SPI path?
 * </ol>
 *
 * <p>The decoder variants all consume the exact same captured, uncompressed one-row
 * RowBinaryWithNamesAndTypes body. The two live raw-response variants use the same literal SQL and
 * matched physical pool size. The full R2DBC variant uses the production public SPI path with
 * {@code rowDecoder=native}.
 *
 * <p>This is a diagnostic benchmark. It must not be used to justify removing {@link
 * RowDecodingScheduler}: {@link RowBinaryDecoder#decodeRows} is safe here only because its source
 * is already fully in memory. A live Reactor Netty response can make the current
 * FluxInputStreamBridge-backed reader wait for future chunks and therefore must stay off the event
 * loop.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PointQueryPipelineIsolationBenchmark {

  private static final long ROWS = 10_000;
  private static final int POOL_SIZE = 8;
  private static final long POINT_ID = 1L;
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final int CLIENT_V2_READ_BUFFER_SIZE = 8 * 1024;

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = " + POINT_ID;

  /** client-v2 public query path, stopped before binary row decoding. */
  @State(Scope.Benchmark)
  public static class ClientV2RawResponseState {

    private Client client;

    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
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

      final long bytes = drainClientV2Response(client);
      if (bytes <= 0) {
        throw new IllegalStateException("client-v2 point query returned an empty response body");
      }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      client.close();
    }
  }

  /** This driver's Reactor Netty transport path, stopped before RowBinary decoding. */
  @State(Scope.Benchmark)
  public static class OurTransportRawResponseState {

    private ClickHouseHttpTransport transport;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      transport =
          new ClickHouseHttpTransport(
              BenchmarkEnvironment.httpUrl(),
              Authentication.basic(
                  BenchmarkEnvironment.username(), BenchmarkEnvironment.password()),
              POOL_SIZE);

      final long bytes = drainOurTransport(transport);
      if (bytes <= 0) {
        throw new IllegalStateException("our point query returned an empty response body");
      }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      transport.disposeLater().block(Duration.ofSeconds(10));
    }
  }

  /**
   * One captured, uncompressed RowBinaryWithNamesAndTypes point-query response plus the same
   * production-sized decode scheduler the connector uses for poolSize=8.
   */
  @State(Scope.Benchmark)
  public static class CapturedResponseState {

    private byte[] responseBytes;
    private RowDecodingScheduler scheduler;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);

      final ClickHouseHttpTransport captureTransport =
          new ClickHouseHttpTransport(
              BenchmarkEnvironment.httpUrl(),
              TransportOptions.defaults()
                  .withAuthentication(
                      Authentication.basic(
                          BenchmarkEnvironment.username(), BenchmarkEnvironment.password()))
                  .withResponseCompression(ResponseCompression.NONE));

      try {
        responseBytes =
            captureTransport
                .query(ClickHouseQuery.of(SELECT_BY_ID_SQL))
                .aggregate()
                .asByteArray()
                .block(TIMEOUT);
      } finally {
        captureTransport.disposeLater().block(Duration.ofSeconds(10));
      }

      if (responseBytes == null || responseBytes.length == 0) {
        throw new IllegalStateException("captured point-query response body is empty");
      }

      scheduler = RowDecodingScheduler.withWorkerCount(POOL_SIZE);

      // Fail setup immediately if the captured shape unexpectedly stops being native-decodable.
      consumeRawNative(responseBytes);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      scheduler.dispose();
    }
  }

  /** Full public R2DBC path with rowDecoder=native and matched poolSize=8. */
  @State(Scope.Benchmark)
  public static class FullR2dbcNativeState {

    private OurDriverPointQueryClient client;

    @Setup(Level.Trial)
    public void setUpTrial() {
      BenchmarkEnvironment.start();
      PointQueryTable.seed(ROWS);
      client =
          new OurDriverPointQueryClient(new OurDriverPointQueryClient.NativeDecoder(POOL_SIZE));

      // Validate the public path outside the measured region; JMH warmup handles performance
      // warmup.
      client.query(POINT_ID).block(TIMEOUT);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      client.close();
    }
  }

  /**
   * client-v2 request + matched connection pool + raw response drain, with no
   * ClickHouseBinaryFormatReader.
   */
  @Benchmark
  public void clientV2RawResponse(final ClientV2RawResponseState state, final Blackhole blackhole)
      throws Exception {
    blackhole.consume(drainClientV2Response(state.client));
  }

  /**
   * Reactor Netty request + matched connection pool + ByteBuf->byte[] response drain, with no
   * RowBinaryDecoder.
   */
  @Benchmark
  public void ourTransportRawResponse(
      final OurTransportRawResponseState state, final Blackhole blackhole) {
    blackhole.consume(drainOurTransport(state.transport));
  }

  /**
   * NATIVE reader over already-in-memory bytes. Includes FluxInputStreamBridge +
   * NativeRowBinaryReader + Flux.generate, but deliberately no RowDecodingScheduler hop.
   */
  @Benchmark
  public void nativeDecodeRaw(final CapturedResponseState state, final Blackhole blackhole) {
    blackhole.consume(consumeRawNative(state.responseBytes));
  }

  /** NATIVE production decode API over the exact same captured bytes, including scheduler hop. */
  @Benchmark
  public void nativeDecodeScheduled(final CapturedResponseState state, final Blackhole blackhole) {
    blackhole.consume(
        consumeScheduled(state.responseBytes, state.scheduler, RowBinaryDecoderMode.NATIVE));
  }

  /** CLICKHOUSE-mode scheduled control over the exact same captured bytes. */
  @Benchmark
  public void clickHouseDecodeScheduled(
      final CapturedResponseState state, final Blackhole blackhole) {
    blackhole.consume(
        consumeScheduled(state.responseBytes, state.scheduler, RowBinaryDecoderMode.CLICKHOUSE));
  }

  /** Production public R2DBC SPI path with rowDecoder=native. */
  @Benchmark
  public void fullR2dbcNative(final FullR2dbcNativeState state, final Blackhole blackhole) {
    final PointResult result = state.client.query(POINT_ID).block(TIMEOUT);
    if (result == null) {
      throw new IllegalStateException("full R2DBC native point query returned null");
    }
    blackhole.consume(result.checksum());
  }

  private static long drainClientV2Response(final Client client) throws Exception {
    long totalBytes = 0;
    try (QueryResponse response = client.query(SELECT_BY_ID_SQL).get(60, TimeUnit.SECONDS);
        InputStream body = response.getInputStream()) {
      final byte[] buffer = new byte[CLIENT_V2_READ_BUFFER_SIZE];
      int bytesRead;
      while ((bytesRead = body.read(buffer)) != -1) {
        totalBytes += bytesRead;
      }
    }
    return totalBytes;
  }

  private static long drainOurTransport(final ClickHouseHttpTransport transport) {
    return transport
        .query(ClickHouseQuery.of(SELECT_BY_ID_SQL))
        .asByteArray()
        .reduce(0L, (total, chunk) -> total + chunk.length)
        .block(TIMEOUT);
  }

  private static long consumeRawNative(final byte[] responseBytes) {
    final DecodedRow row =
        RowBinaryDecoder.decodeRows(
                Flux.just(ByteBuffer.wrap(responseBytes)),
                ResponseCompression.NONE,
                RowBinaryDecoderMode.NATIVE)
            .single()
            .block(TIMEOUT);
    if (row == null) {
      throw new IllegalStateException("native raw decode returned null");
    }
    return checksum(row);
  }

  private static long consumeScheduled(
      final byte[] responseBytes,
      final RowDecodingScheduler scheduler,
      final RowBinaryDecoderMode mode) {
    final DecodedResult result =
        RowBinaryDecoder.decode(
                Flux.just(ByteBuffer.wrap(responseBytes)),
                scheduler,
                ResponseCompression.NONE,
                mode)
            .block(TIMEOUT);
    if (result == null) {
      throw new IllegalStateException("scheduled decode returned null result");
    }

    final DecodedRow row = result.rows().single().block(TIMEOUT);
    if (row == null) {
      throw new IllegalStateException("scheduled decode returned null row");
    }
    return checksum(row);
  }

  private static long checksum(final DecodedRow row) {
    final String label = (String) row.valueAt(0);
    final BigDecimal amount = (BigDecimal) row.valueAt(1);
    return 31L * label.hashCode() + amount.unscaledValue().longValue();
  }
}

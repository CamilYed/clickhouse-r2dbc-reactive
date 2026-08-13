package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * Level 1 "full-table streaming scan": {@code SELECT id, label, amount FROM
 * benchmark_point_query} with no {@code WHERE}, streaming every row of {@link PointQueryTable}
 * back to the caller. Where {@link PointQueryBenchmark} isolates one round trip's protocol
 * overhead, this class measures sustained throughput and time-to-first-row over a long-running
 * result — the scenario this driver's non-blocking, backpressure-aware transport exists for, per
 * ROADMAP.md's Phase 5 "Query mix" design.
 *
 * <p>The whole-method {@code SampleTime} JMH records is closer to time-to-last-row (drain the
 * entire scan); time-to-first-row is a separate metric JMH has no built-in support for, so it's
 * recorded into its own {@link Histogram} per driver and logged at the end of the trial rather
 * than folded into JMH's own result — see ROADMAP.md's "What's measured, and how".
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class StreamingScanBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingScanBenchmark.class);

  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;

  private static final long TTFR_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /** Row-count tier — see ROADMAP.md's Phase 5 "Dataset" table for what each tier is for. */
  @Param({"10000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private Histogram ourDriverTtfr;
  private Histogram clientV2Ttfr;

  /** Starts the shared container, seeds {@link PointQueryTable}, resets both TTFR histograms. */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(), BenchmarkEnvironment.username(),
            BenchmarkEnvironment.password());
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .build();
    ourDriverTtfr = new Histogram(1, TTFR_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
    clientV2Ttfr = new Histogram(1, TTFR_HIGHEST_TRACKABLE_VALUE_MICROS, 3);
  }

  /** Releases both clients' connection pools, logs each driver's time-to-first-row summary. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    logTtfr("ourDriver", ourDriverTtfr);
    logTtfr("clientV2", clientV2Ttfr);
  }

  /**
   * This driver: streams every row via {@link RowBinaryDecoder#decodeRows}, timing the gap
   * between subscribe and the first emitted row into {@link #ourDriverTtfr}, then draining the
   * rest of the stream (this method's own JMH-measured latency is effectively time-to-last-row).
   */
  @Benchmark
  public void ourDriver(final Blackhole blackhole) {
    final long startNanos = System.nanoTime();
    final AtomicLong firstRowNanos = new AtomicLong(-1);
    final Flux<ByteBuffer> body =
        ourTransport.query(ClickHouseQuery.of(SELECT_ALL_SQL)).asByteArray().map(ByteBuffer::wrap);
    final long rowCount =
        RowBinaryDecoder.decodeRows(body)
            .doOnNext(
                row -> {
                  firstRowNanos.compareAndSet(-1, System.nanoTime());
                  blackhole.consume(row);
                })
            .count()
            .block(Duration.ofSeconds(60));
    recordTtfr(ourDriverTtfr, startNanos, firstRowNanos.get());
    blackhole.consume(rowCount);
  }

  /**
   * client-v2: streams every row via {@link ClickHouseBinaryFormatReader#next()}, timing the gap
   * between issuing the query and the first successfully read row into {@link #clientV2Ttfr}.
   */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    final long startNanos = System.nanoTime();
    long firstRowNanos = -1;
    long rowCount = 0;
    try (QueryResponse response = clientV2.query(SELECT_ALL_SQL).get(60, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        if (firstRowNanos == -1) {
          firstRowNanos = System.nanoTime();
        }
        blackhole.consume(reader.getLong(1));
        blackhole.consume(reader.getString(2));
        blackhole.consume(reader.getBigDecimal(3));
        rowCount++;
      }
    }
    recordTtfr(clientV2Ttfr, startNanos, firstRowNanos);
    blackhole.consume(rowCount);
  }

  private static void recordTtfr(
      final Histogram histogram, final long startNanos, final long firstRowNanos) {
    if (firstRowNanos <= 0) {
      return;
    }
    histogram.recordValue(Math.max((firstRowNanos - startNanos) / 1000, 0));
  }

  private static void logTtfr(final String driverName, final Histogram histogram) {
    if (histogram.getTotalCount() == 0) {
      return;
    }
    LOG.info(
        "{} time-to-first-row (µs, n={}): mean={}, p50={}, p99={}, p99.9={}, max={}",
        driverName,
        histogram.getTotalCount(),
        String.format("%.1f", histogram.getMean()),
        histogram.getValueAtPercentile(50),
        histogram.getValueAtPercentile(99),
        histogram.getValueAtPercentile(99.9),
        histogram.getMaxValue());
  }
}

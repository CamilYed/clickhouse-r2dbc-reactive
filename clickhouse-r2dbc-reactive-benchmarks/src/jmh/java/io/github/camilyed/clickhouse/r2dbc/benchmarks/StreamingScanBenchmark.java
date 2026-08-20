package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
 * Level 1 "full-table streaming scan": {@code SELECT id, label, amount FROM benchmark_point_query}
 * with no {@code WHERE}, streaming every row of {@link PointQueryTable} back to the caller. Where
 * {@link PointQueryBenchmark} isolates one round trip's protocol overhead, this class measures
 * sustained throughput and time-to-first-row over a long-running result — the scenario this
 * driver's non-blocking, backpressure-aware transport exists for, per docs/PERFORMANCE.md's Phase 5
 * "Query mix" design.
 *
 * <p>The whole-method {@code SampleTime} JMH records is closer to time-to-last-row (drain the
 * entire scan); time-to-first-row is a separate metric JMH has no built-in support for, so it's
 * recorded into its own {@link Histogram} per driver and logged at the end of the trial rather than
 * folded into JMH's own result — see docs/PERFORMANCE.md's "What's measured, and how". Rows/sec
 * isn't computed here either: derive it from a run's {@code rows} param divided by that run's mean
 * {@code us/op}, as docs/PERFORMANCE.md's results tables do, rather than adding a redundant JMH
 * metric.
 *
 * <p>Previously documented here as a known asymmetry (see {@link PointQueryBenchmark}'s Javadoc,
 * which this inherited): this driver used to materialize each row into a {@code Map<String,
 * Object>} via {@link RowBinaryDecoder#decodeRows}, while client-v2 reads three typed values
 * directly off its reader with no intermediate collection. Since docs/PERFORMANCE.md's Phase 5
 * "Optimization phase" section (hypothesis H1), {@link RowBinaryDecoder#decodeRows} emits a compact
 * {@code DecodedRow} (a plain {@code Object[]}) instead — closing most, not yet confirmed all, of
 * that gap at this benchmark's scale. Re-run this class after the redesign to confirm.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class StreamingScanBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingScanBenchmark.class);

  private static final String SELECT_ALL_SQL =
      "SELECT id, label, amount FROM " + PointQueryTable.NAME;

  private static final long TTFR_HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(60);

  /**
   * Row-count tiers — see docs/PERFORMANCE.md's Phase 5 "Dataset" table for what each tier is for.
   * A single-tier run (only 10,000 rows) is dominated by fixed per-request cost (HTTP round trip,
   * ClickHouse query startup, first-chunk latency) rather than sustained per-row streaming cost;
   * running 10k/100k/1M side by side is what actually shows whether a latency gap is fixed overhead
   * (flat across tiers) or per-row cost (grows with {@code rows}). {@code 10_000_000}+ ("large"
   * tier) is deliberately not included here — a manual, opt-in {@code @Param} edit for a
   * release-gate run, not routine local iteration (see docs/PERFORMANCE.md's Phase 5 "Dataset"
   * table).
   */
  @Param({"10000", "100000", "1000000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;
  private Histogram ourDriverTtfr;
  private Histogram clientV2Ttfr;

  /** Starts the shared container, seeds {@link PointQueryTable}, resets both TTFR histograms. */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            BenchmarkEnvironment.username(),
            BenchmarkEnvironment.password());
    decodingScheduler = RowDecodingScheduler.defaults();
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

  /**
   * Releases both clients' connection pools and this driver's decode scheduler, logs each driver's
   * time-to-first-row summary.
   */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    decodingScheduler.dispose();
    logTtfr("ourDriver", ourDriverTtfr);
    logTtfr("clientV2", clientV2Ttfr);
  }

  /**
   * This driver: streams every row via {@link RowBinaryDecoder#decode} — the real production decode
   * path (off the Netty event loop, via {@link #decodingScheduler}), not the scheduler-free {@link
   * RowBinaryDecoder#decodeRows} test/benchmark shortcut — timing the gap between subscribe and the
   * first emitted row into {@link #ourDriverTtfr}, then draining the rest of the stream (this
   * method's own JMH-measured latency is effectively time-to-last-row).
   *
   * <p>The first-row check is a plain array-backed flag read-then-set, not an {@code AtomicLong}
   * compare-and-swap. Unlike an earlier version of this benchmark (back when it called {@link
   * RowBinaryDecoder#decodeRows}, which applies no {@code publishOn}/{@code subscribeOn} of its
   * own), row emission now happens on {@link #decodingScheduler}'s worker thread, not this method's
   * calling thread — but the plain field is still safe to read after {@code .block()} returns,
   * since blocking on a {@link reactor.core.publisher.Mono}/{@link Flux}'s completion is itself a
   * happens-before edge (the same guarantee any {@code java.util.concurrent} blocking wait gives),
   * not because of same-thread execution. An {@code AtomicLong#compareAndSet} would still be
   * unnecessary overhead here: nothing reads {@code firstRowNanos} concurrently with the write,
   * only after the whole stream has completed.
   */
  @Benchmark
  public void ourDriver(final Blackhole blackhole) {
    final long startNanos = System.nanoTime();
    final long[] firstRowNanos = {-1L};
    final Flux<ByteBuffer> body =
        ourTransport.query(ClickHouseQuery.of(SELECT_ALL_SQL)).asByteArray().map(ByteBuffer::wrap);
    final long rowCount =
        RowBinaryDecoder.decode(body, decodingScheduler)
            .flatMapMany(DecodedResult::rows)
            .doOnNext(
                row -> {
                  if (firstRowNanos[0] == -1L) {
                    firstRowNanos[0] = System.nanoTime();
                  }
                  blackhole.consume(row);
                })
            .count()
            .block(Duration.ofSeconds(60));
    recordTtfr(ourDriverTtfr, startNanos, firstRowNanos[0]);
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

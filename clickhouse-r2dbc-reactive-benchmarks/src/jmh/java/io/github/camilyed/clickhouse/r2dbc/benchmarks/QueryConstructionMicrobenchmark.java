package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Task #309 (docs/performance/latency-path-isolation.md's "Updated recommendation"): before
 * building Variant C's transport-acquisition-before-decoder-admission prototype, checks the
 * cheaper, more targeted hypothesis that a flat per-request object-construction cost — not a
 * queueing/admission-ordering effect — explains {@code LatencyPathVariantABenchmark}'s trusted,
 * reproduced ~4-5% mean deficit on {@code SELECT 1}/point at both concurrency 1 and 8 (Variant B
 * already ruled out the {@code asByteArray()} copy as the cause; GC was ruled out earlier via
 * {@code -prof gc}).
 *
 * <p>No network, no container, no transport, no decoding — isolates exactly the object
 * construction {@link io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseStatement}'s
 * constructor and {@code execute()} perform per query, matching that class's source line for
 * line (see its {@code executeOneBindingSet}/constructor):
 *
 * <ul>
 *   <li>{@link #parameterNamesInPointSql} — {@link ClickHouseQuery#parameterNamesIn(String)},
 *       run once per {@code ClickHouseStatement} construction, not per {@code execute()} call.
 *   <li>{@link #uuidGeneration} / {@link #uuidGenerationContended} — {@link
 *       UUID#randomUUID()}, the first thing {@link ClickHouseQuery#of(String)} does. {@code
 *       UUID.randomUUID()} draws from a JDK-shared {@code SecureRandom} instance guarded by a
 *       single lock — a plausible source of a flat, load-independent-looking deficit if that
 *       lock or its underlying entropy source is the bottleneck, which is exactly why {@link
 *       #uuidGenerationContended} repeats the same call under {@code @Threads(8)}, matching
 *       {@code LatencyPathVariantABenchmark}'s own {@code -t8} concurrency level, to see whether
 *       contention changes the picture the single-threaded number alone can't show.
 *   <li>{@link #queryOfSelect1} — the full {@code ClickHouseQuery.of(sql)} call {@code
 *       ClickHouseStatement.executeOneBindingSet} makes for every query, {@code SELECT 1} shape
 *       (no parameters to encode).
 *   <li>{@link #queryOfPointWithParameters} — the same call plus {@link
 *       ClickHouseQuery#withParameters(Map)}, the point-lookup shape (one bound {@code UInt64}
 *       parameter, encoded via {@code Long#toString()}).
 * </ul>
 *
 * <p>{@link #SELECT_BY_ID_SQL} mirrors {@code LatencyPathVariantABenchmark}/{@code
 * LatencyPathVariantBBenchmark}'s own point-query SQL text and {@link PointQueryTable#NAME}, so
 * the scanned/parsed SQL shape here is the real one, not a simplified stand-in.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class QueryConstructionMicrobenchmark {

  private static final String SELECT_1_SQL = "SELECT 1";

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private static final Map<String, Object> POINT_PARAMETERS = Map.of("id", 42L);

  /** Isolates {@code UUID.randomUUID()} alone, single-threaded — see class Javadoc. */
  @Benchmark
  public void uuidGeneration(final Blackhole blackhole) {
    blackhole.consume(UUID.randomUUID());
  }

  /**
   * The same call as {@link #uuidGeneration}, under {@code @Threads(8)} — matches {@code
   * LatencyPathVariantABenchmark}'s {@code -t8} concurrency level, to check whether the shared
   * {@code SecureRandom} lock behind {@code UUID.randomUUID()} becomes a bottleneck under
   * concurrency the single-threaded number can't reveal.
   */
  @Benchmark
  @Threads(8)
  public void uuidGenerationContended(final Blackhole blackhole) {
    blackhole.consume(UUID.randomUUID());
  }

  /** {@code ClickHouseQuery.of(sql)} alone, {@code SELECT 1} shape — no parameters to encode. */
  @Benchmark
  public void queryOfSelect1(final Blackhole blackhole) {
    blackhole.consume(ClickHouseQuery.of(SELECT_1_SQL));
  }

  /**
   * {@code ClickHouseQuery.of(sql).withParameters(...)}, point-lookup shape — the full per-{@code
   * execute()} construction cost {@code ClickHouseStatement.executeOneBindingSet} pays.
   */
  @Benchmark
  public void queryOfPointWithParameters(final Blackhole blackhole) {
    blackhole.consume(ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(POINT_PARAMETERS));
  }

  /**
   * {@code ClickHouseQuery.parameterNamesIn(sql)} alone, point-lookup SQL shape — the one-time,
   * per-{@code ClickHouseStatement}-construction scan {@link
   * io.github.camilyed.clickhouse.r2dbc.core.ClickHouseSqlPlaceholderScanner} performs. That
   * scanner's own Javadoc already reports a non-JMH sanity check putting this well under 1
   * microsecond; this benchmark exists to give that a proper JMH number for direct comparison
   * against the other numbers here.
   */
  @Benchmark
  public void parameterNamesInPointSql(final Blackhole blackhole) {
    blackhole.consume(ClickHouseQuery.parameterNamesIn(SELECT_BY_ID_SQL));
  }
}

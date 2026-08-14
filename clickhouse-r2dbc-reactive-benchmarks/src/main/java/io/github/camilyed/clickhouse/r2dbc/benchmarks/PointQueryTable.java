package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.util.SplittableRandom;

/**
 * The narrow, two-column table the point-query and burst-concurrency benchmarks read from — see
 * docs/PERFORMANCE.md's Phase 5 section ("Dataset") for why this is a separate, narrow table rather
 * than reusing the wide multi-type table: a narrow table isolates connection/protocol overhead from
 * decode cost, which is exactly what the point-query benchmarks are meant to measure in isolation.
 *
 * <p>Seeded entirely server-side (no client-side row generation, no CSV round-trip) via {@code
 * INSERT ... SELECT ... FROM numbers(N)} — fast even at the {@code large} row-count tier, and it
 * exercises nothing about this driver or client-v2 while doing it, since {@link
 * BenchmarkEnvironment#executeAdminSql} talks to ClickHouse directly.
 */
public final class PointQueryTable {

  /** The table name every point-query/burst benchmark reads from once {@link #seed} has run. */
  public static final String NAME = "benchmark_point_query";

  private PointQueryTable() {}

  /**
   * Drops and recreates {@link #NAME} with {@code rowCount} rows, {@code id} running from {@code 1}
   * to {@code rowCount} so any id from {@link #deterministicIds} always hits a real row.
   */
  public static void seed(final long rowCount) {
    BenchmarkEnvironment.executeAdminSql("DROP TABLE IF EXISTS " + NAME);
    BenchmarkEnvironment.executeAdminSql(
        "CREATE TABLE "
            + NAME
            + " (id UInt64, label String, amount Decimal(18,4)) ENGINE = MergeTree ORDER BY id");
    BenchmarkEnvironment.executeAdminSql(
        "INSERT INTO "
            + NAME
            + " SELECT number + 1 AS id, concat('label-', toString(number)) AS label, "
            + "(number % 100000) / 100.0 AS amount FROM numbers("
            + rowCount
            + ")");
  }

  /**
   * A fixed-seed, pre-generated pool of valid {@code id}s for a table seeded with {@code rowCount}
   * rows — deliberately not {@code Math.random()} called inside a benchmark's hot path (costs
   * little relative to a real network round trip, but makes runs non-reproducible and can give
   * client-v2 and this driver different access patterns across a run). Call once in {@code @Setup},
   * then cycle through the returned array during measurement; both benchmark methods built from the
   * same {@code seed} see the identical id sequence.
   */
  public static long[] deterministicIds(final long rowCount, final int poolSize, final long seed) {
    final SplittableRandom random = new SplittableRandom(seed);
    final long[] ids = new long[poolSize];
    for (int i = 0; i < poolSize; i++) {
      ids[i] = 1 + random.nextLong(rowCount);
    }
    return ids;
  }
}

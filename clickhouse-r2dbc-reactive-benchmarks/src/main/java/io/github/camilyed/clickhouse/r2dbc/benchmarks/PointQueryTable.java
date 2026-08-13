package io.github.camilyed.clickhouse.r2dbc.benchmarks;

/**
 * The narrow, two-column table the point-query and burst-concurrency benchmarks read from — see
 * ROADMAP.md's Phase 5 section ("Dataset") for why this is a separate, narrow table rather than
 * reusing the wide multi-type table: a narrow table isolates connection/protocol overhead from
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
   * to {@code rowCount} so a uniformly-random point lookup (see {@link #randomId}) always hits a
   * real row.
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

  /** A uniformly random {@code id} in range for a table seeded with {@code rowCount} rows. */
  public static long randomId(final long rowCount) {
    return 1 + (long) (Math.random() * rowCount);
  }
}

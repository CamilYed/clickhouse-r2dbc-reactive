package io.github.camilyed.clickhouse.r2dbc.macrobench.dataset;

/**
 * Table names and the shared analytics query text both backends run - kept here once instead of
 * duplicated in {@code R2dbcBenchmarkQueryBackend} and {@code ClientV2BenchmarkQueryBackend}, so
 * both genuinely execute identical SQL, not two hand-written copies that could quietly drift.
 * {@link #POINT_TABLE} is the narrow point-query/stream table, same shape and isolation reasoning
 * as {@code clickhouse-r2dbc-reactive-benchmarks}' {@code PointQueryTable}; {@link #ORDERS_TABLE}
 * and {@link #CATEGORIES_TABLE} back the {@code analytics} scenario's real {@code JOIN}/{@code
 * GROUP BY} query, per ROADMAP.md's Phase 12 ("real multi-JOIN/GROUP BY/aggregation query", not a
 * synthetic single-table scan).
 */
public final class BenchmarkDataset {

  /** The narrow point-query/stream table both backends read from. */
  public static final String POINT_TABLE = "macrobench_point";

  /** The analytics scenario's dimension table (category id -> name). */
  public static final String CATEGORIES_TABLE = "macrobench_categories";

  /** The analytics scenario's fact table (one row per synthetic order). */
  public static final String ORDERS_TABLE = "macrobench_orders";

  /** How many distinct categories {@link #CATEGORIES_TABLE} is seeded with. */
  static final int CATEGORY_COUNT = 20;

  /**
   * The analytics scenario's query text - a real {@code JOIN} against {@link #CATEGORIES_TABLE},
   * {@code GROUP BY}/{@code sum}/{@code count} aggregation over {@link #ORDERS_TABLE}. Both
   * backends run this exact string.
   */
  public static final String ANALYTICS_SQL =
      "SELECT c.category_name AS category, count() AS order_count, sum(o.amount) AS total_amount "
          + "FROM "
          + ORDERS_TABLE
          + " o INNER JOIN "
          + CATEGORIES_TABLE
          + " c ON o.category_id = c.category_id "
          + "GROUP BY c.category_name ORDER BY total_amount DESC";

  private BenchmarkDataset() {}
}

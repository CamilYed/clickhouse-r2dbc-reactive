package io.github.camilyed.clickhouse.r2dbc.benchmarks;

/**
 * Five small dimension/lookup tables joined (four via {@code JOIN}, one via a scalar subquery)
 * against {@link PointQueryTable} by {@link MixedWorkloadRapidRefreshCancelBenchmark}'s heavy
 * queries — six tables touched per query in total (the fact table plus these five), a more
 * representative "dashboard analytical query" shape than a single-table {@code GROUP BY} alone.
 *
 * <p>Scoped to that one benchmark class rather than merged into {@link PointQueryTable} itself:
 * every other benchmark in this suite reuses {@link PointQueryTable} as a plain two-column fact
 * table, and none of them need these dimensions, so keeping them separate avoids widening a shared
 * table's schema for the sake of one benchmark.
 */
final class HeavyWorkloadDimensionTables {

  /**
   * Joined via {@code id % bucketModulus = bucket_id} — seeded wide enough (120 rows) to cover
   * every {@code bucketModulus} {@link MixedWorkloadRapidRefreshCancelBenchmark}'s heavy queries
   * use (10 through 120).
   */
  static final String BUCKET_DIM_NAME = "benchmark_heavy_workload_bucket_dim";

  /** Joined via {@code id % 5 = region_id} — a fixed-cardinality dimension. */
  static final String REGION_DIM_NAME = "benchmark_heavy_workload_region_dim";

  /** Joined via {@code id % 8 = segment_id} — a fixed-cardinality dimension. */
  static final String SEGMENT_DIM_NAME = "benchmark_heavy_workload_segment_dim";

  /** Joined via {@code id % 4 = channel_id} — a fixed-cardinality dimension. */
  static final String CHANNEL_DIM_NAME = "benchmark_heavy_workload_channel_dim";

  /**
   * Not joined directly — read via a scalar subquery in the heavy queries' {@code WHERE} clause
   * ({@code amount > (SELECT min_amount FROM ... WHERE tier_id = ...)}), so the sixth table is
   * touched through a subquery rather than another {@code JOIN}, per this benchmark's own design
   * goal of exercising both query shapes.
   */
  static final String TIER_THRESHOLD_NAME = "benchmark_heavy_workload_tier_threshold";

  /** How many distinct buckets {@link #BUCKET_DIM_NAME} carries. */
  static final int BUCKET_DIM_ROWS = 120;

  /** How many distinct regions {@link #REGION_DIM_NAME} carries. */
  static final int REGION_DIM_ROWS = 5;

  /** How many distinct segments {@link #SEGMENT_DIM_NAME} carries. */
  static final int SEGMENT_DIM_ROWS = 8;

  /** How many distinct channels {@link #CHANNEL_DIM_NAME} carries. */
  static final int CHANNEL_DIM_ROWS = 4;

  /** How many distinct tiers {@link #TIER_THRESHOLD_NAME} carries. */
  static final int TIER_THRESHOLD_ROWS = 3;

  private HeavyWorkloadDimensionTables() {}

  /** Drops and recreates all five dimension/lookup tables, seeded entirely server-side. */
  static void seed() {
    seedBucketDim();
    seedRegionDim();
    seedSegmentDim();
    seedChannelDim();
    seedTierThreshold();
  }

  private static void seedBucketDim() {
    BenchmarkEnvironment.executeAdminSql("DROP TABLE IF EXISTS " + BUCKET_DIM_NAME);
    BenchmarkEnvironment.executeAdminSql(
        "CREATE TABLE "
            + BUCKET_DIM_NAME
            + " (bucket_id UInt32, bucket_name String, weight Float64) ENGINE = MergeTree ORDER BY bucket_id");
    BenchmarkEnvironment.executeAdminSql(
        "INSERT INTO "
            + BUCKET_DIM_NAME
            + " SELECT number AS bucket_id, concat('bucket-', toString(number)) AS bucket_name, "
            + "(number % 17) / 17.0 AS weight FROM numbers("
            + BUCKET_DIM_ROWS
            + ")");
  }

  private static void seedRegionDim() {
    BenchmarkEnvironment.executeAdminSql("DROP TABLE IF EXISTS " + REGION_DIM_NAME);
    BenchmarkEnvironment.executeAdminSql(
        "CREATE TABLE "
            + REGION_DIM_NAME
            + " (region_id UInt32, region_name String) ENGINE = MergeTree ORDER BY region_id");
    BenchmarkEnvironment.executeAdminSql(
        "INSERT INTO "
            + REGION_DIM_NAME
            + " SELECT number AS region_id, concat('region-', toString(number)) AS region_name "
            + "FROM numbers("
            + REGION_DIM_ROWS
            + ")");
  }

  private static void seedSegmentDim() {
    BenchmarkEnvironment.executeAdminSql("DROP TABLE IF EXISTS " + SEGMENT_DIM_NAME);
    BenchmarkEnvironment.executeAdminSql(
        "CREATE TABLE "
            + SEGMENT_DIM_NAME
            + " (segment_id UInt32, segment_name String, multiplier Float64) ENGINE = MergeTree ORDER BY segment_id");
    BenchmarkEnvironment.executeAdminSql(
        "INSERT INTO "
            + SEGMENT_DIM_NAME
            + " SELECT number AS segment_id, concat('segment-', toString(number)) AS segment_name, "
            + "1.0 + (number % 5) / 10.0 AS multiplier FROM numbers("
            + SEGMENT_DIM_ROWS
            + ")");
  }

  private static void seedChannelDim() {
    BenchmarkEnvironment.executeAdminSql("DROP TABLE IF EXISTS " + CHANNEL_DIM_NAME);
    BenchmarkEnvironment.executeAdminSql(
        "CREATE TABLE "
            + CHANNEL_DIM_NAME
            + " (channel_id UInt32, channel_name String) ENGINE = MergeTree ORDER BY channel_id");
    BenchmarkEnvironment.executeAdminSql(
        "INSERT INTO "
            + CHANNEL_DIM_NAME
            + " SELECT number AS channel_id, concat('channel-', toString(number)) AS channel_name "
            + "FROM numbers("
            + CHANNEL_DIM_ROWS
            + ")");
  }

  private static void seedTierThreshold() {
    BenchmarkEnvironment.executeAdminSql("DROP TABLE IF EXISTS " + TIER_THRESHOLD_NAME);
    BenchmarkEnvironment.executeAdminSql(
        "CREATE TABLE "
            + TIER_THRESHOLD_NAME
            + " (tier_id UInt32, min_amount Float64) ENGINE = MergeTree ORDER BY tier_id");
    BenchmarkEnvironment.executeAdminSql(
        "INSERT INTO "
            + TIER_THRESHOLD_NAME
            + " SELECT number AS tier_id, number * 100.0 AS min_amount FROM numbers("
            + TIER_THRESHOLD_ROWS
            + ")");
  }
}

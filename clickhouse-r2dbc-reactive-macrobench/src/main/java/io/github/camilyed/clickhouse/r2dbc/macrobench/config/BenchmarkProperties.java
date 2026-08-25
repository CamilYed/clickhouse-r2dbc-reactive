package io.github.camilyed.clickhouse.r2dbc.macrobench.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code benchmark.*} configuration - which backend(s) are active ({@code benchmark.backend}, see
 * {@code io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend#fromProperty}), how large
 * the seeded dataset is, and the physical connection pool size both backends are pinned to.
 * Defaults here are a local-smoke-test size, not ROADMAP.md's Phase 12 "trusted" sizing (~5M-row
 * fact table) - override via {@code benchmark.point-rows}/{@code benchmark.analytics-rows} (or the
 * {@code MACROBENCH_POINT_ROWS}/{@code MACROBENCH_ANALYTICS_ROWS} environment variables
 * application.yml maps them to) for a real run.
 *
 * <p>{@code poolSize} defaults to {@code 8} (matching the physical-connection count ROADMAP.md's
 * Phase 12 fairness config and {@code clickhouse-r2dbc-reactive-benchmarks}' own matched-pool JMH
 * benchmarks already standardize on) rather than {@code null}, deliberately: leaving either backend
 * at its own unstated default pool size produces a comparison whose configuration nobody can
 * reconstruct later, and the two defaults aren't even equal to begin with - client-v2's own default
 * is 10 physical connections ({@code ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS}), this
 * driver's is {@code max(availableProcessors, 8) * 2} (at least 16, see
 * docs/operations/connection-pooling.md) - so an unpinned run silently compares two different pool
 * sizes and calls it a driver comparison. Override via {@code benchmark.pool-size} (or the {@code
 * MACROBENCH_POOL_SIZE} environment variable) if you deliberately want a different, still-equal,
 * pool size for both backends.
 *
 * <p>{@code unpinR2dbcPool} (default {@code false}) is a deliberate escape hatch from that pinning,
 * for the opposite kind of experiment: deliberately mismatched pools, e.g. to saturate client-v2's
 * fixed {@code poolSize} under concurrent load while this driver runs at its own larger, CPU-scaled
 * default ({@code max(availableProcessors, 8) * 2}) and has headroom to spare - the same "one side
 * pinned, one side left at its own default" shape {@code clickhouse-r2dbc-reactive-benchmarks}'
 * {@code DefaultPoolSlowQueryThroughputBenchmark} already uses at the JMH level. When {@code true},
 * {@code R2dbcBackendConfiguration} never sets {@code transportMaxConnections} at all - {@code
 * poolSize} still applies to client-v2 unchanged.
 */
@ConfigurationProperties("benchmark")
public record BenchmarkProperties(
    String backend, long pointRows, long analyticsRows, Integer poolSize, boolean unpinR2dbcPool) {

  private static final String DEFAULT_BACKEND = "dual";
  private static final long DEFAULT_ROW_COUNT = 100_000;
  private static final int DEFAULT_POOL_SIZE = 8;

  public BenchmarkProperties {
    if (backend == null || backend.isBlank()) {
      backend = DEFAULT_BACKEND;
    }
    if (pointRows <= 0) {
      pointRows = DEFAULT_ROW_COUNT;
    }
    if (analyticsRows <= 0) {
      analyticsRows = DEFAULT_ROW_COUNT;
    }
    if (poolSize == null) {
      poolSize = DEFAULT_POOL_SIZE;
    }
  }
}

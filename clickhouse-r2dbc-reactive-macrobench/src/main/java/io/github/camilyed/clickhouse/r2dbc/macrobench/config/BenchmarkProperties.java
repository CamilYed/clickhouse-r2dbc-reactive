package io.github.camilyed.clickhouse.r2dbc.macrobench.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code benchmark.*} configuration - which backend(s) are active ({@code benchmark.backend}, see
 * {@code io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend#fromProperty}) and how
 * large the seeded dataset is. Defaults here are a local-smoke-test size, not ROADMAP.md's Phase
 * 12 "trusted" sizing (~5M-row fact table) - override via {@code benchmark.point-rows}/{@code
 * benchmark.analytics-rows} (or the {@code MACROBENCH_POINT_ROWS}/{@code
 * MACROBENCH_ANALYTICS_ROWS} environment variables application.yml maps them to) for a real run.
 */
@ConfigurationProperties("benchmark")
public record BenchmarkProperties(String backend, long pointRows, long analyticsRows) {

  private static final String DEFAULT_BACKEND = "dual";
  private static final long DEFAULT_ROW_COUNT = 100_000;

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
  }
}

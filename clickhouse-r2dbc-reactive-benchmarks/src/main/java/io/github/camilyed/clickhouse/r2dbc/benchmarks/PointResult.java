package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.math.BigDecimal;

/**
 * The application-level object both {@code thisDriver} and {@code clientV2} must decode a point
 * query into for a fair headline benchmark comparison - see {@code
 * CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md} section 2.2. Earlier benchmarks in this module let each
 * side consume its result differently (this driver counting a {@code DecodedRow}, client-v2 pulling
 * individual typed columns) - acceptable for a diagnostic microbenchmark, but it means a headline
 * "which is faster" comparison isn't really comparing the same amount of work. Both sides must map
 * their raw result to exactly this record before anything is measured.
 */
public record PointResult(String label, BigDecimal amount) {

  /**
   * An order-independent combination of both fields, used as the sole value fed to JMH's {@code
   * Blackhole}/reduced across a whole workload - see section 8 of the plan referenced above for
   * why: it forces both implementations to have actually decoded real values (the JIT can't discard
   * an unused result), without depending on completion order under concurrency.
   */
  public long checksum() {
    return 31L * label.hashCode() + amount.unscaledValue().longValue();
  }
}

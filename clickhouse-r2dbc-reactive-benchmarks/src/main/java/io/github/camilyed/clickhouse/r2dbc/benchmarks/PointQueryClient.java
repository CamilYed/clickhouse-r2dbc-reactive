package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.time.Duration;
import reactor.core.publisher.Mono;

/**
 * A single point-query capability, implemented once against this driver's public R2DBC SPI and once
 * against client-v2's public API, so a headline benchmark can drive both through the exact same
 * outer Reactor harness - see {@code CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md} section 7. The
 * benchmark class that uses this must never import {@code ClickHouseHttpTransport}, {@code
 * RowBinaryDecoder}, or {@code DecodedRow} directly - only through whichever adapter implements
 * this interface, which is exactly what an application built on this driver would call.
 */
public interface PointQueryClient extends AutoCloseable {

  /** Looks up one row by {@code id} and maps it to a {@link PointResult}. */
  Mono<PointResult> query(long id);

  /**
   * Issues {@code warmupCalls} sequential {@link #query(long)} calls, cycling through {@code
   * idsToWarmWith}, and blocks on each - moves first-connection/first-query costs (DNS resolution,
   * class loading, first connection-pool expansion, decoder initialization) out of whatever
   * measurement follows. See the plan's section 5 ("Prewarm both implementations explicitly").
   * Default implementation is sequential and blocking, deliberately - a prewarm step runs in
   * {@code @Setup}, never inside a measured benchmark method, so there is nothing to optimize here.
   */
  default void prewarm(final long[] idsToWarmWith, final int warmupCalls) {
    for (int i = 0; i < warmupCalls; i++) {
      final long id = idsToWarmWith[i % idsToWarmWith.length];
      query(id).block(Duration.ofSeconds(10));
    }
  }

  @Override
  void close();
}

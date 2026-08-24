package io.github.camilyed.clickhouse.r2dbc.macrobench.backend;

import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * One query path this macrobenchmark drives end to end through a real Spring Boot WebFlux request -
 * either this driver or client-v2 directly, behind the identical REST endpoint contract (see {@code
 * BenchmarkController}). Three of the four scenarios ROADMAP.md's Phase 12 describes are here
 * ({@code point}, {@code analytics}, {@code stream}); the fourth ({@code cancel} - abort in-flight
 * requests, correlate against {@code system.processes}/{@code system.query_log}) is deliberately
 * not part of this interface yet, since it needs cancellation-signal wiring specific to each
 * backend rather than a query method - tracked as a PR1 follow-up in ROADMAP.md rather than rushed
 * into this first cut unverified.
 *
 * <p>Both implementations ({@code R2dbcBenchmarkQueryBackend}, {@code
 * ClientV2BenchmarkQueryBackend}) share the fairness config ROADMAP.md's Phase 12 section
 * specifies: 8 physical connections, no outer {@code io.r2dbc.pool} on the r2dbc side (this
 * driver's own {@code ClickHouseHttpTransport} Reactor Netty {@code ConnectionProvider} is already
 * the real pool), {@code useAsyncRequests(true)} on client-v2, response compression on both.
 */
public interface BenchmarkQueryBackend {

  /** Which backend this is - used to route {@code /benchmark/{backend}/...} requests. */
  Backend kind();

  /** Point lookup: one row by primary key from the seeded point-query table. */
  Mono<PointRow> point(long id);

  /**
   * Real analytical query: a {@code JOIN} between the seeded fact and dimension tables, {@code
   * GROUP BY}/{@code sum}/{@code count} aggregation - not a synthetic single-table scan.
   */
  Mono<List<CategoryTotal>> analytics();

  /** Streams up to {@code limit} rows from the seeded point-query table, in {@code id} order. */
  Flux<PointRow> stream(long limit);
}

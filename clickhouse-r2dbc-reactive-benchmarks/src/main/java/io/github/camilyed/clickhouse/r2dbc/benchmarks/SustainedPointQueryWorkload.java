package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import org.HdrHistogram.Recorder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Runs a fixed number of point queries under a bounded, sustained logical concurrency. */
final class SustainedPointQueryWorkload {

  private final int requestCount;
  private final int concurrency;

  SustainedPointQueryWorkload(final int requestCount, final int concurrency) {
    this.requestCount = requestCount;
    this.concurrency = concurrency;
  }

  Mono<Long> execute(
      final LongSupplier idSupplier,
      final LongFunction<Mono<Long>> query,
      final Recorder latencyRecorder) {
    return Flux.range(0, requestCount)
        .flatMap(ignored -> timedQuery(idSupplier, query, latencyRecorder), concurrency)
        .reduce(0L, Long::sum);
  }

  private Mono<Long> timedQuery(
      final LongSupplier idSupplier,
      final LongFunction<Mono<Long>> query,
      final Recorder latencyRecorder) {
    final long id = idSupplier.getAsLong();
    final long startNanos = System.nanoTime();
    return query
        .apply(id)
        .doOnNext(
            ignored ->
                latencyRecorder.recordValue(Math.max((System.nanoTime() - startNanos) / 1000, 0)));
  }
}

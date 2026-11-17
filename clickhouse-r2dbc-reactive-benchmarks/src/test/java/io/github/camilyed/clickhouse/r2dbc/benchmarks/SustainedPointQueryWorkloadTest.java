package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class SustainedPointQueryWorkloadTest {

  @Test
  void shouldIssueEveryQueryWithoutExceedingTheConfiguredConcurrency() {
    // given
    final AtomicLong ids = new AtomicLong();
    final AtomicInteger activeQueries = new AtomicInteger();
    final AtomicInteger maximumActiveQueries = new AtomicInteger();
    final AtomicInteger completedQueries = new AtomicInteger();
    final Recorder latencyRecorder = new Recorder(TimeUnit.SECONDS.toMicros(1), 3);
    final SustainedPointQueryWorkload workload = new SustainedPointQueryWorkload(6, 3);

    // when
    final Mono<Long> checksum =
        workload.execute(
            ids::incrementAndGet,
            id ->
                Mono.defer(
                    () -> {
                      final int active = activeQueries.incrementAndGet();
                      maximumActiveQueries.accumulateAndGet(active, Math::max);
                      return Mono.delay(Duration.ofSeconds(1))
                          .map(ignored -> id)
                          .doOnNext(ignored -> completedQueries.incrementAndGet())
                          .doOnTerminate(activeQueries::decrementAndGet);
                    }),
            latencyRecorder::recordValue);

    // then
    StepVerifier.withVirtualTime(() -> checksum)
        .thenAwait(Duration.ofSeconds(2))
        .expectNext(21L)
        .verifyComplete();
    final Histogram recordedLatencies = latencyRecorder.getIntervalHistogram();
    assertThat(completedQueries).hasValue(6);
    assertThat(maximumActiveQueries).hasValue(3);
    assertThat(activeQueries).hasValue(0);
    assertThat(recordedLatencies.getTotalCount()).isEqualTo(6);
  }
}

package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

class IterationLatencyRecorderTest {

  @Test
  void shouldKeepWarmupLatenciesOutOfTheMergedMeasurementHistogram() {
    // given
    final IterationLatencyRecorder latencies =
        new IterationLatencyRecorder(TimeUnit.SECONDS.toMicros(1));
    latencies.record(100);

    // when
    final Histogram warmup = latencies.finishWarmupIteration();
    latencies.record(200);
    latencies.record(300);
    final Histogram measurement = latencies.finishMeasurementIteration();

    // then
    assertThat(warmup.getTotalCount()).isEqualTo(1);
    assertThat(measurement.getTotalCount()).isEqualTo(2);
    assertThat(latencies.mergedMeasurementHistogram().getTotalCount()).isEqualTo(2);
    assertThat(latencies.mergedMeasurementHistogram().getMinValue()).isEqualTo(200);
    assertThat(latencies.mergedMeasurementHistogram().getMaxValue()).isEqualTo(300);
  }
}

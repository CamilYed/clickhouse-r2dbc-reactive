package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/** Records iteration latencies while merging measurement iterations separately from warmup. */
final class IterationLatencyRecorder {

  private static final int SIGNIFICANT_DIGITS = 3;

  private final Recorder intervalRecorder;
  private final Histogram mergedMeasurementHistogram;

  IterationLatencyRecorder(final long highestTrackableValue) {
    intervalRecorder = new Recorder(highestTrackableValue, SIGNIFICANT_DIGITS);
    mergedMeasurementHistogram = new Histogram(highestTrackableValue, SIGNIFICANT_DIGITS);
  }

  void record(final long value) {
    intervalRecorder.recordValue(value);
  }

  Histogram finishWarmupIteration() {
    return intervalRecorder.getIntervalHistogram();
  }

  Histogram finishMeasurementIteration() {
    final Histogram interval = intervalRecorder.getIntervalHistogram();
    mergedMeasurementHistogram.add(interval);
    return interval;
  }

  Histogram mergedMeasurementHistogram() {
    return mergedMeasurementHistogram;
  }
}

#!/usr/bin/env bash
# Phase 11 PR2 (see ROADMAP.md): the open question this exists to help answer is whether client-v2's
# throughput edge (docs/performance/results.md's cloud-verified matched-pool result) is bought with
# materially more platform threads/RSS than this driver's bounded, non-blocking pipeline uses. JMH's
# own `-prof hs_thr` (wired in .github/workflows/benchmark.yml's trusted profile) reports thread
# counts per JMH iteration already, forked-JVM-scoped; RSS has no JMH-native profiler equivalent, so
# this script is the /proc half, sampled at whole-forked-JVM-process granularity instead.
#
# Usage: sample-resources.sh <parent-pid> <out-csv> [interval-seconds]
#
# Runs until <parent-pid> (the `./gradlew jmh` process this is launched alongside) exits, sampling
# every `interval-seconds` (default 2). Each sample sums RSS (KB) and live thread count across every
# process currently matching "org.openjdk.jmh.runner.ForkedMain" on the command line - JMH's own
# forked-runner main class, not the Gradle daemon or wrapper process itself, so this doesn't pick up
# Gradle's own JVM's unrelated RSS/thread count. Deliberately coarse, not per-iteration or
# per-benchmark-method: this workflow runs one JMH fork at a time, sequentially, so at most one
# matching process is expected per sample - summed rather than picking "the first match" so this
# doesn't silently drop data if that assumption is ever wrong (e.g. a future change overlaps forks).
#
# Writes one CSV row per sample: timestamp_epoch,rss_kb,thread_count,jvm_count. A sample is skipped
# (no row written) when no matching JVM is currently alive (e.g. between forks) - jvm_count in every
# written row is always >= 1, so a reader never has to guess whether a 0 means "measured zero" or "no
# JVM was up".

set -euo pipefail

PARENT_PID="$1"
OUT_FILE="$2"
INTERVAL_SECONDS="${3:-2}"

echo "timestamp_epoch,rss_kb,thread_count,jvm_count" >"$OUT_FILE"

while kill -0 "$PARENT_PID" 2>/dev/null; do
  total_rss_kb=0
  total_threads=0
  jvm_count=0
  for pid in $(pgrep -f "org.openjdk.jmh.runner.ForkedMain" 2>/dev/null || true); do
    if [ -r "/proc/$pid/status" ]; then
      rss="$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)"
      threads="$(awk '/^Threads:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)"
      total_rss_kb=$((total_rss_kb + ${rss:-0}))
      total_threads=$((total_threads + ${threads:-0}))
      jvm_count=$((jvm_count + 1))
    fi
  done
  if [ "$jvm_count" -gt 0 ]; then
    echo "$(date +%s),${total_rss_kb},${total_threads},${jvm_count}" >>"$OUT_FILE"
  fi
  sleep "$INTERVAL_SECONDS"
done

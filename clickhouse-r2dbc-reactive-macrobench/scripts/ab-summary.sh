#!/usr/bin/env bash
set -euo pipefail

# Runs run-ab.sh (see that script's own header for what ab does/doesn't prove - closed-loop,
# local-only smoke tool, not a substitute for ROADMAP.md's Phase 12 PR2 k6/wrk2 methodology)
# across a matrix of backend x scenario combinations, and prints ONE comparison table instead
# of ab's full verbose output per run. That's the whole point of this script: eyeballing r2dbc
# vs client-v2 across three scenarios by scrolling through six raw ab reports is error-prone;
# a table you can read in one glance isn't.
#
# Usage: ab-summary.sh [requests] [concurrency]
#   Defaults: 2000 requests, concurrency 10 - same defaults as run-ab.sh itself.
#   Override which backends/scenarios run via env vars (space-separated):
#     BACKENDS="r2dbc client-v2" SCENARIOS="point analytics stream" ab-summary.sh 20000 100
#   Set KEEP_LOGS=1 to keep each run's full ab output instead of discarding it after the table
#   is printed (path is echoed at the end either way).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REQUESTS="${1:-2000}"
CONCURRENCY="${2:-10}"
BACKENDS="${BACKENDS:-r2dbc client-v2}"
SCENARIOS="${SCENARIOS:-point analytics stream}"
KEEP_LOGS="${KEEP_LOGS:-0}"

RESULTS_DIR="$(mktemp -d)"
if [ "$KEEP_LOGS" != "1" ]; then
  trap 'rm -rf "$RESULTS_DIR"' EXIT
fi

echo "Sweeping: backends=[$BACKENDS] scenarios=[$SCENARIOS] requests=$REQUESTS concurrency=$CONCURRENCY"
echo

printf '%-10s %-10s %10s %10s %10s %10s %10s %10s %8s\n' \
  "backend" "scenario" "rps" "mean(ms)" "p50(ms)" "p95(ms)" "p99(ms)" "max(ms)" "failed"
printf '%s\n' "-------------------------------------------------------------------------------------------"

for backend in $BACKENDS; do
  for scenario in $SCENARIOS; do
    logfile="$RESULTS_DIR/${backend}-${scenario}.log"
    if ! "$SCRIPT_DIR/run-ab.sh" "$backend" "$scenario" "$REQUESTS" "$CONCURRENCY" >"$logfile" 2>&1; then
      printf '%-10s %-10s %10s\n' "$backend" "$scenario" "FAILED (see $logfile)"
      continue
    fi

    rps=$(awk '/Requests per second:/ {print $4}' "$logfile")
    mean=$(awk '/Time per request:/ && /\(mean\)/ && !/across/ {print $4}' "$logfile")
    p50=$(awk '$1=="50%"{print $2}' "$logfile")
    p95=$(awk '$1=="95%"{print $2}' "$logfile")
    p99=$(awk '$1=="99%"{print $2}' "$logfile")
    max=$(awk '$1=="100%"{print $2}' "$logfile")
    failed=$(awk '/Failed requests:/ {print $3}' "$logfile")

    printf '%-10s %-10s %10s %10s %10s %10s %10s %10s %8s\n' \
      "$backend" "$scenario" "$rps" "$mean" "$p50" "$p95" "$p99" "$max" "$failed"
  done
done

echo
if [ "$KEEP_LOGS" == "1" ]; then
  echo "Full ab output per run kept at: $RESULTS_DIR"
else
  echo "Full ab output per run was in a temp dir, deleted on exit - rerun with KEEP_LOGS=1 to keep it."
fi

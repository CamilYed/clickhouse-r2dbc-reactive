#!/usr/bin/env bash
set -euo pipefail

# Local, quick r2dbc-vs-client-v2 comparison using Apache Bench (ab -
# https://httpd.apache.org/docs/2.4/programs/ab.html) against a running
# clickhouse-r2dbc-reactive-macrobench instance - see this module's README.md's "Running locally".
#
# ab is a CLOSED-loop load generator: it waits for each response before sending the next one at
# a given concurrency, which hides tail-latency behavior under real overload the way an open-loop
# generator (k6/wrk2) doesn't. ROADMAP.md's Phase 12 fairness config is explicit that ab stays a
# "local smoke-test tool only" for exactly this reason - a real "trusted", published number needs
# k6/wrk2 with paired A/B rounds (not built yet, tracked as PR2 in ROADMAP.md's Phase 12 PR
# sequence). This script is for local iteration while working on the driver, not for a headline
# result.
#
# Explicit warmup phase, discarded before the measured run: JIT warmup, connection-pool
# establishment, and (for the r2dbc backend) Reactor Netty's own connection-provider warmup all
# skew a cold first run. ab has no built-in warmup concept, so this script runs ab twice and only
# reports the second run's numbers - the same "deliberate warmup phase excluded from measurement"
# requirement ROADMAP.md's Phase 12 states for the eventual trusted methodology too.
#
# Usage: run-ab.sh <r2dbc|client-v2> <point|analytics|stream> [requests] [concurrency]
BACKEND="${1:?usage: $0 <r2dbc|client-v2> <point|analytics|stream> [requests] [concurrency]}"
SCENARIO="${2:?usage: $0 <r2dbc|client-v2> <point|analytics|stream> [requests] [concurrency]}"
REQUESTS="${3:-2000}"
CONCURRENCY="${4:-10}"
WARMUP_REQUESTS=200
BASE_URL="${MACROBENCH_BASE_URL:-http://localhost:8081}"

case "$SCENARIO" in
  point) PATH_SUFFIX="point/1" ;;
  analytics) PATH_SUFFIX="analytics" ;;
  stream) PATH_SUFFIX="stream?limit=100" ;;
  *)
    echo "Unknown scenario '$SCENARIO' (expected point|analytics|stream)" >&2
    exit 1
    ;;
esac

if ! command -v ab >/dev/null 2>&1; then
  echo "ab (Apache Bench) not found - install apache2-utils (Debian/Ubuntu) or httpd (brew, macOS)." >&2
  exit 1
fi

URL="${BASE_URL}/benchmark/${BACKEND}/${PATH_SUFFIX}"

echo "=== Warmup: ${WARMUP_REQUESTS} requests against ${URL} (discarded) ==="
ab -n "$WARMUP_REQUESTS" -c "$CONCURRENCY" -q "$URL" >/dev/null

echo "=== Measured run: ${REQUESTS} requests, concurrency ${CONCURRENCY}, against ${URL} ==="
ab -n "$REQUESTS" -c "$CONCURRENCY" "$URL"

#!/usr/bin/env bash
set -euo pipefail

# Runs the JMH benchmark suite (clickhouse-r2dbc-reactive-benchmarks) wrapped with
# `caffeinate -d -i` on macOS, so a long multi-fork run isn't interrupted by the machine sleeping
# mid-run - see clickhouse-r2dbc-reactive-benchmarks/README.md's note on the same pattern.
#
# Defaults to a "trusted" 3-fork/3-warmup-iteration run (docs/PERFORMANCE.md's confidence
# warning: the plain `jmh` task's 1-fork/1-warmup default is a sanity check, not a number to
# trust). Any extra arguments passed to this script are forwarded to the Gradle `jmh` task
# as-is, so you can narrow the run or override the defaults, e.g.:
#
#   ./scripts/run-benchmarks.sh -Pjmh.includes=StreamingScanBenchmark
#   ./scripts/run-benchmarks.sh -Pjmh.includes=StreamingScanBenchmark -Pjmh.profilers=gc
#   ./scripts/run-benchmarks.sh -Pjmh.forks=1 -Pjmh.warmupIterations=1   # quick sanity check only
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEFAULT_ARGS=(-Pjmh.forks=3 -Pjmh.warmupIterations=3)
EXTRA_ARGS=("$@")

# If the caller passed their own -Pjmh.forks / -Pjmh.warmupIterations, don't also pass the
# defaults - Gradle would just take the last value, but this keeps `./gradlew jmh -q` output
# free of confusing duplicate property flags.
GRADLE_ARGS=(":clickhouse-r2dbc-reactive-benchmarks:jmh")
for arg in "${DEFAULT_ARGS[@]}"; do
  key="${arg%%=*}"
  if ! printf '%s\n' "${EXTRA_ARGS[@]:-}" | grep -qF -- "${key}="; then
    GRADLE_ARGS+=("${arg}")
  fi
done
GRADLE_ARGS+=("${EXTRA_ARGS[@]}")

GRADLE_CMD=("${REPO_ROOT}/gradlew" "${GRADLE_ARGS[@]}")

echo "Running: ${GRADLE_CMD[*]}"
echo "(results written under clickhouse-r2dbc-reactive-benchmarks/build/results/jmh/ - not committed to git)"
echo

if [[ "$(uname)" == "Darwin" ]] && command -v caffeinate >/dev/null 2>&1; then
  caffeinate -d -i "${GRADLE_CMD[@]}"
else
  "${GRADLE_CMD[@]}"
fi

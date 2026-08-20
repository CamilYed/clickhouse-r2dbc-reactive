#!/usr/bin/env bash
set -euo pipefail

# Runs the full local pre-PR gate (CLAUDE.md "Before opening a PR: ./gradlew spotlessCheck clean
# build must pass locally"): Spotless formatting check, all unit tests, transport contract tests,
# and the real-ClickHouse Testcontainers integration tests (connector module), across every
# module - not just the modules touched by the current change. This is the full run to trust
# before merging a change to shared/production code such as FluxInputStreamBridge.
#
# Wrapped with `caffeinate -d -i` on macOS so the Testcontainers-backed integration tests (which
# can run long) don't get interrupted by the machine sleeping mid-run - see
# clickhouse-r2dbc-reactive-benchmarks/README.md's note on the same pattern for long JMH runs.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GRADLE_CMD=("${REPO_ROOT}/gradlew" spotlessCheck clean build)

echo "Running: ${GRADLE_CMD[*]}"
echo "(this runs every module: unit tests, transport contract tests, and the real-ClickHouse"
echo " Testcontainers integration tests in the connector module - can take a while)"
echo

if [[ "$(uname)" == "Darwin" ]] && command -v caffeinate >/dev/null 2>&1; then
  caffeinate -d -i "${GRADLE_CMD[@]}"
else
  "${GRADLE_CMD[@]}"
fi

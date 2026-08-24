#!/usr/bin/env bash
set -euo pipefail

# Starts a single, long-lived ClickHouse container for "trusted" benchmark runs - see
# CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md section 2.5 / section 5: without this, each JMH fork
# starts its own separate Testcontainers-managed ClickHouse process (see BenchmarkEnvironment's
# Javadoc), so thisDriver and clientV2 can end up compared against two different server instances -
# even same image, same dataset, server startup state/page cache/Docker scheduling can still differ
# between them. With this script, every fork instead points at the one server started here.
#
# Keep this image tag in sync with BenchmarkEnvironment.CLICK_HOUSE_IMAGE by hand - there is no
# automated link between this script and that Java constant.
IMAGE="clickhouse/clickhouse-server:26.7.3.19"
CONTAINER_NAME="clickhouse-r2dbc-reactive-benchmark"
HTTP_PORT="${BENCH_CLICKHOUSE_HTTP_PORT:-28123}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --memory + the mounted config.d override keep this in sync with BenchmarkEnvironment's
# CONTAINER_MEMORY_BYTES / SERVER_MEMORY_USAGE_BYTES - see that class's Javadoc for why both
# a Docker-level limit and an explicit max_server_memory_usage are needed (auto-detection alone
# under Docker Desktop's Linux VM proved unreliable). Bump by hand in both places if changed.
if docker ps --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "Container ${CONTAINER_NAME} is already running."
else
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker run -d \
    --name "${CONTAINER_NAME}" \
    --memory=7000m \
    --memory-swap=7000m \
    -v "${SCRIPT_DIR}/benchmark-memory-limit.xml:/etc/clickhouse-server/config.d/benchmark-memory-limit.xml:ro" \
    -p "${HTTP_PORT}:8123" \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    "${IMAGE}"
  echo "Started ${CONTAINER_NAME} (${IMAGE}) - HTTP on port ${HTTP_PORT}, memory=7000m."
fi

echo
echo "Export these before running a trusted benchmark, then seed/warm up as usual:"
echo "  export BENCH_CLICKHOUSE_URL=http://localhost:${HTTP_PORT}"
echo "  export BENCH_CLICKHOUSE_USER=default"
echo "  export BENCH_CLICKHOUSE_PASSWORD="
echo
echo "Stop it when done: docker rm -f ${CONTAINER_NAME}"

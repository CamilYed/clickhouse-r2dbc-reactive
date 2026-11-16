# clickhouse-r2dbc-reactive-benchmarks

JMH benchmarks comparing this driver with client-v2 at the public API, transport, decoder,
streaming, concurrency, and cancellation boundaries.

The documentation website is the source of truth:

- [Performance status](../docs/performance/index.md) — what can currently be claimed.
- [Methodology](../docs/performance/methodology.md) — what each benchmark measures.
- [Running the benchmarks](../docs/performance/running-benchmarks.md) — local and cloud commands.
- [Historical results](../docs/performance/results.md) — older versioned measurements.

The module is not published and is not part of `./gradlew build` or `./gradlew check`. Local runs
require Docker and JDK 21.

## Local quick start

Run one sanity-check fork against a Testcontainers-managed server:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh
```

Run one benchmark class:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=PublicApiMatchedPoolThroughputBenchmark
```

Use a long-lived local ClickHouse process when container startup per fork would add avoidable
noise:

```
./scripts/start-benchmark-clickhouse.sh
export BENCH_CLICKHOUSE_URL=http://localhost:28123
export BENCH_CLICKHOUSE_USER=default
export BENCH_CLICKHOUSE_PASSWORD=

./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh
```

Local defaults are suitable for compilation and smoke checks, not for a public performance claim.
Raw JSON is written below `build/results/jmh/` and is not committed.

## Trustworthy comparisons

Use the repository's `benchmark.yml` GitHub Actions workflow. It runs both implementations in the
same job and VM against the same ClickHouse process. Choose `trusted-clean` for latency or
throughput comparisons and retain the workflow URL and artifact with any published result.

The current build uses client-v2 `0.11.2`. Historical result pages were measured before that
upgrade and must not be relabelled as current numbers without a fresh run.

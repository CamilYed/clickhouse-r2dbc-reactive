# clickhouse-r2dbc-reactive-benchmarks

JMH benchmarks comparing this driver against `client-v2`, at multiple levels — what each class
measures and the latest results: [docs/PERFORMANCE.md](../docs/PERFORMANCE.md).

Not published, not part of `./gradlew build`/`check`. Requires Docker and JDK 21.

## Running

Against a Testcontainers-managed server (default — one container per JMH fork):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh
```

Against a long-lived external server instead (faster iteration — no container startup per fork):

```
./scripts/start-benchmark-clickhouse.sh
export BENCH_CLICKHOUSE_URL=http://localhost:28123
export BENCH_CLICKHOUSE_USER=default
export BENCH_CLICKHOUSE_PASSWORD=

./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh
```

Results are written under `build/results/jmh/` (JSON format). Not committed to git — inspect
locally or upload as a CI artifact.

To run one benchmark class only:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=PointQueryBenchmark
```

To run with a JMH profiler (e.g. the GC profiler for allocation numbers):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=DecoderOnlyBenchmark -Pjmh.profilers=gc
```

To run multiple forks (needed before trusting a result — see docs/PERFORMANCE.md's confidence
warning; the plain `jmh` task defaults to 1 fork/1 warmup iteration, a sanity check, not a
trustworthy number):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.forks=3 -Pjmh.warmupIterations=3
```

Flags compose freely, e.g.:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=BoundedPoolConcurrencyBenchmark -Pjmh.forks=3 -Pjmh.warmupIterations=3
```

For a long unattended run (the full suite, or any 3-fork run), wrap with `caffeinate -d -i` on
macOS to prevent the machine sleeping mid-run.

## Machine

All numbers currently recorded in docs/PERFORMANCE.md were measured on a single MacBook Pro
14-inch (Nov 2023, Apple M3 Pro, 36 GB RAM) — a shared consumer laptop, not an isolated
benchmarking rig. Treat absolute numbers as specific to that machine; the driver-vs-driver
comparisons (same hardware/JVM/data on both sides) are the portable part.

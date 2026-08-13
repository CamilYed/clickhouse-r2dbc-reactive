# clickhouse-r2dbc-reactive-benchmarks

JMH benchmarks comparing this driver against `client-v2`, at multiple levels. Full design and
rationale: [ROADMAP.md's Phase 5 section](../ROADMAP.md#phase-5-later--load-and-performance-testing).

Not published, not part of `./gradlew build`/`check`. Requires Docker (Testcontainers starts a real
`clickhouse/clickhouse-server:latest`, shared across the whole run) and JDK 21.

## Running

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh
```

Results are written under `build/results/jmh/` (JSON format). Not committed to git — inspect
locally or upload as a CI artifact.

To run one benchmark class only:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=PointQueryBenchmark
```

## Status

First slice only: `PointQueryBenchmark` (Level 1 — raw transport + decode, this driver's
`ClickHouseHttpTransport`/`RowBinaryDecoder` vs client-v2's `Client`/`ClickHouseBinaryFormatReader`),
proving the module/dataset/comparison-level design end to end. The remaining query shapes (full
table scan, wide multi-type decode, aggregation, INSERT, and the reactive-vs-blocking concurrency
burst scenario) are designed in ROADMAP.md but not yet implemented — same three comparison levels,
same `BenchmarkEnvironment`/dataset-table pattern this first class establishes.

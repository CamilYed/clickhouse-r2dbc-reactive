# clickhouse-r2dbc-reactive-benchmarks

JMH benchmarks comparing this driver against `client-v2`, at multiple levels. Full design and
rationale: [ROADMAP.md's Phase 5 section](../ROADMAP.md#phase-5-later--load-and-performance-testing).

Not published, not part of `./gradlew build`/`check`. Requires Docker (Testcontainers starts a real,
version-pinned `clickhouse/clickhouse-server`, one container per JMH fork — see
`BenchmarkEnvironment`'s Javadoc) and JDK 21.

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

Two Level 1 classes so far, both run for real against Docker/ClickHouse (see ROADMAP.md's Phase 5
section for numbers):

- `PointQueryBenchmark` — a real single-row lookup against a seeded table.
- `TrivialQueryBenchmark` — `SELECT 1`, no table at all; isolates protocol/connection overhead from
  the storage-engine lookup `PointQueryBenchmark` also pays for.

Next up (recommended order, per ROADMAP.md's Phase 5 "Recommended next benchmarks"):
`StreamingScanBenchmark` (TTFR/TTLR/rows-per-second/bytes-per-second over a large streamed result —
the highest-value benchmark for this project's actual architecture). After that: wide multi-type
decode, aggregation, INSERT, the reactive-vs-blocking concurrency burst scenario, and the
backpressure/pool-saturation/cancellation benchmarks — all designed in ROADMAP.md, not yet built.
Same `BenchmarkEnvironment`/dataset-table pattern `PointQueryBenchmark` established.

**Fairness fixes applied after the first run** (a real run surfaced real gaps — not designed away
in the abstract): the ClickHouse image is now version-pinned rather than `latest`; client-v2 now
uses the same `{id:UInt64}`/`query_params` parameterization mechanism as this driver rather than an
inlined SQL literal; benchmark ids are pre-generated once per trial with a fixed seed
(`PointQueryTable#deterministicIds`) instead of called via `Math.random()` inside the hot path.
One known, deliberately-not-forced-to-match asymmetry remains: this driver materializes each row
into a `Map<String, Object>`, client-v2 reads typed values directly — see `PointQueryBenchmark`'s
own Javadoc for why, and what would isolate it (a future transport-only benchmark with no decode at
all).

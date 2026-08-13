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

To run with a JMH profiler (e.g. the GC profiler for allocation numbers):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=DecoderOnlyBenchmark -Pjmh.profilers=gc
```

**Both flags only work as of the `build.gradle.kts` fix below — before that, they were silently
ignored and every `jmh` invocation ran the entire suite with no profiler, regardless of what was
passed on the command line.** The me.champeau.jmh plugin doesn't read `-P` project properties on
its own; `includes`/`profilers` had to be explicitly wired from `project.property(...)` in the
build script for these flags to do anything.

## Status

Two Level 1 classes run for real against Docker/ClickHouse (see ROADMAP.md's Phase 5 section for
numbers):

- `PointQueryBenchmark` — a real single-row lookup against a seeded table.
- `TrivialQueryBenchmark` — `SELECT 1`, no table at all; isolates protocol/connection overhead from
  the storage-engine lookup `PointQueryBenchmark` also pays for.

A third, `StreamingScanBenchmark` — full-table scan over the same seeded table (no `WHERE`), at
`10k`/`100k`/`1M` row tiers, measuring both JMH's own SampleTime (effectively time-to-last-row) and
a separate `HdrHistogram`-backed time-to-first-row per driver (logged at trial teardown, not in
JMH's own JSON — see the class's own Javadoc). Its first run had a real methodology bug (TTFR
instrumentation ran an `AtomicLong` compare-and-swap on every row, not just the first) — fixed, then
re-run for real. **Confirmed, trustworthy result: this driver is genuinely slower for streaming
scans, and the gap grows sharply with row count (≈13% → ≈55% → ≈80% slower, 10k → 100k → 1M rows)**
— unlike `PointQueryBenchmark`/`TrivialQueryBenchmark`, which still favor this driver. See
ROADMAP.md's Phase 5 "Optimization phase" section for the full numbers, the ranked hypothesis list
(verified against this repo's actual source, not just inspection), and the investigation plan.

Two new diagnostic classes localize that gap, **run for real, question answered**:

- `TransportOnlyStreamingBenchmark` — raw response bytes only, no row decoding at all. **This
  driver wins by 51–78%, growing to a 4.6x margin at 1M rows.** The non-blocking transport is a
  genuine, previously-invisible strength.
- `DecoderOnlyBenchmark` — decodes one captured response payload from memory repeatedly, no network
  at all. **This driver loses by 150–190% (a consistent ~2.5–2.9x), flat across all three row
  tiers.** This is where the entire `StreamingScanBenchmark` regression actually lives.

**Conclusion: transport is this driver's strength, not its weakness — decode/materialization is the
confirmed, localized bottleneck**, with a smaller secondary cost in the transport-to-decode bridge
hand-off (sum of the two isolated numbers runs 22–28% under the combined `StreamingScanBenchmark`
figure for this driver, near-zero for client-v2). Full tables and reasoning: ROADMAP.md's Phase 5
"Optimization phase" section.

`-prof gc` on `DecoderOnlyBenchmark`, **run for real, H0 and H1 both confirmed**: client-v2 allocates
a remarkably stable ~296 bytes/row at every tier; this driver allocated ~872 bytes/row before any
fixes. **H0** (the `byte[1]` per single-byte `read()` call) is fixed and confirmed by a real
re-benchmark: a clean, reproducible ~24 bytes/row reduction at both 100k/1M (872 → 848), matching the
predicted cost exactly — real, but only ~4% of the gap. **H1** (the per-row
`new LinkedHashMap<>(reader.next())` copy) is now confirmed too, via a genuine single-variable
diagnostic (`ourDriverWithoutMapCopy`, below): **576.0 bytes/row and ~77% of decode latency at 1M
rows, agreeing to four significant figures across a 10x row-count change.** H0 + H1 account for
essentially the entire original allocation gap. Mechanically explained by reading client-v2's actual
source: `new LinkedHashMap<>(...)` triggers `RecordWrapper.entrySet()`, which allocates a fresh
`HashSet` plus one `SimpleImmutableEntry` per column on every call — real, measured, and now
source-confirmed, not inferred. Full reasoning, numbers, and the exact quoted source: ROADMAP.md's
Phase 5 "Optimization phase" section.

**H0 fixed and confirmed**: `FluxInputStreamBridge` now reuses a single-element buffer instead of
allocating `new byte[1]` per call — plus a new black-box test for the single-byte `read()` overload,
which had zero test coverage before this (every existing test only exercised the bulk
`read(byte[], int, int)` path via `readAllBytes()`).

**H1 confirmed via `DecoderOnlyBenchmark.ourDriverWithoutMapCopy`**: identical to `ourDriver` — same
bridge, same reader settings, same `Flux.generate` shape — except the final `LinkedHashMap` copy is
skipped. A single-variable change against `ourDriver`, isolating H1's cost directly: 576.0 bytes/row,
~173.7ms of the 1M-row decode time. One caveat: this diagnostic comes in *below* client-v2's own
baseline (272 vs 296 B/row), but the two aren't doing equivalent work — the diagnostic discards each
row without calling any getter, while client-v2's benchmark calls three. Not yet a "faster than
client-v2" result; that needs a production-shaped prototype with real value access.

Next: still not yet started — a compact `Object[]`-per-row prototype (replacing
`Flux<Map<String, Object>>`) with once-per-result shared column metadata, benchmarked with equivalent
per-value access against client-v2's getters, before committing to the full `RowBinaryDecoder`
redesign. Wide multi-type decode, aggregation, INSERT, and the reactive-vs-blocking concurrency burst
scenario stay queued behind this — all designed in ROADMAP.md, not yet built.

**Fairness fixes applied after the first run** (a real run surfaced real gaps — not designed away
in the abstract): the ClickHouse image is now version-pinned rather than `latest`; client-v2 now
uses the same `{id:UInt64}`/`query_params` parameterization mechanism as this driver rather than an
inlined SQL literal; benchmark ids are pre-generated once per trial with a fixed seed
(`PointQueryTable#deterministicIds`) instead of called via `Math.random()` inside the hot path.
One known, deliberately-not-forced-to-match asymmetry remains: this driver materializes each row
into a `Map<String, Object>`, client-v2 reads typed values directly — see `PointQueryBenchmark`'s
own Javadoc for why, and what would isolate it (a future transport-only benchmark with no decode at
all).

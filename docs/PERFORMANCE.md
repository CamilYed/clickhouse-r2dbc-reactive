# Performance & Benchmarking

Full record of Phase 5 (load/performance testing) — design, every benchmark run, every finding,
the H0/H1/H2 optimization investigation, and the ongoing compact-row redesign. Moved out of
[ROADMAP.md](../ROADMAP.md) on 2026-08-14 because that file had grown too large to navigate; this
content is otherwise unchanged (same headings, same anchors) — only the file it lives in changed.
See [ROADMAP.md](../ROADMAP.md) for everything else (Phases 0–4, 6, production readiness).

Companion doc: [../clickhouse-r2dbc-reactive-benchmarks/README.md](../clickhouse-r2dbc-reactive-benchmarks/README.md)
for how to actually run the benchmarks.

## Are we faster than client-v2? — read this first

One table, every benchmark class, latest numbers. **Green = this driver wins, red = client-v2 wins.**
Every row is single-fork unless noted — see the "Confidence" column before treating any of this as
final; multi-fork reconfirmation is the single biggest open item in this whole file.

| Benchmark | What it measures | Latest verdict | Confidence |
| --- | --- | --- | --- |
| `TrivialQueryBenchmark` (`SELECT 1`) | Protocol/connection floor | 🟢 this driver ~7% faster (mean), up to ~32% faster at p99.9 — 🔴 slower at p99.99 only | Single fork, one run (2026-08-13) |
| `PointQueryBenchmark` (parameterized 1-row lookup) | Protocol + real row lookup | 🟢 this driver ~6% faster (mean), up to ~26% faster at p99.9 | Single fork, re-run twice, consistent (2026-08-13) |
| `StreamingScanBenchmark` @ 10k rows | Full scan, small | 🟢 this driver ~21% faster (mean) | Single fork, **needs multi-fork confirmation** (2026-08-14) |
| `StreamingScanBenchmark` @ 100k rows | Full scan, medium | 🟢 this driver ~13% faster (mean) | Single fork, **needs multi-fork confirmation** (2026-08-14) |
| `StreamingScanBenchmark` @ 1M rows | Full scan, large | 🟢 this driver ~1.8% faster (mean), tighter tail (max 178ms vs 254ms) | Single fork, **needs multi-fork confirmation** (2026-08-14) |
| `DecoderOnlyBenchmark` (decode only, no network) | Raw decode cost, `Map`-based benchmark harness (pre-`DecodedRow`) | 🔴 ~13–16% slower, ~48–56 B/row more allocated, once compared fairly (equivalent getter calls both sides) | 3-fork confirmed (2026-08-13) — **describes the old benchmark harness, not yet re-measured against production `nextRowValues()`** |
| `ConcurrencyBenchmark` `@Threads(8)`, mean → p99 | 8 concurrent threads, blocking, both sides' *default* (unmatched) pools | 🔴 ~4% slower (mean), degrading to ~15% slower at p99 | Single fork, one run (2026-08-14) — **see below: likely an unmatched-pool artifact, not architectural** |
| `ConcurrencyBenchmark` `@Threads(8)`, p99.9 → max | Same run, extreme tail | 🟢 ~7% faster at p99.9, up to ~46% faster at max | Single fork, one run (2026-08-14) |
| `BoundedPoolConcurrencyBenchmark`, pool=8, concurrency=8/32/128 | Non-blocking `flatMap`/async `CompletableFuture`, **matched** 8-connection pool both sides — the actual motivating scenario | 🟢 this driver wins on **every** percentile at **every** concurrency level: ~4–6% faster mean, up to ~26% faster at p99.9/max | Single fork, one run (2026-08-14) — **needs multi-fork confirmation, but the cleanest, most consistent result in this whole file** |

**Bottom line today: once pool size is actually matched between the two drivers, this driver wins
across the board — including under concurrency, which the earlier `ConcurrencyBenchmark` result had
left genuinely unclear.** At every single-threaded level re-measured after the `DecodedRow` redesign
(`StreamingScanBenchmark`, all three tiers; `TrivialQueryBenchmark`/`PointQueryBenchmark`), this
driver wins. `ConcurrencyBenchmark`'s `@Threads(8)` mean/p99 regression turned out to most likely be
an artifact of **unmatched connection pool configuration** between the two drivers (neither side had
its pool size set explicitly) rather than a real architectural cost — `BoundedPoolConcurrencyBenchmark`,
which explicitly matches both sides to the same 8-connection pool and drives logical concurrency
non-blocking/async on both sides, shows this driver winning on every single percentile at every
concurrency level tested (8/32/128), including the mean and p99 that looked like a regression before.
See the dedicated section near the end for the numbers and the "why" read. The `DecoderOnlyBenchmark`
red row still measures a *different, older* code path (pre-`DecodedRow`) and isn't a current
regression. What's left before any of this is publishable: (1) every "needs multi-fork confirmation"
row above — still the single biggest open item; (2) `BoundedPoolConcurrencyBenchmark`'s small first
pass (one pool size, three concurrency levels) could still be widened once multi-fork confirms it
holds.

---

> [!TIP]
> **Status at a glance (2026-08-14).** This file is a full investigation log, long by design — read
> top to bottom for the story, or use this box to jump straight to where things actually stand.
>
> | Done | Open |
> | --- | --- |
> | H0 (`byte[1]` alloc) — fixed, confirmed | Multi-fork (`-Pjmh.forks=3`) reconfirmation of the newest `StreamingScanBenchmark` numbers |
> | H1 (`LinkedHashMap` per row) — fixed via the `DecodedRow` redesign | Multi-fork confirmation of `BoundedPoolConcurrencyBenchmark`'s clean win — still single-fork |
> | `DecodedRow` redesign — **build-verified** (2026-08-14): compiles, `StreamingScanBenchmark` green, `ourDriver` now beats `clientV2` at all three tiers | Multi-fork (`-Pjmh.forks=3`) reconfirmation of `StreamingScanBenchmark`/`ConcurrencyBenchmark` |
> | `ClickHouseHttpTransport(baseUrl, Authentication, maxConnections)` — added and **test-verified green** (2026-08-14) | Full `./gradlew spotlessCheck clean build` on the whole session's work (only compilation + individual benchmarks/one test confirmed so far) |
> | `BoundedPoolConcurrencyBenchmark` — **build-verified and run** (2026-08-14): this driver wins on every percentile at every concurrency level (8/32/128) against a matched 8-connection pool — the cleanest result in this whole file | Widen the matrix (more pool sizes/concurrency levels) once multi-fork confirms this first pass |
> | | Wide multi-type decode / aggregation / INSERT benchmarks — designed, not built |
> | | Performance charts for the main `README.md` — deliberately deferred to the very end of this phase |
>
> See ["`ConcurrencyBenchmark`'s `@Threads(8)` shape, run for real"](#concurrencybenchmarks-threads8-shape-run-for-real-2026-08-14)
> near the end for the newest numbers, or jump to the very last section for the current
> guardrail/priority list.

---

## Phase 5 (later) — Load and performance testing

**Design finalized 2026-08-13; implementation starting.** Phase 4 is signed off, `integration-tests`
is deleted (see the module map), so this is now unblocked and starting. Explicit request driving this design: measure **this driver vs client-v2** (the same
library `core` already reuses for row decoding, and the one anyone evaluating this driver will
naturally compare it against), at **multiple levels**, professionally enough to trust the numbers —
not a couple of ad hoc `SELECT 1` timings.

**Prior art read before designing this**, not designed from scratch: `ClickHouse/clickhouse-java`'s
own `performance` module (mounted at `/Users/kamil/Projects/clickhouse-java/performance`) is a real,
maintained JMH suite comparing their own client V1 vs V2 vs JDBC, with a scheduled (not per-PR) GitHub
Actions job. Reused directly: JMH itself (not Gatling — see "Tooling" below), `GCProfiler` for
allocation, `SampleTime` mode for percentiles, `@Threads(N)` for one shape of concurrency, and the
"don't run this on every PR" cadence. Deliberately *not* reused: their CSV-file dataset (generated
client-side, checked into a file, then re-uploaded) — ClickHouse can generate large synthetic
datasets server-side in seconds via `INSERT ... SELECT ... FROM numbers(N)`/`generateRandom(...)`,
which is both faster and more "pro" than a client-side CSV round-trip.

---

### Module

New module `clickhouse-r2dbc-reactive-benchmarks` (not published — measurement tooling, not a
library; added to `nonPublishedModules` already). Depends on `core`, `transport-http`, `connector`,
and `testkit` (for `BaseClickHouseIntegrationTest`-style container lifecycle reuse — a benchmark run
still needs a real ClickHouse instance and shouldn't reinvent that), plus `com.clickhouse:client-v2`
(already in the version catalog, pinned `0.9.0` — same version `core` reuses for decoding, so the
comparison is against the exact client-v2 build this project already depends on, not some other
version) as the comparison baseline. Depending on `core`/`transport-http` directly (not just
`connector`'s public SPI) is intentional and doesn't violate the Architecture rule ("a module
should not reach into another module's internals") — `ClickHouseHttpTransport` and
`RowBinaryDecoder` are already `public final class`, part of each module's own public API, not
internals; benchmarking below the R2DBC SPI layer is exactly how "which level costs what" gets
answered.

**Gradle/JMH tooling**: the `me.champeau.jmh` Gradle plugin (the standard, actively-maintained
community JMH plugin — gives a `jmh` task, JSON/text result output, and proper annotation-processor
wiring without hand-rolling what clickhouse-java's Maven `exec-maven-plugin` setup does manually).
**Not Gatling** — Gatling is built for HTTP/network load-testing DSLs (simulate many external
clients hitting an API); what's being measured here is a Java library's in-process call cost and
concurrency behavior, which is exactly JMH's problem, including its own `@Threads(N)` and
custom-harness-inside-`@Benchmark` support for the concurrency scenario below. Nothing added to the
main build's `check`/`build` tasks — a benchmark run is a separate, explicit `./gradlew
:clickhouse-r2dbc-reactive-benchmarks:jmh` invocation, per CLAUDE.md's "don't gate the main loop on
a multi-minute run."

---

### Comparison levels ("różne poziomy")

Three levels, deliberately kept as separate benchmark classes rather than one giant parameterized
suite, so a regression at one level (e.g. "our decode got slower") isn't masked or conflated with
another (e.g. "our connection pooling got slower"):

1. **Raw transport + decode** — `ClickHouseHttpTransport.query(...)`/`RowBinaryDecoder.decode(...)`
   directly vs client-v2's `Client.query(...)` + `ClickHouseBinaryFormatReader`. Answers: how much
   does *this driver's own* HTTP-adapter-plus-decode pipeline cost relative to client-v2's, with the
   R2DBC SPI layer removed from the comparison entirely.
2. **Public R2DBC SPI** — `ClickHouseConnection`/`ClickHouseStatement`/`ClickHouseResult` (what an
   actual driver consumer calls) vs client-v2's `Client` API directly. Answers: what does a real user
   of this driver actually pay, R2DBC-shape translation included.
3. **Concurrency/burst, reactive vs blocking** — the scenario that originally motivated this whole
   project (see Phase 1: "~11 concurrent queries per user action"). Not a third *code* level so much
   as a third *axis*: this driver's non-blocking pipeline multiplexing many logical queries over a
   small connection pool vs client-v2's blocking `Client`, one thread per in-flight query. This is
   where a fair comparison needs the most care — see below.

A fourth level (through Spring's `DatabaseClient`, or client-v2's JDBC wrapper) is explicitly **out
of scope for the first pass** — it would measure Spring/JDBC overhead on top of both drivers, not
either driver itself, and can be added later as its own separate concern if real usage data ever
asks for it.

---

### Dataset

Server-side generated, not client-side-generated-then-uploaded: `INSERT INTO t SELECT ... FROM
numbers(N)` (cheap columns) and `generateRandom('col Type, ...', seed)` (for types `numbers()`
can't produce directly — `String`, `UUID`, `Array`, etc.) via a `CREATE TABLE ... ENGINE =
MergeTree` + `INSERT INTO ... SELECT * FROM generateRandom(...) LIMIT N`, run once per benchmark
`@Setup(Level.Trial)`, timed and logged so a slow *setup* never gets misread as a slow *query*.
Table shape reuses `RealWorldTableAgainstRealClickHouseTest`'s wide, multi-type-column shape for the
decode-heavy benchmarks (not a narrow two-column toy table — real decode cost is dominated by column
*variety*, not just row count) plus a narrow two/three-column table for the point-query/burst
benchmarks (isolates connection/protocol overhead from decode cost).

Row-count tiers, run via JMH's `@Param` (same mechanism clickhouse-java uses for `limit`), so a
local dev loop and a "real" run are the same code, different parameters:

| Tier | Rows | Purpose |
| --- | --- | --- |
| `smoke` | 10,000 | Fast local iteration while writing/debugging a benchmark itself — not a real number. |
| `default` | 1,000,000 | The number actually reported/tracked for regressions — large enough that per-row costs dominate one-time connection setup, small enough to run in CI's time budget. |
| `large` | 50,000,000+ | Manual/opt-in only (`-Prows=large` or similar), for a genuine "does this scale" check before a release — not run routinely, disk/time cost is real. |

---

### Query mix

Not one query shape — this driver's whole value proposition (non-blocking, streaming, backpressure)
shows up differently depending on the shape:

- **Point/trivial** — `SELECT 1`, and a parameterized single-row lookup (`SELECT ... WHERE id =
  {id:UInt32}` for us, `client.query(...)` with the equivalent for client-v2). Measures the
  protocol/connection-overhead floor, where decode cost is negligible and framing/header/network
  cost dominates.
- **Full-table streaming scan** — `SELECT * FROM t` over the `default`/`large` tier table. Measures
  sustained throughput, time-to-first-row, and allocation under long streaming — the scenario
  `GCProfiler` matters most for.
- **Wide multi-type decode** — same query shape as above but against the wide `RealWorldTable`-style
  table, to isolate "cost per row of varied-type decoding" from "cost per row of narrow decoding."
- **Analytical aggregation** — `SELECT category, count(), avg(amount), quantile(0.95)(amount) FROM t
  GROUP BY category ORDER BY count() DESC` (ClickHouse's actual core use case — ClickHouse does the
  heavy lifting server-side; this mostly measures small-result-set decode + round-trip overhead, but
  it's the shape a real analytics query actually has, not a synthetic stand-in).
- **INSERT** — both this driver's literal-embedded small `INSERT` (`ClickHouseStatement`) and its
  streaming `insertStreaming` vendor extension, vs client-v2's `client.insert(...)`.

---

### Concurrency/burst — the scenario that needs the most care

JMH's `@Threads(N)` (what clickhouse-java uses) models "N platform threads, each independently
calling a blocking client" — a fair, standard way to benchmark a *blocking* client under load. It
does not model what this driver is actually for: **one small connection pool, many logical
concurrent queries multiplexed non-blockingly over it.** Modeling that inside one JMH thread with a
custom harness (`Flux.range(0, N).flatMap(i -> runQuery(), concurrency).blockLast()`, `SampleTime` or
`SingleShotTime` mode, `concurrency` = the connection pool's `maxConnections`) is what proves the
actual claim. Both shapes get benchmarked side by side, deliberately not collapsed into one number,
because they answer different questions:

- **`@Threads(N)` shape** — client-v2 with N platform threads and N connections vs this driver with N
  platform threads each blocking on `.block()` and a pool of N connections. A same-resources
  comparison; expected to be roughly comparable, since neither side's concurrency model gets to use
  its advantage here.
- **Custom reactive-harness shape** — this driver with a *small* pool (e.g. 4–8 connections)
  serving N=~50–100 concurrent logical queries via `flatMap` concurrency, wall-clock time to
  complete all N, vs client-v2 needing N platform threads (or an external executor) to attempt the
  same concurrency with N connections. This is the actual "~11 concurrent queries per user action"
  motivating scenario from Phase 1, scaled up — the one where fewer OS threads/connections doing the
  same logical work is the entire point, and where raw single-query latency numbers alone would
  mislead.

Per-request latency *within* one burst is recorded via `HdrHistogram` inside the benchmark method
(JMH's own percentile machinery works at "one `@Benchmark` invocation = one op" granularity, which
is too coarse for "N sub-operations happened inside one burst") and reported alongside JMH's own
result JSON, not folded into it.

---

### What's measured, and how

- **Throughput / latency (p50/p95/p99/p99.9)** — JMH `SampleTime` mode for single-query benchmarks
  (built-in percentile reporting, same as clickhouse-java's own choice); `HdrHistogram` for
  intra-burst percentiles in the concurrency scenario (see above).
- **Time to first row** — not something JMH measures natively: record `System.nanoTime()` at
  subscribe and again at the first `onNext`, collect into a histogram, report as a custom metric
  alongside the JMH result file.
- **Allocation / retained memory under streaming** — JMH's built-in `GCProfiler`
  (`-prof gc`), exactly as clickhouse-java uses it — `gc.alloc.rate`/`gc.alloc.rate.norm` need no
  custom instrumentation.
- **Connection pool behavior under the burst scenario** — log `r2dbc-pool`'s
  `ConnectionPoolMetrics` (acquired/idle/pending count) at intervals during the benchmark; not a
  pass/fail number, a diagnostic trend to eyeball when interpreting a burst result.
- **Off-heap/Netty buffer usage** — `PooledByteBufAllocator.DEFAULT.metric()`'s `toString()` logged
  periodically during a long streaming benchmark; same status — diagnostic, not an automated gate,
  useful for spotting a leak trend during a `large`-tier run.
- **CPU usage** — deliberately *not* built as an automated numeric pipeline for the first pass
  (real ROI is low relative to the effort of doing it reliably in a shared CI runner). Recommended
  as a manual, qualitative companion tool instead: run a `large`-tier benchmark under
  `async-profiler` locally for a flame graph when investigating a specific regression, rather than
  gating every run on a CPU-percent number.

---

### Reporting

JMH's own JSON result format (`-rf json`, or the `me.champeau.jmh` plugin's equivalent config),
matching clickhouse-java's own convention. Raw JSON result files are **not committed to git** (they're
noisy binary-ish artifacts, one per run) — treated as CI/local run output, uploaded as a workflow
artifact if/when a scheduled CI job exists (see below), otherwise just inspected locally. A small
JSON→Markdown summary script is a reasonable follow-up once the benchmark classes themselves exist
and there's real output to summarize — not built speculatively ahead of having any numbers.

---

### When this runs

Not on every PR/push — a multi-minute-to-hour JMH run has no place in the fast feedback loop this
project's CI otherwise protects. Matching clickhouse-java's own practice: a `workflow_dispatch`
(manual trigger, e.g. before a release) plus an optional scheduled (nightly/weekly) job once the
suite is stable enough to trust unattended — decided once the classes exist and a first baseline run
has actually happened, not designed in the abstract now.

---

### TDD note

This module is measurement tooling, not driver behavior — CLAUDE.md's red-green-refactor workflow
is scoped to production code with behavior to protect, and explicitly carves out "skeleton/plumbing
code with no behavior yet" as the exception. A JMH `@Benchmark` method has no assertion to be red
about; its "correctness" is that it measures the right thing, which is a design/review question
(does this benchmark actually isolate what it claims to), not a TDD one. `BaseClickHouseIntegrationTest`-style
setup code and any real assertions this module *does* need (e.g. "the burst benchmark actually ran N
queries, not N-1") still follow this project's normal testing rules.

---

### First slice, run for real (2026-08-13)

`clickhouse-r2dbc-reactive-benchmarks` scaffolded (`me.champeau.jmh`, `BenchmarkEnvironment`,
`PointQueryTable`) with one benchmark class, `PointQueryBenchmark` (Level 1 — raw transport, this
driver vs client-v2, point lookup against the `smoke`-tier 10,000-row table). Run against real
Docker/ClickHouse (`./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh`), not just compiled:

- **Numbers (1 fork, 1×10s warmup, 3×10s measurement — a sanity check, not yet a trustworthy
  baseline; JMH's own output explicitly warns against over-reading a single short run like this):**
  this driver mean 1115.8 ± 3.8 µs/op vs client-v2 mean 1235.6 ± 6.6 µs/op; p99 1589 µs vs 2165 µs.
  This driver read faster on this run through p99.9; both had noisy, low-sample tail percentiles
  (p99.99/p100) not worth reading into yet.
- **Real finding, not assumed: JMH forks a fresh JVM per `@Benchmark` method by default.**
  `BenchmarkEnvironment`'s `static` container field is therefore shared only within one fork, not
  across the whole `jmh` run — the log showed two separate `clickhouse/clickhouse-server` containers
  starting on two different ports (one for `clientV2`, one for `ourDriver`), each paying the ~4s
  Testcontainers startup independently. Documented in `BenchmarkEnvironment`'s own Javadoc (the
  original version claimed "one container for the whole run," which this run proved wrong). Accepted
  as a real, known cost for now rather than forcing everything into one fork (which would trade away
  JMH's normal isolation between benchmark methods) — revisit if the `large` tier's repeated
  container-plus-reseed cost per fork turns out to dominate a real run's wall time.

Remaining query shapes (full table scan, wide multi-type decode, aggregation, INSERT) and the
concurrency/burst scenario are designed above but not yet built — next.

---

### Fairness/reproducibility review of `PointQueryBenchmark` (2026-08-13)

An external review of the first run (a written benchmark-strategy document, checked against this
project's own code rather than taken at face value) confirmed the overall Level 1/2/3 design above
independently arrived at the same shape, and caught four concrete, real problems in
`PointQueryBenchmark` specifically — all fixed:

- **Unpinned ClickHouse image (`:latest`).** A performance baseline that silently tracks whatever
  ClickHouse changes underneath it isn't reproducible run-to-run. Pinned to
  `clickhouse/clickhouse-server:26.7.3.19` in `BenchmarkEnvironment` — confirmed via Docker Hub that
  `latest` currently resolves to exactly this image (same digest), so this changes nothing about
  today's numbers, only future reproducibility.
- **Asymmetric parameterization.** This driver used `{id:UInt64}` + ClickHouse's `param_<name>`
  mechanism; client-v2 received a plain inlined literal. Fixed by reading client-v2's own
  `Client#query(String, Map, QuerySettings)` Javadoc directly (in the mounted client-v2 source) and
  confirming it documents the identical `{name:Type}`/`query_params` contract — both benchmark
  methods now build the exact same SQL text and parameter map.
- **`Math.random()` inside the `@Benchmark` hot path.** Not necessarily a dominant cost next to a
  real network round trip, but non-reproducible across runs and a potential source of different
  access patterns between the two methods. Fixed: `PointQueryTable.deterministicIds(rowCount,
  poolSize, seed)` pre-generates a fixed pool once in `@Setup(Level.Trial)` via `SplittableRandom`;
  the benchmark methods only advance an `AtomicLong` cursor through it.
- **Environment/version metadata not recorded.** `BenchmarkEnvironment.start()` now logs the
  ClickHouse server version (queried from the running container, not assumed from the image tag),
  JDK version, and OS/arch alongside every run.

**Physical test hardware (2026-08-13), recorded here since `BenchmarkEnvironment`'s own logging
covers JDK/OS/arch/ClickHouse version but not the host machine itself:** all `DecoderOnlyBenchmark`,
`TransportOnlyStreamingBenchmark`, and `StreamingScanBenchmark` runs in this Phase 5 section were
executed on a MacBook Pro 14-inch (Nov 2023), Apple M3 Pro chip, 36 GB unified memory, macOS Tahoe
26.5.2 — a single consumer laptop, not an isolated/pinned benchmarking rig (no CPU governor pinning,
no isolated cores, background OS/user processes free to run). This matters for two things already
seen in this investigation: (1) the single-fork run's non-reproducible numbers (see the "Methodology
correction" note below) are exactly the kind of noise a shared, non-isolated machine produces, which
is why multiple forks are now required before trusting a result; (2) absolute numbers here (e.g.
"224 ms at 1M rows") are specific to this machine and should be read as *relative* comparisons
between this driver and client-v2 on identical hardware/JVM/data, not as portable absolute
performance claims for other hardware.

**One asymmetry deliberately left as-is, not force-fit into false equivalence:** this driver
materializes each row into a `Map<String, Object>` (see `RowBinaryDecoder`'s own Javadoc for the
lifetime-safety reason), while client-v2 reads typed values directly off its reader with no
intermediate map. Documented directly in `PointQueryBenchmark`'s Javadoc rather than silently
accepted — isolating it needs a *separate* transport-only benchmark (raw bytes, checksum, no decode
at all) sitting alongside the transport+decode one, not a rewrite of this one. Named as a concrete
next benchmark, not yet built.

**Re-run after the fixes, confirmed green (2026-08-13).** Same 1 fork/1×10s warmup/3×10s
measurement shape as before — still a sanity check, not a trustworthy baseline (JMH's own output
repeats that warning every run). New numbers, now on equal parameterization and a pinned server:

All latency rows are µs/op (lower = faster). **Reading this table: "this driver is X% FASTER"
means this driver's latency number is X% lower/better than client-v2's — the opposite wording
("SLOWER") means this driver's number is worse. Never read "higher"/"lower" alone as good or bad —
they only describe the raw number, not which direction is better.**

| | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| mean | 1142.8 µs | 1075.1 µs | **this driver 5.9% FASTER** |
| p50 | 1122.3 µs | 1058.8 µs | **this driver 5.7% FASTER** |
| p90 | 1226.8 µs | 1155.1 µs | **this driver 5.8% FASTER** |
| p95 | 1280.0 µs | 1189.9 µs | **this driver 7.0% FASTER** |
| p99 | 1577.0 µs | 1273.9 µs | **this driver 19.2% FASTER** |
| p99.9 | 2564.0 µs | 1904.9 µs | **this driver 25.7% FASTER** |
| sample count (30s) | 26,227 (≈874 ops/s) | 27,884 (≈930 ops/s) | **this driver 6.3% higher throughput** |

Notable shift from the pre-fix run: the gap **widens at the tail** (p99/p99.9) rather than staying
flat — this driver's tail latency held closer to its own median than client-v2's did, even in this
single-connection, zero-concurrency scenario. Consistent with, but not yet proof of, the stability-
under-load claim the architecture is actually designed for — that needs the concurrency/burst
benchmark (not built yet) to test directly, not inferred from a serial point-query benchmark.
Treat this table the same way as before: one short single-fork run, not a published claim.

---

### `TrivialQueryBenchmark`, run for real (2026-08-13)

`SELECT 1`, no table — isolates protocol/connection overhead from the storage-engine lookup
`PointQueryBenchmark` also pays for. Same fork/warmup/measurement shape as the runs above; run
alongside a repeat of `PointQueryBenchmark` in the same invocation (confirmed still green, same
5–7% mean/median edge as the fairness-fixed run above — not re-tabulated here since nothing about
it changed).

All latency rows are µs/op (lower = faster; verdict column spells out which side wins, see the
reading note on the `PointQueryBenchmark` table above).

| | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| mean | 591.1 µs | 548.5 µs | **this driver ≈7.2% FASTER** |
| p50 | — | — | **this driver ≈6.3% FASTER** |
| p90 | — | — | **this driver ≈10.6% FASTER** |
| p95 | — | — | **this driver ≈10.1% FASTER** |
| p99 | — | — | **this driver ≈7.2% FASTER** |
| p99.9 | — | — | **this driver ≈31.6% FASTER** |
| p99.99 | 2586.8 µs | 4650.7 µs | **this driver SLOWER here** (only percentile where it loses) |
| p100 (max) | 15106.0 µs | 8847.4 µs | **this driver 41.4% FASTER** |
| sample count (30s) | 50,699 (≈1690 ops/s) | 54,652 (≈1822 ops/s) | **this driver ≈7.8% higher throughput** |

Same shape as `PointQueryBenchmark`: this driver reads faster from the mean through p99.9, and the
gap widens sharply at p99.9 (31.6%, the widest yet). Two honest caveats, not smoothed over:

- **p50/p90/p95/p99/p99.9 rows are percentage deltas only** — the absolute µs figures from this run
  weren't retained, only the computed percentages against client-v2. Re-run and capture raw JSON
  output (`build/results/jmh/`) before citing this table anywhere more permanent than this doc.
- **p99.99 flips against this driver, in both benchmarks run so far.** `PointQueryBenchmark`'s
  re-run above already showed this driver worse at p99.99/p100; here it's worse at p99.99 but
  better again at p100/max. With only ~1 sample in 10,000–50,000 landing in that bucket, this is
  far more likely sampling noise than a real regression — but it's now shown up twice, not once, so
  it's a "watch, don't ignore" finding rather than a one-off. Worth a real look once
  `StreamingScanBenchmark`/`ConcurrencyBenchmark` exist and a profiler (JMH's own `-prof gc`/async-
  profiler) can attribute it to GC, connection-pool churn, or something else, rather than guessing
  from percentile tables alone.

One environment detail surfaced in this run's logs, not yet acted on: this driver's Reactor Netty
stack logs `Unable to load io.netty.resolver.dns.macos.MacOSDnsServerAddressStreamProvider` on
macOS (missing `netty-resolver-dns-native-macos`, a native optional dependency). Deliberately not
added yet — this benchmark resolves `localhost` only, which doesn't hit the DNS resolver Netty is
warning about, so there's no concrete reason yet to believe this warning explains the p99.99
finding above. Worth revisiting with actual profiling evidence before spending a dependency-version
matching exercise on a hunch.

---

### `StreamingScanBenchmark`, run for real (2026-08-13) — first result where this driver is slower

Full scan (`SELECT id, label, amount FROM benchmark_point_query`, `smoke` tier, 10,000 rows), no
`WHERE`. Same run also re-executed `PointQueryBenchmark`/`TrivialQueryBenchmark` (a third
confirmation of both — same shape as the tables above, exact numbers below), from the JSON result
file (`-rf json`), not a console paste, so every figure here is exact:

**Superseded by the confirmed, corrected 3-tier result further below — kept only as the historical
record that first surfaced this as worth investigating.** Latency rows are µs/op (lower = faster):

| | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| mean | 3946.5 µs | 4460.8 µs | **this driver 13.0% SLOWER** |
| p50 | 3846.1 µs | 4366.3 µs | **this driver 13.5% SLOWER** |
| p90 | 4816.9 µs | 5210.1 µs | **this driver 8.2% SLOWER** |
| p95 | 5103.6 µs | 5390.3 µs | **this driver 5.6% SLOWER** |
| p99 | 5513.2 µs | 5857.3 µs | **this driver 6.2% SLOWER** |
| p99.9 | 6363.9 µs | 8266.1 µs | **this driver 29.9% SLOWER** |
| p99.99 / p100 (max) | 21266.4 µs | 26247.2 µs | **this driver 23.4% SLOWER** |
| sample count (30s) | 7,597 (≈253 ops/s) | 6,723 (≈224 ops/s) | **this driver 11.5% lower throughput** |

**This driver is slower here, at every percentile, not just the tail — the first benchmark in this
suite where that's true.** Not glossed over: `PointQueryBenchmark`/`TrivialQueryBenchmark` both
decode exactly *one* row per operation; `StreamingScanBenchmark` decodes 10,000. Leading hypothesis,
not yet confirmed: the per-row `LinkedHashMap` allocation `RowBinaryDecoder.emitNextRow` does for
every row (documented as a known, deliberate asymmetry in `PointQueryBenchmark`'s own Javadoc,
tolerable at n=1 row) stops being negligible at n=10,000 rows per operation — 10,000 map allocations
(plus their internal `Node` entries) is a plausible dominant cost that a single-row benchmark simply
can't surface. `client-v2`'s `ClickHouseBinaryFormatReader` reads typed values off its reader
directly, no intermediate collection per row.

**Not yet confirmed, and deliberately not acted on until it is** — per this project's own testing
discipline (assert on behavior/measurements, not assumptions): the next concrete step is re-running
`StreamingScanBenchmark` with JMH's `GCProfiler` (`-Pjmh.profilers=gc` / `-prof gc` — already named
in this section's own "What's measured, and how" design for exactly this scenario) to check
`gc.alloc.rate.norm` for `ourDriver` vs `clientV2` directly, rather than inferring allocation cost
from latency numbers alone. If allocation rate confirms the hypothesis, a follow-up benchmark
variant that reuses/pools row objects (or a decode path that never materializes a
`Map<String,Object>` at all) becomes a concrete, evidence-backed thing to try — not before.

Time-to-first-row (the `HdrHistogram`-based custom metric `StreamingScanBenchmark` also records)
isn't in this run's JSON — that's logged to the console via SLF4J at trial teardown, not into JMH's
own result file. Not captured this run; worth asking for the console log (or re-running with
`tee`) next time TTFR specifically matters.

---

### Correction: the "13% slower" result above has a real methodology bug (2026-08-13)

Caught by review, not by re-running: `ourDriver`'s time-to-first-row instrumentation called
`AtomicLong#compareAndSet` unconditionally on **every** row, not just the first —

```java
.doOnNext(row -> {
    firstRowNanos.compareAndSet(-1, System.nanoTime());   // CAS + System.nanoTime() every row
    blackhole.consume(row);
})
```

— while `clientV2`'s equivalent check was already the cheap plain branch:

```java
if (firstRowNanos == -1) {
    firstRowNanos = System.nanoTime();   // only on the first row
}
```

At 10,000 rows that's 10,000 unconditional `System.nanoTime()` calls plus 10,000 CAS operations on
`ourDriver`'s side, against effectively one of each on `clientV2`'s — instrumentation overhead
baked directly into the very numbers the table above reports, not something outside the measured
window. **The table above should not be read as a real performance comparison until re-run** — it
may be measuring this driver's TTFR instrumentation cost as much as its actual decode/transport
cost. Left in place (not deleted) as the historical record of the bug, not as a trustworthy result.

Fixed in `StreamingScanBenchmark.ourDriver`: the first-row check is now a plain array-backed
flag (`long[] firstRowNanos = {-1L}`, checked with a plain `==` and set with a plain assignment),
matching `clientV2`'s shape exactly — no `AtomicLong`, no CAS. Safe because `RowBinaryDecoder
.decodeRows` applies no `publishOn`/`subscribeOn`, so `Flux.generate` emits every row synchronously
on the calling thread; there was never a real cross-thread visibility need the CAS was buying.

**Two more fixes made at the same time, per the same review, before any re-run:**

- **Dataset now scales.** `@Param({"10000", "100000", "1000000"})` instead of a single 10,000-row
  tier. A single small tier can't distinguish fixed per-request overhead (HTTP round trip, query
  startup, first-chunk latency) from genuine per-row streaming cost — if a gap is flat across tiers
  it's the former; if it grows with `rows` it's the latter. `10_000_000`+ ("large" tier) stays a
  manual, opt-in edit for a release-gate run, not routine iteration.
- **Rows/sec, derived, not a new metric.** No new JMH instrumentation added (avoiding stacking
  another "did we get the benchmark code itself right" risk on top of the one just found) — computed
  from a completed run's own numbers: `rows / (mean_us / 1_000_000)`. Recorded per tier the next
  time this table is filled in for real.

**Deliberately not done, per the same review's own sequencing:** no change to
`RowBinaryDecoder`/production code. The per-row `Map<String, Object>` allocation hypothesis from the
section above is still just a hypothesis — confirming or ruling it out needs the fixed benchmark's
own numbers across 10k/100k/1M first (does the gap grow with `rows`, or stay flat?), and `-prof gc`
after that if it does grow. Acting on a number produced by a buggy benchmark would have meant
"fixing" a problem that may not exist.

---

### `StreamingScanBenchmark`, re-run with the TTFR fix and three tiers (2026-08-13) — confirmed real

**This is the trustworthy result — read this table, not the two above.** Fixed TTFR instrumentation
(no more CAS-per-row), three row-count tiers, from JSON (`-rf json`), exact figures. Latency rows
are µs/op — **lower is faster; the verdict column always names which driver wins:**

| rows | client-v2 mean | this driver mean | verdict | client-v2 rows/s | this driver rows/s |
| --- | --- | --- | --- | --- | --- |
| 10,000 | 4,038.6 µs | 4,575.3 µs | **this driver 13.3% SLOWER** | 2.48 M | 2.19 M |
| 100,000 | 21,948.3 µs | 33,961.3 µs | **this driver 54.7% SLOWER** | 4.56 M | 2.94 M |
| 1,000,000 | 153,972.2 µs | 276,924.2 µs | **this driver 79.9% SLOWER** | 6.49 M | 3.61 M |

p50/p99 confirm the same trend, not just the mean:

| rows | p50 verdict | p99 verdict |
| --- | --- | --- |
| 10,000 | this driver 13.0% SLOWER | this driver 13.1% SLOWER |
| 100,000 | this driver 55.8% SLOWER | this driver 53.4% SLOWER |
| 1,000,000 | this driver 80.0% SLOWER | this driver 68.0% SLOWER |

**Answer to this section's own question ("does the gap grow with rows, or stay flat?"): it grows,
sharply and monotonically — 13% → 55% → 80%.** This rules out fixed per-request overhead (HTTP
round trip, query startup, first-chunk latency) as the explanation — a fixed cost would shrink as a
*percentage* of a longer-running operation, not grow six-fold. **This driver's per-row decode path
is now a confirmed, real, worth-fixing regression, not a benchmark artifact.** At the same time:
`TrivialQueryBenchmark`/`PointQueryBenchmark` (one row decoded) still favor this driver by 5–11%,
unaffected by any of this — the fixed-request path is genuinely good; the *sustained streaming*
path is genuinely not, yet. Both things are true at once, and the README/any external claim must
say both, not average them into one number.

---

### Optimization phase: what's actually in the code, and the investigation plan

An external review (uploaded findings/optimization-plan document, cross-checked against this
repository's real source below, not taken at face value) independently converged on the same
"per-row decode/materialization" diagnosis and proposed a ranked hypothesis list and a
profile-before-you-touch-anything methodology. Verified against the actual code:

- **H1: per-row `LinkedHashMap` copy — confirmed to exist, and sharper than the external review's
  own framing once client-v2's actual internals are read (the mounted `clickhouse-java` source, not
  assumed).** `RowBinaryDecoder.emitNextRow` does `sink.next(new LinkedHashMap<>(reader.next()))`
  for every row. Traced what `reader.next()` itself costs on client-v2's side
  (`AbstractBinaryFormatReader`): it does **not** allocate a fresh `Map` or array per row either —
  it keeps two reused `Object[]` buffers (`currentRecord`/`nextRecord`) that are swapped, not
  reallocated, each call, and wraps whichever one is current in a lightweight `RecordWrapper` (a
  `Map` facade over the array, name→index lookup, no hash table built). So client-v2's own per-row
  cost is one small wrapper object plus the unavoidable boxed value objects (`Long`/`String`/
  `BigDecimal` — this driver must allocate the same ones). **Our `new LinkedHashMap<>(...)` copy
  constructor then iterates that wrapper's entries and builds a brand-new hash table — real hashing,
  real `Node` allocations, an extra cost client-v2's own path never pays**, on top of the values
  both sides already allocate. This sharpens the "preferred direction": copying each row into a
  plain `Object[]` (with once-per-query shared column-name→index metadata for name-based lookup, not
  a `Map` at all) would drop the per-row rehash entirely while keeping the same lifetime-safety
  reasoning `RowBinaryDecoder`'s own Javadoc already gives for not exposing client-v2's `RecordWrapper`
  directly. Still a hypothesis about *how much* it costs, not a measurement, until an allocation
  profiler says so — but now a precisely-scoped one, not a guess.
- **H0 (new — found by reading `FluxInputStreamBridge`/client-v2's `BinaryStreamReader` directly,
  not in the external review): `InputStream.read()`'s single-byte overload allocates a fresh
  `byte[1]` on every call.**
  ```java
  // FluxInputStreamBridge
  @Override
  public int read() throws IOException {
    final byte[] singleByte = new byte[1];   // new allocation, every call
    final int bytesRead = read(singleByte, 0, 1);
    return bytesRead == -1 ? -1 : singleByte[0] & 0xFF;
  }
  ```
  Traced client-v2's `BinaryStreamReader` (the mounted `clickhouse-java` source, not assumed):
  fixed-width columns (`UInt64`, the `Decimal` this table uses) go through `readNBytes`, which loops
  on the bulk `read(byte[], offset, length)` overload — no extra allocation there. But
  `readVarInt`/`readByteOrEOF` — used for every `String` column's length prefix, called once per row
  for `PointQueryTable`'s `label` column — call the single-byte `read()` directly. At 1,000,000 rows
  that's 1,000,000 avoidable tiny-array allocations, on top of H1's `LinkedHashMap` cost, purely
  from this adapter class's naive single-byte path. A real finding, cheap to fix (a reusable
  one-element buffer field — this class is only ever read by one dedicated worker thread, per its
  own Javadoc, so no thread-safety concern), but **not yet fixed** — same discipline as everything
  else here: measure its actual share of allocations first, don't fix on inspection alone.
- **H2: the blocking bridge itself (`FluxInputStreamBridge` → client-v2's blocking reader).**
  Queue/synchronization/wakeup cost per chunk hand-off — real, but this project's architecture
  accepts a bounded, backpressure-respecting blocking bridge as a deliberate interim design (see
  this class's own Javadoc); the question here is only how much of the *measured* cost it accounts
  for, not whether to remove it outright.
- **H3: `ByteBuf → byte[] → ByteBuffer` in `.asByteArray().map(ByteBuffer::wrap)`.** Verified this
  is real production code, not just the benchmark harness — `ClickHouseResult.java:69` uses the
  identical pattern the benchmarks do. Lower priority than H0/H1: this copies once per network
  chunk (tens to low thousands for a 1M-row response), not once per row.
  H4: `RESPONSE_CHUNK_DEMAND = 4` (already documented in `RowBinaryDecoder` as an unbenchmarked
  placeholder) — plausible for sustained-throughput pipelining, independent of H0/H1/H3.
  H5: `Flux.generate`'s own per-row Reactor machinery — lowest priority, investigate only if a
  meaningful gap remains after H0/H1/H3/H4 are quantified and addressed.

**Investigation plan, in order — profile before touching `RowBinaryDecoder` or
`FluxInputStreamBridge`, per this project's own testing discipline (measure, don't infer):**

1. **JMH's built-in GC profiler** (`-prof gc`, no extra tooling needed) on `StreamingScanBenchmark`
   at `rows=1000000` for both drivers — `gc.alloc.rate.norm` (bytes/op) directly answers "how many
   bytes does each driver allocate to decode 1M rows," and dividing by 1,000,000 gives allocated
   bytes/row, the first number this investigation actually needs.
2. **Two new diagnostic isolation benchmarks** (below), to split "is it transport or is it decode"
   and "is it the `Map` copy or something else in decode" instead of guessing from one combined
   number.
3. **CPU/allocation flame graph** (`async-profiler`, if/when set up — not yet part of this project's
   tooling) only if steps 1–2 don't already make the dominant cost obvious.
4. **Only then**, a benchmark-only prototype of a `Map`-free row representation (e.g. `Object[]` +
   shared column-name-to-index metadata) to quantify how much of the gap it actually closes, before
   any production `RowBinaryDecoder`/`ClickHouseRow` change.

**Two new diagnostic benchmarks written (not yet run):**

- `TransportOnlyStreamingBenchmark` — `ClickHouseHttpTransport.query(...)` consuming raw bytes
  (chunk-length sum only, no `RowBinaryDecoder` involved at all) vs client-v2's own
  `QueryResponse#getInputStream()` drained in a plain byte-counting loop. Answers whether H2/H3
  (bridge, byte-array copying) account for a meaningful share on their own, with decode removed from
  the picture entirely. Same `rows` tiers as `StreamingScanBenchmark`.
- `DecoderOnlyBenchmark` — captures one full response body once per trial (outside the measured
  region, via `.aggregate().asByteArray()`), then benchmarks `RowBinaryDecoder.decodeRows` vs
  client-v2's `RowBinaryWithNamesAndTypesFormatReader` (constructed directly, no `Client`/
  `QueryResponse` — the exact class `RowBinaryDecoder` itself wraps) over the *same* captured bytes,
  no network at all. Answers whether H0/H1 (decode/materialization) account for the gap in isolation
  from transport.

Run both:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes='TransportOnlyStreamingBenchmark|DecoderOnlyBenchmark'
```

---

### Both isolation benchmarks run for real (2026-08-13) — the question is answered

Same invocation also re-ran `StreamingScanBenchmark`; its combined numbers this run (6.0% / 53.7% /
98.2% slower, 10k/100k/1M) differ slightly from the earlier confirmed run (13.3% / 54.7% / 79.9%) —
normal run-to-run variance, same qualitative shape (grows sharply with rows). This run's own
combined figures are the ones used in the "sum of parts" comparison below, for internal consistency
within one JMH invocation.

**Transport is a genuine strength. Decode is the entire problem.** From JSON, exact figures. All
latency is µs/op (lower = faster):

| rows | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| 10,000 | 2,602.8 | 1,265.1 | **this driver 51.4% FASTER** |
| 100,000 | 12,273.8 | 4,562.4 | **this driver 62.8% FASTER** |
| 1,000,000 | 67,045.4 | 14,605.8 | **this driver 78.2% FASTER (4.6x)** |

`TransportOnlyStreamingBenchmark` (raw bytes, zero decode) — this driver's Reactor Netty transport
beats client-v2's blocking HTTP client outright, and the margin **grows** with response size. This
is a real, previously-invisible advantage that `StreamingScanBenchmark`'s combined number was
masking the whole time.

| rows | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| 10,000 | 843.1 | 2,111.3 | **this driver 150.4% SLOWER (2.5x)** |
| 100,000 | 7,484.3 | 21,595.6 | **this driver 188.5% SLOWER (2.9x)** |
| 1,000,000 | 81,547.1 | 222,911.7 | **this driver 173.4% SLOWER (2.7x)** |

`DecoderOnlyBenchmark` (same captured bytes, in memory, zero network) — client-v2 decodes
**~12–13M rows/sec** consistently across all three tiers; this driver decodes **~4.5–4.7M rows/sec**,
also consistently across tiers. The ratio (≈2.5–2.9x) barely moves with row count, unlike the
combined benchmark's growing percentage — this is a flat, structural per-row cost, exactly what H0
(the `byte[1]` allocation) and H1 (the `LinkedHashMap` rehash) predict, not a scaling artifact.

**One more real, secondary finding — the sum of the two isolated benchmarks doesn't equal the
combined `StreamingScanBenchmark` number, and the gap itself differs by driver:**

| rows | this driver: transport+decode | this driver: combined | extra | client-v2: transport+decode | client-v2: combined | extra |
| --- | --- | --- | --- | --- | --- | --- |
| 10,000 | 3,376.4 | 4,324.6 | +28.1% | 3,445.9 | 4,078.6 | +18.4% |
| 100,000 | 26,158.1 | 33,488.2 | +28.0% | 19,758.1 | 21,793.7 | +10.3% |
| 1,000,000 | 237,517.5 | 290,081.1 | +22.1% | 148,592.5 | 146,387.1 | **−1.5%** |

client-v2's combined number is close to (even slightly under, at 1M — within sampling noise) the
sum of its own parts, consistent with its simple blocking "read network, then decode" sequencing.
This driver's combined number runs a consistent 22–28% **above** the sum of its own parts — a real,
separate cost the two isolation benchmarks don't capture individually.

**Correction (2026-08-13, caught by a follow-up review before this was overstated further): that
22–28% gap is not proven to be `FluxInputStreamBridge` specifically.** `StreamingScanBenchmark` and
`TransportOnlyStreamingBenchmark`/`DecoderOnlyBenchmark` are not mathematically equivalent —
the live combined pipeline also has network chunking, producer/consumer overlap, decoder demand,
and scheduling effects that summing two separately-run isolated benchmarks doesn't reproduce; on
top of that, `DecoderOnlyBenchmark` decodes one aggregated in-memory payload, not the same fragmented
stream a live network response actually arrives as. Correct framing: this is **additional
live-pipeline integration cost** — `FluxInputStreamBridge`'s queue/wakeup hand-off is a strong
candidate for it, given the class's own design, but it is not yet isolated as *the* cause the way
H0/H1 now are. A future benchmark that replays a captured payload through the same chunk boundaries
and timing a live response would have would be needed to actually attribute this delta.

**Conclusion, stated with the confidence the data actually supports: this driver's non-blocking
transport is not the bottleneck — it's this driver's best-measured strength, and the combined
`StreamingScanBenchmark` number was hiding that fact. The large majority of the streaming regression
lives in decode/materialization (H0 + H1), with a smaller, not-yet-precisely-attributed
live-pipeline integration cost on top.** H3 (`ByteBuf`→`byte[]` copying) is **ruled out as the
primary/dominant cause** — the transport benchmark that pays that exact cost is the one where this
driver wins by up to 4.6x — but not proven to contribute zero cost; `TransportOnlyStreamingBenchmark`
demonstrates it isn't dominant, not that it's free.

**Caveats carried over from JMH's own output, not glossed over:** this run's JMH reported it used
an *experimental* "Compiler Blackholes" mode and explicitly warned to "exercise extra caution when
trusting the results" and to keep Blackhole mode consistent across any numbers being compared —
this run's own four benchmark classes all ran in the same invocation, so mode is consistent
*within* this run, but any future comparison against an older run's numbers should be re-run fresh
rather than diffed against historical figures. One point-in-run wobble, noted not hidden:
`PointQueryBenchmark` in this same run showed this driver and client-v2 essentially tied (1164.0 µs
vs 1159.1 µs, 0.4% apart) rather than this driver's usual 5–7% edge seen in three prior runs —
treated as this run's noise, not a reversal, given the weight of prior evidence; `TrivialQueryBenchmark`
in the same run still showed the usual ≈8.5% edge.

**Next step, sharpened by this result:** `DecoderOnlyBenchmark` is now the right target for `-prof
gc` — it isolates decode from network entirely, so an allocation profile of it directly attributes
bytes/row to H0 vs H1 without any transport noise in the way:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=DecoderOnlyBenchmark -Pjmh.profilers=gc
```

---

### `-prof gc` on `DecoderOnlyBenchmark`, run for real (2026-08-13) — H1 confirmed dominant, H0 minor

First real payoff of fixing `-Pjmh.includes`/`-Pjmh.profilers` (see the build.gradle.kts fix above,
also committed) — this run took 4m33s and executed only `DecoderOnlyBenchmark`, not the whole suite.
`gc.alloc.rate.norm` (bytes allocated per operation, JMH's own metric) ÷ `rows` gives allocated
bytes/row directly:

| rows | client-v2 B/row | this driver B/row | ratio | extra B/row (this driver) |
| --- | --- | --- | --- | --- |
| 10,000 | 296.1 | 835.7 | 2.82x | +539.6 (high error bars this tier — noisiest sample count) |
| 100,000 | 296.0 | 872.1 | 2.95x | +576.0 |
| 1,000,000 | 296.0 | 872.0 | 2.95x | +576.0 |

**client-v2 allocates a remarkably stable ~296 bytes/row at every tier — this driver allocates a
remarkably stable ~872 bytes/row at 100k/1M (872.1 and 872.0 — agreement to three significant
figures across a 10x row-count difference is strong evidence this is a real, structural per-row
cost, not noise).** The allocation ratio (≈2.95x) tracks the latency ratio measured earlier
(≈2.7–2.9x) closely — allocation pressure is very likely the direct mechanical cause of the latency
gap, not a coincidental correlation with some unrelated CPU cost.

**Attributing the extra ≈576 bytes/row to H0 vs H1, from what the code actually does:**
`RowBinaryDecoder.emitNextRow` calls `reader.next()` (client-v2's own decode — the same ~296
bytes/row client-v2's own benchmark pays) and *then* wraps that result in `new LinkedHashMap<>(...)`
— this driver structurally pays client-v2's entire cost first, plus its own map materialization on
top. A `byte[1]` allocation (H0) is a small, fixed ~24 bytes (a 1-byte array's header/padding on a
typical 64-bit JVM with compressed oops) and fires once per row for this table (one varint length
prefix, for the `label` String column) — accounting for roughly 4% of the extra 576 bytes, at most.

**Correction (2026-08-13, caught before this was overstated further): the remaining ≈552 bytes/row
is not proven to be the `LinkedHashMap` copy specifically — that number was derived by subtracting
H0's estimate from the total, which silently folds in every other difference between the two
decode paths too: `FluxInputStreamBridge`'s own `StreamSignal`/`BlockingQueue` machinery (present in
this driver's path, absent from client-v2's baseline, which reads a plain `ByteArrayInputStream`
with no bridge at all), `Flux.generate`'s per-row Reactor state, and `ListDecodingRowBinaryReader`
vs. the base reader class. Subtraction across a multi-variable difference isn't attribution — it's
a total.** H1 is still the strongest candidate (matches the ranked-hypothesis reasoning and the
external review's own read of the code), but **not yet quantified**. Quantifying it needs a
single-variable-change benchmark — see `ourDriverWithoutMapCopy` below — not more arithmetic on the
existing numbers.

One more supporting observation: `gc.alloc.rate` (MB/sec, not bytes/op) came out similar for both
drivers (≈3.1–3.8 GB/sec, all tiers, both drivers) — consistent with both drivers running against
roughly the same machine-level allocation/GC throughput ceiling, which means bytes-per-row is
functioning as a direct, near-linear lever on this driver's own latency here, not a number that's
disconnected from what's actually slow.

**What this changes about the plan:** H0 is real, cheap, and safe to fix immediately — a reusable
single-element buffer in `FluxInputStreamBridge.read()` instead of `new byte[1]` every call, no
design decision involved, easy to verify in isolation. H1 is not a quick fix — it means either
changing what `RowBinaryDecoder.decodeRows`/`RowBinaryDecoder.decode` hand back (today's public
`Flux<Map<String, Object>>` / `DecodedResult` shape) or adding a leaner internal path underneath it,
and needs to account for every caller of that API (`connector`'s `ClickHouseResult`/`ClickHouseRow`,
this benchmark module) before anything changes — a real design task, not a one-line fix. Sequencing:
fix and verify H0 first (isolated, low-risk, real but modest win — expect `DecoderOnlyBenchmark` to
drop by roughly the ~24-bytes-of-576 share, a few percent, not a game-changer on its own), *then*
scope the `LinkedHashMap`-avoidance design for H1 with the full picture of what depends on the
current row shape.

**H0 fixed and confirmed by a real re-run (2026-08-13).** `FluxInputStreamBridge` now has a
`private final byte[] singleByteReadBuffer = new byte[1]` field, reused by `read()` instead of
allocated per call — safe because this class's own Javadoc already establishes it's read by exactly
one dedicated worker thread, never concurrently. Also closed a real black-box test gap while here: no
existing `FluxInputStreamBridgeTest` test exercised the single-byte `read()` overload at all
(`readAllBytes()`, used by every existing test, only calls the bulk `read(byte[], int, int)` overload
internally) — added `shouldReadOneByteAtATimeViaTheSingleByteReadOverload`.

Re-running `DecoderOnlyBenchmark -Pjmh.profilers=gc` after the fix:

| rows | this driver B/row, before H0 | this driver B/row, after H0 | reduction |
| --- | --- | --- | --- |
| 100,000 | 872.1 | 848.05 | −24.0 |
| 1,000,000 | 872.0 | 848.01 | −24.0 |

A clean, reproducible ~24 bytes/row reduction at both tiers — matches the predicted fixed cost of one
`byte[1]` header exactly. Latency barely moved (≈227ms → ≈226ms at 1M), confirming H0 was real but
never the dominant term. **H0 is closed.**

**`DecoderOnlyBenchmark.ourDriverWithoutMapCopy` run for real (2026-08-13) — H1 confirmed, not just
suspected.** Same `FluxInputStreamBridge` (with the H0 fix), same
`RowBinaryWithNamesAndTypesFormatReader` settings, same `Flux.generate` per-row emission shape as
`ourDriver` — the *only* difference is that `reader.next()` is still called (client-v2's own per-row
cost is still paid, unchanged) but its result is discarded instead of copied into
`new LinkedHashMap<>(...)`. A genuine single-variable-change isolation, not a subtraction.

| rows | `ourDriver` B/row | `ourDriverWithoutMapCopy` B/row | H1 cost (B/row) | `ourDriver` latency | `ourDriverWithoutMapCopy` latency |
| --- | --- | --- | --- | --- | --- |
| 100,000 | 848.05 | 272.05 | 576.0 | 22.71 ms | 5.27 ms |
| 1,000,000 | 848.01 | 272.01 | 576.0 | 224.98 ms | 51.31 ms |

**H1 = 576.0 bytes/row, agreeing to four significant figures across a 10x row-count change — as clean
an isolation result as this investigation has produced.** That's the entire original ≈576 bytes/row
gap this investigation started from: H0's ~24 B/row plus H1's ~576 B/row account for essentially all
of the allocation difference between this driver and client-v2, with nothing large left unattributed.
Latency: removing the map copy cuts `ourDriver`'s decode time by ~77% (4.38x) at 1M rows — H1 is not
just an allocation cost, it's the dominant CPU cost in the decode path.

**Mechanically confirmed against the actual `clickhouse-java` source (read directly, not taken from
the external review at face value) — `AbstractBinaryFormatReader.RecordWrapper`:**

```java
// RecordWrapper — private static nested class
private final WeakReference<Object[]> recordRef;
private final WeakReference<TableSchema> schemaRef;

@Override
public Set<Entry<String, Object>> entrySet() {
  int i = 0;
  Set<Entry<String, Object>> entrySet = new HashSet<>();
  for (ClickHouseColumn column : schemaRef.get().getColumns()) {
    entrySet.add(new AbstractMap.SimpleImmutableEntry(column.getColumnName(), recordRef.get()[i++]));
  }
  return entrySet;
}
```

`reader.next()` itself is cheap — it swaps the reused `currentRecord`/`nextRecord` `Object[]` buffers
(allocated once, not per row) and wraps whichever is current in one `RecordWrapper`. But
`new LinkedHashMap<>(reader.next())` runs `HashMap.putMapEntries`, which iterates
`recordWrapper.entrySet()` — and that method allocates a fresh `HashSet` plus one
`SimpleImmutableEntry` per column on *every single call*, which `LinkedHashMap`'s own constructor
then copies into a second hash table (one `LinkedHashMap.Entry` per column). For this three-column
table, one row emission is: `RecordWrapper` + 2×`WeakReference` + `HashSet` + 3×`SimpleImmutableEntry`
+ `LinkedHashMap` + 3×`LinkedHashMap.Entry` — eight-plus objects to move three already-decoded values
into a map client-v2 never needed to build in the first place. The measured 576 bytes/row is fully
explained by real, read source code, not inference.

**H1 is closed — confirmed both experimentally and mechanically. Full decomposition, now measured end
to end:**

| stage | B/row |
| --- | --- |
| pre-H0 driver | 872.0 |
| − H0 (`byte[1]`) | −24.0 |
| post-H0 driver | 848.0 |
| − H1 (`LinkedHashMap` copy) | −576.0 |
| without map copy | 272.0 |

**One caveat, important not to overstate:** `ourDriverWithoutMapCopy` (272 B/row, ~51.3ms at 1M) comes
in *below* client-v2's own baseline (296 B/row, ~88.1ms) — but the two benchmark bodies aren't doing
equivalent work. `clientV2` calls `getLong`/`getString`/`getBigDecimal` per row (materializing and
consuming three typed values); `ourDriverWithoutMapCopy` calls `reader.next()` and discards the result
without touching any column value. **This does not yet mean "our decode is faster than client-v2"** —
it means the bridge/Reactor/`Flux.generate` machinery isn't itself responsible for the regression,
which is the question this diagnostic was built to answer. A fair speed claim needs the actual
production replacement (compact row, real per-value access) benchmarked against client-v2's
getter-based access, not this discard-only diagnostic.

**Architectural implication, now evidence-backed rather than inspection-only:** the fix is not
`sink.next(reader.next())` — `RecordWrapper` holds its row through `WeakReference`s into client-v2's
*reused* `currentRecord`/`nextRecord` buffers, so exposing it downstream would hand callers a view
that can change or go stale as decoding continues. What this data supports: replace
`Flux<Map<String, Object>>` with a compact per-row `Object[]` snapshot (one copy, no hash table) plus
once-per-result shared column-name→index metadata, matching R2DBC's index/name row-access shape
directly instead of routing through a `Map`.

**`DecoderOnlyBenchmark.ourDriverCompactRow` — the fair, production-shaped comparison — run and
confirmed (2026-08-13).** `ourDriverWithoutMapCopy` proved the bridge/Reactor machinery isn't the
regression, but wasn't a fair number to compare against `clientV2` (it discards each row untouched,
no getter calls, while `clientV2`'s benchmark does real per-value work). `ourDriverCompactRow` closes
that gap: same `FluxInputStreamBridge`/`Flux.generate` shape, but instead of
`new LinkedHashMap<>(reader.next())` it calls the same three typed getters `clientV2` calls
(`getLong(1)`/`getString(2)`/`getBigDecimal(3)`) and packs them into a plain `Object[3]` — a real
retained per-row object, the shape a production `DecodedRow` would need, built without ever touching
`RecordWrapper.entrySet()`.

**Methodology correction first — the initial single-fork run was not trustworthy.** The first
`-Pjmh.forks` run (default `fork.set(1)`, one warmup iteration) gave numbers that didn't reproduce
between runs of byte-for-byte identical code and data (`ourDriver` at 1M: 848 B/row one run, 808
B/row the next; `ourDriverWithoutMapCopy` at 100k: 272 B/row one run, 248 B/row the next) — a single
fork can't separate a real structural cost from that particular JVM instance's own JIT/GC/TLAB state.
Added `-Pjmh.forks`/`-Pjmh.warmupIterations` wiring to `build.gradle.kts` (same pattern as
`includes`/`profilers`) and re-ran with 3 forks × 3 iterations (`Cnt: 9` per GC metric below):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=DecoderOnlyBenchmark -Pjmh.profilers=gc -Pjmh.forks=3 -Pjmh.warmupIterations=3
```

**With 3 forks, B/row numbers converge tightly and the H0/H1 decomposition holds exactly:**

| rows | clientV2 B/row | `ourDriverWithoutMapCopy` B/row | `ourDriverCompactRow` B/row | `ourDriver` B/row |
| --- | --- | --- | --- | --- |
| 10,000 | 296.08 | 248.46 | 327.84 | 835.1 (±33.6 — noisy tier, one fork/iteration hit a GC outlier) |
| 100,000 | 296.01 | 272.05 | 351.99 | 848.05 |
| 1,000,000 | 296.00 | 272.00 | 344.00 (±20.2 — larger error than other rows, see below) | 848.01 |

`ourDriver` and `ourDriverWithoutMapCopy` at 100k/1M now match the original H0/H1-decomposition
numbers exactly (848.0 / 272.0) — the earlier 808/248 readings were single-fork noise, not a real
per-tier effect. **H0 (~24 B/row) and H1 (~576 B/row) stand confirmed, unchanged.**

**The fair comparison's real result: a genuine, moderate, previously-overstated-by-noise residual
gap remains between `ourDriverCompactRow` and `clientV2`'s baseline.**

| rows | `ourDriverCompactRow` latency | `clientV2` latency | slower by | B/row gap |
| --- | --- | --- | --- | --- |
| 10,000 | 934.3 us | 744.6 us | +25.5% | +31.8 |
| 100,000 | 9052.9 us | 7807.9 us | +15.9% | +56.0 |
| 1,000,000 | 94532.4 us | 83652.2 us | +13.0% | +48.0 (within the 1M tier's own ±20.2 error margin of the 100k tier's 56.0) |

Unlike the single-fork run's erratic 8%–40% swing, this is a coherent, converging signal: **removing
the `LinkedHashMap` closes the vast majority of the original gap (H1's 576 B/row, ~77% of latency),
but ~13–16% of latency and ~48–56 bytes/row remain** once the comparison is fair (equivalent getter
calls on both sides). The most likely source, by elimination: `clientV2`'s benchmark reads from a
plain `ByteArrayInputStream` with no bridge and no Reactor involved at all; `ourDriverCompactRow`
still goes through the full `FluxInputStreamBridge` (queue/`StreamSignal`) + `Flux.generate`
pipeline — this is the H2 hypothesis this investigation named early on and never isolated on its own.
**Not yet quantified as its own single-variable measurement — same discipline as H1: no attributing
this by elimination-reasoning alone without a dedicated isolation benchmark**, but at ~13–16% it's a
tolerable cost for what the bridge buys (real backpressure, no blocking the Netty event loop), not
the kind of multi-x regression H1 was.

**One more noise signal worth recording, not chasing right now:** both `ourDriver` and
`ourDriverWithoutMapCopy` show a single extreme `p1.00` outlier at the 10k tier only (790.6ms and
132.1ms respectively, against p0.9999 values under 4ms and 1ms) — one sample, out of tens/hundreds of
thousands, in one fork/iteration. Consistent with a one-off safepoint/GC/OS scheduling stall, not a
reproducible tail-latency property; doesn't change any conclusion above (means and B/row are stable
across 9 samples), but would need investigating before any p99.9+ SLA claim is made.

**Decision point:** the compact-row direction is now validated by a fair, multi-fork-confirmed
comparison — it recovers essentially all of H1's cost and lands within ~13–16% of client-v2's own
decode speed, a reasonable trade for a reactive, non-blocking pipeline. Scoping the production
`RowBinaryDecoder`/`DecodedResult`/connector-layer redesign around this compact `Object[]` + shared
metadata shape is now justified by evidence, not inspection. An H2 isolation benchmark (bridge/Reactor
cost alone, zero row materialization) remains open as a smaller follow-up if the ~13–16% residual
gap is ever worth chasing further.

**Production redesign implemented (2026-08-13) — written while offline, NOT YET BUILD-VERIFIED.**
Per the decision point above, `RowBinaryDecoder` no longer emits `Flux<Map<String, Object>>`. It now
emits `Flux<DecodedRow>`, a new record wrapping a plain `Object[]` snapshot (`equals`/`hashCode`
overridden via `Arrays.equals`/`Arrays.hashCode`, since a record's generated versions would compare
the array by reference). `DecodedResult` and `decodeRows`/`decode` were updated to match.

Rather than reusing the benchmark's typed-getter approach (`getLong`/`getString`/`getBigDecimal`),
production takes a cheaper path found by re-reading client-v2's actual `next()` implementation:
`ListDecodingRowBinaryReader` (this project's own subclass, already used for `Array`/`Nested`
decoding) gained a package-private `nextRowValues()` method that calls `next()` (paying its fixed,
small `RecordWrapper`/`WeakReference` cost — unavoidable without reimplementing client-v2's internal
buffer-swap logic, which this project has already drawn a boundary around not depending on) and then
clones the reader's own already-decoded `currentRecord` array directly — `protected Object[]
currentRecord` on `AbstractBinaryFormatReader`, confirmed accessible from a subclass in a different
package/module via ordinary protected inheritance, verified by reading the actual `clickhouse-java`
source (not assumed). This avoids per-column getter dispatch entirely, not just the `Map` copy — a
plausibly *cheaper* path than `DecoderOnlyBenchmark.ourDriverCompactRow` measured, though not yet
confirmed by a benchmark exercising `nextRowValues` itself (a natural next diagnostic, not yet built).

The connector layer (`ClickHouseRow`, `ClickHouseResult`, `ClickHouseRowMetadata`) was updated to
match: `ClickHouseRowMetadata` now precomputes a name→index `Map` once per result in its constructor
(replacing a per-call, case-insensitive linear scan over its column list), and `ClickHouseRow.get`
reads `DecodedRow.valueAt(index)` directly instead of a name-keyed `Map` lookup. Test-only code that
consumed the old `Map<String, Object>` shape ergonomically (`RealClickHouseQueryAbility.queryRows`,
used by the ~16-method `RealWorldTableAgainstRealClickHouseTest` type-coverage suite via
`ClickHouseRowAssert`) was deliberately left returning `Map<String, Object>` — it now rebuilds that
map once per row from the new `DecodedResult` at the test-DSL boundary only, specifically so that
large test suite needs zero changes; only the handful of tests that touched `RowBinaryDecoder`/
`ClickHouseResult`/`ClickHouseRow` directly (`RowBinaryDecoderTest`, `ClickHouseResultTest`,
`ClickHouseRowTest`, `SelectOneAgainstRealClickHouseTest`) and two benchmark call sites
(`PointQueryBenchmark`, `TrivialQueryBenchmark`) needed updating for the new type. Full file list:
`DecodedRow.java` (new), `DecodedResult.java`, `RowBinaryDecoder.java`, `ListDecodingRowBinaryReader.java`,
`ClickHouseRow.java`, `ClickHouseResult.java`, `ClickHouseRowMetadata.java`, plus the test/benchmark
files just named. A new `DecodedRowTest.java` covers the record's overridden `equals`/`hashCode`.

Also added to `DecoderOnlyBenchmark`: `compactRowDirectLoop` and `compactRowFluxNoBridge`, a small H2
factorial matrix (direct loop → `Flux.generate` without the bridge → `ourDriverCompactRow` with the
bridge → `clientV2`) to isolate where the ~13–16% residual gap from the fair-comparison result above
actually lives — bridge, Reactor, or the row object itself — instead of attributing it to "the
bridge" by elimination. Not yet run.

**This entire redesign was written without the ability to compile or run a single test** — this
session had no local JDK 21 (only JDK 11) and no network access to provision one (Testcontainers/JDK
downloads are both blocked by the sandbox's network allowlist). Every file above was hand-verified by
re-reading the exact existing code it touches and mirroring established patterns precisely, but per
this project's own hard rule ("commit only after confirmed green output"), **none of it has been
committed.** Before trusting or committing any of it:

```
./gradlew spotlessApply
./gradlew :clickhouse-r2dbc-reactive-core:test :clickhouse-r2dbc-reactive-connector:test :clickhouse-r2dbc-reactive-transport-http:test
./gradlew clean build
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=DecoderOnlyBenchmark -Pjmh.profilers=gc -Pjmh.forks=3 -Pjmh.warmupIterations=3
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=StreamingScanBenchmark -Pjmh.forks=3 -Pjmh.warmupIterations=3
```

The first two confirm the redesign actually compiles and every existing behavior still holds
(especially `RealWorldTableAgainstRealClickHouseTest`'s full type-coverage suite, which changed
zero lines but depends entirely on `ClickHouseRowAssert`/`RealClickHouseQueryAbility` being correct).
The GC-profiled `DecoderOnlyBenchmark` run confirms `nextRowValues()`'s real bytes/row and latency —
compare against the `ourDriverCompactRow`/`clientV2` numbers above. `StreamingScanBenchmark`
re-confirms the effect end to end, the same benchmark that originally caught the regression.

**Deferred to the very end of this phase, once performance is fully confirmed:** nice-looking charts
of the final benchmark numbers, styled to match GitHub's colour scheme, pasted into the main
`README.md`. Explicitly not now — noted here so it isn't lost.

**Guardrail, explicit:** every future change from this investigation reruns
`TrivialQueryBenchmark`/`PointQueryBenchmark` alongside `StreamingScanBenchmark`'s three tiers — a
streaming fix that regresses the fixed-request path (this driver's genuine current strength) is not
an acceptable trade. Concurrency (`ConcurrencyBenchmark`, Level 3) stays queued behind this
investigation — per this project's own priority reasoning, but now the priority is real:
`StreamingScanBenchmark` surfaced an actual, sizable regression concurrency work shouldn't be built
on top of unmeasured.

---

### Redesign confirmed by a real build (2026-08-14)

> [!IMPORTANT]
> First real build of the `DecodedRow` redesign against a real JDK 21 (this session's own sandbox
> only has JDK 11, so everything above this point was written and reviewed without ever compiling —
> see the "written while offline" note above). `./gradlew … jmh` on `StreamingScanBenchmark`
> reported **BUILD SUCCESSFUL** — the redesign compiles, and the full existing pipeline
> (`core`/`connector`/`transport-http` test abilities/`benchmarks`) links against it correctly. This
> does **not** yet confirm the unit test suite itself is green — `jmh` only compiles `main`/`jmh`
> source sets, it does not run `test`. `./gradlew spotlessCheck clean build` is still outstanding.

`StreamingScanBenchmark`, single fork (`Cnt: 7848/1352/195` — one fork, default warmup), 10k/100k/1M
tiers, mean latency (µs/op, lower is faster):

| rows | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| 10,000 | 3,820.3 | 2,999.6 | **this driver ≈21.5% FASTER** |
| 100,000 | 22,198.7 | 19,324.7 | **this driver ≈13.0% FASTER** |
| 1,000,000 | 155,048.1 | 152,277.9 | **this driver ≈1.8% FASTER** |

This driver also has the tighter tail at 1M — max (p1.00) 177.7ms vs client-v2's 253.8ms. **First
run in this whole investigation where this driver wins the full-scan benchmark outright, at every
tier measured**, after losing it badly (13%/55%/80% slower) at the start of this investigation.

> [!WARNING]
> **Not yet a trustworthy final number, for the exact reason this project already learned once this
> session: this was a single-fork run.** The earlier `DecoderOnlyBenchmark` B/row numbers proved a
> single fork can disagree with itself by 15–40 B/row between separate runs of identical code — the
> fix was `-Pjmh.forks=3 -Pjmh.warmupIterations=3`. The advantage also **shrinks sharply with scale**
> (21.5% → 13.0% → 1.8%), which is worth understanding on its own — real diminishing returns from a
> fixed remaining cost becoming a smaller share of a bigger number, or single-fork noise landing
> favorably at 1M — not something to read into without a multi-fork rerun:
>
> ```
> ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=StreamingScanBenchmark -Pjmh.forks=3 -Pjmh.warmupIterations=3
> ```

**Still open after this run, unchanged from the status box at the top of this file:** the H2 factorial
matrix (`compactRowDirectLoop`/`compactRowFluxNoBridge`) has been run once, single-fork, with
internally inconsistent (non-monotonic) numbers at the 1M tier — needs the same multi-fork treatment
before any H2 number goes in this doc as confirmed. The wide-decode/aggregation/INSERT benchmarks are
still not started. Charts for the main `README.md` stay deferred until all of the above is confirmed,
per the explicit instruction already recorded above.

---

### `ConcurrencyBenchmark`'s `@Threads(8)` shape, run for real (2026-08-14)

First Level 3 (concurrency) data point, and the first result in this whole investigation with a
genuinely mixed outcome instead of a clean win or loss. `ConcurrencyBenchmark` runs the exact same
parameterized point lookup `PointQueryBenchmark` measures single-threaded, but with 8 JMH worker
threads (`@Threads(8)`) hammering the same shared `ClickHouseHttpTransport`/`Client` instances
concurrently. Single fork, one run — same caveat as every other run on this page. Latency in µs/op
(lower is faster):

| percentile | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| mean | 2,112.4 | 2,204.1 | **this driver ≈4.3% SLOWER** |
| p50 | 1,962.0 | 2,027.5 | **this driver ≈3.3% SLOWER** |
| p90 | 2,850.8 | 3,072.0 | **this driver ≈7.8% SLOWER** |
| p95 | 3,239.9 | 3,571.7 | **this driver ≈10.2% SLOWER** |
| p99 | 4,333.6 | 4,964.4 | **this driver ≈14.6% SLOWER** |
| p99.9 | 8,364.0 | 7,761.4 | **this driver ≈7.2% FASTER** |
| p99.99 | 34,197.9 | 23,154.2 | **this driver ≈32.3% FASTER** |
| p100 (max) | 46,202.9 | 24,739.8 | **this driver ≈46.5% FASTER** |

**A clean crossover, not noise-shaped:** this driver is consistently a bit worse from the mean
through p99 (the gap widening steadily — 3% → 8% → 10% → 15% — not flat, not erratic), then flips to
consistently better from p99.9 through the max, by a growing margin. Both halves of this pattern are
internally coherent, which is more consistent with a real structural effect than sampling noise, but
neither half is confirmed by a second fork yet.

**A plausible read, not yet verified:** under single-threaded load (`PointQueryBenchmark`), this
driver won by ~6% mean. Under 8 concurrent threads, that reverses to ~4% slower mean — something about
concurrent access costs this driver more than it costs client-v2 on the *typical* request, which
lines up with the still-unattributed H2 residual (~13–16% slower once compared fairly at the decode
level, never isolated to bridge vs. Reactor vs. something else — see the H2 section above) becoming
more visible under contention rather than being a new finding. The tail flip (this driver dramatically
better at p99.9+) is consistent with the architectural pitch this driver is actually built on:
`clientV2`'s blocking client can have a worker thread fully stall on connection acquisition or a slow
socket with nothing else to do; this driver's non-blocking pipeline degrades more gracefully under the
same contention, at the cost of a bit more per-request overhead in the common case. **Not yet proven —
just the most obvious mechanical story, and one this project's own discipline says needs a profiler
(connection-pool metrics, `-prof gc`, or async-profiler under load) before being written up as fact.**

**What's still missing before this is a complete Level 3 picture:** (1) a multi-fork rerun, same as
every other number on this page; (2) the custom bounded-pool reactive-harness shape — the actual
"~11 concurrent queries per user action" scenario that motivated this project, not yet run (see
below — it's now written); (3) connection-pool-level diagnostics (both sides' pool metrics during the
run) to actually attribute the mean/p99 regression instead of theorizing about it from latency
numbers alone.

**External review (a second, independent read of the `@Threads(8)` result above, checked against the
actual benchmark code rather than taken at face value) converged on the same read this doc already
had, and sharpened it into one explicit instruction: don't optimize based on the ~4% mean/p99 gap
yet — `@Threads(8)` measures "8 blocking callers, both sides' default connection pools," not the
scenario this project is actually built around.** `@Threads(8)` stays the "same blocking-caller
resources" baseline, not the main concurrency verdict. Agreed, and acted on below.

---

### `ClickHouseHttpTransport`'s `maxConnections` knob — smaller gap than first claimed (2026-08-14)

**Correction:** the previous section (and the `ConcurrencyBenchmark` commit) said building the
bounded-pool harness was "blocked on adding a `maxConnections`-style knob to `ClickHouseHttpTransport`."
Checked before acting on that claim, not assumed twice: `ClickHouseHttpTransport(baseUrl,
maxConnections)` already existed — the actual gap was narrower, no public constructor combined a
bounded pool *with* `Authentication`, which every non-anonymous benchmark/real deployment needs.
Added `ClickHouseHttpTransport(baseUrl, Authentication, maxConnections)`, a small additive overload
that only plumbs an existing `ConnectionProvider.create(name, maxConnections)` call through the
existing private canonical constructor — the same pattern the two existing `maxConnections`/
`Authentication`-only constructors already use separately. One test added
(`shouldReturnTheConfiguredResponseBodyWhenAuthenticationAndMaxConnectionsAreBothConfigured`),
mirroring the file's existing simple wiring-proof style — **not yet compiled/run in this session**
(no JDK 21 available here), same caveat as every other production change made this way today.

### `BoundedPoolConcurrencyBenchmark`, run for real (2026-08-14) — the cleanest result in this file

Implements the scenario the external review above named explicitly: `POOL_SIZE = 8` physical
connections (fixed, not parameterized — see "small first pass" below), serving `concurrency`
logical point lookups at once, `@Param({"8", "32", "128"})`. This driver via `Flux.range(0,
concurrency).flatMap(..., concurrency)` — every logical query subscribed immediately, no blocked JMH
worker thread per query, Reactor Netty's pool queues what doesn't fit in the 8 live connections.
client-v2 via its own `Client#query`'s `CompletableFuture`-returning async API (not a blocking call
per thread either) with `setMaxConnections(8)`/`enableConnectionPool(true)` configured to match.

**Deliberately a small first pass, not the full matrix the external review proposed** (5 concurrency
levels × 3 pool sizes = 15 combinations): one fixed pool size, three concurrency levels — see the
"Deliberately a small first pass" reasoning below for why.

Build-verified (2026-08-14): the new constructor's test
(`shouldReturnTheConfiguredResponseBodyWhenAuthenticationAndMaxConnectionsAreBothConfigured`)
confirmed green, and the benchmark itself compiled and ran. Single fork. Latency in µs/op (lower is
faster):

| concurrency | client-v2 mean | this driver mean | verdict |
| --- | --- | --- | --- |
| 8 | 9,085.2 | 8,692.6 | **this driver ≈4.3% FASTER** |
| 32 | 36,860.7 | 34,816.2 | **this driver ≈5.5% FASTER** |
| 128 | 145,218.9 | 138,413.2 | **this driver ≈4.7% FASTER** |

**This driver wins on every percentile (p50 through max) at every concurrency level tested — not
just the mean.** p50 is faster by 4.2–5.5%; p90 by 4.0–6.0%; p99 by 1.6% (concurrency=8) up to 11.9%
(concurrency=128, the widest gap at any percentile in this run); p99.9 through max by 19–26% at
concurrency 32/128 (one single-sample p99.9 blip at concurrency=8 where this driver was marginally
slower — low sample count in that exact bucket, not inconsistent with everything else in the row).

**This resolves the earlier `ConcurrencyBenchmark` `@Threads(8)` puzzle.** That run showed this
driver *losing* on mean/p50–p99 while winning dramatically on the tail — a genuinely confusing,
partially-contradictory result. The two benchmarks differ in exactly the way the external review
above pointed at: `@Threads(8)` used **blocking** calls on both sides with **unmatched, default**
connection pool sizes; `BoundedPoolConcurrencyBenchmark` matches both sides to the same 8-connection
pool and drives concurrency **non-blocking/async** on both sides. With that one variable controlled
for, the "this driver is worse on the common case" signal disappears entirely — consistent with the
mean/p99 regression having been a pool-configuration artifact of the `@Threads(8)` harness, not a
real cost of this driver's architecture. **Not upgraded to "proven" without a profiler pass and a
multi-fork rerun — but this is now the stronger, more carefully controlled result, and it points the
opposite direction from the one `@Threads(8)` suggested.**

**Deliberately a small first pass, not the full matrix the external review proposed** (5 concurrency
levels × 3 pool sizes = 15 combinations): given every benchmark on this page already costs real
wall-clock time on a single shared laptop, and this project's own experience this session with
long/noisy runs, a small first signal before committing to a larger sweep was the more disciplined
choice, per direct instruction. **That first signal is now in, and it's clean and consistent across
all three concurrency levels already tested — a reasonable basis to widen the matrix (more pool
sizes, higher concurrency levels) once multi-fork confirms this first pass holds.**

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=BoundedPoolConcurrencyBenchmark -Pjmh.forks=3 -Pjmh.warmupIterations=3
```


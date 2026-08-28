# Roadmap

The short, current-only version: what's released, what's in progress, what's next, what's later,
and what's explicitly not planned. For the full phase-by-phase history, engineering rationale, and
every finding that led to each decision below, see
[engineering/roadmap-archive.md](engineering/roadmap-archive.md) — nothing was deleted, it was
archived (2026-08-23) as part of [Phase 9](#phase-9--documentation--website-redesign) below. For
exactly what shipped in each release, see [CHANGELOG.md](CHANGELOG.md).

## Released

| Version | Date | Highlights |
| --- | --- | --- |
| [0.2.1](CHANGELOG.md#021--2026-08-22) | 2026-08-22 | Post-0.2.0 hardening: Enum8/Enum16 normalized to `String`, connection-reuse fix, Netty leak-detection lane, parameter-binding type matrix, demo NDJSON streaming fix. |
| [0.2.0](CHANGELOG.md#020--2026-08-20-phase-7-operational-control--r2dbc-correctness) | 2026-08-20 | Operational control & R2DBC correctness: driver observability SPI, R2DBC compatibility lane (TCK), transport pool options, statement timeout, release/documentation sync. |
| [0.1.0](CHANGELOG.md#010--2026-08-14) | 2026-08-14 | First Maven Central release: fully reactive `SELECT`/`INSERT`/batch, R2DBC SPI surface, Spring WebFlux demo, first performance pass. |

## In progress

- **0.2.2 (unreleased)** — HTTP response compression (LZ4, on by default, matching client-v2's own
  default), the opt-in native RowBinary scalar decoder, and a fix so the bundled demo actually
  disposes the driver's `ConnectionFactory` at Spring shutdown. See
  [CHANGELOG.md's Unreleased section](CHANGELOG.md#unreleased--022).
- **Phase 8 — post-0.2.0 hardening.** Most items shipped in 0.2.1/0.2.2 (see table above). The
  non-idempotent `release.yml` `USER_MANAGED` finalization step is now fixed (`deployment_id`
  resume input, 2026-08-24). **client-v2 bumped 0.9.8 → 0.10.0** (2026-08-24): checked the 0.10.0
  changelog's Breaking Changes entirely against production imports — none apply, since `core` only
  uses `BinaryStreamReader`/`RowBinaryWithNamesAndTypesFormatReader`-family decode classes (never
  `Client`/`Client.Builder`, which is where every breaking change in that release lives), and
  `transport-http`/`connector` construct `ServerException` directly from the raw HTTP response
  rather than receiving it from client-v2's own `Client`, so the 503/unknown-status reclassification
  doesn't apply either. The changelog's `getBigDecimal` truncation fix also turned out irrelevant —
  traced it to `NumberConverter`/`SerializerUtils`/`ValueConverters` (the typed-getter path), which
  this driver's decode path never calls — but that investigation surfaced a real, pre-existing gap:
  `Decimal128`/`Decimal256` (the `BigInteger`-backed wide tiers) had no test coverage at all.
  Closed with `shouldDecodeLargeDecimalTypes` in `RealWorldTableAgainstRealClickHouseTest`. Only the
  `benchmarks` module's `Client.Builder` baseline setup and 503-retry comparison need a look after
  this bump — flagged, not yet done. Still open: Phase 8 P2 doc/policy items (TLS scope, minimum
  ClickHouse version, multi-host contract, compound statements), a current-`main` demo integration
  lane alongside the published-release lane, and a public vendor extension for per-statement
  ClickHouse settings. Full detail:
  [roadmap-archive.md's Phase 8](engineering/roadmap-archive.md#phase-8--post-020-hardening-021).

## Next (not started, plan written)

- **[Phase 9 — Documentation & website redesign](#phase-9--documentation--website-redesign)** —
  shrink this repo's docs down to three layers (GitHub README, a documentation website, an
  engineering archive) instead of two overloaded files. Planned 2026-08-22, this file's own
  restructuring is step 1 of it.
- **[Phase 10 — Cloud benchmark pipeline](#phase-10--cloud-benchmark-pipeline)** — a GitHub
  Actions workflow that runs this driver and client-v2 in the same job/VM/ClickHouse process, so
  benchmark work has a repeatable environment off the local MacBook. Planned 2026-08-22. **Done and
  validated**: `.github/workflows/benchmark.yml` + `scripts/benchmarks/analyze.py` are built,
  confirmed working end to end, and now also capture runner CPU/RAM specs and cache the ClickHouse
  image across runs. The `trusted` profile's matched-pool result has been confirmed stable across
  two independent runs (2026-08-23) — see [Phase 10](#phase-10--cloud-benchmark-pipeline) below and
  [docs/performance/results.md's cloud-verified section](docs/performance/results.md#cloud-verified-matched-pool-real-async-on-both-sides-2026-08-23).
- **[Phase 11 — Benchmark methodology hardening](#phase-11--benchmark-methodology-hardening)** —
  the cloud-verified result above changed the performance story (client-v2 now ahead on
  throughput/latency, this driver ahead on allocation) substantially enough that the methodology
  itself needs hardening before any further driver optimization. Planned 2026-08-23. **Closed**
  2026-08-24, all five PRs done.
- **[Phase 12 — Spring Boot end-to-end macrobenchmark](#phase-12--spring-boot-end-to-end-macrobenchmark-in-progress)**
  — a real WebFlux application (`DatabaseClient` → this driver or client-v2 → ClickHouse) measuring
  what the JMH suite structurally can't: HTTP/JSON/cancellation overhead layered on top of the
  driver. PR1 infrastructure is complete for point/analytics/stream with matched pool sizing and a
  manual smoke workflow. Next is PR2: an open-loop, paired A/B baseline with resource collection;
  cancellation correlation remains an explicit follow-up.

## Later (deferred, was blocked on Phase 11)

[Phase 11](#phase-11--benchmark-methodology-hardening) is now closed (PR1-PR5 done 2026-08-24):
resource-model measurement, root-cause profiling, and one evidence-driven optimization attempt all
happened, and the optimization attempt (`decoderWorkerCount`) was evaluated and explicitly not
adopted as a default — see PR5's entry for the full result. Further driver optimization is no
longer blocked on methodology, but nothing new is queued up from Phase 11 itself beyond the
architectural admission-control question PR5's follow-up left open (see its entry below), and the
JDK 21 virtual-thread decoder experiment — trusted run completed 2026-08-24: no pinning, tied
throughput, ~45% more allocation than the platform-thread decoder at matched concurrency, not
adopted as a default (see PR5's entry for the full result). Older items not
yet folded into this phase, still parked: [rewriting the decode path off client-v2's blocking
reader](#experiment-idea-not-a-decision--rewrite-the-decode-path-off-client-v2s-blocking-reader) —
an option flagged during PR3's `RowDecodingScheduler` fix, explicitly not decided or started.
Response-compression parity and the `@OperationsPerInvocation(4096)` verification, both previously
listed here, are resolved — see Phase 11's PR1.

## Explicitly not planned

- **Native TCP protocol transport.** Read ClickHouse's own Native Protocol spec first: it doesn't
  solve the multiplexing problem (one query per TCP connection, same pooling story as HTTP), and
  the protocol surface is an order of magnitude bigger than HTTP + RowBinary. Full reasoning:
  [roadmap-archive.md's Phase 1](engineering/roadmap-archive.md#decision-http-now-native-tcp-not-now-revisit-only-after-benchmarks).
- **A blind 100% line-coverage gate.** Deliberate — see [CLAUDE.md's Coverage
  section](CLAUDE.md#coverage). The real gate is a named test per "fully reactive" property, not a
  percentage.
- **Performance/benchmark iteration on the current local (MacBook M3 Pro) environment.** Waits for
  Phase 10's cloud pipeline — see "Later" above.

## Module map

| Module | Depends on | Responsible for | Published |
| --- | --- | --- | --- |
| `core` | client-v2 (decoders only) | Transport-independent domain: query/settings/`query_id`, the `Transport` port, the `Flux<ByteBuffer>`→`InputStream` bridge, row decoding. No Netty, no HTTP, no R2DBC types. | Yes |
| `transport-http` | `core` | The adapter implementing `core`'s `Transport` port over Reactor Netty. | Yes |
| `connector` | `core`, `transport-http` | The adapter implementing the R2DBC SPI on top of `core`. | Yes |
| `testkit` | `core` | Shared test infrastructure: a controlled fake HTTP server for deterministic wire-level scenarios, plus a real-ClickHouse Testcontainers DSL. | Yes |
| `benchmarks` | `core`, `transport-http`, `connector`, `testkit`, client-v2 | JMH throughput/latency/allocation measurement, this driver vs client-v2. | No |
| `examples/spring-boot-webflux-demo` | `connector` (runtime-only) | A runnable Spring Boot + WebFlux app proving the driver works end to end through Spring's R2DBC integration. | No |

Full reasoning behind this split (why five modules, why `testkit` isn't a Gradle test-fixtures
source set, why an `integration-tests` module was scaffolded and later deleted):
[roadmap-archive.md's module map](engineering/roadmap-archive.md#module-map).

---

## Phase 9 — Documentation & website redesign

**In progress.** Plan captured 2026-08-22 from a detailed information-architecture review, covering
README/ROADMAP restructuring and a new GitHub Pages documentation website. Docs-only — production
Java code doesn't change as part of this phase, beyond the tiny link fixes this restructuring
itself required. Full plan, target shape, and content-ownership decisions:
[roadmap-archive.md's Phase 9](engineering/roadmap-archive.md#phase-9--documentation--website-redesign).

Sequencing (small PRs, not one rewrite):

1. **Information architecture** (this PR) — archive the old `ROADMAP.md` into
   `engineering/roadmap-archive.md`, write this short new one, move superseded review/current-work
   docs into `engineering/`, create the `docs/` skeleton (`guide/`, `concepts/`, `reference/`,
   `operations/`, `performance/`, `architecture/`, `project/`, `internals/`), fix links.
2. **README rewrite** — short landing page (~150-250 lines); remove the full configuration table,
   full pooling reference, full performance report; link out to `docs/`.
3. **Split existing docs** into the new `docs/` tree (configuration, known limitations, pooling,
   Spring, architecture, performance, production readiness) — move existing facts, don't rewrite
   already-verified technical claims.
4. **VitePress scaffold** — `package.json`, `docs/.vitepress/*`, `docs/index.md`, the GitHub Pages
   workflow, dark/light theme, local search, navigation.
5. **Homepage styling** — hero, architecture component, feature cards, performance preview,
   production-readiness panel.

## Phase 10 — Cloud benchmark pipeline

**Stage 1 built and confirmed working (fast profile, run #4, 2026-08-23).** Plan captured
2026-08-22, directly answering the blocker every deferred performance item above has been waiting
on: a repeatable benchmark environment off the local MacBook. `.github/workflows/benchmark.yml`
(fast/trusted `workflow_dispatch` profiles,
weekly fast sanity schedule) and `scripts/benchmarks/analyze.py` (JMH `results.json` + captured
stdout latency logs + run metadata → `summary.md` + three charts) exist and are wired to
`PublicApiMatchedPoolThroughputBenchmark`. `analyze.py`'s parsing/aggregation logic is verified
against synthetic JMH-shaped fixtures (both profiles, plus a no-latency-data edge case) — it has
**not** yet been run against a real CI pass, since building it didn't happen inside an environment
with Docker/JDK 21 available to actually execute JMH. A `workflow_dispatch` run is the next step
before trusting the pipeline itself, let alone any number it produces. Building the pipeline (a
GitHub Actions workflow + a Python analysis script) is CI/tooling infrastructure, not benchmark
iteration itself — it doesn't need an exception to the standing "performance work waits for a
proper environment" rule, it's the prerequisite that rule has been naming all along. Actual
benchmark re-runs stay deferred until this pipeline exists and its results are validated stable.
Non-negotiable constraint: `thisDriver` and client-v2 run in the same job, on the same VM, against
the same ClickHouse process, every time — never separate CI jobs, which would compare two machines
under two different sets of noise, not two drivers. Full design (trust model for cloud numbers,
staged rollout, JMH JSON as source of truth):
[roadmap-archive.md's Phase 10](engineering/roadmap-archive.md#phase-10--cloud-benchmark-pipeline).

## Phase 11 — Benchmark methodology hardening

**PR1-PR5 done, phase closed 2026-08-24.** Triggered by the 2026-08-23 cloud-verified matched-pool result
(client-v2 ahead ~5-9% on throughput and lower on p50-p99 latency, this driver ahead ~2.7-2.9x on
allocation per query — replacing the earlier retracted "~4x" claim; see
[results.md](docs/performance/results.md#cloud-verified-matched-pool-real-async-on-both-sides-2026-08-23)).
That result is a real trade-off, not a clean win either way, so before tuning the driver any
further the benchmark methodology itself needs to be fully defensible first. Five PRs, in order —
each one gated on the previous, no driver optimization before PR5:

1. **PR1 — correctness/reporting only (done, docs/Javadoc, no behavior change).**
   `methodology.md` corrected: client-v2's async mode is thread-based (blocking HTTP dispatched
   onto a cached-thread-pool executor), not non-blocking — the earlier "no thread is blocked per
   in-flight query on either side" line overstated it. `analyze.py`'s `summary.md` and
   `results.md` now label p50-p99 honestly as "mean of iteration-level percentiles", not a merged
   global percentile (see PR4 for the real fix). `PublicApiMatchedPoolThroughputBenchmark`'s
   `@OperationsPerInvocation(4096)` Javadoc corrected — it had the mechanism backwards ("divides",
   not "multiplies") and left an open self-flagged question; empirically verified against the
   2026-08-23 trusted run's raw `results.json` instead (both drivers' throughput stays flat
   ~800-890 ops/s across concurrency 8/32/128, the signature of a pool-bound benchmark, only
   coherent if the reported unit really is logical queries/sec). `index.md`'s stale "the
   concurrency scenario is still where this driver wins most decisively" line corrected to
   reflect the current allocation-only advantage. Response-compression fairness reconfirmed
   already symmetric (both sides default to LZ4) — not reopened, no fix needed.
2. **PR2 — resource-model measurement (code, no driver optimization). Done 2026-08-24; first real
   `trusted` CI run (2026-08-24) caught a real bug, fixed in PR4 — see that entry.** The open
   question is whether client-v2's throughput edge is bought with materially more platform threads
   than this driver's bounded, non-blocking pipeline uses.
   `PublicApiMatchedPoolThroughputBenchmark` now splits `ThisDriverState`/`ClientV2State` into
   separate `@State(Scope.Benchmark)` classes, so a fork running one `@Benchmark` method never
   constructs, prewarms, or holds open the other side's client/pool/threads — matters once
   thread/RSS counts are measured per-process, not just for hygiene. Fixed
   `OurDriverPointQueryClient`'s benchmark-only lifecycle leak: it retained only the logical
   `Connection` and never disposed the owning `ClickHouseConnectionFactory`, so the transport pool
   and `RowDecodingScheduler` outlived every trial. The trusted CI profile also runs a background
   `/proc` sampler (`scripts/benchmarks/sample-resources.sh`) for whole-run RSS/thread coverage,
   surfaced as an optional section in `analyze.py`'s `summary.md` when present — the sampler is
   whole-run, not per-driver/per-concurrency-tier (see its own header comment for why splitting
   that further is still open). **Originally also added `-prof hs_thr` for per-fork thread counts —
   the first real `trusted` CI run of this code (2026-08-24) failed outright with
   `ClassNotFoundException: hs_thr`: JMH 1.37 has no built-in HotSpot thread-count profiler under
   that or any other name (confirmed by reading `org.openjdk.jmh.profile.ProfilerFactory`'s
   source).** That was a bug introduced here and never actually exercised until that failure —
   removed in PR4, whose entry below has the fix and the corrected profiler list. The `/proc`
   sampler remains the only thread-count source in this pipeline. Next: an actual `trusted` CI run
   of PR4's corrected profiler list to produce real numbers.
3. **PR3 — control experiments (code). `DefaultPoolSlowQueryThroughputBenchmark` done, verified
   post-fix**, the rest planned. Every matched-pool benchmark artificially equalizes both sides'
   connection pools — this new class instead leaves each driver at its own out-of-the-box default
   (this driver's Reactor Netty default, ≥16; client-v2's fixed default of 10), with queries slowed
   via `sleep(0.5)`/`sleep(1.0)` so a real difference in default pool size actually has something to
   queue behind (existing point queries finish in low single-digit ms — too fast to show contention
   even at a deliberately tiny matched pool). `concurrency` currently sweeps 8/32 only, a small
   first pass — see the class's own Javadoc for why 128 isn't in scope yet. The first `trusted` run
   (2026-08-23) surfaced a real bug rather than the intended comparison: `RowDecodingScheduler`
   defaulted to one worker per CPU core, completely independent of `transportMaxConnections`, so on
   the 4-core CI runner `thisDriver` was capped at 4-way decode concurrency regardless of its much
   larger connection pool — that run's numbers weren't published; fixed by sizing
   `RowDecodingScheduler` to the resolved connection pool size instead (see
   [connection-pooling.md](docs/operations/connection-pooling.md#the-decode-worker-pool-tracks-this-pools-size-not-the-cpu-core-count)).
   **Re-run confirmed the fix**: at concurrency=8 (below both pools' capacity) the two drivers tie;
   at concurrency=32, `thisDriver`'s 16-connection default pool now correctly beats client-v2's
   10-connection default by ~2x — see
   [results.md](docs/performance/results.md#default-pool-slow-query-this-drivers-larger-default-pool-wins-once-its-actually-used-2026-08-23)
   for the full numbers. **Remaining three items done 2026-08-24, not yet re-run/interpreted on
   CI:**
   - `ClientV2ExecutorAggressivenessBenchmark`: isolates how much of client-v2's throughput edge
     over this driver is its default `Executors.newCachedThreadPool()` async-dispatch executor
     versus something architectural, by comparing client-v2 against itself — same pool size, same
     query, same concurrency sweep as `PublicApiMatchedPoolThroughputBenchmark`, only the executor
     differs (a new `ClientV2PointQueryClient(FixedExecutorPoolSize)` constructor swaps in
     `Executors.newFixedThreadPool(poolSize)`, owned and shut down by that class since client-v2
     itself never closes a caller-supplied executor). Deliberately doesn't involve this driver — see
     the class's own Javadoc for why.
   - `PoolSizeSweepThroughputBenchmark`: fixes `concurrency=32` and sweeps `poolSize` across
     `4/8/16/32` for both drivers — a real scalability curve, not just the headline benchmark's one
     matched-pool snapshot. Manual-only: swept across four pool sizes, it's meaningfully more
     expensive than the headline benchmark at the trusted profile's 3 forks/5 warmup iterations, so
     it's a `workflow_dispatch` dropdown option only, never the weekly schedule (which stays pinned
     to `PublicApiMatchedPoolThroughputBenchmark` regardless).
   - `ConnectionPerOperationThroughputBenchmark`: measures this driver's R2DBC `Connection` through
     the shape Spring's `DatabaseClient` actually uses — `factory.create()` → statement →
     `connection.close()` per operation, via a new `OurDriverConnectionPerOperationPointQueryClient`
     (a separate class, not a third constructor on `OurDriverPointQueryClient`, since the difference
     is the shape of `query()` itself, not just configuration) — alongside, not replacing, the
     existing reuse-one-connection benchmark. client-v2's side is unchanged from
     `ClientV2PointQueryClient(int)`: it has no separate logical-connection object to open/close per
     operation, so its existing shape already is "per operation" in the sense this class cares
     about.

   All three are wired into `benchmark.yml`'s `workflow_dispatch` benchmark dropdown (raw
   `results.json`/`raw-stdout.log` only — like every benchmark except
   `PublicApiMatchedPoolThroughputBenchmark`, `analyze.py` doesn't parse their shapes). Next: actual
   CI runs to produce real numbers for all three.
4. **PR4 — root-cause the throughput/latency gap (profiling only, no driver changes). Code done
   2026-08-24, not yet re-run/interpreted on CI.**
   - **Fixed PR2's `-prof hs_thr` bug, found by the first real `trusted` CI run of this pipeline
     (2026-08-24).** That run failed outright: `Profilers failed to initialize...
     java.lang.ClassNotFoundException: hs_thr`. Confirmed by reading JMH 1.37's
     `org.openjdk.jmh.profile.ProfilerFactory` source: its `BUILT_IN` map has no `hs_thr` entry —
     the registered built-in profiler IDs in this JMH version are `async, cl, comp, gc, jfr, stack,
     perf, perfnorm, perfasm, mempool, xperfasm, dtraceasm, pauses, safepoints, perfc2c`, none of
     which is a HotSpot thread-count profiler under any name. `hs_thr` was simply never a valid
     profiler in JMH 1.37 — PR2's assumption was wrong and this had never actually been exercised
     until this run. Removed from `benchmark.yml`'s trusted profile rather than replaced: no JMH
     1.37 built-in covers per-fork thread counts, so `scripts/benchmarks/sample-resources.sh`'s
     `/proc` sampler (whole-run, not per-fork) remains the only thread-count source in this
     pipeline.
   - **Scoped down to JFR only, not JFR/async-profiler both.** async-profiler needs a downloaded
     native agent jar, which this project's sandboxed development environment has no network path
     to fetch or verify; JFR ships in the JDK itself and needs no extra artifact, so it's what's
     wired in. Revisit async-profiler later only if JFR's output turns out not to be enough.
   - The trusted CI profile now runs JMH's own `-prof jfr:dir=/tmp/jfr-output` alongside `gc` (not
     `hs_thr` — see above), on `PublicApiMatchedPoolThroughputBenchmark` at concurrency=8/32/128,
     both drivers — a first look at `FluxInputStreamBridge`'s cross-thread handoff (the leading
     suspect
     — the same mechanism was already root-caused once at streaming-scan scale, see
     [results.md](docs/performance/results.md#full-table-scan-found-partially-fixed-and-the-fixs-own-measurement-is-unstable-at-1m))
     vs. Reactor operator overhead vs. row mapping vs. LZ4 decode vs. connection acquisition. **Known
     limitation, confirmed by reading JMH's `JavaFlightRecorderProfiler` source upstream**: it writes
     one `profile.jfr` per (benchmark method, `@Param` combination), overwritten by each successive
     fork — with `forks=3`, only the last fork's recording survives on disk, not a three-fork merge.
     Acceptable for this phase's exploratory purpose (point PR5 at a specific hot path), not
     something `analyze.py` parses automatically — `.jfr` files are uploaded as raw artifacts under
     `benchmark-results/jfr/`, read locally with JDK Mission Control or `jfr print`/`jfr summary`.
   - Replaced PR1's "mean of iteration-level percentiles" label with a true merged-HdrHistogram
     calculation: `PublicApiMatchedPoolThroughputBenchmark` now accumulates every measurement
     iteration into one running `Histogram` per JMH fork (`Histogram#add`, lossless — both
     histograms share the same trackable-range/precision settings) and logs it once at trial
     teardown (`logMergedLatencySummary`, distinguishable in the log by the literal text `TRUE
     MERGED`). `analyze.py` prefers this line when present: with `forks=1` (fast profile) the
     result is an exact global percentile; with `forks=3` (trusted profile) it's a mean of three
     already-exact per-fork percentiles, not dozens of approximate per-iteration ones. Falls back to
     the old per-iteration-average behavior automatically when reprocessing a pre-PR4 log that has
     no `TRUE MERGED` lines.
   - Re-running `BoundedPoolConcurrencyBenchmark` and finishing
     `MixedWorkloadRapidRefreshCancelBenchmark`'s incomplete `thisDriver` run need no code change —
     both already have `.useAsyncRequests(true)` from an earlier fix; confirmed by reading both
     classes. Purely "needs an actual CI run", tracked as part of this PR's own "not yet re-run on
     CI" status, not a separate code item.
   - Built `MixedWorkloadRapidRefreshCancelBenchmark`'s no-cancellation companion,
     `MixedWorkloadRapidRefreshPileUpBenchmark`: identical users/queries/pooling/think-time, but
     `Flux.flatMap` instead of `switchMap` — every refresh runs to completion and piles up against
     the pool instead of being cancelled by the next one. Wired into `benchmark.yml`'s
     `workflow_dispatch` dropdown, not the weekly schedule (same reasoning as PR3's manual-only
     additions).
   - Next: an actual `trusted` CI run to produce real JFR/merged-histogram/pile-up numbers.
   - **Update (2026-08-24, mega sweep):** that `trusted` run happened — both drivers timed out on
     every fork (`thisDriver`'s 120s blocking-read timeout, client-v2's 10s connection-acquire
     timeout), zero usable data. The pile-up load this class deliberately creates exceeds what the
     shared 4-core GitHub Actions runner can drain within either timeout. Removed the class and its
     `benchmark.yml` dropdown entry rather than keep a benchmark that structurally can't produce a
     result on this pipeline's hardware — see docs/performance/results.md's "Open follow-ups" entry
     for the same note.
5. **PR5 — one evidence-driven optimization, only if PR4's profiling points at something
   specific.** PR4's first real trusted run (2026-08-24) did: p90-p99 per-query latency for
   `thisDriver` ran 15-25% behind `clientV2` at every tested concurrency (8/32/128), consistently
   across all 3 forks per tier — while p50 stayed tied (thisDriver even slightly ahead at
   concurrency=32/128) and `gc.time` stayed equal between drivers (201-255ms vs 221-246ms across
   the whole run) despite `thisDriver` allocating ~3.3x less per query throughout. That combination
   rules out GC pauses as the tail-latency cause and points at decode-worker queueing instead: this
   driver's `RowDecodingScheduler` is fixed at exactly `transportMaxConnections` workers (see
   [connection-pooling.md](docs/operations/connection-pooling.md#the-decode-worker-pool-tracks-this-pools-size-not-the-cpu-core-count)),
   so a query whose decode has to wait for a free worker pays that wait as pure added latency with
   no corresponding allocation cost — exactly the observed signature. (The 6 `.jfr` files this run
   produced weren't usable for a frame-level confirmation: no JDK with the `jfr` CLI tool, no sudo,
   and no verified Python JFR parser were available in the sandbox that did this analysis — a real
   tooling gap, not a data gap; they remain available for anyone with JDK Mission Control to inspect
   directly.)
   - **Code done 2026-08-24, not yet re-run/interpreted on CI.** Added `decoderWorkerCount`
     (`ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT`) — an explicit R2DBC option
     overriding `RowDecodingScheduler`'s worker count independently of `transportMaxConnections`,
     defaulting to `null` (unchanged, pool-coupled behavior) when not set; see
     [connection-pooling.md](docs/operations/connection-pooling.md#widening-the-decode-pool-beyond-the-connection-pool-decoderworkercount)
     for the full option and TDD-covered in `ClickHouseConnectionFactoryTest`
     (`shouldSizeTheDecoderSchedulerToAnExplicitDecoderWorkerCountEvenWhenLargerThanThePool`,
     `shouldRejectANonPositiveDecoderWorkerCount`).
   - New `DecoderWorkerCountThroughputBenchmark`: compares `thisDriver` against itself — `poolSize`
     fixed at 8 on both sides (matching the headline benchmark), `concurrency` sweeping 8/32/128 —
     only `decoderWorkerCount` differs: `CoupledDecoderState` leaves it at the default (8, today's
     behavior); `WidenedDecoderState` sets it to 32 (4x the pool) via the new
     `OurDriverPointQueryClient.ExplicitDecoderWorkerCount` constructor. Reuses the merged-
     HdrHistogram-per-fork logging from `PublicApiMatchedPoolThroughputBenchmark` (the `TRUE
     MERGED` log line) since the p90-p99 comparison this class exists to make is exactly what that
     logging makes trustworthy at `forks=3`. Wired into `benchmark.yml`'s `workflow_dispatch`
     dropdown, manual-only (same reasoning as PR3's additions).
   - **Trusted CI run (2026-08-24): decision made, default unchanged.** At `concurrency=8`
     (matching `poolSize`), `widenedDecoder` did shrink the tail as hypothesized — p90 ~14.4k µs vs
     `coupledDecoder`'s ~16.4k µs, p99 ~24.5k vs ~27.6k (roughly 10-12% better), throughput and
     `gc.time` unchanged. But at `concurrency=32` and `concurrency=128`, `widenedDecoder` produced
     **zero usable measurements** — every one of the 6 fork-runs (3 forks × 2 concurrency tiers)
     failed every iteration with `reactor.netty...PoolAcquirePendingLimitException: Pending acquire
     queue has reached its maximum size of 16`. `coupledDecoder` ran cleanly at the same
     concurrencies, same `poolSize=8`, same everything except the decoder worker count.
   - **Root cause, confirmed against the source, not just inferred from the failure:**
     `RowBinaryDecoder.decode`'s `Mono.fromCallable(() -> newReader(source, compression))
     .subscribeOn(reactorScheduler)` is where `source` — the transport response stream, and with it
     the underlying HTTP connection acquisition + request send — actually gets subscribed to for the
     first time. That means `RowDecodingScheduler`'s worker count isn't only a decode-throughput
     knob: it's also, incidentally, the real admission-control gate on how many queries can even
     start touching the connection pool at once. Coupling it 1:1 to `poolSize` (today's default)
     keeps in-flight query admission at roughly `poolSize`, safely under Reactor Netty's own default
     pending-acquire-queue limit (`2 × maxConnections` = 16 here). Widening it to 4x removes that
     incidental protection — at `concurrency` 32/128 the driver now tries to admit far more
     simultaneous queries than the connection pool's own admission control (8 connections + 16
     queued = 24 max) can hold, and the 25th+ is rejected outright rather than just queued with
     added latency.
   - **Decision: do not change the default.** The tail-latency win at matched concurrency is real
     but the failure mode at higher concurrency is categorically worse than the problem PR5 set out
     to fix (a request denied outright vs. a slower p99). `decoderWorkerCount` stays as the
     already-implemented opt-in escape hatch (see
     [connection-pooling.md](docs/operations/connection-pooling.md#widening-the-decode-pool-beyond-the-connection-pool-decoderworkercount)),
     with this finding documented there as the reason widening it requires also reconsidering
     `transportPendingAcquireMaxCount`, not just the decoder — never a name/casual default change.
     `RowDecodingScheduler`'s pool-coupling (PR3, task #273) turns out to be load-bearing for a
     reason beyond the one originally documented (avoiding a hidden, smaller-than-the-pool
     concurrency ceiling): it's also this driver's only real admission control today. Phase 11 PR5
     is closed on this finding; no exact-same-config re-run of `PublicApiMatchedPoolThroughputBenchmark`
     is warranted since no default changed.
   - **Follow-up in progress (code done 2026-08-24, not yet run on CI):** whether widening the
     decoder together with an explicit, deliberately-sized `transportPendingAcquireMaxCount`
     recovers the concurrency=8 win at 32/128 too without the outright failures — and, more
     fundamentally, whether admission control belongs on `RowDecodingScheduler` at all or should be
     its own explicit mechanism, since today it's an accident of where `subscribeOn` happens to
     sit. Working theory for *why* this might work: today's shape is two queues in series (the
     decoder's bounded-elastic queue, then Reactor Netty's own pending-acquire queue), and tandem
     queueing is a known way to compound tail latency beyond what either queue alone would produce
     at the same throughput — matching the shape of the observed symptom (p50 tied, only p90-p99
     diverges). If confirmed, widening the decoder collapses it back to one queue (the physical
     connection pool, which is the real, unavoidable bottleneck either way).
   - New `DecoderAndPendingAcquireWidenedThroughputBenchmark`: a fourth `OurDriverPointQueryClient`
     constructor (`ExplicitDecoderWorkerCountAndPendingAcquireLimit(poolSize, decoderWorkerCount,
     pendingAcquireMaxCount)`) sets `decoderWorkerCount=32` and `transportPendingAcquireMaxCount=256`
     together (both deliberately generous, not minimally tuned, same reasoning as PR5's own
     `WIDENED_DECODER_WORKER_COUNT`), swept across `concurrency` 8/32/128, same merged-histogram
     logging pattern as the rest of Phase 11's benchmarks. Deliberately a separate class from
     `DecoderWorkerCountThroughputBenchmark` rather than a third state bolted onto it — that class's
     own two states already answer "does widening the decoder alone help or hurt"; this class's
     result should be read against its already-recorded `coupledDecoder` numbers, not re-measured.
     Wired into `benchmark.yml`'s `workflow_dispatch` dropdown, manual-only.
   - `ClickHouseConnectionFactory` now logs its effective settings (resolved `decoderWorkerCount`,
     `transportMaxConnections`, `transportPendingAcquireMaxCount`, `transportPendingAcquireTimeout`
     — `"unset"` for whichever fall back to Reactor Netty's own defaults) once at construction, via
     `logEffectiveSettings` — explicit user guidance (2026-08-24): a caller debugging a tail-latency
     or pending-acquire-queue problem in production shouldn't need to already know this
     investigation to find the numbers in play.
   - **Trusted CI run (2026-08-24): tandem-queueing theory not confirmed, follow-up closed, default
     stays unchanged.** All three concurrencies completed with zero `PoolAcquirePendingLimitException`
     failures — the immediate goal (stop the outright rejections) was met. But the tail-latency win
     did not carry over: at `concurrency=8` the combined config landed between `coupledDecoder` and
     `widenedDecoder`-alone (p90/p95/p99 still ~5-9% better than coupled, but p50 ~10% worse and
     slightly behind decoder-widening-alone); at `concurrency=32` it was roughly tied with
     `coupledDecoder` (p50/p90 marginally better, p95/p99 marginally worse); at `concurrency=128` it
     was **worse than `coupledDecoder` across every percentile including p50** (roughly +5-9%).
     Throughput stayed flat around 700-750 ops/s at every concurrency tested — no scaling beyond the
     8-connection pool, exactly as expected for a physically pool-bound workload.
   - **Root cause of why the theory didn't hold, from an independently JFR-profiled analysis of this
     same run** (JDK Mission Control locally, filling the frame-level gap this sandbox's tooling
     couldn't close — see PR4's entry): `jdk.ThreadPark` stacks show decoder-scheduler threads
     spending the overwhelming majority of their time blocked in
     `FluxInputStreamBridge.takeNextSignal`/`ArrayBlockingQueue.take` — i.e. waiting for network
     bytes, not doing CPU decode work. At `concurrency=32`/`128`, roughly 31 of the 32 widened
     decoder workers were parked at any given moment (aggregate park time ≈ 30-31 threads
     continuously parked across the whole recording), while active JVM thread count rose from 34
     (coupled, 8 workers) to 53 (widened, 32 workers) — 19 extra threads, matching the 19 extra
     decoder-worker names observed, for no measurable throughput or consistent latency benefit. The
     "two queues in series, collapse to one" framing was also arithmetically wrong as stated: with
     `decoderWorkerCount=32`, at most ~32 requests can ever be admitted to the transport layer at
     once regardless of `concurrency`, so `concurrency=128` never produces anywhere near "120
     pending acquisitions" — a decoder-scheduler queue still exists upstream of the transport at
     that concurrency; widening `transportPendingAcquireMaxCount` didn't remove a queue, it just
     stopped the (much smaller, ~24-request) transport-level queue from overflowing.
   - **Real takeaway:** `RowDecodingScheduler`'s worker count was never purely a decode-parallelism
     knob — decode is I/O-wait-dominated, not CPU-bound, so more workers mostly means more threads
     parked waiting on the same 8 physical connections' worth of network throughput, at a real
     resource cost (extra JVM/OS threads) or the reasoning above corrects itself. The accidental
     admission-control role this pool plays (see the earlier entry above) is doing more of the real
     work than decode parallelism ever was. **Decision, confirmed twice now: do not change the
     `decoderWorkerCount`/`transportPendingAcquireMaxCount` defaults.** Both stay opt-in escape
     hatches, not defaults.
   - **Methodology gaps this run also surfaced** (not yet acted on, logged here so they aren't lost):
     this benchmark's own Javadoc overstated the "collapses to one effective queue" framing and the
     120-pending-acquisitions worst case — needs correcting to reflect the ~32-admission ceiling
     above; JFR recordings are still one-per-fork-overwritten (the same documented JMH limitation as
     PR4's, not new, but this run's JFR analysis is therefore of the *last* fork only, not all
     three); the first measurement iteration of every fork ran ~30-40% slower than iterations 2-3
     even after 5 warmup iterations, a large enough and systematic enough gap to warrant a
     profiler-free control run before trusting this run's own throughput/allocation numbers as a
     clean baseline; `TRUE MERGED` histograms are still per-fork, not merged *across* forks (would
     need the encoded histogram, not just its computed percentiles, persisted and merged offline);
     `metadata.json` doesn't record the experiment-specific knobs (`decoderWorkerCount`,
     `transportPendingAcquireMaxCount`, JMH version, profiler list) that made this run interpretable
     only because they were still visible in source/log output.
   - **Not started, explicit user guidance (2026-08-24), now further gated on the above:** the
     longer-term goal of these values auto-sizing themselves relative to each other without a caller
     ever setting `decoderWorkerCount`/`transportPendingAcquireMaxCount` by hand doesn't have a clear
     target shape yet, since this run showed widening isn't simply "the more the better" once
     I/O-wait-dominated decode is accounted for — the actual auto-tuning rule (if one exists) would
     need to come from the deeper architectural question below, not from this experiment.
   - **Next, if this line of work continues:** separate architectural question, not a tuning
     question — should connection acquisition be able to happen without first consuming a blocking
     decoder-worker slot, so a decoder worker is only assigned once a response stream actually has
     bytes ready to read? That would let admission control (how many queries may be in flight)
     and decode-worker sizing (how much CPU-bound decode parallelism is useful) be governed by two
     separate, purpose-built numbers instead of one number doing both jobs by accident. Not started;
     needs its own smaller characterization experiment before any implementation, per the same
     "measure before deciding" gate PR5 itself was built on.
   - **JDK 21 virtual-thread decoder experiment, code done 2026-08-24, not yet run on CI** — a
     different fix for the same I/O-wait-dominated finding above, motivated directly by it: instead
     of widening the platform-thread pool further (more parked platform threads, real resource
     cost, no throughput gain), run decode tasks on JDK 21 virtual threads, which park/unpark
     cheaply and don't hold a platform thread while blocked. Explicit user instruction: build the
     variant, and review client-v2's actual decode-path source first to check whether virtual
     threads make sense there (pinning risk).
     - **Pinning-risk review, against the actual upstream source** (the `clickhouse-java` repo is a
       connected folder, resolving an earlier session's "no jar access to check statically"
       limitation): grepped `RowBinaryWithNamesAndTypesFormatReader.java` and
       `BinaryStreamReader.java` for `synchronized`. Exactly one hit,
       `BinaryStreamReader.ArrayValue#asList()` — a pure in-memory list-view cache over an
       already-fully-read array (`Array.get`/`Array.set` in a loop), never blocks on I/O while
       holding the monitor. The actual blocking reads (`BinaryStreamReader.readNBytes` →
       `inputStream.read(buffer, offset, len)`, and the single-byte `input.read()` path) run
       directly against the caller-supplied `InputStream` with no `BufferedInputStream`/
       `DataInputStream` wrapping in between (confirmed via `AbstractBinaryFormatReader`'s
       constructor and `RowBinaryWithNamesAndTypesFormatReader`'s own constructor — the stream this
       driver passes in, `FluxInputStreamBridge`, is used unwrapped). `FluxInputStreamBridge` itself
       has zero `synchronized` usage (grep-verified). **Conclusion: this driver's decode path is
       Loom-friendly today** — no `synchronized` block sits on the hot blocking-read path, on either
       side of the client-v2/driver boundary.
     - `RowDecodingScheduler.virtualThreads(maxConcurrency)`: a new factory alongside
       `withWorkerCount`, same worker-count contract (a caller-visible capacity guarantee, not an
       implementation detail — `workerCount()` returns `maxConcurrency` for this variant too),
       different mechanism underneath. Runs every task on its own named virtual thread
       (`Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(...))`), gated by a private
       `AdmissionGatedExecutorService` wrapping a `Semaphore(maxConcurrency)` around task execution —
       preserves "at most `maxConcurrency` decode tasks running at once" exactly, the same
       admission-control contract PR5's root-cause finding showed this pool incidentally provides,
       just with cheap parked virtual threads instead of held platform threads for the rest. New
       `isVirtualThreadBacked()` accessor. TDD-covered in `RowDecodingSchedulerTest`, including a
       black-box concurrency-cap proof (`shouldCapConcurrentDecodeTasksAtTheProvidedMaxConcurrencyEvenOnVirtualThreads`
       — a `CountDownLatch`-gated task set proves via Awaitility that a 4th task never starts while
       3 are already running with `maxConcurrency=3`, no `Thread.sleep`, no loop/conditional inside
       the test body, per CLAUDE.md's testing rules).
     - New R2DBC option `ClickHouseConnectionFactoryProvider.DECODER_USE_VIRTUAL_THREADS` (default
       `false`, unchanged behavior) — an experimental, opt-in escape hatch, same shape as
       `decoderWorkerCount` was for PR5, not yet a trusted-benchmark-validated default. Wired through
       `ClickHouseConnectionFactory.from`, surfaced in `logEffectiveSettings`'s log line
       (`decoderUsesVirtualThreads=...`) alongside the other concurrency-relevant settings, and
       through a package-private `decoderUsesVirtualThreads()` test window, same pattern as
       `decoderWorkerCount()`. TDD-covered in `ClickHouseConnectionFactoryTest`
       (`shouldUsePlatformThreadDecoderByDefault`, `shouldUseVirtualThreadDecoderWhenExplicitlyEnabled`,
       `shouldStillCoupleTheVirtualThreadDecoderMaxConcurrencyToThePoolSize`).
     - New `VirtualThreadDecoderThroughputBenchmark`: compares `thisDriver` against itself, same
       `poolSize=8` and `concurrency` 8/32/128 sweep as `DecoderWorkerCountThroughputBenchmark` —
       only the decoder's thread type differs, worker count held equal (`WORKER_COUNT = POOL_SIZE`)
       on both sides, deliberately not widened, since this experiment isolates thread type as the
       one variable, not capacity (that question already has its own benchmark/answer above). New
       fifth `OurDriverPointQueryClient` constructor
       (`VirtualThreadDecoder(poolSize, decoderWorkerCount)`), same reasoning as the earlier
       `ExplicitDecoderWorkerCount*` records for why a distinct type instead of a boolean parameter.
       Reuses the same `TRUE MERGED` merged-histogram logging as the rest of Phase 11's benchmarks.
       Wired into `benchmark.yml`'s `workflow_dispatch` dropdown; `build.gradle.kts`'s `jmh` block
       gained a `-Pjmh.jvmArgsAppend` passthrough (comma-split, same shape as `jmh.profilers`), and
       `benchmark.yml` appends `-Djdk.tracePinnedThreads=full` automatically whenever this benchmark
       class is selected (either profile) — a pinned virtual thread prints its full stack to
       `raw-stdout.log`, so pinning would show up directly in the run's own artifact rather than
       only as an unexplained latency regression.
     - **Correctness fix, 2026-08-24, caught before any benchmark run:** an independent review
       (external LLM-generated implementation notes, cross-checked against the actual code before
       acting on them) flagged that `AdmissionGatedExecutorService.execute()` originally used
       `admission.acquireUninterruptibly()` — a task still waiting for a permit when its Reactor
       subscription is cancelled would ignore that cancellation and could still run once a permit
       later freed up, since `acquireUninterruptibly()` swallows interrupts entirely. Cancellation
       propagation is a property this driver treats as load-bearing everywhere else (see
       `RowBinaryDecoder.decode`'s own cancellation test), so a decoder-scheduler variant that could
       silently ignore it would have been a real regression, not a cosmetic one. Fixed by switching
       to interruptible `Semaphore#acquire()`: a task interrupted while waiting for a permit now
       restores the interrupt flag and returns without ever calling `command.run()`. Since
       `ExecutorService#shutdownNow()` (what `dispose()` calls on the owned executor) interrupts
       every running/queued task, this is also what makes `dispose()` actually terminate virtual
       threads still parked waiting for a permit, rather than leaving them stuck.
     - Three new black-box tests in `RowDecodingSchedulerTest`, each exercising the fix through the
       real `Scheduler.schedule(Runnable)`/`Disposable` API (not the private executor directly):
       `shouldNeverRunACommandThatWasCancelledWhileWaitingForAnAdmissionPermit` (a disposed,
       still-waiting task must never run even once its permit frees up),
       `shouldTerminateAWaitingTaskWhenTheSchedulerIsDisposedBeforeAPermitFreesUp` (`dispose()` must
       not leave a waiting task stuck forever), and
       `shouldStillReleaseTheAdmissionPermitWhenACommandThrows` (a failing task must never leak
       admission capacity). All three rely on `Semaphore`'s own documented interrupt-priority
       behavior (an interrupted waiter never receives a permit, regardless of a concurrent release)
       rather than on any additional synchronization this class would otherwise need to add just to
       make the tests deterministic.
     - New production-path tests in `RowBinaryDecoderTest`, run through the real decode chain
       (`RowBinaryDecoder.decode` → `RowDecodingScheduler.virtualThreads` → `FluxInputStreamBridge` →
       client-v2's reader) rather than only synthetic `Runnable` submission:
       `shouldDecodeEveryRowInOrderThroughAVirtualThreadBackedSchedulerFromFragmentedChunks` (wire
       bytes delivered one byte at a time, proving the bridge's block/resume cycle works correctly
       across many fragments on a virtual thread, not just a single chunk) and
       `shouldReadEveryRowOnAVirtualThreadWhenUsingTheVirtualThreadBackedScheduler` (asserts the
       thread name observed while reading a row actually starts with the virtual-thread prefix, not
       just that decoding succeeded).
     - `RowDecodingScheduler.virtualThreads(int)` and `AdmissionGatedExecutorService`'s own Javadoc
       corrected to say precisely what's bounded: *active* decode concurrency, not the number of
       virtual threads created — a caller submitting far more tasks than `maxConcurrency` at once
       gets that many additional parked (not running) virtual threads, which is cheap but not
       zero-cost, and is a materially different queueing shape than
       `Schedulers.newBoundedElastic(workerCount, queuedTaskCapacity, ...)`'s bounded queue.
     - **Trusted-run result, 2026-08-24 (`poolSize=8`, `concurrency` 8/32/128, 3 forks, 5 warmup
       iterations, `-Djdk.tracePinnedThreads=full`):**
       - **Throughput: tied at every concurrency level.** `platformThreadDecoder` 810–837 ops/s vs
         `virtualThreadDecoder` 804–826 ops/s, with ~30% error bars on both sides — the difference is
         well inside measurement noise. Expected, in hindsight: both variants are admission-gated to
         the same `maxConcurrency` (tied to `poolSize`), which is exactly the property that makes the
         result barely move across the 8/32/128 sweep on either side too — virtual threads' actual
         advantage (cheap concurrency *beyond* what a platform-thread pool could hold) never gets
         exercised when both sides are capped at the same number.
       - **Allocation: virtual threads cost ~45–48% more per op** (28.2–28.7 KB/op vs 19.2–19.4 KB/op,
         consistent across all three concurrency levels), with correspondingly more GC activity
         (53–62 vs 47–51 GC cycles, 228–310ms vs 190–250ms GC time). Consistent with the real cost of
         `Executors.newThreadPerTaskExecutor` creating a fresh virtual thread per decode task, instead
         of reusing threads from a fixed platform pool.
       - **Pinning: none.** `jfr print --events jdk.VirtualThreadPinned` against all six
         `profile.jfr` recordings (both variants, all three concurrency levels) returned zero events —
         confirms the static pinning-risk review above empirically, not just by source inspection.
       - **Verdict: not adopted as a default.** At matched admission-gated concurrency, this
         implementation trades a real, measured allocation/GC cost for no throughput benefit. Safe
         (no pinning), but not worth it in its current shape. The scenario where virtual threads could
         still plausibly help — logical concurrency swept *past* the platform-thread-pool size while
         platform threads stay capped at `poolSize` (test matrix "C" from the reviewed hints doc,
         §16) — was not run; left as a follow-up if this line of work continues, not started because
         nothing in this result motivates it yet.
       See [connection-pooling.md](docs/operations/connection-pooling.md#an-alternative-fix-for-the-same-finding-decoderusevirtualthreads)
       for the option's own docs.

### `FluxInputStreamBridge` queue/copy overhead — measured, ruled out as a bottleneck (2026-08-24)

A reviewed external hints doc (external LLM-generated implementation notes on
`FluxInputStreamBridge`, cross-checked against the actual source before acting on them — same
due-diligence pattern as the virtual-thread experiment above) argued that `ArrayBlockingQueue.take()`
showing up in JFR mixes two very different costs: real queue/coalescing/copy overhead (removable)
versus legitimate waiting for the next network chunk (not removable by any queue implementation).
It proposed a step-by-step plan starting with a zero-copy multi-buffer read (dropping the current
`coalesce()` path's `ArrayList` + merged-`ByteBuffer` allocation + full-byte copy) before touching
anything else, specifically warning not to build a faster queue first without measuring which cost
actually dominates.

New `FluxInputStreamBridgeMicrobenchmark` (`clickhouse-r2dbc-reactive-benchmarks`, no ClickHouse
container, no production code touched) measured the *current* implementation directly, isolating
pure CPU cost from network wait:

- **`producerAhead`** (every chunk already queued before the first `read()` — `Flux.fromIterable`
  emits synchronously up to the bridge's initial demand): a consistent **~40–50 ns/chunk** across
  every chunk-size/response-size combination (e.g. 1024 B × 1024 chunks: 41.5us/op → 40.5ns/chunk;
  4096 B × 256 chunks: 10.1us/op → 39.6ns/chunk). This is the actual, current CPU cost of the
  queue/coalescing/copy path this hints doc's zero-copy step would remove.
- Compared against the **~8–12us/chunk** figure `FluxInputStreamBridge`'s own "Chunk coalescing"
  Javadoc cites from real `StreamingScanBenchmark` instrumentation — a **~200x gap**. Extrapolated
  to a 1M-row scan (~3545 chunks per that same instrumentation), the *entire* queue/coalescing/copy
  cost is ~150–180us out of a ~94,000–131,000us total operation — even a hypothetical 100%
  elimination of that cost (zero-copy's realistic upper bound) would be immeasurable against the
  total, and nowhere close to explaining the still-unresolved fork-to-fork 1M variance from
  [docs/performance/results.md](docs/performance/results.md#why-the-1m-number-wont-sit-still) — see
  task list item "Confirm chunk-handoff hypothesis for StreamingScanBenchmark regression": this
  result rules out the *queue/copy* portion of that hypothesis specifically, not chunk handoff or
  cross-thread wait timing in general, which remains open.
- **`consumerAheadNetworkDelayed`** (a dedicated background thread trickling chunks with an intended
  10us gap, so `read()`'s `queue.take()` genuinely blocks between chunks) has a methodology caveat
  worth recording honestly: the actual measured gap came out to **~75–80us/chunk**, not the
  requested 10us, consistently across every configuration — `LockSupport.parkNanos`'s real
  resolution on the (shared, virtualized) GitHub-hosted CI runner used for this run is coarser than
  requested, not a property of the bridge. The scenario still qualitatively confirms `queue.take()`
  dominates once a producer is slower than the consumer, but its absolute numbers aren't precise
  enough to compare quantitatively against the 8–12us production figure the way `producerAhead`'s
  numbers are.

**Verdict: this is the hints doc's own "Possible result C"** (*"neither changes real ClickHouse
benchmark... queue overhead was not the real bottleneck. Focus on: waiting for network, decoder
worker scheduling, blocking client-v2 reader, admission control."*) — confirmed directly from the
baseline measurement alone, without needing to build the proposed zero-copy variant at all: a ~200x
gap between the removable cost and the cost the doc itself cites as the real bottleneck is decisive.
**Not building the zero-copy/SPSC-queue candidate** — the plan's steps 4 onward (zero-copy read,
`StreamSignal.Data` allocation removal, SPSC ring buffer, spin/park wait strategy) are not worth the
implementation and correctness-testing cost (see the doc's own required race/cancellation test list)
for a ceiling this small. `FluxInputStreamBridgeMicrobenchmark` stays in the repo as the
baseline measurement and as a decisive negative result, not as unused scaffolding.

### Experiment idea, not a decision — rewrite the decode path off client-v2's blocking reader

Flagged 2026-08-24 while investigating the `RowDecodingScheduler`/connection-pool coupling bug
(Phase 11 PR3, see [connection-pooling.md](docs/operations/connection-pooling.md#the-decode-worker-pool-tracks-this-pools-size-not-the-cpu-core-count)).
This driver reuses client-v2's `RowBinaryWithNamesAndTypesFormatReader` for decoding — a proven,
spec-compliant `RowBinaryWithNamesAndTypes` parser, but one written against a blocking
`InputStream`, not Reactor's non-blocking primitives. That's why `RowDecodingScheduler` needs to
exist at all: every row read is a blocking call that has to be explicitly moved off Reactor Netty's
event-loop threads (see [fully-reactive.md's new
section](docs/concepts/fully-reactive.md#why-row-decoding-needs-its-own-scheduler-at-all) for the
full mechanism). The pool-coupling bug just fixed was a symptom of this design: a second, separate
concurrency ceiling (the decode scheduler) that has to be kept manually in sync with the connection
pool, rather than not existing in the first place.

The more radical alternative: write this driver's own non-blocking `RowBinaryWithNamesAndTypes`
decoder directly against the `Flux<ByteBuffer>`/`ChunkBuffer` stream this driver already has,
instead of bridging into an `InputStream` for client-v2's reader. If done, this would remove
`RowDecodingScheduler` entirely — no dedicated worker pool, no second concurrency budget to keep
matched to the connection pool, one less moving part in the whole pipeline. It also removes this
driver's last remaining *behavioral* (not just build-time) dependency on client-v2 for the query
path.

**Not a decision — an option to weigh, deliberately not started:**

- Real cost: `RowBinaryWithNamesAndTypes` is ClickHouse's binary wire format, with its own type
  encoding for every column type this driver already supports (see the type-coverage table in
  [README.md](README.md)) — reimplementing and re-verifying all of that correctly is a substantial,
  correctness-critical undertaking, not a quick rewrite.
- Real benefit is currently unquantified: PR2's resource-model measurement (thread count, CPU, RSS)
  is the planned next step that would actually show how much `RowDecodingScheduler`'s thread hop
  costs today, before spending the effort above to remove it. Don't commit to the rewrite before
  that number exists.
- If PR2/PR4's profiling shows the cross-thread handoff is a small, absorbable cost, this idea stays
  parked — reusing client-v2's decoder remains the right trade-off (one proven parser to maintain,
  not two).

## Phase 12 — Spring Boot end-to-end macrobenchmark (in progress)

Proposed 2026-08-24 via an external review doc (cross-checked against current `main`, same
due-diligence pattern as the virtual-thread and `FluxInputStreamBridge` reviews above — see task
list item "incorporate macrobench + performance review doc" for the full source). Everything in
[Phase 11](#phase-11--benchmark-methodology-hardening) and the [mega
sweep](docs/performance/results.md#full-mega-sweep--every-scenario-one-run-2026-08-24) measures
this driver at the JMH/public-SPI level. PR1 now supplies the real Spring Boot WebFlux application,
`DatabaseClient`, HTTP server, and JSON/NDJSON encoding. The layer still missing is a trusted
open-loop measurement: does the small JMH-level latency gap still matter once that whole request
path is included, and does streaming/allocation behavior hold up the same way end to end?

**Goal:** `load generator → Spring Boot WebFlux → same endpoint contract → this driver or client-v2
→ same ClickHouse instance → same SQL/data/response DTO`. Complements the JMH suite, does not
replace it.

**Shape of the module** (new, non-published — `clickhouse-r2dbc-reactive-macrobench`, depending on
`project(":clickhouse-r2dbc-reactive-connector")` so it benchmarks current source, not the
published release the way `examples/spring-boot-webflux-demo` deliberately does):

- One `BenchmarkQueryBackend` interface, two implementations (`R2dbcBenchmarkQueryBackend`,
  `ClientV2BenchmarkQueryBackend`), four scenarios each: `point` (per-request overhead),
  `analytics` (real multi-JOIN/GROUP BY/aggregation query, server-side-seeded ~5M-row fact table +
  dimensions via `INSERT ... SELECT FROM numbers(...)`), `stream` (10k/100k/1M-row NDJSON, not
  buffered JSON), `cancel` (abort in-flight HTTP requests, correlate against
  `system.processes`/`system.query_log` via a query-id prefix, confirm pool recovery).
- `benchmark.backend=both` (dual, for local A/B) vs `benchmark.backend=r2dbc`/`client-v2`
  (isolated — required for trusted CPU/RSS/thread measurements, since an idle backend's threads
  still contaminate process-level resource numbers in dual mode).
- Primary fairness config: 8 physical connections both sides, **no outer `io.r2dbc.pool`** on the
  R2DBC side (the real pool is already `ClickHouseHttpTransport`'s Reactor Netty
  `ConnectionProvider` — an outer logical pool would add a queue this project's own docs already
  say most users don't need, contaminating the primary comparison; a separate scenario can quantify
  its cost later), `useAsyncRequests(true)` explicit on client-v2 (the same fairness bug Phase 11
  PR1 already fixed once — see [archive.md](docs/performance/archive.md)), response compression on
  both.
- Open-loop load (k6 constant-arrival-rate or wrk2), not `ab` (closed-loop, hides tail behavior
  under overload) — `ab` stays a local smoke-test tool only. Paired A/B rounds alternating order (≥5
  pairs), deliberate warmup phase excluded from measurement, `system.query_log` correlation to
  separate "HTTP end-to-end" from "ClickHouse execution time," process CPU/RSS/thread/GC collection
  per isolated run. Manual `macro-benchmark.yml` workflow, not on every PR.

**Current PR sequence:** PR1 infrastructure is complete. PR2 is the next step: add the open-loop
load generator, paired A/B rounds, resource collector, then run and document a matched-pool
baseline. The previously proposed copy/admission/decoder optimization sequence has already been
investigated independently by PR #99 and produced no justified production change; do not revive it
from this older plan. Any production optimization after PR2 requires new macrobenchmark evidence
and a separate before/after design.

**PR1 status (2026-08-24): infrastructure built and smoke/local-run verified.** The
`clickhouse-r2dbc-reactive-macrobench`
module exists with the `point`/`analytics`/`stream` scenarios, both backends, dataset seeding, the
`BenchmarkController` endpoint contract, and a manual `.github/workflows/macro-benchmark.yml`
smoke check (boots the app, seeds a small dataset, curls each active backend's endpoints — not a
load test). Deliberately narrower than the full PR1 description above, disclosed rather than
silently dropped: the `cancel` scenario is not implemented (needs per-backend cancellation-signal
wiring plus `system.processes`/`system.query_log` correlation — a materially different, riskier
piece than the other three scenarios, tracked as a PR1 follow-up rather than rushed in
unverified), and there's no k6/wrk2 open-loop load generator, paired-A/B-round script, or resource
collector yet — those are PR2's actual job per the sequence above, not scope creep to add here. A
small `scripts/run-ab.sh` was added for quick local iteration (warmup pass discarded, then a
measured `ab` run per backend/scenario) — explicitly documented as a local tool only, not a
substitute for PR2's open-loop methodology, since `ab` is closed-loop and can't show tail-latency
behavior under real overload.
The module now participates in the repository's regular Gradle build and test gates; its controller,
backend selection, and benchmark properties have unit coverage. The manual workflow remains a smoke
check, not a trusted load test.

**First real local run + a fairness bug it surfaced (2026-08-24).** The user ran
`scripts/ab-summary.sh stress` (50000 requests, concurrency 200, warmup 5000) locally against both
backends — the first actual execution of this module, not just a source review. Numbers are in
`clickhouse-r2dbc-reactive-macrobench/README.md`'s "Local results" section. That first run exposed
exactly the fairness gap this Phase's own "8 physical connections both sides" design goal (above)
was meant to prevent: neither backend had an explicit pool size wired up yet, and their defaults
aren't equal — this driver defaulted to Reactor Netty's `max(availableProcessors, 8) * 2` (24
connections on that machine), client-v2 to its own default of 10. Fixed same-day: `BenchmarkProperties`
now carries a `poolSize` field (`benchmark.pool-size` / `MACROBENCH_POOL_SIZE`, default `8`), wired
into both `R2dbcBackendConfiguration` (`transportMaxConnections`) and `ClientV2BackendConfiguration`
(`setMaxConnections`), so every future run is pinned to an equal, explicit, known pool size by
default instead of silently comparing two different resource budgets. Also added: `scripts/ab-summary.sh`
(runs the full backend x scenario matrix, prints one comparison table instead of six raw `ab`
reports) and a `stress` profile (`ab-summary.sh stress` — 50000 requests/concurrency 200/warmup
5000, vs the `quick` default's 2000/10/200) for heavier local load than the smoke-test defaults.
The README's documented numbers predate the pool-size fix and are explicitly labeled unreliable for
that reason - PR2's actual trusted baseline (matched pool confirmed, real dataset sizing, k6/wrk2
open-loop load, dedicated CI machine) is still open.

**Candidate follow-up findings the review doc flagged from reading current `main`** (documented
here, none implemented yet — do not reopen without new evidence, per the doc's own instruction):

- **Byte-copy in the decode path.** `ClickHouseResult.decodePlain`/`decodeObserved` do
  `response.body().asByteArray().map(ByteBuffer::wrap)` — a `byte[]` allocation per inbound Netty
  chunk before it reaches `FluxInputStreamBridge`. Already tracked: task list item "Benchmark:
  remove ByteBuf->byte[]->ByteBuffer copy in ClickHouseResult.decode." Doc's explicit warning worth
  keeping: don't naively swap in `ByteBuf.nioBuffer()` — Netty ref-counting/lifetime means
  `FluxInputStreamBridge` retaining a chunk past `onNext` makes a zero-copy view unsafe unless
  ownership/release is redesigned; build an isolation benchmark first (parallel to the disciplined
  approach the [`FluxInputStreamBridge` queue/copy
  investigation](#fluxinputstreambridge-queuecopy-overhead--measured-ruled-out-as-a-bottleneck-2026-08-24)
  above already used), only adopt if the real macro/JMH numbers improve.
- **Decoder scheduler doubles as transport admission control.** `RowBinaryDecoder.decode`'s
  `subscribeOn(RowDecodingScheduler)` means transport subscription/connection-acquire begins under
  decoder-scheduler admission — two different concerns (decode scheduling, transport admission)
  accidentally sharing one gate. This is the same territory as the [experiment idea
  above](#experiment-idea-not-a-decision--rewrite-the-decode-path-off-client-v2s-blocking-reader)
  and the still-open [matched-pool latency
  gap](docs/performance/results.md#open-follow-ups) — a narrower, concrete prototype question
  (*can transport acquisition happen without consuming a decoder worker, while the blocking reader
  still never runs on the event loop?*) worth trying before committing to that experiment's much
  larger "own non-blocking decoder" scope.
- **Small-query fixed overhead, unmeasured.** `ClickHouseStatement`'s per-construction
  `parameterNamesIn(sql)`, plus per-execution `ClickHouseQuery` construction (UUID `query_id`,
  parameter-encoding map, immutable-map copy) — plausible contributor to the small, consistent
  point-query/`SELECT 1` deficit the mega sweep shows, not yet profiled. Build a microbenchmark
  around `Connection.createStatement(sql)`/bind/execute-preparation and profile allocation before
  reaching for a cache — explicitly not a global unbounded SQL cache.
- **Formal latency-path-isolation plan (2026-08-25), supersedes the three informal bullets above
  with a disciplined A/B/C/D benchmark ladder.** A second, more rigorous ChatGPT-authored brief
  ("CLAUDE_LATENCY_PATH_ISOLATION") formalizes exactly the three items above into one controlled
  experiment instead of three separate ad-hoc investigations. Explicitly diagnostic-only — **no
  production code changes** in this pass, not even client-v2 modifications beyond a
  benchmark-local, non-published, minimal-type-coverage adapter (Variant D). Plan, condensed:
  - **Variant A** — exact current production path (`ClickHouseHttpTransport` →
    `response.body().asByteArray().map(ByteBuffer::wrap)` → `FluxInputStreamBridge` →
    `RowDecodingScheduler` → client-v2's blocking RowBinary reader → `DecodedRow`), faithfully
    reproduced, not simplified. Baseline.
  - **Variant B** — same path, but avoid the `ByteBuf`→`byte[]`→`ByteBuffer` copy only if ownership
    can be proven correct (no use-after-release, no leak, correct cancel/error/full-consumption
    cleanup, verified under `-Dio.netty.leakDetection.level=paranoid`). Never naively retain
    `ByteBuf.nioBuffer()` past `onNext`.
  - **Variant C** — the most important one: prototype transport response acquisition/subscription
    starting *before* decoder-scheduler admission, instead of today's
    `Mono.fromCallable(() -> newReader(source, compression)).subscribeOn(decoderScheduler)` (which
    means `RowDecodingScheduler` gates transport admission too, not just decode). Must not buffer
    the whole response, block the event loop, or change cancellation/connection-reuse/pool-size/
    decoder/compression. If genuinely impossible without a major redesign, document exactly why
    instead of forcing it.
  - **Variant D** (optional, only after A/B/C) — a benchmark-local `BinaryInput` interface
    (`read()`/`readFully(...)`) adapting only the minimum client-v2 `BinaryStreamReader` source
    needed to remove the blocking-`InputStream` boundary, supporting only the types the benchmark
    actually needs (UInt64/String/Float64/Decimal/Nullable) — not full type coverage, not a
    published fork, not upstream-modifying. Stop if the measured gain is negligible; do not evolve
    into a full decoder.
  - **Scenarios**: `SELECT 1`, single-row point lookup, 10k-row stream — deliberately *not* the
    1M-row scan the mega sweep already covers, since fixed per-query overhead is exactly what a
    large scan amortizes away.
  - **Concurrency**: start at `pool=8`, `concurrency=1` (isolates fixed overhead, minimal queueing)
    and `concurrency=8` (full matched-pool utilization) before touching 32/128 (introduces queueing
    that can hide a ~100µs fixed cost).
  - **Explicitly out of scope for this pass**: virtual-thread decoder default, decoder worker
    widening, pending-acquire tuning, `ArrayBlockingQueue`/SPSC replacement,
    `FluxInputStreamBridge` coalescing rewrite, response compression defaults, default pool size, a
    full custom RowBinary decoder, and the Spring Boot macrobenchmark above — separate questions,
    not this one.
  - **Deliverable**: exact pipeline diagram from source, exact subscription/scheduling boundary
    locations, A/B/C/D result table (clean 3-fork/5-warmup/5-measurement runs, no JFR, plus a
    separate 1-fork JFR diagnostic run), a hypothesis-ranking decision table (byte-copy /
    scheduler-admission-placement / InputStream-adaptation / fixed-statement-setup — keep/reject
    each with the measured delta), and **exactly one** recommended next production change (or an
    explicit "no production change justified yet"). Falls back to profiling
    `ClickHouseStatement`/`ClickHouseQuery` construction (the third informal bullet above) only if
    A/B/C/D don't explain the gap.
  - **Status: Variant D confirms a real per-row/per-column reader-layer cost at 10k-row scale
    (~21.6% faster, ~79x combined error, cross-checked) — the strongest lead in the ladder.
    `point`'s earlier single-row "win" retracted (sign-flipped on a second trusted `-t8` run, same
    fate as Variant C).** Production decision (2026-08-25): rather than continue benchmarking
    (network-free type matrix, profiler, maintenance-cost estimate), `stream10k`'s decisive,
    cross-validated result was taken as sufficient grounds to implement a real native
    `RowBinaryWithNamesAndTypes` decoder directly in `core`, scoped to a safe, incremental rollout:
    `NativeRowBinaryReader` decodes ClickHouse's scalar types natively (`Int8`-`Int64`/`UInt8`-`UInt64`,
    `Int128`/`UInt128`/`Int256`/`UInt256`,
    `Float32`/`64`, `Bool`, `String`, `FixedString(n)`, `Decimal(P,S)`/`32`/`64`/`128`/`256`, and
    `Nullable(T)` wrapping any of those — semantics cross-checked byte-for-byte against client-v2's
    own `BinaryStreamReader`), while `RowBinaryDecoder` parses the `RowBinaryWithNamesAndTypes`
    header exactly once and falls back to the existing, unmodified `ListDecodingRowBinaryReader`
    (fed a `SequenceInputStream` replaying the already-consumed header bytes) for any result
    containing even one column outside that native set — `Date*`/`DateTime*`/`Time*`,
    `Interval*`, `IPv4`/`IPv6`, `UUID`, `Enum8`/`16`, `Array`, `Map`,
    `Tuple`, `Nested`, `LowCardinality`, `JSON`, `Variant`/`Dynamic`, geo types,
    `AggregateFunction`/`SimpleAggregateFunction`, `QBit`, `BFloat16` all still decode exactly as
    before, byte-for-byte identical to today's production path. `EmptyRowBinaryReader` handles the
    DDL/no-header-at-all case the same way it always has. Implemented test-first (`RowBinaryWireFormat`,
    `ColumnDecoder`/`ScalarColumnDecoder`/`DecimalColumnDecoder`/`FixedStringColumnDecoder`,
    `NativeColumnTypeResolver`, `RowBinaryHeader`, `NativeRowBinaryReader`, `EmptyRowBinaryReader` each
    with dedicated unit tests, plus `RowBinaryDecoderTest` extended with native-path,
    mixed-native-plus-unsupported fallback, `Nullable`, and no-header end-to-end cases). Not yet
    compiled/run in this sandbox (JDK 11 only, no network) — pending the user's own
    `./gradlew spotlessApply spotlessCheck clean build`, which also re-runs
    `RealWorldTableAgainstRealClickHouseTest` as the regression net for every type not natively
    covered.
  - **Correction (2026-08-27, external review of PR #99):** the `~21.6% faster` `stream10k` number
    above is **retracted** — the review found `MinimalRowBinaryReader` decoded `UInt8`/`UInt64` as
    `Long` (not the production decoder's `Short`/`BigInteger`) and that the benchmark blackholed
    client-v2's typed-getter output against this prototype's raw array: different Java
    representations and different consumption work on each side, not decode cost alone. That type
    mismatch is fixed (`MinimalRowBinaryReader`, `RowBinaryReaderTypeMatrixBenchmark` now documents
    the remaining getter-choice asymmetry), and real ClickHouse type-coverage correctness between
    `CLICKHOUSE`/`NATIVE` modes is proven (`RealWorldTableAgainstRealClickHouseTest`, parametrized
    over both, including `Int128`/`UInt128`/`Int256`/`UInt256` as of `88020ed`).
  - **Trusted decoder-only result (2026-08-27, commit `88020ed`, 3 forks/5 warmup/3 measurement,
    `gc,jfr`):** the symmetric, apples-to-apples comparison against the exact production
    `RowBinaryDecoder` path (`DecoderOnlyBenchmark#thisDriver` vs `#thisDriverNative`, same captured
    `RowBinaryWithNamesAndTypes` bytes, same `FluxInputStreamBridge`, only `RowBinaryDecoderMode`
    differing) confirms `NATIVE` is faster at the isolated decode boundary for a `UInt64 + String +
    Decimal(18,4)` row shape: ~15.18%/13.62%/14.57% lower mean latency and ~11.5%/11.4%/11.4% lower
    allocation than `CLICKHOUSE`, at 10k/100k/1M rows respectively, with mean/p50/p90/p95 all
    agreeing on direction. **This proves the decode layer is faster in isolation — it does not yet
    prove the whole driver is faster for real point queries**, since network, transport, the decoder
    scheduler, and connection pooling all sit between the decoder and an application. That question
    is what `PublicApiMatchedPoolThroughputBenchmark#thisDriverNative` (added alongside this
    correction — see `OurDriverPointQueryClient.NativeDecoder`) exists to answer next.
  - **Trusted public-API result (2026-08-27, commit `3d66d62`, 3 forks/5 warmup/3 measurement,
    `gc,jfr`, `poolSize=8`, `concurrency=8/32/128`):** run against a single pinned external
    ClickHouse (`scripts/start-benchmark-clickhouse.sh`), not per-fork Testcontainers, so all three
    paths (`clientV2`/`thisDriver`/`thisDriverNative`) shared one server instance. **Small
    success, not strong success — matches the "small success" branch of the decision rule above.**
    JMH throughput for `thisDriver` vs `thisDriverNative` is statistically indistinguishable at
    every concurrency level (differences of 1-4% against ±18-23% error bars). Merged per-query
    latency (mean/p50/p90/p95) shows `NATIVE` consistently ~1-2% lower at `concurrency=8`/`32`, and
    essentially flat (p99 marginally worse) at `concurrency=128` — real but modest, nowhere near the
    isolated decoder's ~14-15%: network, transport, `RowDecodingScheduler`, and the 8-connection
    pool dominate this benchmark's cost, diluting the decoder's share of total latency. The one
    clean, consistent signal is allocation: `NATIVE` uses ~7-8% less per query
    (`gc.alloc.rate.norm`) than `CLICKHOUSE` at all three concurrency levels (e.g. 18933→17449 B/op
    at `concurrency=128`) — smaller than the isolated ~11.4% (more allocation happens outside the
    decoder at this layer — `Row`/`Result`/Reactor plumbing — diluting the decoder's own share) but
    directionally unambiguous, unlike the noisy throughput/latency numbers. **Decision (matching
    section 19's "small success" branch): keep `NATIVE` opt-in; do not prioritize the JFR-suggested
    decoder micro-optimizations (`UInt64` `readReversed`, `String` scratch buffer, `PushbackInputStream`
    peek removal) over other work — the isolated 14-15% win is real but doesn't clearly translate into
    a public-API win worth chasing further right now.** Both R2DBC modes also trail `clientV2` in
    this same run, more so at `concurrency≥32` (e.g. `clientV2` p90 ≈34.8ms vs `thisDriver`/
    `thisDriverNative` ≈41-42ms at `concurrency=32`) — a pre-existing, already-documented gap
    unrelated to the decoder question this benchmark exists to answer. See `NativeRowBinaryReader`'s
    Javadoc for the same caveat at the code level.
  - **Trusted-clean public-API result (2026-08-27, commit `8c64d73`, 3 forks/5 warmup/5 measurement,
    no profiler, `poolSize=8`, `concurrency=8/32/128`):** step (1)-(2) of the refined execution order
    below, done. Same pinned external ClickHouse as the profiled trusted run above, but with
    `-prof gc,jfr` removed and 5 measurement iterations instead of 3, specifically to rule out
    profiler overhead as a confound on this latency-sensitive comparison. Throughput for
    `thisDriver`/`thisDriverNative` is again statistically indistinguishable at every concurrency
    (824.7/848.1/850.8 ops/s vs 815.5/844.7/840.2 ops/s - a ~0.4-1.3% spread against +/-23-61 ops/s
    error bars), both still trailing `clientV2` (882.0/867.5/858.3 ops/s) by the same ~2-8% margin as
    before. Merged per-query latency, computed by hand from the raw per-fork `TRUE MERGED` log lines
    (`analyze.py` still only recognizes `thisDriver`/`clientV2` - the documented gap in
    `PublicApiMatchedPoolThroughputBenchmark`'s own Javadoc, not fixed as part of this run): `NATIVE`
    vs `CLICKHOUSE` (`thisDriver`) p50 is now flat-to-marginally-worse (+0.5%/+0.6%/+0.75% at
    `concurrency=8/32/128`, not the ~1-2% lower this profiled run previously suggested - that earlier
    edge did not reproduce cleanly and may itself have been partly a profiler-interaction artifact),
    while p99 is marginally better (-1.9%/-0.6%/-1.5%) - both effects small enough to be within
    ordinary run-to-run noise, not a repeatable win either direction. **The one clean, decision-useful
    signal: the tail-latency gap vs `clientV2` is essentially identical regardless of decode mode.**
    p99 for `thisDriverNative` is 27.9%/21.8%/22.0% higher than `clientV2` at `concurrency=8/32/128`;
    p99 for `thisDriver` (`CLICKHOUSE`) is 30.4%/22.5%/23.9% higher - the same gap, within a couple of
    points, whichever decoder runs. That's a clean (no profiler confound this time), independent
    confirmation of this investigation's standing hypothesis: the tail-latency deficit against
    `clientV2` sits upstream of decoding entirely (bridge/scheduler/pool/transport), so no decoder
    change - `NATIVE` or further micro-optimization of it - will close it. No allocation figures this
    run (profiler intentionally disabled). **Decision: step (2)'s "stop here" condition (`NATIVE`
    clearly ahead end to end) did not hold - still effectively tied with `CLICKHOUSE`, both still
    behind `clientV2` on tail latency by the same margin.** Step (3), the focused
    `PointQueryPipelineIsolationBenchmark`, has since completed; see the trusted-clean isolation
    result below. It found transport parity and a measurable but small scheduler boundary, leaving no
    production optimization justified by this investigation.
    `feature/305-phase12-macrobench-pr1` merged (`fc494a0`),
    go-ahead received. Working on branch `feature/314-latency-path-isolation`. Deliverable 1 (exact
    pipeline diagram + boundary locations) and **Variant A** (`LatencyPathVariantABenchmark`, trusted
    3-fork runs at both `-t 1`/`-t 8`) are done — a flat ~2.6-4.9% `thisDriver` mean deficit vs.
    client-v2 on `SELECT 1`/point, streaming already ~9-17% faster with no change needed. **Variant B**
    (avoid the `ByteBuf`→`byte[]`→`ByteBuffer` copy) is also done and **settled, not adopted**:
    `ZeroCopyByteBufInputStreamBridge`'s ownership is proven correct (leak-clean real-HTTP run), but a
    dedicated network-free microbenchmark isolated the copy's true cost at real `SELECT 1`/point
    response sizes to 15-35ns/call — real and reproducible, but negligible against the ~600-1150µs
    real round trip, and not the source of Variant A's deficit. Both classes stay as diagnostic,
    benchmark-local artifacts. See
    [docs/performance/latency-path-isolation.md](docs/performance/latency-path-isolation.md) for the
    full diagram, variant status table, and both variants' full result tables/reasoning. **Task #309**
    (profiling `ClickHouseStatement`/`ClickHouseQuery` construction) is also done and rejected:
    `QueryConstructionMicrobenchmark` measured `ClickHouseQuery.of`/`.withParameters`/UUID-generation
    cost (including under 8-way contention) at 20-150x too small to explain either concurrency level's
    deficit. Elimination list now complete (GC, the copy, and construction cost all ruled out) —
    **Variant C** (transport-acquisition-before-decoder-admission) is the only remaining hypothesis:
    `LatencyPathVariantCBenchmark` prototypes calling `FluxInputStreamBridge.subscribeTo`
    (subscription = HTTP send) eagerly on the calling thread, before `subscribeOn(decodeScheduler)`,
    instead of inside it as production does today. **Done, verdict: inconclusive, not adopted.**
    Single-fork sanity at `-t8` looked promising (~1.5-2.0% faster for early acquisition) but did not
    reproduce in the trusted 3-fork `-t8` rerun (SELECT 1 ~0.1% noise-level, point flipped to ~1.3%
    slower). The trusted `-t1` run then showed the opposite of the predicted signature entirely — a
    real, sizeable effect (~3.65%/~1.22% faster for early acquisition, 9x/3.5x combined error) at the
    concurrency level where an admission-gate-contention mechanism shouldn't apply at all (nothing
    contends for the pool or decoder scheduler at `-t1`). All four (scenario, concurrency)
    combinations flip character between single-fork and trusted runs, and the two trusted rows per
    scenario disagree with each other too — no coherent direction survives across independent runs.
    Retracted/set aside per this project's standing discipline, same practical outcome as Variant
    B/task #309 but for a different reason: not "effect too small," but "effect inconsistent and
    non-reproducible at a magnitude too large to just average away." **All four original candidate
    hypotheses (GC, the copy, construction cost, admission-gate ordering) now examined; none reliably
    explains Variant A's deficit — "no production change justified yet" per this investigation's own
    plan.** Variant D not started.
  - **Deferred architectural candidate (measured 2026-08-27, not adopted): bypass
    `FluxInputStreamBridge`/`RowDecodingScheduler` for the native path, not just the reader.**
    `FluxInputStreamBridge` (bridges reactive `Flux<ChunkBuffer>` to a blocking `InputStream`) and
    `RowDecodingScheduler` (the thread hop that lets a blocking reader run off the Netty event loop)
    are shared by `CLICKHOUSE` and `NATIVE` alike — swapping the reader never touched them. For a
    large scan this fixed per-request setup cost is amortized across thousands of rows, which is
    exactly why `DecoderOnlyBenchmark`'s ~14-15% per-row win showed up clearly there; for a
    point-query (one row), that same fixed cost is paid once and amortized over nothing, so it can
    dominate total latency far more than the ~1-2 rows' worth of decode time `NativeRowBinaryReader`
    actually saves — matching the 2026-08-27 public-API result above almost exactly (large isolated
    win, negligible-to-small end-to-end win). The proposed target shape: decode directly off Reactor
    Netty's `ByteBuf`/`Flux<ByteBuffer>` via a genuinely non-blocking streaming parser, with no
    blocking-`InputStream` adaptation and no dedicated decode-scheduler thread hop, so `NATIVE`
    becomes an actually different execution model, not just a different reader plugged into the same
    pipeline. This is a materially bigger undertaking than anything done in this investigation so
    far — it means redesigning `RowBinaryDecoder`'s execution model (cancellation, backpressure, and
    the virtual-thread decoder option were all built around today's blocking-pull-on-a-worker-thread
    shape), not adding another `RowBinaryReader` implementation. The completed isolation benchmark
    below measured the scheduler boundary at ~51us mean for a one-row response, but that is only
    ~2.4% of the full ~2.17ms R2DBC request. That evidence is not strong enough to justify this
    redesign's cancellation, backpressure, fragmentation, buffer-ownership, and event-loop-fairness
    risk. Keep this as a possible separate future project only if a real workload or new profile
    demonstrates a materially larger benefit; do not start it as a continuation of PR #99.
    **Refined execution order (2026-08-27, updated 2026-08-27 after the trusted-clean run below):**
    (1) **done** — added a `trusted-clean` JMH profile (3 forks/5 warmup/5 measurement, no JFR/GC
    profiler — the current `trusted` profile's JFR overhead is itself a confound for a
    latency-sensitive point-query comparison) alongside the existing `trusted` (commit `8c64d73`);
    `scripts/benchmarks/analyze.py` was *not* extended to chart `thisDriverNative` as originally
    planned (still only recognizes `thisDriver`/`clientV2` — see
    `PublicApiMatchedPoolThroughputBenchmark`'s own Javadoc) — the trusted-clean result below was
    instead computed by hand from the raw `TRUE MERGED` log lines, which was cheap enough for one run
    that fixing the script wasn't worth blocking on; (2) **done, "stop here" condition not met** — see
    the "Trusted-clean public-API result" entry below: `NATIVE` is not clearly ahead end to end,
    still effectively tied with `CLICKHOUSE`; (3) **done — trusted-clean pipeline isolation** —
    `PointQueryPipelineIsolationBenchmark`, GitHub Actions run `33099710434`, 3 forks/5 warmup/5
    measurement, no profiler, one pinned ClickHouse, `poolSize=8`. Full result:

    | Variant | Mean (us) | p50 (us) | p90 (us) | p95 (us) | p99 (us) |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | `clientV2RawResponse` | 2081.411 | 2052.096 | 2260.992 | 2445.312 | 2830.336 |
    | `ourTransportRawResponse` | 2075.412 | 2054.144 | 2199.552 | 2400.256 | 2715.648 |
    | `nativeDecodeRaw` | 1.406 | 1.274 | 1.380 | 1.506 | 2.292 |
    | `nativeDecodeScheduled` | 52.850 | 49.600 | 64.384 | 68.096 | 77.440 |
    | `clickHouseDecodeScheduled` | 54.839 | 51.648 | 65.536 | 70.016 | 80.768 |
    | `fullR2dbcNative` | 2168.968 | 2146.304 | 2297.856 | 2457.600 | 2838.528 |

    The raw transport means differ by only ~6us (~0.3%) and p50 by ~2us, with our transport
    nominally ahead on mean/p90/p95/p99: no transport deficit is present before decoding. The actual
    one-row native reader costs ~1.4us; scheduling the same captured bytes costs ~52.9us, isolating a
    real ~51.4us fixed scheduler/orchestration boundary. `CLICKHOUSE` vs `NATIVE` at this row shape
    differs by only ~2us once both use that same boundary. `fullR2dbcNative` is ~87.6us above
    `clientV2RawResponse`, but those variants deliberately perform different work, so the benchmark
    is not an additive model and that remainder must not be labeled SPI overhead by subtraction.
    The scheduler boundary is nevertheless only ~2.4% of the full ~2.17ms request. **Final PR #99
    decision: merge the diagnostic benchmark and documentation, make no production change, keep
    `NATIVE` opt-in, and stop this optimization line.** Do not remove `RowDecodingScheduler` or move
    the current InputStream-backed reader onto the Netty event loop. A direct incremental
    `ByteBuffer` parser remains a separate future project requiring new workload evidence, not the
    next step from this result.
- **Benchmark-only teardown leak, found and fixed 2026-08-24** (broader than the doc's own single-class
  claim): all 9 "manual pipeline" benchmark classes (`AggregationBenchmark`,
  `BoundedPoolConcurrencyBenchmark`, `ConcurrencyBenchmark`, `MatchedPoolThreadsConcurrencyBenchmark`,
  `MixedWorkloadRapidRefreshCancelBenchmark`, `PointQueryBenchmark`, `StreamingScanBenchmark`,
  `TransportOnlyStreamingBenchmark`, `TrivialQueryBenchmark`) constructed their own
  `ClickHouseHttpTransport` in `@Setup(Level.Trial)` but never called `.dispose()` on it in
  `@TearDown(Level.Trial)` — confirmed via direct grep across the module, not just the one class
  the doc named. Fixed directly (benchmark hygiene, no production code touched) rather than filed
  as a future candidate, since it was small, mechanical, and verifiable on the spot.

**Explicitly not reopened without new evidence** (per the doc's own list, matching this project's
existing verdicts): `ArrayBlockingQueue`/SPSC ring-buffer tuning (ruled out, see the
`FluxInputStreamBridge` section above), virtual-thread decoder as default (tied throughput, ~45%
more allocation, not adopted), `decoderWorkerCount` widening past pool size (Phase 11 PR5, no
help), response-compression parity (already symmetric).

## Working with Claude / IntelliJ

[CLAUDE.md](CLAUDE.md) is the source of truth for how code in this repo should look and be tested —
point any Claude session (Cowork, Claude Code CLI, or the IntelliJ plugin) at this repo and it picks
the rules up automatically. `AGENTS.md` exists so other agent tooling picks up the same rules.
Suggested split: Cowork for repo-wide planning, research, and multi-file scaffolding; the IntelliJ
plugin for tight edit-run-debug loops on one module.

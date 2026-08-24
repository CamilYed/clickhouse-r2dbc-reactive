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
  default), and a fix so the bundled demo actually disposes the driver's `ConnectionFactory` at
  Spring shutdown. See [CHANGELOG.md's Unreleased section](CHANGELOG.md#unreleased--022).
- **Phase 8 — post-0.2.0 hardening.** Most items shipped in 0.2.1/0.2.2 (see table above). Open
  items: Phase 8 P2 doc/policy items (TLS scope, minimum ClickHouse version, multi-host contract,
  compound statements, client-v2 upgrade checklist), a current-`main` demo integration lane
  alongside the published-release lane, a non-idempotent `release.yml` `USER_MANAGED` finalization
  step, and a public vendor extension for per-statement ClickHouse settings. Full detail:
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
  itself needs hardening before any further driver optimization. Planned 2026-08-23.

## Later (deferred, was blocked on Phase 11)

[Phase 11](#phase-11--benchmark-methodology-hardening) is now closed (PR1-PR5 done 2026-08-24):
resource-model measurement, root-cause profiling, and one evidence-driven optimization attempt all
happened, and the optimization attempt (`decoderWorkerCount`) was evaluated and explicitly not
adopted as a default — see PR5's entry for the full result. Further driver optimization is no
longer blocked on methodology, but nothing new is queued up from Phase 11 itself beyond the two
follow-ups PR5 parked: widening the decoder together with an explicit
`transportPendingAcquireMaxCount` (not just the decoder alone), and reconsidering whether admission
control belongs on `RowDecodingScheduler` at all. Older items not yet folded into this phase, still
parked, same reasoning as before: a JDK 21 virtual-thread decoder-scheduler experiment (see
[roadmap-archive.md's deferred section](engineering/roadmap-archive.md#deferred--performancebenchmark-work-stays-out-of-scope-until-a-proper-benchmark-environment-exists)),
and [rewriting the decode path off client-v2's blocking
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
   - Parked as a genuinely separate follow-up, not part of PR5: whether widening the decoder
     together with an explicit, deliberately-sized `transportPendingAcquireMaxCount` recovers the
     concurrency=8 win at 32/128 too without the outright failures — and, more fundamentally,
     whether admission control belongs on `RowDecodingScheduler` at all or should be its own
     explicit mechanism, since today it's an accident of where `subscribeOn` happens to sit.

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

## Working with Claude / IntelliJ

[CLAUDE.md](CLAUDE.md) is the source of truth for how code in this repo should look and be tested —
point any Claude session (Cowork, Claude Code CLI, or the IntelliJ plugin) at this repo and it picks
the rules up automatically. `AGENTS.md` exists so other agent tooling picks up the same rules.
Suggested split: Cowork for repo-wide planning, research, and multi-file scaffolding; the IntelliJ
plugin for tight edit-run-debug loops on one module.

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

## Later (deferred, blocked on Phase 11)

Further driver optimization work stays out of scope until [Phase 11](#phase-11--benchmark-methodology-hardening)'s
resource-model measurement and root-cause profiling exist — see that phase for the current PR
breakdown (PR2-PR5) and [roadmap-archive.md's deferred section](engineering/roadmap-archive.md#deferred--performancebenchmark-work-stays-out-of-scope-until-a-proper-benchmark-environment-exists)
for older items not yet folded into it: a `RowDecodingScheduler` worker-count tuning pass and a JDK
21 virtual-thread decoder-scheduler experiment. Response-compression parity and the
`@OperationsPerInvocation(4096)` verification, both previously listed here, are resolved — see
Phase 11's PR1.

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
Non-negotiable constraint: `ourDriver` and client-v2 run in the same job, on the same VM, against
the same ClickHouse process, every time — never separate CI jobs, which would compare two machines
under two different sets of noise, not two drivers. Full design (trust model for cloud numbers,
staged rollout, JMH JSON as source of truth):
[roadmap-archive.md's Phase 10](engineering/roadmap-archive.md#phase-10--cloud-benchmark-pipeline).

## Phase 11 — Benchmark methodology hardening

**PR1 done, PR2-PR5 planned.** Triggered by the 2026-08-23 cloud-verified matched-pool result
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
2. **PR2 — resource-model measurement (code, no driver optimization).** Add thread count / process
   CPU / RSS / GC count-time capture to a trusted-run profiling mode (JFR or `/proc` metrics on the
   Linux runner) — the open question is whether client-v2's throughput edge is bought with
   materially more platform threads than this driver's bounded, non-blocking pipeline uses.
   Separate `ourDriver`/`client-v2` JMH `@State` so a fork measuring one implementation doesn't
   also initialize/prewarm the other — matters once thread/RSS counts are measured per-process, not
   just for hygiene. Fix `OurDriverPointQueryClient`'s benchmark-only lifecycle leak (it retains
   only the logical `Connection`, never disposes the owning `ClickHouseConnectionFactory`'s
   transport pool/decoder scheduler).
3. **PR3 — control experiments (code). `DefaultPoolSlowQueryThroughputBenchmark` done, first CI run
   not published, re-run pending**, the rest planned. Every matched-pool benchmark artificially
   equalizes both sides' connection pools — this new class instead leaves each driver at its own
   out-of-the-box default (this driver's Reactor Netty default, ≥16; client-v2's fixed default of
   10), with queries slowed via `sleep(0.5)`/`sleep(1.0)` so a real difference in default pool size
   actually has something to queue behind (existing point queries finish in low single-digit ms —
   too fast to show contention even at a deliberately tiny matched pool). `concurrency` currently
   sweeps 8/32 only, a small first pass — see the class's own Javadoc for why 128 isn't in scope
   yet. The first `trusted` run (2026-08-23) surfaced a real bug rather than the intended
   comparison: `RowDecodingScheduler` defaulted to one worker per CPU core, completely independent
   of `transportMaxConnections`, so on the 4-core CI runner `ourDriver` was capped at 4-way decode
   concurrency regardless of its much larger connection pool — that run's numbers weren't
   representative of the intended comparison, so they weren't published; fixed by sizing
   `RowDecodingScheduler` to the resolved connection pool size instead (see
   [connection-pooling.md](docs/operations/connection-pooling.md#the-decode-worker-pool-tracks-this-pools-size-not-the-cpu-core-count)).
   Re-run needed before drawing any conclusion from this class. Still planned: a client-v2
   fixed-executor variant (vs. its default aggressive cached thread pool) to isolate how much of its
   throughput edge is executor aggressiveness rather than architecture; a pool-size sweep (4/8/16/32,
   manual profile, not the default weekly run); a connection-per-operation benchmark
   (`factory.create()` → statement → `connection.close()`, the shape Spring `DatabaseClient`
   actually uses) alongside the existing one-`Connection` benchmark, not replacing it.
4. **PR4 — root-cause the throughput/latency gap (profiling only, no driver changes).**
   JFR/async-profiler (CPU, wall-clock, allocation) on `PublicApiMatchedPoolThroughputBenchmark` at
   concurrency=32/128, both drivers, focused on `FluxInputStreamBridge`'s cross-thread handoff (the
   leading suspect — the same mechanism was already root-caused once at streaming-scan scale, see
   [results.md](docs/performance/results.md#full-table-scan-found-partially-fixed-and-the-fixs-own-measurement-is-unstable-at-1m))
   vs. Reactor operator overhead vs. row mapping vs. LZ4 decode vs. connection acquisition. Also:
   replace PR1's "mean of iteration-level percentiles" label with a true merged-HdrHistogram
   calculation (persist a mergeable histogram per iteration/fork, merge post-run instead of
   averaging already-computed percentiles). Re-run `BoundedPoolConcurrencyBenchmark` now that it
   also has `.useAsyncRequests(true)` fixed. Finish `MixedWorkloadRapidRefreshCancelBenchmark`'s
   incomplete `ourDriver` run and build its no-cancellation
   `MixedWorkloadRapidRefreshPileUpBenchmark` companion.
5. **PR5 — one evidence-driven optimization**, only if PR4's profiling points at something
   specific, followed by an exact-same-config trusted re-run to confirm the fix actually moved the
   number.

## Working with Claude / IntelliJ

[CLAUDE.md](CLAUDE.md) is the source of truth for how code in this repo should look and be tested —
point any Claude session (Cowork, Claude Code CLI, or the IntelliJ plugin) at this repo and it picks
the rules up automatically. `AGENTS.md` exists so other agent tooling picks up the same rules.
Suggested split: Cowork for repo-wide planning, research, and multi-file scaffolding; the IntelliJ
plugin for tight edit-run-debug loops on one module.

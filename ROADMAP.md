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
  benchmark work has a repeatable environment off the local MacBook. Planned 2026-08-22.

## Later (deferred, blocked on Phase 10)

Performance/benchmark work stays out of scope until Phase 10's pipeline exists and its results are
validated stable — see
[roadmap-archive.md's deferred section](engineering/roadmap-archive.md#deferred--performancebenchmark-work-stays-out-of-scope-until-a-proper-benchmark-environment-exists)
for the full list: response-compression parity re-run, a benchmark-harness factory-disposal leak
fix, a `MatchedPoolThreadsConcurrencyBenchmark` GC root-cause investigation, mixed heavy-workload
rapid-refresh benchmarks, a `RowDecodingScheduler` worker-count tuning pass, and a JDK 21
virtual-thread decoder-scheduler experiment.

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

**Not started.** Plan captured 2026-08-22, directly answering the blocker every deferred
performance item above has been waiting on: a repeatable benchmark environment off the local
MacBook. Building the pipeline (a GitHub Actions workflow + a Python analysis script) is CI/tooling
infrastructure, not benchmark iteration itself — it doesn't need an exception to the standing
"performance work waits for a proper environment" rule, it's the prerequisite that rule has been
naming all along. Actual benchmark re-runs stay deferred until this pipeline exists and its results
are validated stable. Non-negotiable constraint: `ourDriver` and client-v2 run in the same job, on
the same VM, against the same ClickHouse process, every time — never separate CI jobs, which would
compare two machines under two different sets of noise, not two drivers. Full design (trust model
for cloud numbers, staged rollout, JMH JSON as source of truth):
[roadmap-archive.md's Phase 10](engineering/roadmap-archive.md#phase-10--cloud-benchmark-pipeline).

## Working with Claude / IntelliJ

[CLAUDE.md](CLAUDE.md) is the source of truth for how code in this repo should look and be tested —
point any Claude session (Cowork, Claude Code CLI, or the IntelliJ plugin) at this repo and it picks
the rules up automatically. `AGENTS.md` exists so other agent tooling picks up the same rules.
Suggested split: Cowork for repo-wide planning, research, and multi-file scaffolding; the IntelliJ
plugin for tight edit-run-debug loops on one module.

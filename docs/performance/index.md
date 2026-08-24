# Performance & benchmarking

This section answers three questions: what does each benchmark actually measure
([methodology.md](methodology.md)), what did the latest run find ([results.md](results.md)), and
how should that change the way you use this driver (below). It does not narrate the history of how
the benchmark suite got here — see `git log -- docs/PERFORMANCE.md` (the file this section was
split from) if that archaeology is ever needed. To reproduce these numbers yourself, see
[running-benchmarks.md](running-benchmarks.md). Retracted/fully-superseded result sections live in
[archive.md](archive.md), not deleted, not mixed in with current numbers.

> [!TIP]
> **New here? Start with [results.md's "Full mega sweep" section](results.md#full-mega-sweep--every-scenario-one-run-2026-08-24)** —
> every benchmark scenario this project runs, in one table, from a single 2026-08-24 cloud run.
> Every other section on that page is either the deep-dive behind one of those rows or an earlier
> run being superseded/reconfirmed by it.

> [!IMPORTANT]
> **Read this before any table on these pages.** Every JMH run prints this warning, and it applies
> in full here: benchmarking is easy to get wrong in ways that look like a result. The numbers on
> these pages are real measurements, not claims accepted on faith — but a number is only as
> trustworthy as its **confidence** column says. Single-fork numbers are a first signal. Multi-fork
> (`-Pjmh.forks=3`) numbers are the ones worth acting on. Where a result is single-fork, it says so,
> and it is presented as "here's what we found," not "here's the truth."

## Environment

The primary, trustworthy signal is now the **cloud CI pipeline** (Phase 10), not the local laptop —
every headline number below comes from there. The local M3 Pro remains useful for quick sanity
checks during development, but treat any single-fork local number as a first signal only, not
something to cite.

| | |
| --- | --- |
| **Primary: cloud CI runner** | GitHub Actions `ubuntu-latest` (4 cores, 16 GB RAM), `trusted` profile — 3 forks, 5 warmup iterations, `-prof gc`, one shared ClickHouse container per job. This is what produced the 2026-08-24 mega sweep and every other multi-fork-confirmed number on this page. |
| JDK | Temurin 21 (as pinned in `.github/workflows/benchmark.yml`) |
| ClickHouse | `clickhouse/clickhouse-server`, one container per CI job |
| client-v2 (baseline) | `com.clickhouse:client-v2:0.9.8` (pinned in `gradle/libs.versions.toml`) |
| JMH | `SampleTime` mode for latency benchmarks, `Throughput` mode for throughput-oriented classes (`PublicApiMatchedPoolThroughputBenchmark`, `PoolSizeSweepThroughputBenchmark`, `DecoderWorkerCountThroughputBenchmark`) |
| Secondary: local dev machine | Apple M3 Pro, 12 cores (6P+6E), 36 GB LPDDR5, macOS, Temurin 21.0.8+9-LTS. Used for early exploratory runs before a scenario graduates to the cloud pipeline; single machine, single point in time, no CPU pinning — read any local-only number as *this driver vs. client-v2, same hardware/JVM/data*, not a portable claim. |

## Headline results (2026-08-24 cloud mega sweep)

The 12 core benchmark classes run together in one cloud sweep, 3 forks each — see
[results.md's full mega sweep section](results.md#full-mega-sweep--every-scenario-one-run-2026-08-24)
for the complete table and every chart.

| Scenario | Result |
| --- | --- |
| Full table scan, all tiers (10k/100k/1M rows) | 🟢 **11–12% lower latency at every tier**, including 1M rows — the first cloud run to resolve the earlier local instability at that tier |
| Non-blocking, matched 8-connection pool, real throughput (4 independent runs/classes agree) | 🔴 **client-v2 ~3–20% ahead**, consistently — unresolved, see [Open follow-ups](results.md#open-follow-ups) |
| Non-blocking, matched pool, allocation per query | 🟢 **this driver allocates 2.7–2.9x less**, and the gap widens as concurrency rises — the clearest, most consistent win in the whole sweep |
| Pool size sweep (4/8/16/32) | 🟡 tied at pool=4, but the gap **widens** as the pool gets bigger (client-v2 up to ~7% ahead at pool=32) — new finding, same latency-gap family |
| Aggregation (`GROUP BY`/`avg`/`quantile`, 10k–1M rows) | 🟡 roughly a wash — +13% slower at 10k, within 2% at 100k/1M |
| Default pool, slow query (server-side `sleep()`) | 🟢 **~2x faster at concurrency=32**, tied at concurrency=8 — this driver's larger default pool pays off once it's actually saturated |
| Protocol floor (`SELECT 1`, point lookup) | 🟡 essentially tied to ~8% slower — no meaningful edge either way |
| Blocking `.block()`-per-query calling style, matched pool | 🔴 same latency gap as above — don't call it this way, see below |
| Decoder worker count widened past pool size | 🟡 no help — slightly *worse* than the coupled default |

> [!IMPORTANT]
> **What this sweep changed the story to, in one paragraph:** this driver's clearest, most
> repeatable advantage today is **streaming large result sets** (11–12% lower latency at every
> tier, 10k through 1M rows) and **allocation per query under concurrent load** (2.7–2.9x less,
> growing with concurrency) — not raw throughput or per-query latency under a matched connection
> pool, where client-v2 is currently ahead by a consistent, still-unresolved margin that *widens*
> as the pool or concurrency grows. Four independent benchmark classes
> (`PublicApiMatchedPoolThroughputBenchmark`, `BoundedPoolConcurrencyBenchmark`,
> `MatchedPoolThreadsConcurrencyBenchmark`, `PoolSizeSweepThroughputBenchmark`) now agree on that
> gap, which makes it the single most concrete open question this project has — see
> [Open follow-ups](results.md#open-follow-ups). An earlier "~4x more queries/sec" headline for
> the matched-pool scenario was retracted after being traced to a client-v2 benchmark-harness bug;
> see [archive.md](archive.md) for the full record.

Full tables, charts, and the root-cause analysis behind each row: [results.md](results.md).

## How to use this driver well

Things about this driver that are not obvious from the R2DBC SPI surface alone, gathered from
building and benchmarking it.

**Call it reactively — `Flux`/`Mono`, `flatMap` for concurrency — never `.block()` per query in a
concurrent loop.** This is still correct advice, but not for the magnitude this page used to claim:
see [results.md's cloud-verified matched-pool
result](results.md#cloud-verified-matched-pool-real-async-on-both-sides-2026-08-23) for what
actually holds up under a fair comparison — this driver's real, consistent win in that scenario is
**allocation per query (2.7–2.9x less)**, not throughput or latency, where client-v2 is currently
ahead. What's still true: wrapping each call in `.block()` inside a loop or `@Threads(N)`-style
caller gets you the *even slower* number from the blocking-calls table, with none of the allocation
upside either — this driver was never optimized for that calling style.

**A small, explicit connection pool is normal here, not a limitation.** Because logical concurrent
queries don't need one thread each, a pool sized for physical parallelism against ClickHouse (8,
maybe fewer) can comfortably serve far more concurrent logical work than its connection count —
excess demand queues inside Reactor Netty with nothing blocked. Don't size
`transportMaxConnections` the way you'd size a JDBC pool ("one per expected concurrent request");
size it for how many requests you actually want in flight against ClickHouse at once. See
[../operations/connection-pooling.md](../operations/connection-pooling.md).

**The production decode path had a real, root-caused scheduling cost at large result sets — the fix
helps, but the number at very large result sets (1M+ rows) is not settled yet.** `RowDecodingScheduler`
moves decode work off the Netty event loop for correctness, but the hand-off itself — one
cross-thread `queue.take()` per network chunk in `FluxInputStreamBridge` — turned out to be a real,
measurable cost that scaled with result size. Fixed by opportunistically coalescing already-queued
chunks before crossing threads — see [results.md](results.md#full-table-scan-found-partially-fixed-and-the-fixs-own-measurement-is-unstable-at-1m)
for the full mechanism. 10k and 100k rows are a clear, repeatable win. At 1M rows, results range
from tied with client-v2 to ~30% slower depending on JVM/OS conditions this investigation couldn't
pin down (see [results.md's "Why the 1M number won't sit
still"](results.md#why-the-1m-number-wont-sit-still)) — treat that tier as unresolved, not as a
known gap of a fixed size. The 10k/100k tiers above remain this driver's clearest, most repeatable
wins today; the matched-pool concurrency scenario no longer belongs in that list — see the
"Non-blocking, matched pool" rows above, where client-v2 is currently ahead on throughput and
latency and this driver's advantage is allocation per query, not overall speed.

**Don't call `RowBinaryDecoder.decodeRows` (the scheduler-free shortcut) against a live network
source.** It's safe only when the source is already fully in memory — against a live Reactor Netty
response, it runs decode directly on the thread that's subscribing, which in production is the
event loop. This project's own benchmark suite got this wrong for a long time (every benchmark
querying a live transport used this shortcut, silently skipping the real scheduling cost) before
being fixed — a mistake worth naming so it isn't repeated in application code.

**A demand-gated tick source feeding `Flux.switchMap` doesn't behave the way it looks like it
should.** While building `MixedWorkloadRapidRefreshCancelBenchmark`, `Flux.interval(...)` piped
directly into `switchMap` silently stopped ticking on schedule — `switchMap`'s no-prefetch variant
only pulls the next item from upstream once the *current* inner sequence fully terminates, so a
slow inner sequence throttles the tick source itself, turning an intended "cancel the old, start
the new" race into a plain sequential queue. `Flux.onBackpressureLatest()` between the two fixes
it: it lets the source tick on its own schedule and buffers only the freshest value for
`switchMap`, dropping stale ticks in between — which is also the behaviorally correct choice for
"always show the latest," not just a technical workaround. Worth knowing if your own application
code combines a periodic/event-driven source with `switchMap`.

**Cancellation is best-effort past the server boundary, not a hard guarantee.** See
[../reference/known-limitations.md](../reference/known-limitations.md) for the full detail —
cancelling a subscription always stops this driver from reading further and closes the connection,
but the best-effort `KILL QUERY` sent to actually stop ClickHouse server-side can itself fail
(privilege, connectivity) and is logged, not surfaced back to the caller.

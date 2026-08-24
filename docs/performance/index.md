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

| | |
| --- | --- |
| CPU | Apple M3 Pro, 12 cores (6 performance + 6 efficiency) |
| RAM | 36 GB LPDDR5 |
| OS | macOS, Apple Silicon |
| JDK | Temurin 21.0.8+9-LTS |
| ClickHouse | `clickhouse/clickhouse-server`, run via `scripts/start-benchmark-clickhouse.sh` as an external container (`BENCH_CLICKHOUSE_URL`), not the per-fork Testcontainers path — see that script for the exact image/config |
| client-v2 (baseline) | `com.clickhouse:client-v2:0.9.8` (pinned in `gradle/libs.versions.toml`) |
| JMH | `SampleTime` mode for latency benchmarks, `Throughput` mode for `PublicApiMatchedPoolThroughputBenchmark` |
| This run | **Mixed.** `StreamingScanBenchmark` has been re-run at 3 forks / 3×10s warmup (`-Pjmh.forks=3 -Pjmh.warmupIterations=3`) on the local M3 Pro — see [results.md](results.md#full-table-scan-found-partially-fixed-and-the-fixs-own-measurement-is-unstable-at-1m). `PublicApiMatchedPoolThroughputBenchmark`'s matched-pool result was instead re-run twice on the Phase 10 cloud pipeline (`trusted` profile, GitHub Actions `ubuntu-latest`, 3 forks / 5 warmup iterations each) — see [results.md's cloud-verified section](results.md#cloud-verified-matched-pool-real-async-on-both-sides-2026-08-23) for why (a benchmark-harness fairness bug meant the local number for this scenario couldn't be trusted either, cloud or local). Every other benchmark family remains single-fork local (the `jmh` task's own default), a sanity-check signal, not yet multi-fork confirmed. [results.md](results.md) marks each table accordingly. |

Single machine, single point in time, shared consumer laptop (no CPU pinning, no isolated cores).
Read every comparison as *this driver vs. client-v2, same hardware/JVM/data*, not as a portable
absolute performance claim.

## Headline results (updated 2026-08-23)

| Scenario | Result |
| --- | --- |
| Non-blocking, matched 8-connection pool, real throughput (cloud-verified, 2 independent runs) | 🟡 **client-v2 ~5–9% ahead**, consistently — see the retraction note in [results.md](results.md#non-blocking-matched-pool-numbers-below-are-retracted-pending-re-run) for why the earlier "~4x" claim here is wrong |
| Non-blocking, matched pool, per-query latency (p50–p99) | 🔴 **client-v2 ~5–18% lower**, consistently — unresolved, see [Open follow-ups](results.md#open-follow-ups) |
| Non-blocking, matched pool, allocation per query | 🟢 **this driver allocates 2.7–2.9x less**, and the gap widens as concurrency rises — the one part of the original story that holds up |
| Full table scan, 10k rows | 🟢 **~12% lower latency**, consistent across repeated 3-fork runs |
| Full table scan, 100k rows | 🟢 **roughly 4–11% lower latency**, a real but noisier win |
| Full table scan, 1M rows | 🟡 **unresolved — anywhere from tied to ~30% higher latency**, depending on the run |
| Transport alone (bytes, no decode) | 🟢 43–45% lower latency at every tier, including 1M rows |
| Decode alone, no network | 🟢 7–14% lower latency at every tier |
| Single-row point lookup / `SELECT 1` floor | 🟡 essentially tied (within ±2%) |
| Blocking `.block()`-per-query calling style, matched pool | 🔴 ~5–8% higher latency — don't call it this way, see below |

> [!IMPORTANT]
> The three "matched pool" rows above replace an earlier "~4x more queries/sec, 3.5–4.1x lower
> latency" headline that turned out to be measured against a client-v2 benchmark-harness bug
> (client-v2 running near-serially, not against its real 8-connection pool) — see
> [results.md's retraction](results.md#non-blocking-matched-pool-numbers-below-are-retracted-pending-re-run)
> for the full story. Left here rather than quietly deleted, since the point of this pipeline is
> honest numbers, including the ones that turn out to have been wrong.

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

# Performance & Benchmarking

This file answers three questions: what does each benchmark actually measure, what did the latest
run find, and how should that change the way you use this driver. It does not narrate the history
of how the benchmark suite got here — see `git log -- docs/PERFORMANCE.md` if that archaeology is
ever needed. Companion doc:
[../clickhouse-r2dbc-reactive-benchmarks/README.md](../clickhouse-r2dbc-reactive-benchmarks/README.md)
for the exact commands to run these yourself.

> [!IMPORTANT]
> **Read this before the tables below.** Every JMH run prints this warning, and it applies in full
> here: benchmarking is easy to get wrong in ways that look like a result. The numbers on this page
> are real measurements, not claims accepted on faith — but a number is only as trustworthy as its
> **confidence** column says. Single-fork numbers are a first signal. Multi-fork (`-Pjmh.forks=3`)
> numbers are the ones worth acting on. Where a result below is single-fork, it says so, and it is
> presented as "here's what we found," not "here's the truth."

---

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
| This run | **Mixed.** `StreamingScanBenchmark` has been re-run at 3 forks / 3×10s warmup (`-Pjmh.forks=3 -Pjmh.warmupIterations=3`) — see "Full table scan" below, including why the 1M-row tier still isn't a trustworthy single number even at 3 forks. Every other benchmark family on this page remains single-fork (the `jmh` task's own default), a sanity-check signal, not yet multi-fork confirmed. Each table below is marked accordingly. |

Single machine, single point in time, shared consumer laptop (no CPU pinning, no isolated cores).
Read every comparison below as *this driver vs. client-v2, same hardware/JVM/data*, not as a
portable absolute performance claim.

---

## Benchmark catalog

What each class actually exercises. Grouped by the question it answers, not by build order.

### Protocol floor — single row, no streaming

| Benchmark | What it measures |
| --- | --- |
| `TrivialQueryBenchmark` | `SELECT 1`, no table. The floor cost of one round trip before any real row decoding happens. |
| `PointQueryBenchmark` | A parameterized single-row lookup (`WHERE id = {id:UInt64}`), calling this driver's transport + decoder directly and client-v2's `Client` + `ClickHouseBinaryFormatReader` directly — both below the R2DBC SPI layer. |
| `PublicApiPointQueryBenchmark` | The same lookup, but through the public R2DBC SPI (`Connection`/`Statement`/`Result`) on this driver's side, and client-v2's public `Client` API on the other — what an actual application calls. Also isolates the cost of a `DriverObservationListener` being configured (`ourDriverNoopObservation` vs. `ourDriverEnabledObservation`). |

### Streaming and decode — where does a full scan's cost actually live

| Benchmark | What it measures |
| --- | --- |
| `StreamingScanBenchmark` | Full table scan (`SELECT * FROM t`, no `WHERE`) at 10k/100k/1M-row tiers, through this driver's real production path — `RowBinaryDecoder.decode(source, RowDecodingScheduler)`, the same call the shipped connector makes, not a benchmark-only shortcut. Also records time-to-first-row separately (logged at trial teardown, not in JMH's own result). |
| `TransportOnlyStreamingBenchmark` | The same query, same row tiers, but only sums response bytes — no row decoding on either side. Isolates transport cost alone. |
| `DecoderOnlyBenchmark` | Decodes one already-captured response payload from memory, repeatedly, at the same three tiers — no network at all. Isolates decode cost alone. `ourDriver` here safely uses `RowBinaryDecoder.decodeRows` (the scheduler-free shortcut) because the source is in-memory, not a live Netty response — see [How to use this driver well](#how-to-use-this-driver-well) for why that distinction matters. The extra `ourDriverWithoutMapCopy`/`ourDriverCompactRow`/`compactRowDirectLoop`/`compactRowFluxNoBridge` methods are retained diagnostic variants from an earlier redesign investigation, not benchmarks of shipped behavior. |
| `AggregationBenchmark` | `GROUP BY` + `count()`/`avg()`/`quantile()` — ClickHouse's actual core use case. Unlike the scan benchmarks, the result set is always ~100 rows regardless of the row-count tier; what scales is server-side aggregation work, not client-side decode volume. |

### Concurrency — blocking vs. non-blocking calling style

| Benchmark | What it measures |
| --- | --- |
| `ConcurrencyBenchmark` | `@Threads(8)`: eight platform threads, each blocking on its own call, both drivers left at their own default (unmatched) connection pool. A same-thread-count, different-pool-budget comparison. |
| `MatchedPoolThreadsConcurrencyBenchmark` | Identical to the above, except both sides get the same explicit 8-connection pool. Isolates whether a `ConcurrencyBenchmark` gap comes from pool mismatch or from the blocking calling style itself. |
| `BoundedPoolConcurrencyBenchmark` | The scenario this driver is actually built for: a deliberately small, **matched** 8-connection pool, driven **non-blocking** — `Flux.flatMap(..., concurrency)` on this driver's side, `CompletableFuture`-returning async calls on client-v2's — at 8/32/128 logical concurrent queries. No thread is blocked per in-flight query on either side; only the physical pool budget is shared. |
| `PublicApiMatchedPoolThroughputBenchmark` | The same idea, through the public R2DBC SPI end to end (not `ClickHouseHttpTransport` directly), reporting real **throughput** (queries/sec) and a separate `HdrHistogram`-based per-query latency log, at the same matched 8-connection pool and 8/32/128 concurrency levels. This is the headline "how many real queries per second" number. |

### Rapid-refresh cancellation — the "user hits refresh" scenario

| Benchmark | What it measures |
| --- | --- |
| `MixedWorkloadRapidRefreshCancelBenchmark` | 32 concurrent simulated user sessions, each firing one of 12 distinct heavy analytical queries (six-table join, `sleep(2)`-controlled fixed duration) and abandoning it 5ms later for the next — modeling a dashboard user rapidly hitting refresh before the previous load finished. This driver's cancellation (`switchMap`) tears down the connection and issues a best-effort `KILL QUERY`; client-v2's `CompletableFuture#cancel(true)` does not actually interrupt the already-running query (a well-known `CompletableFuture` limitation), so its abandoned queries keep competing for the pool and server resources. Deliberately asymmetric pools: this driver uses Reactor Netty's own default, client-v2 gets a generous, explicit 32-connection pool — the point is whether client-v2's pool still gets exhausted by pile-up even when sized generously, not whether a small pool alone explains a slowdown (already ruled out by `MatchedPoolThreadsConcurrencyBenchmark`, above). |

---

## Results (2026-08-20, mostly single fork — `StreamingScanBenchmark` re-run at 3 forks)

### Protocol floor: essentially tied

<p align="center">
  <img src="images/2026-08-20-protocol-floor.png" width="720" alt="Single-row query latency: TrivialQueryBenchmark, PointQueryBenchmark, PublicApiPointQueryBenchmark, this driver vs client-v2">
</p>

| Benchmark | this driver (mean) | client-v2 (mean) | verdict |
| --- | --- | --- | --- |
| `SELECT 1` | 575.0 µs | 585.1 µs | this driver 1.7% faster |
| raw point lookup | 1150.6 µs | 1154.2 µs | tied (0.3%) |
| point lookup via R2DBC SPI | 1158.0 µs | 1142.0 µs | client-v2 1.4% faster |

At the scale of one round trip, the two drivers cost essentially the same. Earlier write-ups of
this page reported this driver 6–7% faster here — that margin is gone in this run. Nothing about
the protocol path changed; the more likely explanation is that earlier single-fork numbers were
themselves noise (see the confidence warning at the top of this page). Treat the protocol floor as
a wash until a multi-fork run says otherwise in either direction.

### Full table scan: found, partially fixed, and the fix's own measurement is unstable at 1M

<p align="center">
  <img src="images/2026-08-20-streaming-scan.png" width="720" alt="StreamingScanBenchmark mean latency by row count, this driver vs client-v2, after the chunk-coalescing fix">
</p>

This section originally reported a growing, unexplained regression (tied at 10k, 19.5% slower at
100k, 56.9% slower at 1M) once every benchmark started going through the real production decode
path (`RowBinaryDecoder.decode` + `RowDecodingScheduler`, replacing an earlier scheduler-free
shortcut that never paid its real off-event-loop cost). That regression is root-caused (see the
isolation section below for the mechanism) and a fix shipped — see
[`FluxInputStreamBridge`](../clickhouse-r2dbc-reactive-core/src/main/java/io/github/camilyed/clickhouse/r2dbc/core/FluxInputStreamBridge.java)'s
"Chunk coalescing" Javadoc section. **A single-fork run initially looked like a near-complete fix
(11.8% gap remaining at 1M); a follow-up 3-fork run told a different, more honest story — see below
for why the single-fork number should not have been trusted.**

3-fork (`-Pjmh.forks=3 -Pjmh.warmupIterations=3`), after raising `RowBinaryDecoder.RESPONSE_CHUNK_DEMAND`
from `4` to `16` (see that constant's Javadoc — more chunks allowed in flight gives the coalescing
fix more material to merge per blocking `take()`):

| Rows | this driver (mean) | client-v2 (mean) | verdict |
| --- | --- | --- | --- |
| 10,000 | 3387 µs | 3855 µs | **this driver 12.1% faster** |
| 100,000 | 13,825–14,525 µs | 15,211–15,548 µs | **this driver roughly 4–11% faster, across repeated runs** |
| 1,000,000 | 94,000–131,439 µs | 96,031–98,617 µs | **unstable: anywhere from a tie to 33% slower, depending on the run** |

10k is a clean, repeatable win. 100k is a real, if noisier, win. 1M is not resolved — see "Why the
1M number won't sit still" below before treating any single 1M number on this page as the answer.

### Why it happened, and why the fix mostly worked

<p align="center">
  <img src="images/2026-08-20-isolation-trio.png" width="720" alt="Isolation: transport only vs decode only vs the full production pipeline, this driver vs client-v2, at 1,000,000 rows, after the chunk-coalescing fix">
</p>

| Isolation (1M rows) | this driver (mean) | client-v2 (mean) | verdict |
| --- | --- | --- | --- |
| transport only (bytes, no decode) | 15,128 µs | 27,644 µs | **this driver 45.3% faster** |
| decode only (in-memory bytes) | 55,323 µs | 64,382 µs | **this driver 14.1% faster** |
| transport + decode (production path, after the fix, best observed fork) | ~94,000 µs | ~96,031 µs | roughly tied, best case |
| transport + decode (production path, after the fix, worst observed fork) | ~131,439 µs | ~98,617 µs | this driver ~33% slower, worst case |

Root cause, confirmed by instrumentation before writing any fix: `FluxInputStreamBridge` (the
adapter between Reactor Netty's push-based chunk delivery and the decoder's blocking pull-based
`InputStream` reads) did one `ArrayBlockingQueue.take()` — a real cross-thread synchronization
point between Netty's event loop and `RowDecodingScheduler`'s worker thread — per network chunk.
A temporary chunk counter added to `StreamingScanBenchmark` showed chunk count scaling almost
perfectly linearly with row count (~285–293 rows per chunk at every tier: 10k rows → ~34 chunks,
100k → ~342, 1M → ~3545), and the estimated per-chunk cost from the timing gap (~8–12µs) was
consistent across tiers — exactly what a fixed per-handoff cost multiplied by a row-count-scaling
handoff count would produce.

**The fix**: `FluxInputStreamBridge.fillCurrent()` still does one blocking `take()` for the first
available chunk, but then opportunistically drains any chunks *already sitting in the queue* with
non-blocking `poll()` calls, merging them into one larger buffer (up to 64KB) before returning —
trading handoffs away only when the producer is already ahead, never adding latency when it isn't.
The backpressure credit model is untouched: every physical chunk dequeued still triggers exactly
one demand-replenishing `request(1)`, just timed at dequeue instead of buffer-exhaustion.
`RowBinaryDecoder.RESPONSE_CHUNK_DEMAND` was also raised from `4` to `16` as part of this
investigation, so more chunks can be outstanding at once for the coalescing loop to find.

**At 1M rows the fix's effect is real but currently unmeasurable to a single number** — see the
next section.

### Why the 1M number won't sit still

3-fork `StreamingScanBenchmark.ourDriver` runs at 1M rows, broken down per-fork (JMH's own
percentile output pools all forks into one histogram, which hides this — the per-fork means come
from parsing `results.json`'s `rawDataHistogram` directly):

| Run | fork A | fork B | fork C | spread |
| --- | --- | --- | --- | --- |
| both drivers, `demand=4` | 97,000 µs | 122,500 µs | 129,400 µs | 33.4% |
| `ourDriver` only, `demand=16`, per-fork Testcontainers | 121,500 µs | 123,000 µs | 104,000 µs | 18.3% |
| `ourDriver` only, `demand=16`, shared external container | 123,700 µs | 94,000 µs | 122,500 µs | 31.6% |

`client-v2` shows none of this: its per-fork means at 1M sit within a 1.5% band across every run
(≈95,400–96,500 µs), run after run. Whatever is happening is specific to this driver's decode path,
not the ClickHouse server or the measurement harness in general.

Three explanations were tested and ruled out, in this order:

1. **GC pauses.** `-prof gc` on the isolated `ourDriver` benchmark showed 344ms of total GC pause
   time across the *entire* 1M-row trial (856 GC events, ~0.4ms each) — nowhere near enough to
   explain a single operation running 30,000+ µs slower than another. Ruled out.
2. **Testcontainers-per-fork** (each JMH fork getting its own freshly-started ClickHouse container,
   with different page-cache/startup state — exactly the confound
   `scripts/start-benchmark-clickhouse.sh` exists to eliminate). Re-run against the shared external
   container (`BENCH_CLICKHOUSE_URL`) still showed a 31.6% spread. Ruled out as the sole cause.
3. **`RowDecodingScheduler` worker-pool sizing.** `StreamingScanBenchmark` issues one query at a
   time, sequentially — only one worker thread from the pool is ever active regardless of
   `DEFAULT_WORKER_COUNT`, so changing pool size can't plausibly explain fork-to-fork variance in a
   single-query benchmark. Not tested empirically (would predictably do nothing), reasoned out.

What's left, none confirmed: JIT/tiered-compilation timing differences between separate JVM fork
launches, and/or OS-level thread scheduling on Apple Silicon's asymmetric performance/efficiency
cores (no QoS/priority hints are set on `RowDecodingScheduler`'s threads, so macOS decides
per-launch which physical cores they land on). Both would need tooling this investigation didn't
have available — `async-profiler`, JFR, or `sudo powermetrics` run locally by someone with hands on
the actual machine — to confirm. Also notable and unresolved: single, extreme per-operation outliers
(one 1M-row scan taking 300,000+ µs against a ~120,000 µs median) appear in some runs and not
others, including runs with identical configuration — see [Open follow-ups](#open-follow-ups).

**What this means for the headline number:** treat "this driver at 1M rows" as a range, not a
point — somewhere between tied-with-client-v2 and ~30% slower, depending on JVM/OS conditions this
investigation could not pin down with the tools available. The 10k and 100k results above don't
show this instability and can be trusted as reported.

### Aggregation: a wash

| Rows | this driver (mean) | client-v2 (mean) | verdict |
| --- | --- | --- | --- |
| 10,000 | 1381 µs | 1339 µs | client-v2 3.2% faster |
| 100,000 | 4216 µs | 4228 µs | tied |
| 1,000,000 | 15,936 µs | 16,994 µs | this driver 6.2% faster |

`GROUP BY`/`count()`/`avg()`/`quantile()` always returns ~100 rows regardless of input size, so
decode cost is small next to the server-side aggregation both drivers pay identically. Neither
driver has a structural advantage here — which is itself useful to know: the streaming-scan
regression above is specifically a large-result-set problem, not a general one.

### Blocking calls: confirms it's the calling style, not the pool

| Shape | this driver (mean) | client-v2 (mean) | verdict |
| --- | --- | --- | --- |
| `@Threads(8)`, default (unmatched) pools | 2321 µs | 2217 µs | client-v2 4.7% faster |
| `@Threads(8)`, matched 8-connection pool | 2321 µs | 2151 µs | client-v2 7.9% faster |

Matching the pool size doesn't close the gap — it widens it slightly. This driver's non-blocking
pipeline has no advantage to offer when the caller blocks a whole platform thread per query; the
scheduler hop from the section above becomes pure overhead with nothing to hide it behind. See
[How to use this driver well](#how-to-use-this-driver-well): this is exactly the calling style not
to use.

### Non-blocking, matched pool: the actual point of this project

<p align="center">
  <img src="images/2026-08-20-bounded-pool-concurrency.png" width="720" alt="BoundedPoolConcurrencyBenchmark burst latency by concurrency level, this driver vs client-v2">
</p>

| Concurrency (pool=8, both sides) | this driver (mean) | client-v2 (mean) | verdict |
| --- | --- | --- | --- |
| 8 | 2700 µs | 9324 µs | **this driver 71.0% faster (3.5x)** |
| 32 | 10,008 µs | 37,086 µs | **this driver 73.0% faster (3.7x)** |
| 128 | 36,723 µs | 148,890 µs | **this driver 75.3% faster (4.1x)** |

Same 8-connection budget on both sides, but driven the way this driver is actually meant to be
called: `Flux.flatMap(..., concurrency)`, no thread blocked per in-flight query. The win doesn't
just hold, it widens as concurrency increases — client-v2 needs a blocked thread (or its own
internal executor) per logical query fighting over the same 8 connections; this driver just queues
inside Reactor Netty with nothing paying rent while it waits.

<p align="center">
  <img src="images/2026-08-20-throughput.png" width="720" alt="Real point-query throughput through the public R2DBC SPI, matched 8-connection pool, this driver vs client-v2">
</p>

| Concurrency (pool=8, both sides) | this driver (ops/s) | client-v2 (ops/s) | verdict |
| --- | --- | --- | --- |
| 8 | 3516 | 900 | **3.9x throughput** |
| 32 | 3615 | 900 | **4.0x throughput** |
| 128 | 3541 | 893 | **4.0x throughput** |

The same story, measured as real throughput through the public R2DBC SPI (not the internal
transport API) against client-v2's public async API: a flat, consistent ~4x more queries per
second at every concurrency level, on identical hardware and an identical physical connection
budget. Client-v2's own throughput doesn't move at all between 8 and 128 concurrent queries — it's
already saturated at 8 concurrent requests against an 8-connection pool, exactly what a
one-thread(or task)-per-query model predicts.

### Rapid-refresh cancellation: incomplete this run

`MixedWorkloadRapidRefreshCancelBenchmark.clientV2` completed (mean 93.1s per 32-user/15-refresh
burst, n=3) but `ourDriver` produced no samples in this run's log — the run was interrupted or the
log truncated before it reached that method. **Not reported as a comparison** — a number for one
side only is not a finding, it's a gap. Re-run this class on its own
(`-Pjmh.includes=MixedWorkloadRapidRefreshCancelBenchmark`) before drawing any conclusion about
cancellation behavior under this workload.

---

## How to use this driver well

Things about this driver that are not obvious from the R2DBC SPI surface alone, gathered from
building and benchmarking it.

**Call it reactively — `Flux`/`Mono`, `flatMap` for concurrency — never `.block()` per query in a
concurrent loop.** The entire measured advantage of this driver (3.5–4x under concurrent load, see
above) comes from not blocking a thread per in-flight query. The moment you wrap each call in
`.block()` inside a loop or `@Threads(N)`-style caller, you get the *slower* number from the
"blocking calls" table above, with none of the upside — this driver was never optimized for that
calling style, and the benchmarks above show it losing there, not winning.

**A small, explicit connection pool is normal here, not a limitation.** Because logical concurrent
queries don't need one thread each, a pool sized for physical parallelism against ClickHouse (8,
maybe fewer) can comfortably serve far more concurrent logical work than its connection count —
excess demand queues inside Reactor Netty with nothing blocked. Don't size
`transportMaxConnections` the way you'd size a JDBC pool ("one per expected concurrent request");
size it for how many requests you actually want in flight against ClickHouse at once.

**The production decode path had a real, root-caused scheduling cost at large result sets — the fix
helps, but the number at very large result sets (1M+ rows) is not settled yet.** `RowDecodingScheduler`
moves decode work off the Netty event loop for correctness (decoding directly on the event loop
would block it for every other in-flight request sharing that loop), but the hand-off itself — one
cross-thread `queue.take()` per network chunk in `FluxInputStreamBridge` — turned out to be a real,
measurable cost that scaled with result size. Fixed by opportunistically coalescing already-queued
chunks before crossing threads (see the "Full table scan" results above and that class's own "Chunk
coalescing" Javadoc section for the full mechanism). 10k and 100k rows are a clear, repeatable win.
At 1M rows, results range from tied with client-v2 to ~30% slower depending on JVM/OS conditions
this investigation couldn't pin down (see ["Why the 1M number won't sit
still"](#why-the-1m-number-wont-sit-still)) — treat that tier as unresolved, not as a known 11.8%
gap. If your workload is dominated by very large single-result scans, the concurrency scenario above
is still where this driver wins most decisively and predictably.

**Don't call `RowBinaryDecoder.decodeRows` (the scheduler-free shortcut) against a live network
source.** It's safe only when the source is already fully in memory (exactly what
`DecoderOnlyBenchmark` uses it for) — against a live Reactor Netty response, it runs decode
directly on the thread that's subscribing, which in production is the event loop. This project's
own benchmark suite got this wrong for a long time (every benchmark querying a live transport used
this shortcut, silently skipping the real scheduling cost) before being fixed — a mistake worth
naming so it isn't repeated in application code.

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

**Cancellation is best-effort past the server boundary, not a hard guarantee.** See the main
[README's Known limitations](../README.md#known-limitations) for the full detail — cancelling a
subscription always stops this driver from reading further and closes the connection, but the
best-effort `KILL QUERY` sent to actually stop ClickHouse server-side can itself fail (privilege,
connectivity) and is logged, not surfaced back to the caller.

---

## Open follow-ups

- **Explain the 1M-row inter-fork variance.** See ["Why the 1M number won't sit
  still"](#why-the-1m-number-wont-sit-still) — GC, Testcontainers-per-fork, and worker-pool sizing
  are ruled out. What's left needs tooling this remote investigation didn't have: `async-profiler`
  or JFR run locally to see where the extra time actually goes in a slow fork, or `sudo powermetrics`
  during a run to check whether `RowDecodingScheduler`'s threads are landing on Apple Silicon's
  efficiency cores in the slow forks and performance cores in the fast ones. Needs someone with
  hands on the actual benchmark machine — a reasonable point to ask a domain expert / performance
  engineer to look at directly, not something to keep guessing at over successive JMH re-runs.
- **Investigate the single extreme outliers** (one operation taking 300,000+ µs against a
  ~120,000 µs median at 1M rows, appearing in some runs and not others under identical
  configuration) — separate phenomenon from the fork-to-fork mean variance above, also not
  explained by the measured GC pause time.
- **Try `MAX_COALESCE_BYTES` past 64KB** as a further one-variable experiment, now that
  `RESPONSE_CHUNK_DEMAND` has already been raised to 16 — not attempted, since the 1M
  instability above makes it hard to tell a real improvement from run-to-run noise until that's
  resolved first.
- **Re-run at 3 forks.** `StreamingScanBenchmark` now has 3-fork data; every other benchmark family
  on this page (protocol floor, aggregation, blocking calls, concurrency, throughput) is still
  single-fork and several of those results (protocol floor, aggregation) are close enough that fork
  noise could plausibly change the verdict.
- **Re-run `MixedWorkloadRapidRefreshCancelBenchmark` to completion** — this run has no `ourDriver`
  data to compare against client-v2's.
- **`-prof gc` on `MatchedPoolThreadsConcurrencyBenchmark`** to root-cause the blocking-caller
  regression (confirmed to exist, not yet explained beyond "blocking forfeits this driver's
  advantage").
- **Build `MixedWorkloadRapidRefreshPileUpBenchmark`**, the no-cancellation counterpart to the
  rapid-refresh scenario above (old and new queries both run to completion) — a named, not-yet-built
  companion benchmark.
- **Widen the concurrency/pool-size matrix** — `BoundedPoolConcurrencyBenchmark` and
  `PublicApiMatchedPoolThroughputBenchmark` currently test one pool size (8) and three concurrency
  levels (8/32/128); a real scalability sweep would cover more of both.

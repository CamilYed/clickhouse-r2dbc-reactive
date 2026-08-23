# Benchmark methodology

What each benchmark class actually exercises. Grouped by the question it answers, not by build
order. See [index.md](index.md) for the confidence warning and environment this applies to, and
[results.md](results.md) for what each of these actually found.

## Protocol floor — single row, no streaming

| Benchmark | What it measures |
| --- | --- |
| `TrivialQueryBenchmark` | `SELECT 1`, no table. The floor cost of one round trip before any real row decoding happens. |
| `PointQueryBenchmark` | A parameterized single-row lookup (`WHERE id = {id:UInt64}`), calling this driver's transport + decoder directly and client-v2's `Client` + `ClickHouseBinaryFormatReader` directly — both below the R2DBC SPI layer. |
| `PublicApiPointQueryBenchmark` | The same lookup, but through the public R2DBC SPI (`Connection`/`Statement`/`Result`) on this driver's side, and client-v2's public `Client` API on the other — what an actual application calls. Also isolates the cost of a `DriverObservationListener` being configured (`ourDriverNoopObservation` vs. `ourDriverEnabledObservation`). |

## Streaming and decode — where does a full scan's cost actually live

| Benchmark | What it measures |
| --- | --- |
| `StreamingScanBenchmark` | Full table scan (`SELECT * FROM t`, no `WHERE`) at 10k/100k/1M-row tiers, through this driver's real production path — `RowBinaryDecoder.decode(source, RowDecodingScheduler)`, the same call the shipped connector makes, not a benchmark-only shortcut. Also records time-to-first-row separately (logged at trial teardown, not in JMH's own result). |
| `TransportOnlyStreamingBenchmark` | The same query, same row tiers, but only sums response bytes — no row decoding on either side. Isolates transport cost alone. |
| `DecoderOnlyBenchmark` | Decodes one already-captured response payload from memory, repeatedly, at the same three tiers — no network at all. Isolates decode cost alone. `ourDriver` here safely uses `RowBinaryDecoder.decodeRows` (the scheduler-free shortcut) because the source is in-memory, not a live Netty response — see [index.md's "How to use this driver well"](index.md#how-to-use-this-driver-well) for why that distinction matters. The extra `ourDriverWithoutMapCopy`/`ourDriverCompactRow`/`compactRowDirectLoop`/`compactRowFluxNoBridge` methods are retained diagnostic variants from an earlier redesign investigation, not benchmarks of shipped behavior. |
| `AggregationBenchmark` | `GROUP BY` + `count()`/`avg()`/`quantile()` — ClickHouse's actual core use case. Unlike the scan benchmarks, the result set is always ~100 rows regardless of the row-count tier; what scales is server-side aggregation work, not client-side decode volume. |

## Concurrency — blocking vs. non-blocking calling style

| Benchmark | What it measures |
| --- | --- |
| `ConcurrencyBenchmark` | `@Threads(8)`: eight platform threads, each blocking on its own call, both drivers left at their own default (unmatched) connection pool. A same-thread-count, different-pool-budget comparison. |
| `MatchedPoolThreadsConcurrencyBenchmark` | Identical to the above, except both sides get the same explicit 8-connection pool. Isolates whether a `ConcurrencyBenchmark` gap comes from pool mismatch or from the blocking calling style itself. |
| `BoundedPoolConcurrencyBenchmark` | The scenario this driver is actually built for: a deliberately small, **matched** 8-connection pool, driven **non-blocking** — `Flux.flatMap(..., concurrency)` on this driver's side, `CompletableFuture`-returning async calls on client-v2's — at 8/32/128 logical concurrent queries. No JMH calling thread is blocked per in-flight query on either side; the physical pool budget is shared, and each side's own execution model differs (see the note below). |
| `PublicApiMatchedPoolThroughputBenchmark` | The same idea, through the public R2DBC SPI end to end (not `ClickHouseHttpTransport` directly), reporting real **throughput** (queries/sec) and a separate `HdrHistogram`-based per-query latency log, at the same matched 8-connection pool and 8/32/128 concurrency levels. This is the headline "how many real queries per second" number. |
| `DefaultPoolSlowQueryThroughputBenchmark` | The non-blocking, public-SPI analog of `ConcurrencyBenchmark`'s "own default pool" idea: each side left at its **own out-of-the-box** pool size (this driver's Reactor Netty default, ≥16; client-v2's fixed default of 10) rather than matched. Every query holds server-side via `sleep(0.5)`/`sleep(1.0)` — real point queries finish too fast (low single-digit ms) for even a small default pool to visibly queue, so this class trades query realism for pool-contention visibility. `concurrency` sweeps only 8/32 (see the class's own Javadoc for why 128 is out of scope here). The first CI run (2026-08-23) turned out to be measuring a `RowDecodingScheduler` bug rather than the intended pool comparison (see [results.md's Open follow-ups](results.md#open-follow-ups) for the root cause and fix); the post-fix re-run is published in [results.md](results.md#default-pool-slow-query-this-drivers-larger-default-pool-wins-once-its-actually-used-2026-08-23). |

Both sides expose asynchronous completion to the benchmark harness — neither one makes the JMH
calling thread block on a query — but their execution models underneath are not the same, and this
difference is what the concurrency benchmarks above are actually measuring, not a benchmark bug:

- **this driver** dispatches the HTTP request through Reactor Netty's non-blocking event-loop I/O —
  no platform thread is dedicated to or blocked by any single in-flight query, on either the caller
  or transport side.
- **client-v2**, with `.useAsyncRequests(true)` enabled (required for a fair comparison — see
  `BoundedPoolConcurrencyBenchmark`'s own Javadoc for why), runs its normal *blocking* HTTP call on
  a thread from its own internal cached-thread-pool executor. The caller isn't blocked, but a real
  platform thread from that executor is, for the duration of each query.

Concretely: this driver needs zero platform threads per in-flight query; client-v2's async mode
still needs one blocked executor thread per in-flight query, just not the JMH caller's thread. Item
5's resource-model measurement (thread count, CPU, RSS) is the planned follow-up to quantify what
that difference actually costs — see [results.md's Open follow-ups](results.md#open-follow-ups).

## Rapid-refresh cancellation — the "user hits refresh" scenario

| Benchmark | What it measures |
| --- | --- |
| `MixedWorkloadRapidRefreshCancelBenchmark` | 32 concurrent simulated user sessions, each firing one of 12 distinct heavy analytical queries (six-table join, `sleep(2)`-controlled fixed duration) and abandoning it 5ms later for the next — modeling a dashboard user rapidly hitting refresh before the previous load finished. This driver's cancellation (`switchMap`) tears down the connection and issues a best-effort `KILL QUERY`; client-v2's `CompletableFuture#cancel(true)` does not actually interrupt the already-running query (a well-known `CompletableFuture` limitation), so its abandoned queries keep competing for the pool and server resources. Deliberately asymmetric pools: this driver uses Reactor Netty's own default, client-v2 gets a generous, explicit 32-connection pool — the point is whether client-v2's pool still gets exhausted by pile-up even when sized generously, not whether a small pool alone explains a slowdown (already ruled out by `MatchedPoolThreadsConcurrencyBenchmark`, above). |

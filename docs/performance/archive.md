# Performance results archive

> **Retracted/fully-superseded result sections, moved out of [results.md](results.md) to keep the
> current page focused on numbers worth trusting today** — same spirit as
> [engineering/roadmap-archive.md](../../engineering/roadmap-archive.md). Kept in full, unedited,
> for the record: what was measured, why it turned out to be wrong or superseded, and what replaced
> it. Nothing here should be cited as a current claim about this driver's performance.

## Non-blocking, matched pool: retracted client-v2 benchmark-harness bug (2026-08-20, superseded 2026-08-24)

> [!WARNING]
> **The `BoundedPoolConcurrencyBenchmark` numbers in this section (3.5–4.1x latency, ~4x
> throughput) were measured against a client-v2 benchmark-harness bug, not a fair comparison.**
> client-v2's async client defaults `ClientConfigProperties.ASYNC_OPERATIONS` to `false`; without
> `.useAsyncRequests(true)` on the `Client.Builder` used here, `Client#query(...)` ran synchronously
> on the calling thread and returned an already-completed future — every client-v2 query in this
> benchmark's `Flux.flatMap(..., concurrency)` therefore ran on one sequential worker, not the
> matched 8-connection pool the table below claims. Confirmed by the math: client-v2's own numbers
> here show flat throughput and latency scaling almost exactly linearly with concurrency (8→32→128
> tracks 4x→4x) — the textbook signature of a single-server queue, not an 8-way pool.
>
> The fix (`.useAsyncRequests(true)`) shipped, and this class was re-run twice: once as
> [`PublicApiMatchedPoolThroughputBenchmark` on 2026-08-23](results.md#cloud-verified-matched-pool-real-async-on-both-sides-2026-08-23),
> once as `BoundedPoolConcurrencyBenchmark` itself in the [2026-08-24 mega
> sweep](results.md#full-mega-sweep--every-scenario-one-run-2026-08-24) — both show client-v2 ahead
> on latency by single-digit-to-low-double-digit percentages, the opposite direction from this
> retracted table and a much smaller magnitude. This section is kept for the record only.

<p align="center">
  <img src="../images/2026-08-20-bounded-pool-concurrency.png" width="720" alt="BoundedPoolConcurrencyBenchmark burst latency by concurrency level, this driver vs client-v2 (retracted)">
</p>

| Concurrency (pool=8, both sides) | this driver (mean) | client-v2 (mean) | verdict |
| --- | --- | --- | --- |
| 8 | 2700 µs | 9324 µs | ~~this driver 71.0% faster (3.5x)~~ retracted |
| 32 | 10,008 µs | 37,086 µs | ~~this driver 73.0% faster (3.7x)~~ retracted |
| 128 | 36,723 µs | 148,890 µs | ~~this driver 75.3% faster (4.1x)~~ retracted |

<p align="center">
  <img src="../images/2026-08-20-throughput.png" width="720" alt="Real point-query throughput through the public R2DBC SPI, matched 8-connection pool, this driver vs client-v2 (retracted)">
</p>

| Concurrency (pool=8, both sides) | this driver (ops/s) | client-v2 (ops/s) | verdict |
| --- | --- | --- | --- |
| 8 | 3516 | 900 | ~~3.9x throughput~~ retracted |
| 32 | 3615 | 900 | ~~4.0x throughput~~ retracted |
| 128 | 3541 | 893 | ~~4.0x throughput~~ retracted |

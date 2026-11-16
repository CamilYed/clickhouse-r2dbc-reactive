# Performance

This project keeps performance claims separate from architectural claims. The transport is
non-blocking and backpressure-aware because named tests prove those contracts; that does not, by
itself, mean every query is faster than another client.

> [!IMPORTANT]
> **A fresh baseline is required before publishing another driver-vs-client-v2 headline.** The
> cloud results in the archive compared against client-v2 `0.9.8`. The current build uses
> client-v2 `0.11.2`, and the experimental native decoder measured in the 2026-08-27 investigation
> was removed on 2026-11-05. Those historical numbers remain useful evidence about the measured
> commits, but they are not a current comparison.

## Current status

The last completed point-query investigation established a narrow conclusion:

- the raw Reactor Netty response path and client-v2 raw-response path were statistically tied;
- the required decode-scheduler boundary cost about 51 microseconds for the captured one-row
  response, around 2.4% of the roughly 2.17 millisecond complete request;
- the native decoder's clear isolated advantage became only about 0.4-1.3% at the complete public
  API boundary;
- no single safe production optimization was justified by the data.

That investigation therefore ended without a transport, scheduling, buffering, or R2DBC-path
change. The duplicate native RowBinary implementation was later removed because its maintenance
cost was not justified by its end-to-end effect. The production decoder still runs off the Reactor
Netty event loop because its `InputStream`-backed reader may wait for later network chunks.

The full evidence is retained in the [historical results](results.md) and the
[latency-path investigation](latency-path-isolation.md). They are archive material, not the first
thing a new user needs to read.

## Next trustworthy comparison

A new performance claim should come from the existing cloud workflow on the current `main` branch:

1. run both implementations in the same GitHub Actions job, VM, and ClickHouse process;
2. use the same explicit connection-pool size and equivalent public APIs;
3. use the `trusted-clean` profile for latency or throughput claims;
4. record the driver commit, client-v2 version, ClickHouse image, JDK, runner hardware, forks, and
   profilers with the result;
5. treat overlapping error intervals as a tie and keep single-fork runs as diagnostics only.

See [Running the benchmarks](running-benchmarks.md) for commands and the cloud workflow. The
[methodology](methodology.md) explains what each benchmark boundary actually measures.

## Guidance that does not depend on old benchmark numbers

- Keep application code reactive. Do not wrap driver calls in blocking futures or call `.block()`
  per query in a concurrent loop.
- Size `transportMaxConnections` for the number of physical requests ClickHouse should process in
  parallel, not for the number of logical subscribers. Excess acquisition demand is bounded by the
  configured pending-acquire policy.
- Consume large results as a stream instead of collecting every row before downstream processing.
- Expect cancellation to stop local consumption and close the request connection. Server-side
  `KILL QUERY` remains best effort; see [Known limitations](../reference/known-limitations.md).

## Performance documentation

- [Methodology](methodology.md) — benchmark boundaries and fairness rules.
- [Running the benchmarks](running-benchmarks.md) — local and cloud execution.
- [Historical results](results.md) — versioned measurements from older code and dependency states.
- [Latency-path investigation](latency-path-isolation.md) — the completed PR #99 elimination
  ladder.
- [Retracted and superseded results](archive.md) — results known to be invalid or fully replaced.

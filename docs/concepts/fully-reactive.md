# What "fully reactive" means here

Returning a reactive type is a necessary but insufficient condition. This driver is treated as
reactive end to end only if it satisfies all of the following:

| Property | Meaning |
| --- | --- |
| Deferred execution | The query is not sent before subscription. |
| Non-blocking I/O | No `Future#get()`, `CompletableFuture#join()`, `block()`, blocking semaphore, or thread-per-request wrapper on the query path. |
| Stream-oriented consumption | Responses are decoded incrementally; large results are not aggregated in memory before rows are emitted. |
| Backpressure-aware delivery | Downstream demand influences upstream work and buffering; intermediate queues stay bounded and documented. |
| Cancellation propagation | Cancelling a subscription removes a queued request or aborts an active exchange, releases buffers and connections, and — once the request has reached the server — sends a best-effort `KILL QUERY` so the server stops too; see [../reference/known-limitations.md](../reference/known-limitations.md) for why that's "best-effort" rather than a hard guarantee. |
| Bounded concurrency | Maximum active requests, maximum pending requests, and pending-acquire timeout are explicit; overload produces a predictable error, not an invisible queue. |
| Deterministic cleanup | Connections, response bodies, buffers, and decoder state are released on completion, error, timeout, and cancellation. |
| Reactive error signalling | Transport and ClickHouse errors surface through `onError` with proper R2DBC exception mapping. |
| No scheduler workaround | Moving blocking I/O to `boundedElastic`/`publishOn`/`subscribeOn` does not count as making the path reactive. |

Every property above is backed by a named test that would fail if it regressed — see
[../../engineering/roadmap-archive.md's Phase 4 sign-off](../../engineering/roadmap-archive.md#phase-4--fully-reactive-sign-off)
for the full evidence table (which test proves which property).

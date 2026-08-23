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

## Why row decoding needs its own scheduler at all

This driver's own transport is non-blocking end to end, but it deliberately reuses client-v2's
`RowBinaryWithNamesAndTypesFormatReader` for the actual byte-to-row decoding, rather than writing a
second RowBinary parser from scratch (see the top-level README's "why reuse client-v2's decoders"
rationale). That reader was written for a blocking world: it reads from a plain `InputStream`, and
each "give me the next row" call blocks the calling thread until enough bytes are available. Reusing
it is a deliberate trade-off — a proven, spec-compliant decoder instead of a second one to
maintain — but it means every row read is, underneath, a blocking call.

The flow, step by step:

1. A query goes out over Reactor Netty, non-blocking — no thread sits waiting for the response.
2. Response bytes arrive asynchronously, in chunks, as a `Flux<ByteBuffer>` — delivered on one of
   Reactor Netty's own event-loop threads. An application typically has only a handful of these
   (roughly one per CPU core), and they are shared across every concurrent request the whole
   process makes.
3. `FluxInputStreamBridge` adapts that `Flux<ByteBuffer>` into a plain `InputStream`, so client-v2's
   reader can consume it the way it expects to.
4. Reading from that `InputStream` is where the blocking call happens. If nothing moved this work
   off the event-loop thread, it would run *on* whichever event-loop thread happened to be driving
   the subscription at that moment — freezing that thread, and with it every other request
   currently sharing it, for however long the read takes.
5. `RowBinaryDecoder.decode` prevents that explicitly: both constructing the reader and every
   subsequent row read are wrapped in `subscribeOn(scheduler)`, telling Reactor to run that work on
   a worker thread from `scheduler`, never on whatever thread happens to request the next element.
6. `RowDecodingScheduler` is that worker pool — a small, dedicated, named (`clickhouse-r2dbc-decoder-*`)
   set of threads whose only job is running these blocking client-v2 calls, so a thread dump
   immediately identifies them.

**Why not just reuse `Schedulers.boundedElastic()`** — Reactor's own built-in answer to "I have a
blocking call, get it off my non-blocking thread"? Because `boundedElastic()` is one shared,
process-wide pool that every other library and unrelated blocking call in the same JVM also reaches
for by default. Row decoding under real load is sustained, systematic work, not the occasional
bursty blocking call `boundedElastic()`'s generous default sizing (10x the CPU core count) is tuned
for. Routing all of it through the shared pool risks two failure modes at once: this driver's decode
load starving unrelated blocking work elsewhere in the same process, and unrelated blocking work
elsewhere delaying this driver's own row decoding. A private, bounded, explicitly-owned scheduler
avoids both — see `RowDecodingScheduler`'s own Javadoc for its lifecycle/disposal contract, and
[../operations/connection-pooling.md](../operations/connection-pooling.md#the-decode-worker-pool-tracks-this-pools-size-not-the-cpu-core-count)
for why its size must track the connection pool size, not the CPU core count (a real bug this
project shipped and fixed once already — the decoder became a smaller, silent concurrency ceiling
underneath the pool when the two weren't kept in sync).

This mechanism — a private scheduler plus explicit `subscribeOn` — is also exactly why "No scheduler
workaround" is listed as its own property in the table above, not satisfied by this pattern alone:
moving blocking work off the calling thread makes the *path* not stall other requests, but it does
not by itself make row decoding itself non-blocking. See the "Rewrite the decode path" idea in
[ROADMAP.md](../../ROADMAP.md) for the more radical alternative this project has considered but not
committed to.

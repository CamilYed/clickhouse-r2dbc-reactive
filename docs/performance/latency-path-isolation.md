# Latency path isolation (2026-08-25 → in progress)

Deliverable of ROADMAP.md's Phase 12 "Formal latency-path-isolation plan (2026-08-25)" — the A/B/C/D
benchmark ladder that supersedes the three informal candidate findings the macrobench review doc
flagged (byte-copy, decoder-scheduler-doubles-as-transport-admission, unmeasured small-query fixed
overhead). Started once `feature/305-phase12-macrobench-pr1` merged into `main`
(`fc494a0`), per the explicit "do not begin before that signal" gate this plan was written under.

**Explicitly diagnostic-only.** No production code changes in this pass, not even client-v2
modifications, beyond a benchmark-local, non-published, minimal-type-coverage adapter (Variant D,
if it's ever built — see below).

## Exact pipeline, from source (2026-08-25, verified against current `main`)

```
ClickHouseHttpTransport.queryWithSummary(query)          [transport-http]
  → httpClient.post().uri(...).response(...)              Reactor Netty HttpClient, event-loop thread
  → Flux<ByteBuf> response  (RowBinaryWithNamesAndTypes, optionally LZ4-framed via compress=1)
  → ByteBufFlux body = ByteBufFlux.fromInbound(response.doOnCancel(killQueryBestEffort))
  → Mono.just(new ClickHouseQueryResponse(writtenRows, body))   -- still fully lazy, nothing sent yet

ClickHouseResult.decodePlain / decodeObserved(response, ...)    [connector]
  → response.body().asByteArray()          <-- Netty ByteBuf -> byte[] COPY happens here (Variant B target)
      .map(ByteBuffer::wrap)               -- Flux<ByteBuffer>, no further copy, just a view
  → RowBinaryDecoder.decode(body, decodingScheduler, compression)   [core]

RowBinaryDecoder.decode(source, scheduler, compression)          [core]
  → Mono.fromCallable(() -> newReader(source, compression))
      .subscribeOn(scheduler.asReactorScheduler())    <-- ADMISSION GATE: RowDecodingScheduler's
                                                            AdmissionGatedExecutorService permit is
                                                            acquired HERE, before anything below runs
      newReader(source, compression):
        FluxInputStreamBridge.subscribeTo(source, RESPONSE_CHUNK_DEMAND=16)
          <-- subscribes to `source` (ultimately the live Netty response) FOR THE FIRST TIME here,
              i.e. transport response acquisition/subscription happens INSIDE the already-scheduled
              callable, gated behind the same decoder-worker admission permit as decoding itself
              (Variant C target: today these are one gate, not two)
        new ListDecodingRowBinaryReader(inputStream, QuerySettings, ByteBufferAllocator)
          <-- client-v2's blocking constructor eagerly reads the RowBinaryWithNamesAndTypes header
              (blocks the decoder-scheduler worker thread, not the event loop - correct today,
              but this blocking read only starts after the admission permit above is already held)
  → .map(reader -> DecodedResult(columnsOf(reader),
        Flux.generate(() -> reader, emitNextRow, closeReader)
            .subscribeOn(scheduler.asReactorScheduler())))   <-- same scheduler, same admission gate,
                                                                  for every subsequent blocking
                                                                  nextRowValues() call as rows stream
```

**Boundary locations (file:method), for anyone re-verifying this independently:**

| Boundary | Location |
| --- | --- |
| HTTP request sent / response received | `ClickHouseHttpTransport.queryWithSummary` (`clickhouse-r2dbc-reactive-transport-http`) |
| `ByteBuf` → `byte[]` → `ByteBuffer` copy (Variant B target) | `ClickHouseResult.decodePlain`/`decodeObserved`, the `response.body().asByteArray().map(ByteBuffer::wrap)` line (`clickhouse-r2dbc-reactive-connector`) |
| Decoder-scheduler admission acquired | `RowBinaryDecoder.decode`'s `Mono.fromCallable(...).subscribeOn(reactorScheduler)` (`clickhouse-r2dbc-reactive-core`) |
| Transport response first subscribed to (Variant C target — currently *inside* the admission-gated callable, not before it) | `RowBinaryDecoder.newReader` → `FluxInputStreamBridge.subscribeTo(source, RESPONSE_CHUNK_DEMAND)` |
| Blocking client-v2 reader construction (reads wire header) | `RowBinaryDecoder.newReader`, `new ListDecodingRowBinaryReader(...)` |
| Per-row blocking decode, same scheduler | `RowBinaryDecoder.decode`'s `Flux.generate(...).subscribeOn(reactorScheduler)` |

One correction to the informal candidate finding this formalizes: `ChunkBuffer`/`bodyAsChunks()`
(mentioned in some earlier notes) do not exist on current `main` — the transport layer returns a
plain `ByteBufFlux`/`Flux<ByteBuf>` throughout; the diagram above reflects the actual current
source, not that earlier, since-superseded shape.

## Variant status

| Variant | What it changes | Status |
| --- | --- | --- |
| **A — baseline** | Nothing; exact production path, pool/decoder-workers pinned to 8 for a controlled comparison | **Built** — `LatencyPathVariantABenchmark` (see below) |
| **B — avoid the `ByteBuf`→`byte[]`→`ByteBuffer` copy** | Only if ownership can be proven correct under `-Dio.netty.leakDetection.level=paranoid` (no use-after-release, no leak, correct cancel/error/full-consumption cleanup) | Not started |
| **C — transport acquisition before decoder-scheduler admission** | Prototype starting `FluxInputStreamBridge.subscribeTo` *before* `subscribeOn(decoderScheduler)`, without buffering the whole response, blocking the event loop, or changing cancellation/connection-reuse/pool-size/decoder/compression | Not started |
| **D — benchmark-local `BinaryInput` adapter (optional, only after A/B/C)** | Minimal `read()`/`readFully(...)` adapter removing the blocking-`InputStream` boundary for only the types this benchmark needs | Not started |

## Variant A — built, not yet run

`LatencyPathVariantABenchmark` (`clickhouse-r2dbc-reactive-benchmarks/src/jmh/java/.../LatencyPathVariantABenchmark.java`),
new class rather than an edit to `TrivialQueryBenchmark`/`PointQueryBenchmark`/`StreamingScanBenchmark`:
those three are this project's ongoing headline-trend benchmarks and are deliberately left at each
driver's own default (unpinned) pool size — see their own Javadoc — so pinning pool=8 into them for
this one study would silently change what their historical numbers mean. This class faithfully
reproduces the same production code paths those three already exercise, with both drivers' pool and
this driver's decoder worker count explicitly pinned to 8 (`POOL_SIZE`), the pattern
`BoundedPoolConcurrencyBenchmark` already established (`ClickHouseHttpTransport(baseUrl,
Authentication, maxConnections)` / client-v2 `enableConnectionPool(true).setMaxConnections(8)
.useAsyncRequests(true)`).

Three scenarios (six `@Benchmark` methods — this driver + client-v2 per scenario, client-v2 kept as
context, not the point of this ladder): `thisDriverSelect1`/`clientV2Select1` (protocol floor, no
table), `thisDriverPoint`/`clientV2Point` (single-row parameterized lookup against
`PointQueryTable`), `thisDriverStream10k`/`clientV2Stream10k` (full 10,000-row scan). Deliberately
not the 1M-row tier `StreamingScanBenchmark` already covers — this ladder targets fixed per-query
overhead, which a large scan amortizes away.

**How to run** (per the plan's "Concurrency" section — `pool=8` fixed in the class; concurrency via
`-Pjmh.threads`, wired into `build.gradle.kts` the same way `jmh.forks`/`jmh.warmupIterations`
already were, specifically for this ladder — an unwired `-P` flag on this plugin is a silent no-op,
per that file's own comment, so this had to be added rather than assumed):

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantABenchmark -Pjmh.threads=1 -Pjmh.forks=3

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantABenchmark -Pjmh.threads=8 -Pjmh.forks=3
```

(`caffeinate -d -i` per `running-benchmarks.md`'s own advice — prevents the MacBook sleeping mid-run
for any multi-fork/unattended benchmark invocation on macOS.)

For the plan's "5-warmup/5-measurement" trusted-run deliverable target (once a quick default pass
confirms the class runs clean first), add `-Pjmh.warmupIterations=5 -Pjmh.iterations=5`
(`jmh.iterations` also newly wired alongside `jmh.threads` above — the build file's own defaults are
1 warmup / 3 measurement iterations, 1 fork).

**Not yet run** — same disclosed sandbox limitation as every other benchmark in this repo: no
network access for the Gradle wrapper distribution or a JDK 21 toolchain in this environment. Needs
a real run (concurrency=1 and concurrency=8, 3 forks each per this project's own "multi-fork numbers
are the ones worth acting on" standard) before its numbers go in the result table below.

## A/B/C/D result table

Not populated yet — Variant A hasn't been run, and B/C/D aren't built. Fill in once each variant has
a trusted (3-fork) run at both concurrency levels:

| Variant | Scenario | Concurrency | `thisDriver` mean (µs) | p99 (µs) | vs. Variant A | vs. client-v2 |
| --- | --- | --- | --- | --- | --- | --- |
| A | SELECT 1 | 1 | — | — | — | — |
| A | SELECT 1 | 8 | — | — | — | — |
| A | point | 1 | — | — | — | — |
| A | point | 8 | — | — | — | — |
| A | stream 10k | 1 | — | — | — | — |
| A | stream 10k | 8 | — | — | — | — |

## Hypothesis-ranking decision table

Not populated yet — depends on the result table above. Will rank byte-copy /
scheduler-admission-placement / `InputStream`-adaptation / fixed-statement-setup, each keep/reject
with the measured delta, per the plan's "Deliverable" section, and end in **exactly one** recommended
next production change (or an explicit "no production change justified yet").

## Explicitly out of scope for this pass

Virtual-thread decoder default, decoder worker widening, pending-acquire tuning,
`ArrayBlockingQueue`/SPSC replacement, `FluxInputStreamBridge` coalescing rewrite, response
compression defaults, default pool size, a full custom RowBinary decoder, and the Spring Boot
macrobenchmark (separate ROADMAP.md Phase 12 track, PR2 still open) — separate questions, not this
one.

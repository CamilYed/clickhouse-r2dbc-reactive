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

## Variant A — built, trusted 3-fork runs done at both concurrency levels

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

**All runs done (2026-08-25, user's machine)**: a single-fork sanity pass, then `-Pjmh.threads=1
-Pjmh.forks=3` (~13m47s) and `-Pjmh.threads=8 -Pjmh.forks=3`, all `BUILD SUCCESSFUL`, no `-prof`/leak
warnings. Full result table above. Next: Variant B/C, then the hypothesis-ranking table — the
tail-latency finding flagged above is the most concrete open thread Variant A surfaced.

## A/B/C/D result table

**Single-fork, single-warmup-iteration sanity run only (2026-08-25, no `-Pjmh.threads` passed, so
JMH's own default of 1 thread — coincidentally the plan's `-t 1` concurrency level).** Per this
project's own confidence standard (see [index.md](index.md#-tip)), a single-fork number is a first
signal, not something to act on — the 3-fork `-t 1`/`-t 8` runs are still needed before drawing any
conclusion. Recorded here so the shape is visible while the trusted runs are pending, not as a
result to build a decision on.

| Variant | Scenario | Concurrency | `thisDriver` mean (µs) | client-v2 mean (µs) | `thisDriver` p99 (µs) | client-v2 p99 (µs) | thisDriver vs. client-v2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A | SELECT 1 | 1 (single-fork, untrusted) | 635.2 ± 2.2 | 609.6 ± 3.9 | 887.8 | 859.4 | ~4.2% slower |
| A | SELECT 1 | 1 (**3-fork, trusted**) | 607.1 ± 0.9 | 582.8 ± 0.8 | 843.8 | 794.6 | ~4.2% slower |
| A | SELECT 1 | 8 (**3-fork, trusted**) | 1493.8 ± 3.5 | 1423.4 ± 3.4 | 3059.7 | 3014.7 | ~4.9% slower |
| A | point | 1 (single-fork, untrusted) | 1239.2 ± 4.9 | 1162.8 ± 3.9 | 1804.3 | 1800.7 | ~6.6% slower |
| A | point | 1 (**3-fork, trusted**) | 1173.6 ± 2.1 | 1143.3 ± 1.5 | 1462.3 | 1447.9 | ~2.6% slower |
| A | point | 8 (**3-fork, trusted**) | 2191.0 ± 5.6 | 2088.1 ± 4.2 | 4276.2 | 4145.2 | ~4.9% slower |
| A | stream 10k | 1 (single-fork, untrusted) | 4227.3 ± 27.8 | 4441.4 ± 31.1 | 5595.5 | 6209.4 | ~4.8% faster |
| A | stream 10k | 1 (**3-fork, trusted**) | 4213.9 ± 15.7 | 5080.5 ± 25.2 | 5595.1 | 7651.3 | ~17.1% **faster** |
| A | stream 10k | 8 (**3-fork, trusted**) | 4132.2 ± 9.5 | 4556.1 ± 5.6 | 6815.7 | 6602.8 | ~9.3% faster mean, ~3.2% **worse** p99 |

All six 3-fork rows now in (2026-08-25). Sample counts at `-t 8` (~328k/344k/174k/158k) are ~4.2–4.4x
the `-t 1` 3-fork run's, not the ~8x a fully unblocked 8-way fan-out would give — consistent with
both sides converging on the shared 8-connection pool as the actual bottleneck once concurrency
matches pool size, exactly what "matched-pool" concurrency is supposed to show.

**Notable tail-latency finding at `-t 8`, not yet investigated — flag for the hypothesis-ranking
table:** `thisDriver`'s p100 (max) is dramatically worse than client-v2's at full pool saturation,
on both scenarios that showed it — `point` 104,988µs vs 38,207µs (~2.7x), `stream 10k` 126,484µs vs
16,450µs (~7.7x) — even though `stream 10k`'s p99 and mean are competitive or better. Mean/p99 alone
would have hidden this; only the max column surfaces it. Plausible candidate explanations, not yet
distinguished: (a) the `RowDecodingScheduler` admission gate queueing a request behind a slow
neighbor once all `POOL_SIZE` permits are held (the exact "decoder scheduler doubles as transport
admission control" mechanism Variant C targets), (b) GC pause outlier (no `-prof gc` run yet), (c)
JIT/Blackhole/JVM-warmup artifact per JMH's own printed caveat above. Needs a profiler run
(`-Pjmh.profilers=gc`, maybe `-lprof` for lock/stack profiling) and a negative-control comparison
(e.g. does `PublicApiMatchedPoolThroughputBenchmark`'s existing matched-pool data show the same
max-latency asymmetry, or is this specific to this class/scenario?) before attributing it to any one
cause — exactly the kind of thing this project's own JMH banner above warns against assuming.

Directionally consistent with the mega sweep's headline ("Protocol floor ... essentially tied to
~8% slower", "Full table scan ... 11–12% lower latency") at concurrency=1; at concurrency=8 the
`SELECT 1`/`point` deficit holds steady at ~5% (not worsening much under load), and `stream 10k`'s
mean advantage narrows from 17.1% (at `-t 1`) to 9.3% (at `-t 8`) while its p99 actually flips to
~3.2% worse — worth folding into the hypothesis-ranking table once B/C are built, alongside the
p100 finding above. Full JMH output (every percentile) is in `build/results/jmh/results.json` on the
machine that ran it, not committed to git per this project's convention.

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

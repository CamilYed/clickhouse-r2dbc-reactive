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
| **B — avoid the `ByteBuf`→`byte[]`→`ByteBuffer` copy** | Only if ownership can be proven correct under `-Dio.netty.leakDetection.level=paranoid` (no use-after-release, no leak, correct cancel/error/full-consumption cleanup) | **Settled — real payoff too small to matter.** Ownership proven correct (leak-clean real-HTTP run); real-HTTP timing inconclusive (≤1.3%, sign-flipping); network-free microbenchmark isolated the true cost: 15-35ns/call at production response sizes, negligible against a ~600-1150µs round trip. Not the source of Variant A's deficit — not adopted. See below. |
| **C — transport acquisition before decoder-scheduler admission** | Prototype starting `FluxInputStreamBridge.subscribeTo` *before* `subscribeOn(decoderScheduler)`, without buffering the whole response, blocking the event loop, or changing cancellation/connection-reuse/pool-size/decoder/compression | **Done — inconclusive, not adopted.** Trusted 3-fork results at both concurrency levels are internally inconsistent (favors early acquisition at `-t1`, doesn't at `-t8` — the opposite of the predicted admission-gate-contention signature) and don't reproduce their own single-fork sanity passes either. Not confirmed, not confidently rejected on mechanism, not actionable. See below. |
| **D — `MinimalRowBinaryReader`: skip client-v2's reader/parsing layer** | Hand-rolled `RowBinaryWithNamesAndTypes` decoder (`UInt8`/`UInt64`/`String`/`Decimal`) reading off `ZeroCopyByteBufInputStreamBridge`, replacing client-v2's `RowBinaryWithNamesAndTypesFormatReader`/`AbstractBinaryFormatReader` — the one layer A/B/C/#309 never isolated | **Reader contributes but does not explain the full gap — scenario-dependent, real for multi-column decode.** Trusted `-t1`+`-t8` both in: `SELECT 1` (1 column) is noise/direction-flip (+5.4% at t1, -0.6% at t8), `point` (2 columns) reproduces at both concurrency levels (+7.27% t1, +5.23% t8, ~11x combined error both times) — consistent with a per-column decode cost, not a fixed per-request one. `*Stream10k` pair (compounding-over-rows check) and network-free `ReaderOnlyMicrobenchmark` (pure reader cost, no transport/scheduler/pool) built next per the plan's "STOP and evaluate" gate. See below. |

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

## Variant B — trusted 3-fork `-t1` run confirms: copy avoidance does not explain the deficit

`LatencyPathVariantBBenchmark` and `ZeroCopyByteBufInputStreamBridge`
(`clickhouse-r2dbc-reactive-benchmarks/src/jmh/.../LatencyPathVariantBBenchmark.java` and
`.../src/main/.../ZeroCopyByteBufInputStreamBridge.java`), built on `feature/314-latency-path-isolation`
per the recommendation above.

**First real run (2026-08-25) caught a construction bug, not a timing result**: all four
`@Benchmark` methods failed identically with `ClientException: Time zone is not set.` —
`readFirstRow` constructed `RowBinaryWithNamesAndTypesFormatReader` with a bare `new
QuerySettings()`, missing the `.setUseTimeZone("UTC")` core's own `RowBinaryDecoder.newReader`
always sets. Fixed (commit `a5599c3`); `LatencyPathVariantABenchmark` never hit this because it goes
through `RowBinaryDecoder.decode()`, not a hand-constructed reader.

**Leak-detection run, after the fix**: `BUILD SUCCESSFUL`, results table below — no leak lines
reported by `LeakRecordingResourceLeakDetector` in the output shared back. (Flagging this rather than
asserting it outright: the pasted output was the results table, not the full scrollback with the
leak detector's own log lines — worth a second look at the full output if there's any doubt before
treating the retain/release contract as verified.)

**The single-fork result itself is a genuine, if untrusted, negative signal against the
copy-avoidance hypothesis**: `copyPathPoint` (1357.7 ± 5.3 µs) vs. `zeroCopyPathPoint` (1351.9 ± 4.8
µs) and `copyPathSelect1` (815.2 ± 3.3 µs) vs. `zeroCopyPathSelect1` (813.0 ± 3.4 µs) — differences
of ~0.4% and ~0.3%, smaller than either side's own margin of error. If the `asByteArray()` copy were
the fixed-overhead source of Variant A's flat ~4-5% deficit, removing it on exactly these same
tiny-response scenarios should have shown up here. It didn't, at least not on one fork. Needs the
trusted 3-fork run before treating this as settled (a single fork is "a first signal, not something
to act on" per this project's own standard — the same standard that already caught one false lead
this ladder produced), but this tempers the Variant A recommendation's confidence rather than
confirming it.

**Trusted 3-fork `-t1` run (2026-08-25), leak-clean (user confirmed no leak lines) — confirms the
single-fork signal, and settles the direction**: `copyPathPoint` 1140.9 ± 1.95 µs vs.
`zeroCopyPathPoint` 1155.5 ± 2.44 µs — zero-copy is **~1.3% slower**, a difference clearly outside
both sides' error bars this time (combined error ~4.4 µs vs. a 14.6 µs gap). `copyPathSelect1` 592.4
± 1.43 µs vs. `zeroCopyPathSelect1` 586.8 ± 1.27 µs — zero-copy is ~0.9% faster, also outside error
bars but tiny and in the *opposite* direction from point.

Two scenarios, both statistically real at 3 forks, pointing opposite ways, both under ~1.5% — this
is not the signature of a ~4-5% fixed-overhead source being removed. **The `asByteArray()` copy does
not explain Variant A's deficit.** Worth noting for calibration: this run's own absolute numbers
(point ~1141µs, SELECT1 ~592µs) are meaningfully lower than the single-fork run's (~1358µs/~815µs)
for both `copyPath*` and `zeroCopyPath*` alike — a reminder that comparing *across* separate JMH
invocations is exactly the trap this class's self-contained-pair design exists to avoid; only the
within-run `copyPath*`-vs-`zeroCopyPath*` comparison above is meaningful.

**Verdict on the copy-avoidance hypothesis: not supported.** Two independent runs (1-fork sanity,
3-fork trusted `-t1`) agree the effect is at most ~1%, inconsistent in direction, and doesn't survive
outside error bars in the single-fork pass at all. This retracts the Variant A recommendation's
premise, the same way the earlier tail-latency finding was retracted — evidence changed the
conclusion, so the doc says so rather than defending the original guess. The `-t8` trusted run is
still worth collecting for completeness (matches Variant A's own two-concurrency-level protocol,
and would catch anything only visible under matched-pool contention), but shouldn't be expected to
reverse this.

| Variant | Scenario | Concurrency | copy path mean (µs) | zero-copy mean (µs) | copy path p99 (µs) | zero-copy p99 (µs) | zero-copy vs. copy |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B | SELECT 1 | 1 (single-fork, untrusted) | 815.2 ± 3.3 | 813.0 ± 3.4 | 1472.5 | 1454.1 | ~0.3% faster (noise-level) |
| B | point | 1 (single-fork, untrusted) | 1357.7 ± 5.3 | 1351.9 ± 4.8 | 2038.2 | 2009.1 | ~0.4% faster (noise-level) |
| B | SELECT 1 | 1 (**3-fork, trusted**) | 592.4 ± 1.43 | 586.8 ± 1.27 | 801.8 | 778.2 | ~0.9% faster |
| B | point | 1 (**3-fork, trusted**) | 1140.9 ± 1.95 | 1155.5 ± 2.44 | 1388.8 | 1417.2 | ~1.3% **slower** |

### Network-free follow-up: is the ≤1.3% real, or this machine's noise floor?

Fair question raised mid-investigation: both runs above were on the same MacBook — is "no meaningful
difference" actually "there's a real effect, buried under this machine's Docker Desktop/network noise
floor"? And would CI look different? Reasoning it through: CI isn't necessarily cleaner — it trades
Mac Docker Desktop's virtualization overhead for cloud multi-tenancy jitter, a different noise source,
not a quieter one. The mean-level round trip here is dominated by hundreds of µs of
network/HTTP/Docker cost; the copy in question is a `memcpy` of tens to low hundreds of bytes, which
should cost nanoseconds. Rerunning the same network-bound benchmark on a different machine doesn't
answer whether the effect is real — it just samples a different noise floor.

`ZeroCopyByteBufInputStreamBridgeMicrobenchmark`
(`clickhouse-r2dbc-reactive-benchmarks/src/jmh/.../ZeroCopyByteBufInputStreamBridgeMicrobenchmark.java`)
removes the noise floor entirely instead: no container, no HTTP, no client-v2 reader, no scheduler
hand-off — just the `asByteArray()`-equivalent copy step in isolation, the same way
`FluxInputStreamBridgeMicrobenchmark` isolated that bridge's own queue/coalescing cost from network
wait and found it decisively negligible (~40-50ns/chunk, ~200x smaller than the production per-chunk
figure). `copyPath`/`zeroCopyPath` each build a fresh `ByteBuf` per invocation (allocation cost
common to both, cancels out of the comparison) and sweep `responseBytes` from 64 bytes (below real
`SELECT 1`/point response sizes) up through 64 KB.

**How to run** (leak-detection first, then the usual sanity/3-fork sequence):

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=ZeroCopyByteBufInputStreamBridgeMicrobenchmark \
  -Pjmh.jvmArgsAppend=-Dio.netty.leakDetection.level=paranoid,-Dio.netty.leakDetection.targetRecords=25,-Dio.netty.customResourceLeakDetector=io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=ZeroCopyByteBufInputStreamBridgeMicrobenchmark -Pjmh.forks=3
```

**First real run caught a genuine leak — in the benchmark harness, not in
`ZeroCopyByteBufInputStreamBridge` itself.** Every `zeroCopyPath` invocation leaked its synthetic
`ByteBuf`, exhausting direct memory (~9.6GB, the default `-XX:MaxDirectMemorySize` ceiling) at the
larger `responseBytes` tiers and producing bogus multi-second p100 values before an `OutOfMemoryError`
killed the run. Cause: `Flux.just(buf)` delivers `onNext` synchronously inside `subscribeTo`'s
constructor call, but — unlike real Reactor Netty `ByteBufFlux` sources — never itself releases the
buffer once `onNext` returns; the bridge's `hookOnNext` retains on the assumption that release will
happen, per Netty's documented "retain past `onNext`" contract, which `Flux.just` doesn't honor.
`LatencyPathVariantABenchmark`'s (sic — `LatencyPathVariantBBenchmark`'s) earlier real-HTTP trusted run
was leak-clean, confirming the bridge class itself is correct against actual Reactor Netty sources —
this was purely a synthetic-harness gap. Fixed (commit `8f8b0e0`) by having the benchmark simulate that
auto-release itself, once, right after `subscribeTo` returns. `copyPath`'s numbers from the failed run
were unaffected (it never touches the leaking code path) and are folded into the clean run below.

**Clean single-fork sanity run, after the fix (2026-08-25) — decisive, and in the opposite direction
from the copy-avoidance hypothesis:**

| `responseBytes` | `copyPath` mean (µs) | `zeroCopyPath` mean (µs) | zero-copy vs. copy |
| --- | --- | --- | --- |
| 64 | 2.552 ± 0.011 | 3.969 ± 0.008 | **+55.5% slower** |
| 256 | 2.587 ± 0.010 | 3.943 ± 0.009 | **+52.4% slower** |
| 1024 | 2.645 ± 0.009 | 4.015 ± 0.008 | **+51.8% slower** |
| 4096 | 2.853 ± 0.016 | 4.155 ± 0.007 | **+45.6% slower** |
| 16384 | 3.836 ± 0.010 | 4.484 ± 0.009 | +16.9% slower |
| 65536 | 7.323 ± 0.026 | 7.112 ± 0.015 | ~2.9% faster |

No leak, no OOM, no multi-second p100 outliers (max p100 across all tiers/both paths: 816µs at
`responseBytes=65536`) — a healthy run. And it settles the question this section opened with: the
effect isn't buried under this machine's noise floor, because at these sample counts (528k-1.04M per
cell) and this error magnitude (±0.01-0.03µs), a 45-55% gap isn't noise on any machine. **Zero-copy is
consistently and substantially *slower* than the copy path at every response size real `SELECT
1`/point queries actually produce (tens to low hundreds of bytes), only catching up around 16-64KB.**
The explanation is straightforward once isolated like this: a `memcpy` of a few dozen/few hundred
bytes is nearly free (`System.arraycopy`-backed, sub-microsecond); `ZeroCopyByteBufInputStreamBridge`'s
own machinery — atomic retain/release refcount operations, a `Deque<ByteBuf>`, sealed-interface
record-pattern dispatch — has its own fixed per-call cost that a tiny copy never gets the chance to
outweigh. Only once the response is tens of KB does the O(n) copy cost finally catch up to that fixed
overhead.

**Confound confirmed — clean rerun (no `-Dio.netty.leakDetection.*`) reverses the direction entirely:**

| `responseBytes` | `copyPath` mean (µs) | `zeroCopyPath` mean (µs) | zero-copy vs. copy |
| --- | --- | --- | --- |
| 64 | 0.147 ± 0.004 | 0.132 ± 0.004 | **~10.2% faster** |
| 256 | 0.149 ± 0.006 | 0.139 ± 0.005 | **~6.7% faster** |
| 1024 | 0.202 ± 0.008 | 0.167 ± 0.005 | **~17.3% faster** |
| 4096 | 0.404 ± 0.015 | 0.296 ± 0.005 | **~26.7% faster** |
| 16384 | 1.233 ± 0.020 | 0.587 ± 0.004 | **~52.4% faster** |
| 65536 | 5.092 ± 0.021 | 2.398 ± 0.003 | **~52.9% faster** |

The under-instrumentation run above was measuring the leak detector's own per-`ByteBuf`-call tax, not
the bridge design — exactly the confound flagged before treating it as final. Clean, `zeroCopyPath` is
*consistently faster* at every tier, and the margin grows with size (as expected: copying is O(n),
avoiding it isn't) — the opposite conclusion from the confounded run, and absolute magnitudes 10-20x
smaller across the board (the leak detector's instrumentation tax was several µs *per call*, dwarfing
the actual few-hundred-nanosecond-to-low-microsecond real costs being measured).

**Reconciling with the real-HTTP ambiguity (Variant A/B, above): both are correct, at different
scales.** At `SELECT 1`/point production response sizes (64-1024 bytes), the clean savings is **15-35
nanoseconds per call** (0.015µs at 64B, 0.035µs at 1024B) — real, reproducible, and utterly negligible
against a ~600-1150µs real network round trip (a ~0.003-0.03% effect). That's exactly why
`LatencyPathVariantBBenchmark`'s real-HTTP run couldn't detect a consistent signal: there wasn't
enough of one to detect at that scale, not because the effect doesn't exist. The savings only becomes
practically visible from 16KB up (~0.6-2.7µs) — sizes this ladder's `SELECT 1`/point scenarios never
produce, and which this driver's streaming scenario already beats client-v2 on without this change
(Variant A: ~9-17% faster mean on `stream 10k`, no copy-avoidance involved).

**Verdict, final: `asByteArray()`'s copy is not the source of Variant A's flat ~4-5% `SELECT
1`/point deficit — settled, not just unsupported.** Copy-avoidance is a genuine, measurable,
reproducible micro-optimization (confirmed at the isolated-CPU-cost level, ruling out both
sealed-interface dispatch — cheap `instanceof`-chain, checked and ruled out — and JVM/environment
noise as explanations), but its absolute payoff at this driver's actual small-response workload is
nanoseconds, not the microseconds the real deficit represents. Not worth production adoption for that
reason, not because it doesn't work. `ZeroCopyByteBufInputStreamBridge`/
`ZeroCopyByteBufInputStreamBridgeMicrobenchmark` stay diagnostic, benchmark-local artifacts per the
"no production code changes in this pass" scope — the finding is complete without promoting them.

**Design, and three deliberate departures from Variant A worth knowing before reading numbers:**

- **Self-contained A/B pair, not a cross-run comparison.** Both `copyPath*` and `zeroCopyPath*`
  `@Benchmark` methods live in *this one class*, share the same `ourTransport` instance and the same
  client-v2 reader call, and differ only in which `InputStream` bridge feeds the reader —
  `core.FluxInputStreamBridge` (the production copy) vs. `ZeroCopyByteBufInputStreamBridge` (the
  prototype). Comparing this class's own two halves against each other isolates the one variable in
  question; comparing this class's numbers against Variant A's separately-run class would not — two
  independent JMH invocations carry run-to-run noise Variant A's own retracted tail-latency finding
  already showed is real.
- **Response compression forced to `NONE`, not the production `LZ4` default.** `core.ClickHouseLz4InputStream`
  (the LZ4 unwrapper) is package-private in `core`; reusing it from the benchmarks module would mean
  widening core's visibility purely for a benchmark, which this pass's "no production code changes"
  scope rules out. Decompression is itself a copy — a separate question from the one this variant
  isolates — so running both paths uncompressed keeps the comparison clean. Applies identically to
  both `copyPath*` and `zeroCopyPath*`, so it doesn't bias the A-vs-B comparison within this class,
  but it does mean this class's absolute numbers aren't directly comparable to Variant A's (which run
  under LZ4).
- **`SELECT 1` and point only, no streaming.** Variant A's deficit showed up on `SELECT 1`/point, not
  streaming (which already favored this driver) — so streaming isn't needed to test this hypothesis,
  and skipping it also avoids needing `ZeroCopyByteBufInputStreamBridge`'s one disclosed, unclosed
  gap: a narrow race where Reactor can deliver a further chunk during `close()`'s hard-cancel cleanup
  sweep (see that class's own Javadoc). Both scenarios here always read to natural completion, never
  early-cancel, so that gap isn't exercised.

**Before trusting any run**, verify no leak with:

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantBBenchmark \
  -Pjmh.jvmArgsAppend=-Dio.netty.leakDetection.level=paranoid,-Dio.netty.leakDetection.targetRecords=25,-Dio.netty.customResourceLeakDetector=io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector
```

then, once that comes back clean, the same sanity-then-trusted-3-fork sequence Variant A used:

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=LatencyPathVariantBBenchmark

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantBBenchmark -Pjmh.threads=1 -Pjmh.forks=3

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantBBenchmark -Pjmh.threads=8 -Pjmh.forks=3
```

## Variant C — done, inconclusive, not adopted

`LatencyPathVariantCBenchmark`
(`clickhouse-r2dbc-reactive-benchmarks/src/jmh/java/.../LatencyPathVariantCBenchmark.java`) — the
last remaining hypothesis after GC, the `asByteArray()` copy (Variant B), and fixed
query/statement construction cost (task #309) were each ruled out. Same self-contained-pair shape
as `LatencyPathVariantBBenchmark`: one class, same transport instance, same client-v2 reader,
`ResponseCompression.NONE` (same package-private-`ClickHouseLz4InputStream` reason as Variant B),
`POOL_SIZE=8`.

**What differs between the two scenarios** (see class Javadoc for the full reasoning): today,
`FluxInputStreamBridge.subscribeTo` — the call that actually subscribes to the transport response
and thereby sends the HTTP request, per this project's own "request not sent before subscription"
boundary — runs *inside* `Mono.fromCallable(...).subscribeOn(decodeScheduler)`, so it waits for a
decoder-scheduler worker permit before the request is even sent
(`currentOrderingSelect1`/`currentOrderingPoint`). The prototype
(`earlyAcquisitionSelect1`/`earlyAcquisitionPoint`) calls `subscribeTo` eagerly, on the calling
thread, before `subscribeOn` is reached — decoupling "wait for a decoder-worker slot" from "wait
for the network" for the first time in this pipeline. Confirmed safe to call this way:
`FluxInputStreamBridge`'s constructor only does a synchronous, non-blocking `source.subscribe(...)`
— actual bytes still arrive later, asynchronously, on Reactor Netty's own event loop, exactly as in
the current-ordering scenario.

**Expected signature of a real effect:** negligible difference at `-t1` (nothing is contending for
either the HTTP pool or the decoder scheduler, so there's no admission queueing to decouple), and a
narrowing gap at `-t8` if this ordering is actually part of Variant A's deficit — matching this
doc's own earlier observation that the deficit is "flat, not growing much with load" (a fixed-cost
signature) rather than "growing with contention" (a queueing signature); if `earlyAcquisition`
doesn't move the `-t8` numbers either, this hypothesis is rejected the same way Variant B and task
#309 were, and the deficit's source remains open.

**How to run** — sanity pass, then the same trusted 3-fork sequence at both concurrency levels
Variant A/B used:

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=LatencyPathVariantCBenchmark

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantCBenchmark -Pjmh.threads=1 -Pjmh.forks=3

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantCBenchmark -Pjmh.threads=8 -Pjmh.forks=3
```

No leak-detection wiring needed here (no `ByteBuf` involved, only `ByteBuffer` via the same
`asByteArray()` path Variant A already uses) — this class carries no correctness risk beyond what
Variant A's existing, already-trusted pipeline already covers; the only thing under test is
ordering.

**Single-fork sanity, `-t1` (default, no `-Pjmh.threads` passed) (2026-08-25):** matches this
project's own confidence standard — a single-fork number is a first signal, not something to act
on. Recorded here so the shape is visible while trusted runs are pending.

| Variant | Scenario | Concurrency | current-ordering mean (µs) | early-acquisition mean (µs) | current p99 (µs) | early p99 (µs) | early vs. current |
| --- | --- | --- | --- | --- | --- | --- | --- |
| C | SELECT 1 | 1 (single-fork, untrusted) | 570.6 ± 1.94 | 574.2 ± 2.26 | 757.8 | 793.6 | ~0.6% **slower** |
| C | point | 1 (single-fork, untrusted) | 1109.1 ± 3.47 | 1119.6 ± 3.28 | 1359.9 | 1364.0 | ~0.9% **slower** |
| C | SELECT 1 | 8 (single-fork, untrusted) | 1601.9 ± 9.32 | 1569.4 ± 5.75 | 3850.2 | 3564.2 | ~2.0% **faster** |
| C | point | 8 (single-fork, untrusted) | 2284.0 ± 8.09 | 2249.5 ± 7.83 | 4808.7 | 4792.3 | ~1.5% **faster** |

Both diffs sit within/at the edge of combined error bars (SELECT 1: 3.6µs diff vs. ±2-2.3µs error
each side; point: 10.5µs diff vs. ±3.3-3.5µs error each side) — noise-level, not a signal either
way. **This matches this section's own prediction above**: no meaningful difference at `-t1`, since
nothing is contending for either the HTTP pool or the decoder scheduler at concurrency 1, so there
is no admission-gate queueing for early acquisition to decouple anything from.

**`-t8` reverses direction, cleanly, in both scenarios — the predicted signature of a real
admission-gate effect.** Unlike the `-t1` pass (diff smaller than combined error, no direction to
trust), the `-t8` diff (SELECT 1: 32.5µs; point: 34.5µs) is roughly **2.1-2.2x the combined error**
in both scenarios, consistently favoring `earlyAcquisition`, and `p50`/`p90`/`p95`/`p99` all move
the same direction, not just the mean — the shape a genuine effect produces, as opposed to one or
two outlier samples dragging a mean around. This is single-fork and untrusted per this project's
own confidence standard (a single-fork number is a first signal, not something to act on) — a
3-fork trusted run at both `-t1` and `-t8` is needed before this can be treated as confirmed, but
unlike Variant B and task #309 (which were rejected outright, 20-150x too small even in their most
generous single-fork framing), this is the first result in this investigation whose *direction* and
*magnitude* both plausibly fit the hypothesis: negligible at `-t1`, real and reproducible-looking at
`-t8`.

**`-t8` trusted 3-fork run does not reproduce the single-fork signal — retracted (2026-08-25).** The
single-fork pass above was called out explicitly as "the first result in this investigation whose
direction and magnitude both plausibly fit the hypothesis." The trusted 3-fork rerun reverses that
call: SELECT 1 now shows no difference at all (1587.8 vs 1589.9µs, ~0.13%, deep inside combined
error bars), and point *flips direction* — `earlyAcquisition` is now ~1.3% **slower**, not faster
(2233.3 vs 2261.5µs, diff ~2.7x combined error, opposite sign from the single-fork pass). Retracted
as a lead, per the same discipline already applied once in this doc to Variant A's tail-latency
finding and to Variant B/task #309's verdicts: a single-fork number is a first signal, not something
to act on, and this one did not survive a trusted rerun.

| Variant | Scenario | Concurrency | current-ordering mean (µs) | early-acquisition mean (µs) | current p99 (µs) | early p99 (µs) | early vs. current |
| --- | --- | --- | --- | --- | --- | --- | --- |
| C | SELECT 1 | 8 (**3-fork, trusted**) | 1587.8 ± 3.77 | 1589.9 ± 3.76 | 3715.1 | 3788.8 | ~0.1% slower (noise-level) |
| C | point | 8 (**3-fork, trusted**) | 2233.3 ± 4.48 | 2261.5 ± 5.95 | 4759.6 | 4816.9 | ~1.3% **slower** |

With the `-t8` signal gone — the concurrency level where an admission-gate-ordering effect would be
expected to show up most clearly — a trusted `-t1` run isn't expected to reverse this, but is still
worth collecting for completeness, matching Variant A/B's own two-concurrency-level protocol before
writing a final verdict.

**Trusted 3-fork `-t1` run (2026-08-25) — surprising, and it does *not* rescue the hypothesis
either.** Contrary to the expectation just above, `-t1` shows a real, sizeable difference — the
opposite of the "negligible at `-t1`" prediction this whole section was built on:

| Variant | Scenario | Concurrency | current-ordering mean (µs) | early-acquisition mean (µs) | current p99 (µs) | early p99 (µs) | early vs. current |
| --- | --- | --- | --- | --- | --- | --- | --- |
| C | SELECT 1 | 1 (**3-fork, trusted**) | 592.9 ± 1.25 | 571.3 ± 1.09 | 788.5 | 746.5 | ~3.65% **faster** |
| C | point | 1 (**3-fork, trusted**) | 1138.2 ± 2.07 | 1124.4 ± 1.90 | 1413.1 | 1372.9 | ~1.22% **faster** |

Both diffs are large relative to error (SELECT 1: 21.6µs diff vs. ±2.3µs combined error, ~9.3x;
point: 13.9µs diff vs. ±4.0µs combined error, ~3.5x) — far too big to dismiss as noise on their own.
But laid out against every other data point collected for this variant, no coherent story survives:

| Scenario | Concurrency | Run | early vs. current |
| --- | --- | --- | --- |
| SELECT 1 | 1 | single-fork | ~0.6% slower |
| SELECT 1 | 1 | **3-fork trusted** | ~3.65% **faster** |
| SELECT 1 | 8 | single-fork | ~2.0% faster |
| SELECT 1 | 8 | **3-fork trusted** | ~0.1% slower (noise) |
| point | 1 | single-fork | ~0.9% slower |
| point | 1 | **3-fork trusted** | ~1.22% **faster** |
| point | 8 | single-fork | ~1.5% faster |
| point | 8 | **3-fork trusted** | ~1.26% **slower** |

Every one of the four (scenario, concurrency) combinations flips character between its single-fork
and trusted-3-fork measurement, and the two trusted rows that exist per scenario don't agree with
each other either: `-t1` trusted favors `earlyAcquisition` clearly in both scenarios; `-t8` trusted
shows no benefit (SELECT 1) or a real cost (point) instead. This is the opposite pattern from what
an admission-gate-contention effect predicts — that mechanism requires concurrency to manifest at
all, so a real, sizeable effect at `-t1` (no contention by construction: one JMH thread, nothing
else contending for the 8-connection pool or the 8-worker decoder scheduler) can't be explained by
decoder-worker admission queueing. A more plausible mechanism for a `-t1`-only effect would be
something unrelated to contention — e.g. `subscribeOn`'s cross-thread hand-off to a bounded-elastic
worker having its own fixed latency that, in the current-ordering path, sits *before* the HTTP
request is even sent (serialized), while early acquisition lets that hand-off latency overlap with
the network round-trip's own latency instead. That would predict a `-t1` benefit independent of
contention — consistent with what `-t1` shows — but doesn't explain why the same overlap wouldn't
also help (or would actively hurt, for `point`) at `-t8`, where the decoder-scheduler's worker
threads are presumably already warm and busy rather than needing a cold hand-off.

**Verdict: inconclusive — not confirmed, and not confidently rejected on mechanism, but not
actionable either way.** Per this project's own standing discipline (a finding that doesn't
reproduce or flips direction across independent runs gets retracted, not defended), the honest
reading of four internally-inconsistent (scenario, concurrency) results is that this benchmark
setup cannot currently distinguish a real, reliable ordering effect from run-to-run noise/drift at
the ~1-4% magnitude being measured here — noting that JMH runs each `@Benchmark` method's forks as
its own sequential block by default (not interleaved with the other methods), so `currentOrdering*`
and `earlyAcquisition*` for the same scenario are never measured concurrently within one invocation,
leaving room for time-of-day-dependent machine/Docker/network drift between them that this design
cannot separate from a genuine code effect. **Not adopted, same practical outcome as Variant B and
task #309, but for a different reason: those were rejected because the measured effect was reliably
too small; this one is set aside because the measured effect, when it appears, is large enough to
matter but does not reproduce in a direction- or magnitude-consistent way.** A confident verdict
either way would need a redesigned, lower-noise comparison (e.g. interleaving both orderings within
a single fork via `@Benchmark` group profiling, or a dedicated JFR run correlating thread hand-off
timing with request-send timing) — out of scope for this diagnostic-only pass.

**All four original candidate hypotheses (GC, the `asByteArray()` copy, fixed query/statement
construction cost, and admission-gate ordering) have now been examined. None provides a reliable,
reproducible explanation for Variant A's flat ~2.6-4.9% mean deficit on `SELECT 1`/point.** Per this
investigation's own plan (see top of this doc), "no production change justified yet" is an explicit,
valid outcome — the deficit remains a real, measured, reproduced-across-two-independent-3-fork-runs
characteristic of the current implementation vs. client-v2, but its source is not established by
any of the four hypotheses this ladder was built to test.

## Variant D — built, awaiting sanity run

The elimination list above covered every hypothesis about what happens *around* client-v2's reader
(the copy feeding it, admission-gate ordering before it, the query object built before calling it).
It never questioned the reader itself. `MinimalRowBinaryReader`
(`clickhouse-r2dbc-reactive-benchmarks/src/main/java/.../MinimalRowBinaryReader.java`) is a minimal,
hand-rolled `RowBinaryWithNamesAndTypes` decoder covering only the four ClickHouse wire types
`TrivialQueryBenchmark`/`PointQueryBenchmark` actually use (`UInt8`, `UInt64`, `String`,
`Decimal(P, S)`), reading directly off `ZeroCopyByteBufInputStreamBridge` — replacing client-v2's
`RowBinaryWithNamesAndTypesFormatReader`/`AbstractBinaryFormatReader`/`BinaryStreamReader` machinery
entirely for the scenarios it covers.

`LatencyPathVariantDBenchmark` deliberately holds the copy-vs-zero-copy question constant (both
scenarios use `ZeroCopyByteBufInputStreamBridge` — Variant B already measured that choice at
15-35ns/call, negligible either way) so this isolates the one remaining untested variable: the
reader/parsing layer itself. Four methods, same self-contained-pair shape as B/C:
`clientV2ReaderSelect1`/`minimalReaderSelect1`/`clientV2ReaderPoint`/`minimalReaderPoint`.

**Scope, deliberately narrow for this first cut**: `StreamingScanBenchmark`'s full-scan shape (same
three columns, many rows) isn't covered yet — extending `MinimalRowBinaryReader` to a streaming
scenario is the natural next step only if this pair shows a real, reproducible effect.

**How to run** — sanity pass, then the same trusted 3-fork sequence at both concurrency levels:

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=LatencyPathVariantDBenchmark

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantDBenchmark -Pjmh.threads=1 -Pjmh.forks=3

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantDBenchmark -Pjmh.threads=8 -Pjmh.forks=3
```

No leak-detection wiring needed (no `ByteBuf` involved beyond what `ZeroCopyByteBufInputStreamBridge`
already handles and Variant B/C already leak-verified).

**Single-fork sanity, `-t1` (default, no `-Pjmh.threads` passed) (2026-08-25): clean build, no
exceptions, `MinimalRowBinaryReader` decoded correctly on the first real run.**

| Variant | Scenario | Concurrency | client-v2 reader mean (µs) | minimal reader mean (µs) | client-v2 p99 (µs) | minimal p99 (µs) | minimal vs. client-v2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| D | SELECT 1 | 1 (single-fork, untrusted) | 580.3 ± 2.15 | 568.2 ± 2.01 | 794.6 | 744.4 | **~2.1% faster** |
| D | point | 1 (single-fork, untrusted) | 1169.9 ± 4.96 | 1152.7 ± 5.37 | 1714.2 | 1687.4 | **~1.5% faster** |

Both diffs favor `minimalReader` and both exceed combined error (SELECT 1: 12.1µs diff vs. ±4.2µs
combined error, ~2.9x; point: 17.3µs diff vs. ±10.3µs combined error, ~1.7x) — unlike Variant C's
single-fork pass, which flip-flopped in *direction* between the two scenarios, this one is at least
directionally consistent across both. Still single-fork and untrusted per this project's own
standard — Variant C's own single-fork pass looked just as clean before its trusted rerun reversed
it, so this is a reason to run the trusted sequence next, not a reason to conclude anything yet.

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantDBenchmark -Pjmh.threads=1 -Pjmh.forks=3
```

**Trusted 3-fork `-t1` run (2026-08-25) — reproduces and strengthens, unlike every other lead in
this ladder.** Where Variant C's promising single-fork pass reversed under a trusted rerun, this one
holds direction and grows:

| Variant | Scenario | Concurrency | client-v2 reader mean (µs) | minimal reader mean (µs) | client-v2 p99 (µs) | minimal p99 (µs) | minimal vs. client-v2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| D | SELECT 1 | 1 (**3-fork, trusted**) | 614.5 ± 1.51 | 581.1 ± 1.37 | 943.1 | 802.8 | **~5.44% faster** |
| D | point | 1 (**3-fork, trusted**) | 1206.4 ± 5.64 | 1118.7 ± 1.90 | 2105.3 | 1378.3 | **~7.27% faster** |

Both diffs are ~11.6x the combined error bars (SELECT 1: 33.4µs diff vs. ±2.9µs; point: 87.7µs diff
vs. ±7.5µs) — decisively outside noise, not a borderline call. `p50`/`p90`/`p95`/`p99` all move the
same direction too. This is the first result in the whole A/B/C/D + task #309 ladder that both
reproduced under a trusted rerun *and* grew rather than shrank or reversed.

**Note on magnitude, before over-reading it**: 5.4-7.3% here is *larger* than Variant A's original
~2.6-4.9% `thisDriver`-vs-client-v2 deficit — expected, not a contradiction, since this comparison
isolates only the reader/parsing layer on an otherwise-identical (zero-copy) input side, while
Variant A compares two complete, independently-implemented pipelines (this driver's Reactor Netty
transport + connection pool vs. client-v2's own transport/pool). The reader-layer cost measured here
could be partly offset elsewhere in this driver's pipeline (e.g. transport/pool differences that
favor this driver), netting out to Variant A's smaller observed total gap — plausible, not yet
confirmed. Worth collecting the `-t8` trusted run next (same two-concurrency-level protocol as
A/B/C) before drawing a final verdict, and — if `-t8` holds too — treating this as the leading
candidate to extend `MinimalRowBinaryReader` toward `StreamingScanBenchmark`'s shape and to weigh
seriously as a real production change, unlike every prior variant in this ladder.

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantDBenchmark -Pjmh.threads=8 -Pjmh.forks=3
```

**Trusted 3-fork `-t8` run (2026-08-25) — scenario-dependent: `SELECT 1` flips, `point` reproduces.**

| Variant | Scenario | Concurrency | client-v2 reader mean (µs) | minimal reader mean (µs) | client-v2 p99 (µs) | minimal p99 (µs) | minimal vs. client-v2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| D | SELECT 1 | 8 (**3-fork, trusted**) | 1594.206 ± 5.288 | 1604.539 ± 3.834 | 3858.4 | 3805.2 | **~0.65% *slower*** (direction flip vs. `-t1`'s +5.44%) |
| D | point | 8 (**3-fork, trusted**) | 2387.531 ± 6.360 | 2262.743 ± 5.116 | 5365.8 | 4898.8 | **~5.23% faster** (same direction as `-t1`'s +7.27%) |

`SELECT 1`'s diff (10.3µs) barely exceeds its combined error (9.1µs, ~1.13x) — inside the "could be
noise" band, and it flipped sign from `-t1`. `point`'s diff (124.8µs) is ~10.9x its combined error
(11.5µs) — decisively outside noise, same direction as `-t1`, at both concurrency levels now.

**Verdict on the t1+t8 pair, applying the plan's Case A/B/C framework jointly across both
concurrency levels and both scenarios, not just the strongest single number:**

- `SELECT 1` (1 column, `UInt8`): **Case A — reject.** Direction flips between `-t1` (+5.44%) and
  `-t8` (-0.65%), and `-t8`'s magnitude sits right at the noise boundary. No reader-layer effect
  established for this scenario.
- `point` (2 columns, `String` + `Decimal`): **reproduces at both concurrency levels, ~11x combined
  error both times (+7.27% at `-t1`, +5.23% at `-t8`)** — this is not Case A. It's short of the
  plan's literal Case C wording ("closes most/all of the gap across SELECT1/point/t1/t8" — it
  doesn't hold for `SELECT 1`), so the honest read is **Case B**: the reader contributes a real,
  reproducible cost for multi-column/typed decoding, but it isn't a blanket effect across every
  query shape.

**Mechanistically coherent, not just numerically consistent:** the one scenario where the effect
disappears (`SELECT 1`) is the one with the least reader work to save (one column, one type,
`UInt8`); the one where it holds (`point`) has two columns and two different types (`String`,
`Decimal`) — more type-dispatch/allocation surface for a specialized reader to avoid. This points at
a **per-column decode cost**, not a fixed per-request one — exactly the question the `*Stream10k`
pair (many rows, same 3-column shape) and a network-free reader-only microbenchmark are built to
settle, per the plan's step-3/step-4 prescription after this "STOP and evaluate" gate.

**Built, not yet run — next two data points before any production-decoder discussion:**

1. `LatencyPathVariantDBenchmark.clientV2ReaderStream10k`/`minimalReaderStream10k` — same transport/
   bridge/scheduler/pool/compression as the existing pair, `PointQueryTable`'s existing 3-column
   shape (`id UInt64`, `label String`, `amount Decimal(18,4)`) at the 10,000-row tier, decoding every
   column of every row. Tells apart "fixed per-request reader cost" (flat vs. row count) from
   "per-row/per-column cost that compounds" (grows with row count) — and, since it reads 3 columns
   instead of `point`'s 2, whether the per-column effect keeps scaling.
2. `ReaderOnlyMicrobenchmark` (new class) — network-free: captures each scenario's exact
   `RowBinaryWithNamesAndTypes` response bytes once in `@Setup(Level.Trial)` over a real connection,
   then every `@Benchmark` method decodes from a fresh `ByteArrayInputStream` over the same in-memory
   bytes, on the calling thread — no HTTP, no `Scheduler`, no connection pool, no
   `ZeroCopyByteBufInputStreamBridge`. Isolates pure header-parsing/type-dispatch/allocation cost
   inside the reader, same three scenarios (`select1`/`point`/`stream10k`) for direct comparison.

```
caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantDBenchmark -Pjmh.threads=1 -Pjmh.forks=3

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=LatencyPathVariantDBenchmark -Pjmh.threads=8 -Pjmh.forks=3

caffeinate -d -i ./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh \
  -Pjmh.includes=ReaderOnlyMicrobenchmark -Pjmh.forks=3
```

`ReaderOnlyMicrobenchmark` runs single-threaded by construction (no network, no scheduler, nothing
concurrency-sensitive to isolate) — one trusted 3-fork run is enough, no separate `-t1`/`-t8` split
needed.

**Not yet decided, and explicitly out of scope until the above two results are in:** whether to build
a production decoder, what its type coverage would need to be, or any change to `core`/`connector`.
Per the plan's guardrails: no client-v2 upstream changes, no production code changes yet, and — if a
custom decoder is eventually considered — a maintenance-cost estimate across ClickHouse's full type
taxonomy first, weighed against a *repeatable* gain, not a single scenario's number.

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

**Tail-latency "finding" above, retracted (2026-08-25) — did not reproduce.** A second, independent
3-fork `-t 8` run (`-Pjmh.profilers=gc`, ~14m04s) gives p100 numbers that don't just differ in
magnitude but **flip which driver has the worse tail**: `point` p100 45,613µs (`thisDriver`) vs
41,877µs (`clientV2`) — now only ~9% apart, not 2.7x — and `stream 10k` p100 17,269µs (`thisDriver`,
now the *better* one) vs 56,033µs (`clientV2`, now the *worse* one) — a complete reversal from the
first run's 126,484µs vs 16,450µs. p99 for `stream 10k` flips too (run 1: `thisDriver` 3.2% worse;
run 2: `thisDriver` 8.4% better). This is exactly the "don't assume the numbers tell you what you
want them to tell" trap JMH's own banner warns about: p99.9/p100 rest on very few samples out of
hundreds of thousands, and are far noisier run-to-run than the mean — the first run's asymmetry was
most likely ordinary tail noise (a slow JIT/GC/scheduler moment landing on one side that run), not a
reproducible architectural effect. Retracted as a lead; not pursuing it further without a
purpose-built low-noise percentile methodology (e.g. this project's own merged-HdrHistogram approach
from Phase 11 PR4) if it resurfaces.

**GC ruled out for the stable mean-level gap.** The `-prof gc` run confirms the already-established
finding — `thisDriver` allocates far less per op (`point`: 18.4 KB vs 89.6 KB; `SELECT 1`: 15.8 KB
vs 87.4 KB; `stream 10k`: 2.53 MB vs 4.58 MB) — but GC *time* is comparable or, for `point`, actually
higher for `thisDriver` (416ms vs 272ms total across 9 iterations) despite the much lower allocation
rate. Spread across ~330k ops that's roughly +1.3µs/op of extra GC time at most, far smaller than the
observed ~90µs mean gap — GC is not the explanation for `point`/`SELECT 1`'s small, stable mean
deficit.

**What did reproduce, consistently, across both independent `-t 8` runs (this is the real signal):**
`SELECT 1` and `point` sit at a stable ~4–5% mean deficit both times (run 1: 4.9%/4.9%; run 2:
4.9%/4.0%) — flat, not growing with the profiler attached, and not explained by GC. `stream 10k`'s
mean advantage is consistently positive both times (run 1: 9.3% faster; run 2: 12.3% faster) — same
direction, some magnitude variance. A flat, load-independent-looking small deficit on the two
fast/small-response scenarios is a different shape than a queueing/admission-ordering effect would
produce (which should show up more at the tail and grow with contention, not as a stable mean
offset) — this points more toward **fixed per-request overhead somewhere in the pipeline** (a
per-chunk copy, or fixed statement/query construction cost) than toward the admission-gate-ordering
hypothesis Variant C specifically targets.

Directionally consistent with the mega sweep's headline ("Protocol floor ... essentially tied to
~8% slower", "Full table scan ... 11–12% lower latency") at concurrency=1; at concurrency=8 the
`SELECT 1`/`point` deficit holds steady at ~4–5% (not worsening much under load, and reproduced
across two independent 3-fork runs — see above), and `stream 10k`'s mean advantage narrows somewhat
from ~17% (at `-t 1`) to ~9–12% (at `-t 8`, two runs) but stays positive both times. Full JMH output
(every percentile) is in `build/results/jmh/results.json` on the machine that ran it, not committed
to git per this project's convention.

**Recommendation for the next step, superseded (2026-08-25):** this section originally recommended
building Variant B (avoid the `ByteBuf`→`byte[]`→`ByteBuffer` copy) over Variant C, reasoning that a
flat, GC-independent mean deficit on the smallest-response scenarios was a better match for a fixed
per-request copy cost than for a queueing effect. Variant B is now built and run (single-fork +
3-fork trusted `-t1`, see above): the copy-avoidance effect measured out at ≤1.3%, inconsistent in
direction between `SELECT 1` and `point`, nowhere near the ~4-5% deficit it was meant to explain.
That verdict retracts this recommendation's premise, not just its conclusion — the same discipline
applied to the earlier tail-latency finding.

**Updated recommendation, superseded again (2026-08-25):** this section then recommended weighing
task #309 (profiling `ClickHouseStatement`/`ClickHouseQuery` construction) before committing to
Variant C, as a cheaper, more targeted check. Task #309 is now done (see below) and also rejected —
construction cost is 20-150x too small to explain the deficit. **Variant C is now the only remaining
hypothesis from this investigation's original candidate list — build it next.**

## Task #309 — query/statement construction overhead: rejected, 20-150x too small

`QueryConstructionMicrobenchmark`
(`clickhouse-r2dbc-reactive-benchmarks/src/jmh/java/.../QueryConstructionMicrobenchmark.java`),
single-fork sanity run (2026-08-25, `-Pjmh.forks=1`, `SampleTime`, ~3m20s) — no leak-detection wiring
needed, no `ByteBuf` involved. Measures exactly what `LatencyPathVariantABenchmark`'s
`thisDriverSelect1`/`thisDriverPoint` methods build per call — `ClickHouseQuery.of(sql)` (+
`.withParameters(...)` for the point shape) — plus, separately, `ClickHouseQuery.parameterNamesIn`,
the SQL placeholder scan a real `ClickHouseStatement` performs once at construction.

**Scoping note, found while writing this section:** `LatencyPathVariantABenchmark`'s benchmark
methods call `ourTransport.query(...)`/`ClickHouseQuery.of(...)` directly, bypassing
`ClickHouseStatement` entirely (see that class's `thisDriverSelect1`/`thisDriverPoint` source) — so
`parameterNamesInPointSql` below is not actually on Variant A's measured hot path. Included anyway,
for completeness, since it's real cost a caller going through the public R2DBC `Statement` API (the
way this driver is actually used) does pay once per statement.

| Benchmark | Mean | p50 | p99 | p99.9 | p100 |
| --- | --- | --- | --- | --- | --- |
| `uuidGeneration` (1 thread) | 121.8 ± 7.1 ns | 84 ns | 167 ns | 333 ns | 462,336 ns |
| `uuidGenerationContended` (`@Threads(8)`) | 2207.2 ± 18.1 ns | 500 ns | 71,168 ns | 203,008 ns | 1,099,776 ns |
| `parameterNamesInPointSql` (not on Variant A's hot path — see above) | 137.1 ± 3.2 ns | 125 ns | 250 ns | 333 ns | 437,760 ns |
| `queryOfSelect1` | 157.9 ± 8.7 ns | 125 ns | 209 ns | 375 ns | 420,864 ns |
| `queryOfPointWithParameters` | 1522.8 ± 50.1 ns | 125 ns | 120,448 ns | 132,864 ns | 453,120 ns |

**Verdict: rejected, decisively, by 1-2 orders of magnitude.** Against the trusted 3-fork deficits
recorded above (Variant A section):

| Scenario | Concurrency | Deficit to explain | Measured construction cost | Ratio |
| --- | --- | --- | --- | --- |
| SELECT 1 | 1 | 24.3µs (4.2%) | 0.158µs (`queryOfSelect1`) | ~150x too small |
| SELECT 1 | 8 | 70.4µs (4.9%) | ~2.24µs (`queryOfSelect1` + contended-UUID delta) | ~31x too small |
| point | 1 | 30.3µs (2.6%) | 1.523µs (`queryOfPointWithParameters`) | ~20x too small |
| point | 8 | 102.9µs (4.9%) | ~3.6µs (`queryOfPointWithParameters` + contended-UUID delta) | ~29x too small |

Even the most generous framing — crediting the *entire* mean UUID-generation slowdown observed under
8-way contention (121.8ns → 2207.2ns, ~18x, from the JDK's single-lock-guarded shared `SecureRandom`)
as extra cost paid only at `-t8` — leaves construction 29-31x too small to explain the `-t8` deficit,
and the gap is wider still at `-t1` where there's no contention. Same shape as Variant B's verdict: a
real, measurable cost, orders of magnitude too small to be the deficit's source.

**Secondary finding, flagged not chased:** `queryOfPointWithParameters`'s tail is far fatter than
`queryOfSelect1`'s at the same percentile — p99 120.4µs vs. `queryOfSelect1`'s 209ns (`queryOfSelect1`
doesn't reach 100µs-scale until p99.99). Single-fork signal only, same Blackhole-mode caveat the JMH
banner warns about — but the shape is consistent with `withParameters(...)`'s extra allocations
(`LinkedHashMap`, per-value encoding, `Map.copyOf`) triggering minor GC roughly 100x more often than
the allocation-free `SELECT 1` path. A tail-latency (p99+) question, not the flat-mean-deficit
question this task exists to answer — worth a note for anyone chasing P99 later, out of scope here.

**`uuidGenerationContended` also corroborates against a contention-driven explanation more broadly:**
if the shared-lock contention behind `UUID.randomUUID()` were a meaningful driver of Variant A's
deficit, the deficit should widen materially between `-t1` and `-t8` beyond what eight-way
pool/connection contention (already known to be present, see Variant A's own sample-count note above)
would produce alone. It does widen (point: 2.6%→4.9%; SELECT 1: 4.2%→4.9%), but by single-digit-µs
amounts fully explainable by that already-known pool contention — not by the tens-of-µs a real
UUID-lock bottleneck would need to contribute at 8x load.

**Task #309 done. Elimination list complete:** GC ruled out (`-prof gc`), the `asByteArray()` copy
ruled out (Variant B), and fixed query/statement construction cost ruled out (this task). **Variant C
is the only remaining hypothesis from this investigation's original candidate list.**

## Hypothesis-ranking decision table

| Hypothesis | Test | Measured delta | Verdict |
| --- | --- | --- | --- |
| GC pauses | `-prof gc` on `LatencyPathVariantABenchmark` (trusted `-t8`) | `thisDriver` allocates far less/op but GC *time* is comparable-or-higher for `point`; spread across ops, ≤ ~1.3µs/op — far below the ~46-103µs deficit | **Reject** |
| `asByteArray()` `ByteBuf`→`byte[]`→`ByteBuffer` copy | Variant B — real-HTTP `-t1` trusted, then network-free `ZeroCopyByteBufInputStreamBridgeMicrobenchmark` | Real-HTTP: ≤1.3%, sign-flipping between scenarios. Isolated cost: 15-35ns/call at real response sizes — ~1000x too small vs. the ~24-103µs deficit | **Reject** |
| Fixed query/statement construction (`ClickHouseQuery.of`/`.withParameters`/UUID generation/SQL placeholder scan) | Task #309 — `QueryConstructionMicrobenchmark`, including 8-way-contended UUID generation | 0.16-3.6µs total (even crediting the full contention penalty) vs. a 24-103µs deficit — 20-150x too small | **Reject** |
| Transport-acquisition-before-decoder-admission ordering | Variant C — `LatencyPathVariantCBenchmark`, single-fork sanity + trusted 3-fork at both `-t1`/`-t8` | Real, sizeable effects appear (up to ~3.65%) but with no consistent direction: favors early acquisition at trusted `-t1` (the concurrency level where the mechanism shouldn't apply), doesn't at trusted `-t8` (where it should); doesn't reproduce its own single-fork sanity pass in any of the four (scenario, concurrency) cells | **Inconclusive — not adopted** (not "too small," but not reproducible) |
| client-v2 reader/parsing machinery (`RowBinaryWithNamesAndTypesFormatReader`/`AbstractBinaryFormatReader`) | Variant D — `LatencyPathVariantDBenchmark`, single-fork sanity + trusted 3-fork at both `-t1`/`-t8` | `SELECT 1` (1 column): direction flips (+5.44% at `-t1`, -0.65% at `-t8`, latter within noise). `point` (2 columns, `String`+`Decimal`): reproduces at both levels (+7.27% `-t1`, +5.23% `-t8`, ~11x combined error both times) | **Reader contributes but does not explain the full gap — real for multi-column decode, not established for the 1-column case.** `*Stream10k` + network-free `ReaderOnlyMicrobenchmark` built, pending run, before any further verdict |

**Recommended next production change: still none, pending Variant D's remaining two data points.**
Three of the five candidate hypotheses tested so far are rejected outright (GC, copy, construction);
admission ordering is inconclusive; the reader layer is the first hypothesis in this ladder with a
real, reproducible, mechanistically-coherent effect (per-column decode cost) — but confirmed for only
one of the two scenarios tested against real HTTP traffic, and not yet checked against the
`*Stream10k`/network-free follow-ups the plan's decision framework requires before considering a
production change. This is still **"no production change justified yet,"** now with a live,
promising lead instead of a closed ladder — the two builds above are the next concrete step, not a
final verdict. Streaming, unaffected by any of this, is already ~9-17% faster and needs no change.

## Explicitly out of scope for this pass

Virtual-thread decoder default, decoder worker widening, pending-acquire tuning,
`ArrayBlockingQueue`/SPSC replacement, `FluxInputStreamBridge` coalescing rewrite, response
compression defaults, default pool size, a full custom RowBinary decoder, and the Spring Boot
macrobenchmark (separate ROADMAP.md Phase 12 track, PR2 still open) — separate questions, not this
one.

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
| **B — avoid the `ByteBuf`→`byte[]`→`ByteBuffer` copy** | Only if ownership can be proven correct under `-Dio.netty.leakDetection.level=paranoid` (no use-after-release, no leak, correct cancel/error/full-consumption cleanup) | **Built, not yet run** — `LatencyPathVariantBBenchmark` (see below) |
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

**Updated recommendation:** build **Variant C** next (transport-acquisition-before-decoder-admission
prototype) — the hypothesis this section previously deprioritized, now the better-supported one by
elimination: GC is ruled out (earlier `-prof gc` run), and the copy is now ruled out (Variant B,
above). Before committing to Variant C specifically, it's also worth weighing task #309 (profiling
`ClickHouseStatement`/`ClickHouseQuery` construction) as a cheaper, more targeted check — a small
fixed per-request object-construction cost is at least as plausible a source of a flat ~4-5% mean
deficit on tiny queries as an admission-gate ordering effect, and is far quicker to isolate than
building Variant C's prototype.

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

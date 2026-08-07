# Roadmap

Working plan for turning the current skeleton into a verified, fully reactive R2DBC driver. Phases
are sequential gates, not a fixed calendar — we don't start a phase until the previous one has a
written, checkable answer, in the same spirit as the
[verified execution-path analysis](https://github.com/ClickHouse/ClickHouse/discussions/113638)
proposed to the ClickHouse team.

## Contents

- [Phase 0 — client-v2 execution-path finding](#phase-0--client-v2-execution-path-finding)
- [Phase 1 — Transport spike](#phase-1--transport-spike)
- [Phase 2 — Core protocol + testkit contract tests](#phase-2--core-protocol--testkit-contract-tests)
- [Phase 3 — Connector (R2DBC SPI surface)](#phase-3--connector-r2dbc-spi-surface)
- [Phase 4 — "Fully reactive" sign-off](#phase-4--fully-reactive-sign-off)
- [Phase 5 (later) — Load and performance testing](#phase-5-later--load-and-performance-testing)
- [Working with Claude / IntelliJ](#working-with-claude--intellij)

## Phase 0 — client-v2 execution-path finding

**Status: done for the transport question. Reuse boundary for decoding is now concrete.**

Verified by cloning `ClickHouse/clickhouse-java` and reading the actual `client-v2` source
(`grep`-audited: zero reactive types anywhere in the module; only two files touch blocking-I/O or
thread-pool primitives at all).

### Transport: confirmed blocking, do not reuse

- `internal/HttpAPIClientHelper.executeRequest()` calls `httpClient.executeOpen(null, req,
  context)` on a `CloseableHttpClient` (Apache HttpClient **5, classic I/O**, not the async
  client) — a synchronous call that blocks the calling thread until response headers arrive.
- The response body is exposed as `delegate.getEntity().getContent()`
  (`TransportResponseImpl.getInputStream()`), a raw `InputStream` tied directly to the socket:
  every subsequent read blocks the calling thread on socket I/O, classic pre-NIO style.
- `Client`'s "async" mode (the only other file using `CompletableFuture`/`ExecutorService` in the
  whole module) just runs the same blocking call on an internal cached thread pool. Confirms the
  earlier finding: not real non-blocking I/O, and exactly the "scheduler workaround" our own
  [CLAUDE.md](CLAUDE.md) rules out.

**Decision: `clickhouse-r2dbc-reactive-transport-http` never calls `Client` or
`HttpAPIClientHelper`. It owns a Reactor Netty HTTP client independently, from the socket up.**

### Decoding: public API, reusable behind a bridge

- The binary format readers actually used for query results
  (`data_formats.RowBinaryWithNamesAndTypesFormatReader`, `ClickHouseBinaryFormatReader`,
  `ClickHouseFormatReader`) are **public classes in a public package**, not `.internal`. Their
  constructors take a plain `java.io.InputStream` plus `QuerySettings`/`TableSchema` (also public,
  and — checked — `TableSchema` has zero import coupling to `Client`/HTTP; `QuerySettings`
  references `Client` only superficially). None of them require an instance of `Client` or
  `HttpAPIClientHelper` to exist.
- Underneath, `data_formats.internal.BinaryStreamReader`/`AbstractBinaryFormatReader` do the actual
  decoding, and they are pull-based: every value (`readByte`, `readVarInt`, `readString`, dates,
  decimals, arrays, geo types, enums, ...) ultimately calls `InputStream.read()` directly. No
  network code inside them — they just consume whatever `InputStream` they're handed.
- This means the ~2,800 lines of ClickHouse-type-system decoding logic (dates/decimals/arrays/
  maps/geo types/enums/nullable handling) are reusable **without touching client-v2's transport or
  its internal package ourselves**, provided we feed them a real `InputStream`.

**Decision: reuse `RowBinaryWithNamesAndTypesFormatReader`/`ClickHouseBinaryFormatReader` (public
API only) in `clickhouse-r2dbc-reactive-core`, fed by a small adapter that bridges Reactor Netty's
`Flux<ByteBuffer>` response chunks into a blocking-shaped `InputStream`.**

That adapter is the one deliberate, contained compromise in this design:

- It runs the reader's pull-based `next()`/`readValue()` calls on a small **dedicated, bounded**
  worker pool — never on the Netty event loop, so the transport's non-blocking multiplexing across
  connections is never affected.
- The bridge `InputStream` must only block waiting on an already-bounded queue of chunks that
  Netty's inbound handler fills; when that queue is full, the handler stops reading from the
  channel (real TCP-level backpressure), and cancellation interrupts the worker. This is different
  from — and much narrower than — wrapping blocking *socket* I/O in a thread pool: the network side
  stays genuinely non-blocking, only the CPU-bound decode step runs off-loop.
- This bridge is exactly the kind of thing that belongs in the Phase 1 transport spike's acceptance
  criteria (bounded buffering, backpressure, cancellation) — it's not extra scope, it's the same
  work already planned, just now with a concrete consumer (`RowBinaryWithNamesAndTypesFormatReader`)
  on the other end instead of a placeholder.
- Documented risk: `RowBinaryWithNamesAndTypesFormatReader` still depends on the `.internal`
  `BinaryStreamReader` under the hood (client-v2's own choice, not ours) — an upstream refactor of
  that internal class could break us without a semver signal on the public reader class. Mitigate
  with a contract test in `testkit` that pins the exact reader behavior we rely on, so a
  `client-v2` version bump that breaks it fails CI immediately instead of silently.

This satisfies the "verified execution-path analysis" the ClickHouse maintainers asked for as the
first contribution — worth writing up as a follow-up comment on
[the discussion](https://github.com/ClickHouse/ClickHouse/discussions/113638) once Phase 1 confirms
the bridge works end to end.

## Phase 1 — Transport spike

Goal: prove the hard properties with the smallest possible surface — `SELECT 1`, then a streamed
multi-row result — against a real, non-blocking HTTP path, per the acceptance criteria already
written into the [README](README.md#suggested-first-validation-spike):

- Request not sent before subscription.
- No blocking network call anywhere on the query path.
- Response consumed as chunks, not aggregated in memory.
- Active/pending request limits are explicit and enforced.
- A queued or active request can be cancelled; resources are released deterministically.
- Verified first against `clickhouse-r2dbc-reactive-testkit`'s controlled local server, then
  against a real ClickHouse instance (Testcontainers).

Only `clickhouse-r2dbc-reactive-transport-http` and enough of `clickhouse-r2dbc-reactive-core` to
represent a query request/response exist at this point — no R2DBC SPI yet.

### Concrete starting sequence

Smallest steps first, each one working end to end before moving to the next — no step depends on
guessing ahead at what a later step needs.

1. **Repo hygiene first.** Resolve the stuck `.git/index.lock` locally, commit and push everything
   already written (`CLAUDE.md`, `AGENTS.md`, `.github/*`, `ROADMAP.md`). Run `gradle wrapper
   --gradle-version 9.7.0` once so `./gradlew` exists. Open the project in IntelliJ, let it import
   the four modules against the version catalog, confirm `./gradlew clean build` passes on the
   empty skeleton (nothing to test yet, but it proves the wiring is sound). Don't write driver code
   before this is green.
2. **`testkit`: a controlled local server that can answer `SELECT 1`.** A minimal Reactor Netty
   `HttpServer` that returns a canned, correctly-framed ClickHouse HTTP response (right headers,
   `RowBinaryWithNamesAndTypes` body for one row). This exists so every later step can be tested
   without a real ClickHouse instance. Keep it deliberately dumb at first — no delayed
   headers/fragmentation/cancellation scenarios yet, those come later once the basic path works.
3. **`transport-http`: send bytes, get bytes back, prove non-blocking.** A Reactor Netty
   `HttpClient` that POSTs a query to the testkit server and exposes the response as
   `Flux<ByteBuffer>` (or Netty's own buffer type at this stage — pick one, don't over-design). No
   decoding yet. Prove, with tests against the testkit server: the request isn't sent before
   subscription, the response body isn't buffered into one blob before being emitted, and
   cancelling the subscription tears down the connection instead of leaking it.
4. **`core`: the `Flux<ByteBuffer>` → `InputStream` bridge**, the one deliberate compromise decided
   in Phase 0. Small, isolated class: bounded internal queue, a dedicated bounded worker pool (not
   `Schedulers.boundedElastic()` shared with everything else — size it deliberately), backpressure
   by pausing the upstream `Flux` when the queue is full, cancellation interrupts the worker. Test
   this in complete isolation from HTTP — feed it a `Flux<ByteBuffer>` by hand in a unit test,
   assert on bytes read through the bridge, no network involved at all.
5. **Wire it together: real decode of `SELECT 1`.** Feed the bridge's `InputStream` into
   `client-v2`'s `RowBinaryWithNamesAndTypesFormatReader`, get back one typed row, wrap it as a
   minimal `Flux<Row>` (whatever the smallest `Row` shape is — expand later, don't design the full
   row/metadata API yet). This is the point where "fully reactive `SELECT 1`" first exists,
   end to end, against the testkit server.
6. **Same thing against real ClickHouse.** Testcontainers ClickHouse, same `SELECT 1` path. If step
   5 was honest, this should mostly just work — if it doesn't, the gap between "controlled server"
   and "real ClickHouse" is itself useful information (framing differences, headers we didn't
   handle, etc.).
7. **Only then**, go back and fill in the acceptance-criteria checklist above properly: cancellation
   at every stage, pool saturation, slow subscriber, a streamed multi-row result instead of one row.
   This is where `testkit`'s controlled server grows up into the full contract-test matrix from
   [CLAUDE.md](CLAUDE.md#test-types-and-tools).

Steps 2–5 are deliberately small enough to each be a single focused session. Resist the urge to
build the general case (arbitrary SQL, arbitrary result shapes, settings, `query_id`) before
`SELECT 1` works end to end through a real non-blocking path — that's Phase 2, not Phase 1.

### Decision: HTTP now, native TCP not now (revisit only after benchmarks)

Read ClickHouse's [Native Protocol specification](https://clickhouse.com/docs/interfaces/specs/NativeProtocol)
before ruling this out — it's a real, actively maintained spec (not the "reverse-engineer it
yourself" state implied by the older overview page), but it settles the question rather than
opening it back up:

- **It does not solve the multiplexing problem the original discussion hoped it might.** The spec
  states plainly: "each TCP connection processes one query at a time — there is no multiplexing."
  A burst of concurrent small queries still needs a connection pool, exactly like HTTP/1.1. Native
  TCP would not change the concurrency story that motivated this whole project.
- **The protocol surface is an order of magnitude bigger than HTTP + RowBinary.** ~50 version-gated
  feature flags (`54032` through `54488+` at the time of reading), each toggling exact byte layout
  of a field — miss one and the stream desyncs with no recovery. Handshake negotiation, optional
  chunked framing, a stateful connection lifecycle (`HANDSHAKE`/`READY`/`READING_RESPONSE`), and a
  separate companion spec (Native Format) for the actual block/column encoding (including sparse
  and versioned `Dynamic`/`JSON` serialization). This is meaningfully more surface to implement
  correctly and keep in sync with upstream than reusing `client-v2`'s public
  `RowBinaryWithNamesAndTypesFormatReader` over HTTP.
- **What it would actually buy us** is lower per-request framing overhead (no HTTP headers, no
  chunked-transfer parsing) — a raw throughput/latency question, which is explicitly Phase 5
  territory, not something blocking "fully reactive."

**Decision: stay on the HTTP interface for Phase 1 through Phase 4.** Do not start a native-TCP
implementation now. Revisit only if Phase 5 benchmarks show HTTP framing overhead is an actual
bottleneck for the target workload — and even then, treat it as an additional transport adapter
behind the same transport boundary, not a rewrite.

## Phase 2 — Core protocol + testkit contract tests

Once the transport spike proves the transport contract, fill in:

- `core`: query request/settings/`query_id` representation, response metadata + row decoding for
  the first chosen wire format, cancellation state — transport-independent.
- `testkit`: the full contract-test matrix from [CLAUDE.md](CLAUDE.md#test-types-and-tools) —
  delayed headers/body, fragmented rows, slow subscriber, pool saturation, cancellation at every
  stage.

Every property in `README.md`'s "What 'fully reactive' means here" table needs at least one test
that would fail if the property regressed.

## Phase 3 — Connector (R2DBC SPI surface)

- `connector`: `ConnectionFactoryProvider`, `Connection`, `Statement`, `Result`, metadata,
  parameter binding, R2DBC exception mapping, explicit unsupported-transaction-semantics handling.
- Integration tests against real ClickHouse via Testcontainers (`clickhouse-r2dbc-reactive-connector`
  test scope already depends on `testcontainers-clickhouse`).
- R2DBC TCK-style behavioral tests where applicable; ClickHouse-specific gaps documented instead of
  silently unsupported.

## Phase 4 — "Fully reactive" sign-off

Before calling the driver "fully reactive", every property in
[README.md's table](README.md#what-fully-reactive-means-here) needs a test that demonstrates it,
not just an API shape that implies it. Concretely, a checklist pass over:

- Deferred execution, non-blocking I/O, stream-oriented consumption, backpressure-aware delivery,
  cancellation propagation, bounded concurrency, deterministic cleanup, reactive error signalling,
  no scheduler workarounds — each with a named test in the `testkit` contract matrix or connector
  integration suite that would fail if that property broke.
- A short write-up (can reuse the ClickHouse discussion thread) showing the evidence per property,
  the same way Phase 0's finding is written up here.

This is the gate before spending time on Maven Central publishing polish or performance work.

## Phase 5 (later) — Load and performance testing

Explicitly deferred, per [CLAUDE.md](CLAUDE.md#performance-testing). Not started until Phase 4 is
signed off. When it starts:

- Throughput, p50/p95/p99 latency, time to first row.
- Allocation/retained memory under streaming.
- Many-small-request workloads (the original production burst scenario — ~11 concurrent queries
  per user action) vs. large single streaming results.
- Comparison against the existing `ClickHouse/clickhouse-java` R2DBC driver as a baseline.
- Tooling choice (JMH for micro-benchmarks, Gatling/similar for load scenarios) decided at that
  point — nothing added to the build now.

## Working with Claude / IntelliJ

- [CLAUDE.md](CLAUDE.md) is the source of truth for how code in this repo should look and be
  tested. Point any Claude session (Cowork, Claude Code CLI, or Claude Code's IntelliJ plugin) at
  this repo and it picks the rules up automatically — no need to repeat them per session.
- For day-to-day coding in IntelliJ: the Claude Code IntelliJ/JetBrains plugin runs in the IDE's
  terminal/tool window against this same working tree, so edits, the Gradle run configurations, and
  the debugger all see the same files a Cowork session would produce. `AGENTS.md` exists so any
  other agent tooling (not just Claude) also picks up the same rules without duplicating them.
- Suggested split: use Cowork for repo-wide planning, research, and multi-file scaffolding (like
  this roadmap and the module skeleton); use the IntelliJ plugin for tight edit-run-debug loops on
  one module, especially once the transport spike needs a debugger attached to a live Reactor
  Netty exchange.
- Before starting Phase 1 in earnest: open the project in IntelliJ, let it import the Gradle build
  (four modules should resolve cleanly against the version catalog), and run `gradle wrapper
  --gradle-version 9.7.0` once if it wasn't already done, so `./gradlew` works both in IntelliJ and
  in CI.

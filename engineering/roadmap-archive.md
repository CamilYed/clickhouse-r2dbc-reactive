# Roadmap archive

> **This is the full historical roadmap, archived 2026-08-23 as part of
> [Phase 9's information-architecture pass](#phase-9--documentation--website-redesign).** It is kept
> in full, unedited beyond the link fixes this move required — every phase's original write-up,
> findings, and reasoning stay exactly as written when each phase actually happened. It is no longer
> the *current* roadmap: for what's released, in progress, next, and explicitly not planned, see the
> short root [ROADMAP.md](../ROADMAP.md). For what shipped in each release, see
> [CHANGELOG.md](../CHANGELOG.md). This file is the "why" and the phase-by-phase detail behind both.

---

Working plan for turning the current skeleton into a verified, fully reactive R2DBC driver. Phases
are sequential gates, not a fixed calendar — we don't start a phase until the previous one has a
written, checkable answer, in the same spirit as the
[verified execution-path analysis](https://github.com/ClickHouse/ClickHouse/discussions/113638)
proposed to the ClickHouse team.

## Contents

- [Module map](#module-map)
- [docs/internals/client-v2-http-reference.md](../docs/internals/client-v2-http-reference.md) — full HTTP wire-protocol audit (compression, auth, headers, errors, TLS, retries)
- [Phase 0 — client-v2 execution-path finding](#phase-0--client-v2-execution-path-finding)
- [Phase 1 — Transport spike](#phase-1--transport-spike)
- [Phase 2 — Core protocol + testkit contract tests](#phase-2--core-protocol--testkit-contract-tests)
- [Phase 3 — Connector (R2DBC SPI surface)](#phase-3--connector-r2dbc-spi-surface)
- [Phase 4 — "Fully reactive" sign-off](#phase-4--fully-reactive-sign-off)
- [Production readiness review](#production-readiness-review)
- [Non-functional requirements: logging, metrics, leaks](#non-functional-requirements-logging-metrics-leaks)
- [Phase 5 (later) — Load and performance testing](../docs/performance/index.md)
- [Phase 6 (later) — Spring WebFlux interop demo](#phase-6--spring-webflux-interop-demo-2026-08-13-reworked-after-a-genuine-bindmarkersfactory-finding--pending-green-confirmation)
- [Phase 7 — Operational control & R2DBC correctness (0.2.0)](#phase-7--operational-control--r2dbc-correctness-020)
- [Phase 8 — Post-0.2.0 hardening (0.2.1+)](#phase-8--post-020-hardening-021)
- [Phase 9 — Documentation & website redesign](#phase-9--documentation--website-redesign)
- [Phase 10 — Cloud benchmark pipeline](#phase-10--cloud-benchmark-pipeline)
- [Working with Claude / IntelliJ](#working-with-claude--intellij)

## Module map

Revised after a direct question ("czy nasz podział na moduły jest ok?"). Five modules exist now,
two are named but deliberately not built yet — same "no scope before its phase" discipline as the
rest of this roadmap.

| Module | Depends on | Responsible for | Published |
| --- | --- | --- | --- |
| `core` | client-v2 (decoders only) | Transport-independent domain: query/settings/`query_id`, the `Transport` port interface, the `Flux<ByteBuffer>`→`InputStream` bridge, row decoding via client-v2's public readers. No Netty, no HTTP, no R2DBC types. | Yes |
| `transport-http` | `core` | The **adapter** that implements `core`'s `Transport` port over Reactor Netty. Owns the socket. Nothing here knows what a "row" is. | Yes |
| `connector` | `core`, `transport-http` | The **adapter** that implements the R2DBC SPI (`ConnectionFactoryProvider`, `Connection`, `Statement`, `Result`, ...) on top of `core`. Thin — R2DBC-shape translation only, no protocol logic of its own. | Yes |
| `testkit` | `core` | Shared test infrastructure, used by every other module's tests: (a) `ControlledClickHouseServer` + `ClickHouseWireFixtures` — a fake HTTP endpoint for deterministic wire-level scenarios; (b) `BaseClickHouseIntegrationTest` + an Ability-pattern DSL for real ClickHouse via Testcontainers (create data, clean up between tests). | Yes (it's a test-support *library*, other people writing ClickHouse R2DBC code could depend on it too — same reason JUnit/AssertJ/Testcontainers themselves are ordinary published artifacts) |
| `benchmarks` (`clickhouse-r2dbc-reactive-benchmarks`, Phase 5, in progress) | `core`, `transport-http`, `connector`, `testkit`, client-v2 (as the comparison baseline) | JMH throughput/latency/allocation measurement, this driver vs client-v2, at multiple levels (raw transport, public R2DBC SPI) — see [docs/performance/index.md](../docs/performance/index.md) for the full design. Not published: it's measurement tooling, not a library. | No |
| `examples/spring-boot-webflux-demo` | `connector` (runtime-only) | A runnable Spring Boot 4.1 + WebFlux app proving the driver works end to end through Spring's R2DBC integration (`DatabaseClient`, raw SQL only — `R2dbcEntityTemplate`/`.bind(...)` don't work against this driver yet, see Phase 6), mirroring [`spring-reactive-transaction-boundary`](https://github.com/CamilYed/spring-reactive-transaction-boundary)'s demo. Also proves, with a real test, that Spring's declarative transaction machinery fails clearly over this driver (see Phase 6). | No |

**`integration-tests` module: scaffolded in Phase 1, deleted (2026-08-13).** It was meant to hold
whole-driver, black-box tests exercised only through the public R2DBC SPI — never `core`/
`transport-http` internals — as its own module so that (slow, Docker-backed) suite wouldn't sit
inside `connector`'s fast build loop. It sat empty for the entire project (`build.gradle.kts` only,
zero source files, confirmed via `find` during the final pre-Phase-5 review pass) because its
intended role turned out to already be covered elsewhere, spread across three places rather than
needing a fourth: `connector`'s own
`ClickHouseConnectionFactoryProviderTest.shouldBeDiscoverableThroughTheStandardR2dbcServiceLoaderBootstrapPath`
(hermetic — proves `ConnectionFactories.get(options)` resolves this driver via the real
`META-INF/services` file), `ClickHouseConnectionFactoryAgainstRealClickHouseTest` (real ClickHouse —
proves `ClickHouseConnectionFactory.from(options)` produces a connection that validates against a
live server), and `examples/spring-boot-webflux-demo` (real ClickHouse, and the only place the
`r2dbc:clickhouse://...` URL *string* form gets parsed and driven end to end through
`ConnectionFactories.get(options)`, a real pool, and `DatabaseClient`). Rather than keep an empty
module around indefinitely as "intentional placeholder" — which reads as dead scaffolding to the
next person, not a deliberate design — removed it outright: `clickhouse-r2dbc-reactive-integration-tests/`
deleted, its entry removed from `settings.gradle.kts` and `build.gradle.kts`'s
`nonPublishedModules`. If a genuine need for a single "whole driver, black box, one place" suite
resurfaces (e.g. Phase 5's benchmarks want a shared "does the full URL-string-to-real-query chain
work" smoke test before measuring performance), it can be rebuilt with actual content driving the
decision, not scaffolded speculatively ahead of one.

Why `testkit` keeps its name despite the expanded scope: the fake `ControlledClickHouseServer`
and the real-ClickHouse Testcontainers DSL are still one thing — "the shared support code so no
other module's tests need Mockito or ad-hoc infrastructure of their own" — which is exactly what
"testkit" means in other ecosystems (`kotlinx-coroutines-test`, `spring-kafka-test`). Note this is
a different concept from Gradle's built-in `java-test-fixtures` plugin feature (a source-set
convention for sharing test code *within* one module) — we don't need that here, a whole module is
the right unit because multiple *other* modules (`transport-http`, `connector`, and now
`benchmarks` too) consume it.

### Why both a fake server and real ClickHouse — not one or the other

This was the confusing part of `ClickHouseWireFixtures`, worth spelling out plainly: it hand-builds
*bytes on the wire*, not because real ClickHouse can't be trusted, but because a fake server can
force conditions a real one won't reliably give you on demand — a response that stalls mid-header,
a body that arrives in three fragments instead of one, a connection that resets after N bytes, a
slow subscriber that must apply backpressure. Those are the Phase 1 acceptance criteria (bounded
concurrency, cancellation, backpressure) and they need to be deterministic and fast in CI, which
means simulated, not "hope a container behaves badly today." Real ClickHouse (via Testcontainers)
answers a different question: does the driver decode what an actual server actually sends, with
real SQL types, real errors, real `query_id` semantics? Both questions are real; neither fake
server nor real ClickHouse alone answers both. That's `testkit`'s two halves.

### Ports & adapters, made concrete

Answering directly: yes, it's needed, and here's the exact shape (not just a label) —

- `core` owns a `Transport` port: a small interface describing "send this query, get back a
  stream of chunks" with no Netty/HTTP types in its signature — the actual contract to be nailed
  down in Phase 1 step 3 once there's a concrete request/response shape to model.
- `transport-http` is the one adapter implementing that port today (over Reactor Netty). If a
  native-TCP adapter is ever built (deferred, see below), it implements the same port — `core`
  doesn't change.
- `connector` adapts the R2DBC SPI (owned by the R2DBC spec, not by us) to `core`. `core` has no
  idea R2DBC exists.
- This is the whole justification: two real seams (transport, R2DBC) where an alternative
  implementation is plausible enough to design for, not hexagonal ceremony applied to code that
  will only ever have one implementation.

---

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
  [CLAUDE.md](../CLAUDE.md) rules out.

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
  - **Concrete instance found in step 6 (real ClickHouse):** the `Map<String, Object>` returned by
    `next()` (client-v2's `RecordWrapper`) stores its values behind a `WeakReference` to the
    reader's internal state, not directly. Reading from it after the reader itself is no longer
    strongly reachable — which happened here simply from `Flux.generate`/`blockFirst()`
    cancelling the subscription after one element — threw a `NullPointerException` from inside
    client-v2, not from our code. Fixed in `core.rowbinary.RowBinaryDecoder` by copying each row into a
    plain `LinkedHashMap` the moment it's read, while the reader is still on the stack; documented
    in that class's Javadoc. `RowBinaryDecoderTest`/`SelectOneAgainstRealClickHouseTest` both
    exercise exactly this cancel-after-one-row shape, so this is already regression-covered, not
    just fixed once and hoped for.

This satisfies the "verified execution-path analysis" the ClickHouse maintainers asked for as the
first contribution — worth writing up as a follow-up comment on
[the discussion](https://github.com/ClickHouse/ClickHouse/discussions/113638) once Phase 1 confirms
the bridge works end to end.

### HTTP protocol surface: what client-v2 actually sends on the wire

Separate from the transport/decoding question above: a full audit of everything
`HttpAPIClientHelper` does — compression (two independent layers, LZ4 is the real default, not
gzip), authentication modes, every header/query-param it sets, error-response semantics (the
`X-ClickHouse-Exception-Code` header is the real success/failure signal, not just HTTP status),
the mid-stream-error caveat's actual mechanics (`wait_end_of_query`), TLS, pooling/timeouts, and
retry classification — is written up in
[docs/internals/client-v2-http-reference.md](../docs/internals/client-v2-http-reference.md), with file:line citations and
a mapping of what each concern means for Phase 1 through Phase 3. Read it before building anything
in `transport-http`/`core` beyond the Phase 1 spike so nothing gets silently skipped — this is the
answer to "we can't skip anything" for the HTTP surface specifically.

---

## Phase 1 — Transport spike

Goal: prove the hard properties with the smallest possible surface — `SELECT 1`, then a streamed
multi-row result — against a real, non-blocking HTTP path, per the acceptance criteria already
written into the [README](../README.md#usage):

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
   [CLAUDE.md](../CLAUDE.md#test-types-and-tools).

Steps 2–5 are deliberately small enough to each be a single focused session. Resist the urge to
build the general case (arbitrary SQL, arbitrary result shapes, settings, `query_id`) before
`SELECT 1` works end to end through a real non-blocking path — that's Phase 2, not Phase 1.

**Deliberate reordering:** rather than a shallow pass through steps 3–6 first and looping back for
edge cases in step 7, `transport-http`'s full contract-test matrix is being built out immediately
after step 3's happy path — still strictly TDD, one scenario at a time, but front-loaded because
these are transport-only concerns (`testkit`'s fake server, no `core`/decode involved) and hardening
the transport boundary before building on top of it catches problems earlier. `core`'s bridge
(step 4) waits until this matrix is green. The scenario list — request not sent before subscription,
streamed not aggregated, cancellation tears down the connection, delayed headers, delayed body,
fragmented chunks, slow subscriber/backpressure, no response/timeout, connection reset mid-response,
cancellation before/while-queued/during-receive, pool saturation, pending-acquire timeout — is the
same one already named in [README's testing strategy](../docs/internals/testing-strategy.md); nothing new, just
sequenced earlier for this one module.

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

---

## Phase 2 — Core protocol + testkit contract tests

Once the transport spike proves the transport contract, fill in:

- `core`: query request/settings/`query_id` representation, response metadata + row decoding for
  the first chosen wire format, cancellation state — transport-independent.
- `testkit`: the full contract-test matrix from [CLAUDE.md](../CLAUDE.md#test-types-and-tools) —
  delayed headers/body, fragmented rows, slow subscriber, pool saturation, cancellation at every
  stage.

Every property in `README.md`'s "What 'fully reactive' means here" table needs at least one test
that would fail if the property regressed.

**Reads only through Phase 1; writes are explicitly future scope, not forgotten.** Everything
built so far (`ClickHouseHttpTransport.query`, `RowBinaryDecoder`) is shaped for `SELECT`-style
read queries — the SQL goes in the URL, the response is a decoded row stream. A reactive `INSERT`
path is a real, separate piece of work (request body instead of/alongside a URL query string, no
row-decoding on the way back, its own acceptance criteria — e.g. does a large reactive insert
stream need the same backpressure treatment as reads do) that hasn't been scoped yet. Named here so
Phase 2/3 planning doesn't quietly assume read-only is the whole driver.

**The driver decodes wire types; it doesn't reimplement ClickHouse SQL.** Functions, aggregates,
materialized views, and similar server-side SQL features need zero special handling from us — the
driver just sends SQL text and decodes whatever `RowBinaryWithNamesAndTypes` comes back, the same
code path regardless of how exotic the query is. What actually needs deliberate test coverage is
the **wire type-decoding surface**: `Int8`..`Int64`/`UInt8`..`UInt64`, `String`/`FixedString`,
`Nullable(T)`, `Array(T)`, `Map(K,V)`, `DateTime`/`DateTime64`, `Decimal`, `Enum8`/`Enum16`, `UUID`,
and so on — a contract-test matrix over real ClickHouse tables with varied column types, once
Phase 3's `BaseClickHouseIntegrationTest` exists. One green `SELECT 1` test (Phase 1 step 6) proves
the transport→bridge→decoder pipeline is wired correctly end to end; it proves nothing about type
coverage, multi-row streaming, or error paths — that's exactly what this phase's contract matrix is
for, not something to infer from the one spike test passing.

**First real pass at type coverage, checked against ClickHouse's own category taxonomy**
(`clickhouse.com/docs/reference/data-types`) —
`RealWorldTableAgainstRealClickHouseTest` in `transport-http`, one test per category:

| Category | Status |
| --- | --- |
| Numeric (`Int8`..`256`/`UInt8`..`256`, `Float32`/`64`, `Decimal`, `Bool`) | ✅ covered |
| String (`String`, `FixedString`) | ✅ covered |
| Network (`IPv4`, `IPv6`) | ✅ covered |
| Date and time | ⚠️ `Date`/`Date32`/`DateTime`/`DateTime64` covered — `Date`/`Date32` decode as `LocalDate`, `DateTime`/`DateTime64` as `ZonedDateTime` (`Date`/`Date32` changed from `ZonedDateTime` to `LocalDate` when client-v2 was bumped to 0.9.8 — a real upstream fix, not a regression: `Date`/`Date32` have no time-of-day or timezone component). `Time`/`Time64` gained real support in `BinaryStreamReader.readValue`'s switch as of the same 0.9.8 bump (absent at the previously-pinned 0.9.0) but are still untested here — now a test gap, not a version gap. |
| Nullable and optional | ✅ `Nullable` covered (including an actual `NULL` value across rows); `LowCardinality` covered too — confirmed against client-v2's own test suite (`DataTypeTests`, our pinned `v0.9.0`) that it's a "virtual type" there (wrapper stripped, dispatches to the underlying type, same shape as `Nullable`), then proven end to end through our own pipeline, not just assumed from client-v2's tests |
| Specialized | ⚠️ `UUID` and `Enum8`/`Enum16` covered; geo types, vector-search (`QBit`), domains not attempted |
| Composite (`Array`/`Tuple`/`Map`/`Nested`) | ✅ `Map`/`Tuple`/`Array`/`Nested` all covered — `Nested` flattens into one `Array(...)` column per sub-field by default (`flatten_nested=1`), so on the wire it's indistinguishable from ordinary `Array` columns, same mechanism, confirmed directly |
| Semi-structured (`JSON`/`Dynamic`/`Variant`) | ⚠️ `JSON` covered (GA since ClickHouse 25.3, no `allow_experimental_json_type` needed) — decoded as a plain `String`: `ClickHouseHttpTransport` sends `output_format_binary_write_json_as_string=1` unconditionally on every query (a no-op when there's no JSON column, so no opt-in `ConnectionFactoryOptions` needed), and `core`'s `RowBinaryDecoder#newReader` sets the matching local `QuerySettings` flag so client-v2 decodes it the same way instead of into its complex `.internal` JSON object representation; `Dynamic`/`Variant` still not attempted, newer and less settled |
| Aggregate function (`AggregateFunction`/`SimpleAggregateFunction`) | 🚫 not attempted — these hold intermediate aggregation state, not literal-insertable values, so proving them needs insert-via-aggregate-query, not `INSERT ... VALUES` |
| Special Data Types (`Expression`/`Interval`/`Nothing`/`Set`) | N/A — query-intermediate constructs, not column/storage types |

**Corrected finding (previous version of this table overstated the Composite block).** Reading
`BinaryStreamReader` directly: `readMap()`/`readTuple()` return a plain `LinkedHashMap`/`Object[]`
regardless of type hints, for element types that aren't themselves `Array`/`Nested`. `Enum8`/
`Enum16` originally returned an `.internal` `EnumValue` here, read only via its `toString()`
override — see "`Enum8`/`Enum16` resolved" below for how this was closed the same way `Array`/
`Nested` was, rather than left as a permanent `toString()`-dependency workaround.

**`Array`/`Nested` resolved** — both route through `BinaryStreamReader.convertArray()`, which only
returns a plain `List` when a `List.class` type hint is supplied, and the public `next()`/
`readRecord()` path this driver used never passed one. Resolved by `core.rowbinary.ListDecodingRowBinaryReader`
(new): overrides the reader's `protected readRecord(Object[])` hook — exposed by client-v2
specifically for subclassing — to supply `List.class` as the type hint for `Array`/`Nested` columns
only, leaving every other type's decoding untouched. This is a deliberate, narrow, tested dependency
on client-v2's `.internal` package (the `BinaryStreamReader binaryStreamReader` protected field, and
its public `readValue(column, typeHint)` method), the same shape of compromise as the Phase 0
`InputStream` bridge: one documented seam, not a general dependency. Covered by a hermetic unit test
in `core` (`RowBinaryDecoderTest.shouldDecodeAnArrayColumnAsAPlainList`, hand-built wire bytes, no
ClickHouse needed) and a real-ClickHouse test (`RealWorldTableAgainstRealClickHouseTest.
shouldDecodeArrayType`).

**`Enum8`/`Enum16` resolved (Phase 8 item 1, 2026-08-21).** Unlike `Array`/`Nested`, client-v2 has no
type-hint mechanism for enums — `readValue(column, null)` always returns its own `.internal`
`EnumValue`, hint or not. `core.rowbinary.ListDecodingRowBinaryReader` closes the same gap a different way:
after `readValue` returns for a column whose `ClickHouseDataType` is `Enum8`/`Enum16`, it calls
`toString()` on the result (the previously-confirmed member-name behavior above) and stores the
plain `String` instead — same "never leak a client-v2 `.internal` type through `Row`" goal as the
`Array`/`Nested` fix, same class, same narrow per-column-type scope. A `Nullable(Enum8)` `NULL` stays
`null`, never the literal string `"null"`. `ClickHouseRowAssert.hasEnumName` now asserts the actual
runtime type is `String`, not just that `toString()` matches, so it would fail again if the internal
type ever leaked back through. **Caught the hard way, immediately after landing this fix:** the demo
(`examples/spring-boot-webflux-demo`) deliberately depends on the last *published* Maven Central
connector release, not this in-repo source (see its `build.gradle.kts`'s own comment and [Phase
6](#phase-6--spring-webflux-interop-demo-2026-08-13-reworked-after-a-genuine-bindmarkersfactory-finding--pending-green-confirmation)) — an initial version of this change also switched
`DatabaseClientOrderEventRepository` to `Row.get("status", String.class)`, which passed the fast
unit-test suite (that module doesn't touch the demo) but failed the demo's real-ClickHouse
integration tests with a 500 (`ClassCastException`, since the *published* release still returns
`EnumValue`). Reverted: the demo keeps the `Object.class`/`toString()` workaround, with its Javadoc
now explaining exactly why, until its dependency is bumped to a release that actually contains this
fix — see [Phase 8 item 11](#phase-8--post-020-hardening-021) for adding a current-`main` demo lane
specifically so this class of mismatch fails fast next time instead of only being caught by chance.

---

## Phase 3 — Connector (R2DBC SPI surface)

**Started.** `ClickHouseConnectionFactoryProvider` (`supports`/`getDriver`/`create`),
`ClickHouseConnectionFactory`, and `ClickHouseConnection` exist, verified against
`r2dbc-spi:1.0.0.RELEASE`'s actual source (not `main`), TDD, black-box tests plus one real-
ClickHouse round trip (`create()` → `validate(REMOTE)`). ClickHouse's HTTP interface has no
persistent session and no real ACID transactions, so `ClickHouseConnection` is always auto-commit;
every transaction/savepoint method either fails with `UnsupportedOperationException` or, where the
spec explicitly allows it (`releaseSavepoint`), no-ops — this is the "explicit
unsupported-transaction-semantics handling" this phase named up front, not an oversight.
`createStatement(sql)` returns a real `ClickHouseStatement`, and `execute()` is now real too:
`core.rowbinary.RowBinaryDecoder.decode(Flux<ByteBuffer>)` (new) returns a `Mono<DecodedResult>` pairing the
column schema (`List<ColumnDescriptor>` — name + ClickHouse's own wire type string, e.g.
`"Nullable(Int32)"`) with the row stream, from one reader instance and one subscription — unlike
`decodeRows`, which only ever exposed rows. `connector` adapts that into r2dbc-spi's `Result` (
`ClickHouseResult`), `Row`/`Readable` (`ClickHouseRow`), `RowMetadata`/`ColumnMetadata` (
`ClickHouseRowMetadata`/`ClickHouseColumnMetadata`), and `Type` (`ClickHouseType`) — verified
against `r2dbc-spi:1.0.0.RELEASE`'s actual `Result`/`Row`/`Readable`/`RowMetadata`/
`ColumnMetadata`/`ReadableMetadata`/`Type` source, not `main`. Deliberate, documented gap:
`ColumnMetadata.getJavaType()`/`Type.getJavaType()` don't attempt to predict the Java class
client-v2 will decode a column into ahead of reading any row — that mapping is an internal decision
of client-v2's `BinaryStreamReader` decode switch, not a static function of `ClickHouseColumn`, and
duplicating it here risked silently drifting from it. `getRowsUpdated()` now returns a real,
server-reported count instead of always completing empty: ClickHouse sends an
`X-ClickHouse-Summary` response header on every request (`SELECT` included, where it reports `0`
written rows), containing JSON with a `written_rows` field (checked against
clickhouse.com/docs/interfaces/http, not assumed) —
`transport.ClickHouseHttpTransport.queryWithSummary` parses it and returns a
`ClickHouseQueryResponse(long writtenRows, ByteBufFlux body)`; `connector.ClickHouseResult.decode`
is the one place that count and `core`'s decoded rows come together into one `Result`, used
identically by `ClickHouseStatement.execute()` and `ClickHouseBatch`. This was flagged during a
production-readiness review as a genuine silent-risk gap (a caller checking `getRowsUpdated()`
after an `INSERT` to confirm the write happened got nothing back, not an error, which could be
misread as "0 rows written" when the insert actually succeeded) — not a safe fail-loud
simplification, so it was fixed rather than just documented; see "Production readiness" below.
`Result.filter`/`flatMap` are implemented (both are abstract in `r2dbc-spi:1.0.0.RELEASE`, no
default), scoped to the only segment kind produced so far (`RowSegment`). Consumption-once is
enforced per `Result` instance, but not transitively across a `filter()`-derived instance sharing
the same row stream — documented as a known limitation, not silently assumed correct.

Parameter binding (`bind`/`bindNull`) is now real too, mapped directly onto ClickHouse's own named
parameterized-query mechanism (checked against clickhouse.com/docs/interfaces/http, not assumed):
`sql` declares each placeholder as `{name:Type}`; the bound value travels as a separate
`param_<name>=<value>` HTTP query parameter, never substituted into the SQL text. `core.ClickHouseQuery`
grew `parameterNamesIn(sql)` (parses declared placeholder names, in first-occurrence order) and
`withParameters(Map<String,Object>)` (encodes bound values into ClickHouse's own `param_<name>`
wire format — its "Escaped" text format: backslash/tab/newline/CR-escaped strings, `\N` for null,
plain `toString()` for numbers/booleans since they have no special characters to escape).
`connector.ClickHouseStatement` parses `sql`'s declared names once at construction;
`bind(String,Object)`/`bindNull(String,Class)` validate against that set (`NoSuchElementException`
for an undeclared name, matching `Statement`'s own documented contract); `bind(int,Object)`/
`bindNull(int,Class)` map the index to the declared name at that position in first-occurrence
order — ClickHouse's own placeholder syntax has no positional form, so this index-to-name mapping
is this driver's own convention. `execute()` throws `IllegalStateException` synchronously (not a
reactive error signal) if any declared parameter is still unbound, per `Statement.execute()`'s own
documented contract. `transport-http`'s `ClickHouseHttpTransport.query()` appends one
`&param_<name>=<url-encoded value>` per entry in `ClickHouseQuery.parameters()`.
`testkit.ControlledClickHouseServer` grew `receivedUri()` so transport tests can assert on the
actual request query string, not just headers.

`Connection.createBatch()` is real too: `ClickHouseBatch` implements `add(String)`/`execute()` per
`r2dbc-spi:1.0.0.RELEASE`'s `Batch` contract — unlike `ClickHouseStatement`, a batched statement
takes complete, literal SQL (no bound parameters), and `execute()` runs every added statement
sequentially (one full round trip per statement, via `concatMap` so the next statement isn't sent
before the previous one's request has gone out), emitting one `Result` per statement in the same
order — needed for a batch like `CREATE TABLE ...` immediately followed by `INSERT INTO ...`
against it. Proven against real ClickHouse
(`ClickHouseBatchAgainstRealClickHouseTest`: create + insert + a `count()`-based select, in one
batch, asserting the select's result reflects the insert that ran before it in the same batch).

The SPI registration file (`META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider`) is still
intentionally **not** added yet — see [Phase 4](#phase-4--fully-reactive-sign-off) below for what's
still outstanding before this driver calls itself registrable/discoverable.

- `connector`: `ConnectionFactoryProvider`, `Connection`, `Statement`, `Result`, metadata,
  parameter binding, R2DBC exception mapping, explicit unsupported-transaction-semantics handling.
- `testkit`: **done** — `BaseClickHouseIntegrationTest` (a singleton Testcontainers
  `ClickHouseContainer`, one static field shared by every subclass across every module, started
  once per test JVM, no explicit stop — Ryuk cleans up) plus a `@BeforeEach` table-dropping cleaner
  that talks to ClickHouse over a plain synchronous `java.net.http.HttpClient`, deliberately not
  through this project's own transport, so cleanup never depends on the driver code under test.
  `transport-http`'s three real-ClickHouse test classes
  (`SelectOneAgainstRealClickHouseTest`/`QueryIdHeaderAgainstRealClickHouseTest`/
  `RealWorldTableAgainstRealClickHouseTest`) already extend it, and `connector`'s own tests do too
  — no per-module reimplementation (the once-planned `integration-tests` module that would also
  have extended it was scaffolded, sat empty, and was later deleted — see the module map). A richer
  Ability-pattern DSL for creating tables/rows (beyond the `RealClickHouseQueryAbility` already in
  `transport-http`'s tests) can grow here once `connector` actually needs one — TDD, same as
  everything else: the first test that needs it drives the method into existence, don't design the
  whole DSL up front.
- `testkit`: `ClickHouseRowAssert` (custom assertion over a decoded row) also moved here from
  `transport-http`'s test sources, so `connector` gets it for free too. Grew
  `hasList`/`hasTuple`/`hasMap`/`hasEnumName`/`hasBigInteger`/`hasFloatCloseTo`/`hasInetAddress`/
  `hasUuid` alongside the original `hasValue`/`hasDecimal`/`hasNullAt`/`hasTypeAt`, so
  `RealWorldTableAgainstRealClickHouseTest` reads its assertions as domain statements instead of
  raw `assertThat((SomeType) row.get(...))` casts scattered through the test body.
- `connector`'s own `src/test`: small, targeted tests for its own classes, extending
  `BaseClickHouseIntegrationTest` where real ClickHouse is the simplest way to prove behavior —
  including the whole-driver, public-R2DBC-SPI-only proof (`ConnectionFactories.get(...)` → real
  connection) that a dedicated `integration-tests` module was originally meant to hold; see the
  [module map](#module-map) for why that module was deleted rather than kept empty.
- R2DBC TCK-style behavioral tests where applicable; ClickHouse-specific gaps documented instead of
  silently unsupported.

---

## Phase 4 — "Fully reactive" sign-off

Before calling the driver "fully reactive", every property in
[README.md's table](../docs/concepts/fully-reactive.md) needs a test that demonstrates it,
not just an API shape that implies it. Concretely, a checklist pass over:

- Deferred execution, non-blocking I/O, stream-oriented consumption, backpressure-aware delivery,
  cancellation propagation, bounded concurrency, deterministic cleanup, reactive error signalling,
  no scheduler workarounds — each with a named test in the `testkit` contract matrix or connector
  integration suite that would fail if that property broke.
- A short write-up (can reuse the ClickHouse discussion thread) showing the evidence per property,
  the same way Phase 0's finding is written up here.

This is the gate before spending time on Maven Central publishing polish or performance work.

### Sign-off (2026-08-13)

Every property in [README.md's table](../docs/concepts/fully-reactive.md) mapped to a named
test that would fail if that property regressed — not just an API shape that implies it:

| Property | Evidence |
| --- | --- |
| Deferred execution | `ClickHouseHttpTransportTest.shouldNotSendTheRequestBeforeSubscription` — awaits 200ms after calling `query()` with nothing subscribed, asserts the fake server never received a request. |
| Non-blocking I/O | No `.block()`/`.blockFirst()`/`.blockLast()`/`Future#get()` anywhere in any module's `src/main` (checked directly, not assumed — `grep -rn "\.block(" .../src/main` across `core`/`transport-http`/`connector` returns nothing). One deliberate, documented exception: `RowBinaryDecoder.decode` constructs client-v2's reader (which blocks reading the RowBinary header) via `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())` — isolating a genuinely blocking client-v2 API off Reactor Netty's event loop, not hiding a fake-reactive network call. This is the one case "No scheduler workaround" (below) explicitly distinguishes from a workaround. |
| Stream-oriented consumption | `ClickHouseHttpTransportTest.shouldEmitEachChunkSeparatelyInsteadOfAggregating` — asserts chunks arrive as separate emissions, not one aggregated buffer. |
| Backpressure-aware delivery | `FluxInputStreamBridgeTest.shouldOnlyRequestAsManyItemsAsTheConfiguredDemand` (upstream demand is bounded by the bridge's credit, not unlimited) and `ClickHouseHttpTransportTest.shouldNotDeliverDataBeforeTheSubscriberRequestsIt`. |
| Cancellation propagation | `QueryCancellationAgainstRealClickHouseTest.shouldStopTheQueryServerSideWhenTheClientCancelsTheSubscription` (real ClickHouse, proves the server-side `KILL QUERY` actually stops execution) plus the hermetic `ClickHouseHttpTransportTest.shouldCloseTheConnectionWhenTheSubscriptionIsCancelled` / `.shouldSendABestEffortKillQueryWhenCancelledAfterTheRequestWasSent` / `.shouldSendABestEffortKillQueryEvenWhenCancelledBeforeAnyResponseArrives`. |
| Bounded concurrency | `ClickHouseHttpTransportTest.shouldNotStartASecondRequestUntilAConnectionIsFree` (pool size 1, second request queues rather than opening a second connection) and `.shouldNotReachTheServerWhenAQueuedRequestIsCancelledBeforeAConnectionIsAcquired` (a queued-but-not-yet-acquired request that's cancelled never reaches the server at all). |
| Deterministic cleanup | `FluxInputStreamBridgeTest.shouldCancelTheUpstreamSubscriptionWhenClosed` and `ClickHouseHttpTransportTest.shouldCloseTheConnectionWhenTheSubscriptionIsCancelled` (`activeConnectionCount()` returns to 0 after cancellation). |
| Reactive error signalling | `ClickHouseHttpTransportTest.shouldSignalAServerErrorWhenTheExceptionCodeHeaderIsPresent` (transport-level: a ClickHouse error surfaces as `onError`, not a `200`-with-error-body silently treated as success) and `ClickHouseStatementErrorMappingTest.shouldFailWithAnR2dbcExceptionCarryingTheServersErrorCode` (R2DBC-level: wrapped into `io.r2dbc.spi.R2dbcException`, catchable via standard R2DBC error handling). |
| No scheduler workaround | Same evidence as "Non-blocking I/O" above: the only `boundedElastic`/`publishOn`/`subscribeOn` usage in any module's `src/main` is the one documented, narrow case in `RowBinaryDecoder.decode`, isolating a blocking client-v2 API call — not moving an otherwise-blocking network call off-thread to disguise it as reactive. |

## Production readiness review

Standing goal, stated explicitly by the user: this driver should reach a state someone could
actually run in production, not just a state that compiles and passes a happy-path test. Every
finding below is triaged into one of two buckets — **silent risk** (wrong or missing behavior a
caller could reasonably rely on without noticing it's wrong until production) versus **safe,
documented limitation** (a real gap, but one that fails loudly or matches a well-established
convention other drivers share) — the distinction the user pushed for directly ("czy świadomie
czegoś nie implementujemy... aby potem nie działało to na produkcji"). Silent-risk findings get
fixed, not just documented.

### Fixed this pass

- **Hardcoded 2-second response timeout with no way to disable it** (`ClickHouseHttpTransport`).
  Every real ClickHouse query taking longer than 2 seconds — routine for an analytical database —
  would have failed outright. No test caught this because every existing test used trivial, fast
  queries. Fixed: response timeout is now `null` (no timeout) by default on every constructor,
  configurable explicitly via a new constructor overload. Silent risk — fixed.
- **`ServerException` (client-v2's, reused by `transport-http` for ClickHouse's own errors) does
  not extend `io.r2dbc.spi.R2dbcException`.** A caller doing standard R2DBC error handling
  (`catch (R2dbcException e)`) around a query would never catch a ClickHouse server error. Fixed:
  `connector.ClickHouseR2dbcException.wrap` walks the failure's cause chain for a `ServerException`
  and reuses its code/message, falling back to a generic wrapped exception for anything else
  (connection reset, pool exhaustion, a local decode bug) — wired into
  `ClickHouseStatement`/`ClickHouseBatch`/`ClickHouseResult` via `onErrorMap`. Silent risk — fixed.
- **`Result.getRowsUpdated()` always completed empty**, regardless of how many rows an `INSERT`
  actually wrote — a caller checking it to confirm a write succeeded got nothing back, not an
  error, which could be misread as "0 rows written" when the insert actually succeeded. Fixed: it
  now parses ClickHouse's own `X-ClickHouse-Summary` response header (`written_rows` field, sent on
  every response including `SELECT`, where it's `0`) via
  `ClickHouseHttpTransport.queryWithSummary`. Found and fixed a real bug surfaced while wiring this
  up: an intermediate version used `Flux.next()` to materialize the response, which cancels the
  underlying HTTP response scope before the body is ever read separately — every batch/statement
  result came back silently empty until caught by
  `ClickHouseBatchAgainstRealClickHouseTest` against real ClickHouse. Silent risk — fixed.
- **`ClickHouseConnection.close()` didn't actually prevent further use** — `createStatement`/
  `createBatch` kept happily creating statements that would run real queries through a "closed"
  connection. A caller or connection-pool implementation that closes a connection and then
  accidentally keeps using it (e.g. a pooled connection returned to the pool while something still
  holds a reference) got silent, unexpected query execution instead of a clear error. Fixed: both
  methods now throw `IllegalStateException` once closed, matching `validate()`'s existing behavior.
  Silent risk — fixed.
- **No way to bound how long establishing the TCP connection itself could take** — only
  `responseTimeout` (time waiting for a response after the request is sent) was configurable, and a
  caller going through the standard R2DBC URL/options bootstrap path (`ConnectionFactories.get(...)`
  rather than constructing `ClickHouseHttpTransport` directly) had no way to configure even that.
  Fixed: `connectTimeout` wired to Netty's `ChannelOption.CONNECT_TIMEOUT_MILLIS`, threaded from
  R2DBC's standard `ConnectionFactoryOptions.CONNECT_TIMEOUT` in `ClickHouseConnectionFactory.from`.
  Silent risk (unbounded hang against an unreachable/firewalled host) — fixed.
- **`RowBinaryDecoder`'s response-chunk backpressure demand was a bare, unexplained `4`.** Not a
  correctness bug, but an unbenchmarked magic number that plausibly affects throughput/latency (too
  low serializes network reads and decoding, too high buffers more memory per in-flight query).
  Named as `RESPONSE_CHUNK_DEMAND` with a Javadoc explaining the trade-off and explicitly deferring
  actual tuning to the not-yet-started performance phase (see below). Documented placeholder, not a
  silent risk — turning a silent magic number into an honest one.
- **`ClickHouseStatement`/`ClickHouseBatch` mutable binding state was undocumented for
  thread-safety.** Not a bug — this matches `java.sql.PreparedStatement` and every other R2DBC
  driver's statement type (bind on one thread, then execute once) — but was silently assumed rather
  than stated. Added explicit "not thread-safe" Javadoc. Safe, now-documented limitation.
- **Cancelling a client-side subscription now sends an explicit, best-effort `KILL QUERY` —
  because ClickHouse's own connection-close detection for HTTP queries turned out not to work.**
  Initially assumed (per ClickHouse's HTTP docs describing a "Cancel HTTP Request" mechanism) that
  closing the connection would be enough to stop server-side execution; verified against a real
  server that it is not — see `QueryCancellationAgainstRealClickHouseTest`'s Javadoc and
  `ClickHouseHttpTransport.queryWithSummary`'s Javadoc for the full trail (matches ClickHouse's own
  docs verbatim, "Running requests don't stop automatically if the HTTP connection is lost", and a
  ClickHouse issue reproducing the same gap even with `cancel_http_readonly_queries_on_client_close
  = 1` enabled, closed "not planned" by ClickHouse's own maintainers:
  [ClickHouse/ClickHouse#92786](https://github.com/ClickHouse/ClickHouse/issues/92786)). Fixed:
  `ClickHouseHttpTransport` now sends `KILL QUERY WHERE query_id = '...' ASYNC` on a separate
  request whenever a subscription is cancelled *after* the original request was actually sent
  (cancelling before that means there's nothing running server-side to kill yet). Caught a real bug
  while building this: the first version gated the kill on *response headers having arrived* rather
  than *the request having been sent* — which passed every hermetic test (fast fixtures) but failed
  against a real slow query in `QueryCancellationAgainstRealClickHouseTest`, because a query whose
  entire result is one big block (as this test's is, deliberately) may not send anything back —
  not even headers — until it's nearly done, long after it's already running server-side and long
  after a caller might reasonably cancel it. Fixed by gating on Reactor Netty's `doAfterRequest`
  hook (`HttpClientState.REQUEST_SENT`) instead, which fires once the request itself is flushed to
  the socket, independent of whether or when a response ever comes back — re-verified against real
  ClickHouse afterward, and locked in hermetically too via a new
  `ClickHouseHttpTransportTest.shouldSendABestEffortKillQueryEvenWhenCancelledBeforeAnyResponseArrives`
  test (a fake server that accepts the request but never responds at all). The existing
  cancellation-before-acquire hermetic tests (proving "cancelling before the server sees anything
  sends nothing") still pass unmodified either way, since in those scenarios the request is never
  actually sent under either gating signal. The kill reuses the same authenticated user as the
  original query — ClickHouse lets a user stop their own queries without a separate `KILL QUERY`
  privilege grant — and is genuinely best-effort: if it fails (most likely the connecting user lacks
  the privilege after all, e.g. under a restricted RBAC setup), the failure is logged at `WARN` via
  a newly added `slf4j-api` dependency and otherwise swallowed; it never surfaces on the caller's
  already-cancelled subscription, and there is no retry of the kill itself.
  `QueryCancellationAgainstRealClickHouseTest` was rewritten to prove the fixed behavior end to end
  (query now stops within ~5s of cancellation, not just "closes the connection and hopes"). Silent
  risk — fixed, with an honest best-effort caveat documented rather than claimed as a hard guarantee.
- **No way to configure a custom trust store for `ssl=true`, so a self-signed or internal-CA
  certificate — which real ClickHouse deployments commonly use — could not be connected to at all.**
  Previously an open gap: a caller on a self-signed or internal-CA certificate — plausibly the more
  common case for a database that usually isn't exposed to the public internet — got a hard
  `SSLHandshakeException` with no way to work around it short of importing the certificate into the
  whole JVM's default trust store (`-Djavax.net.ssl.trustStore`), outside the driver entirely. Fixed,
  per explicit direction to make this "wygodne, elastyczne, latwo konfigurowalne" for a
  Kubernetes/Tanzu-style deployment: `ClickHouseHttpTransport` gained a constructor overload
  accepting raw `byte[]` PEM-encoded certificate bytes, wired into Reactor Netty via
  `Http11SslContextSpec.forClient().configure(builder -> builder.trustManager(...))`; passing a
  non-null certificate together with a non-`https://` base URL is rejected eagerly with
  `IllegalArgumentException` rather than silently ignored. `ClickHouseConnectionFactory.from` exposes
  this as a new `sslRootCert` `ConnectionFactoryOptions` (`ClickHouseConnectionFactoryProvider
  .SSL_ROOT_CERT`), resolved first as a classpath resource then as a filesystem path — the same
  two-step resolution r2dbc-postgresql's own `sslRootCert` option uses, chosen deliberately to match
  a convention other R2DBC drivers already use rather than invent a new one; setting it without
  `ssl=true` fails fast with `IllegalArgumentException`. Verified hermetically against a real
  self-signed-certificate TLS server: `ClickHouseHttpTransportTlsTest
  .shouldSucceedTheHandshakeWhenTheServerCertificateIsExplicitlyTrusted` proves the handshake now
  succeeds and a real response comes back, in contrast to the existing untrusted-certificate test in
  the same class. Silent risk (no supported way to reach a large class of real deployments) — fixed.
- **No retry/reconnect policy of any kind — a transient connection-level failure (a dropped
  request before it reached the server, a momentary "connection refused" while a server restarts)
  surfaced directly to the caller with no automatic retry.** Researched client-v2's own retry
  behavior first rather than guessing (checked our exact pinned version, `v0.9.0`, in source):
  it retries by default (3 attempts) but *only* for failures classified as happening before a
  response was received — `NoHttpResponse`, `ConnectTimeout`, `ConnectionRequestTimeout`,
  `SocketTimeout` — with no retry based on ClickHouse's own server-side retryable error codes in
  that version (`ServerException.isRetryable()` doesn't exist yet at `v0.9.0`). Explicit concern
  raised during design: retrying an `INSERT` that actually reached the server risks applying it
  twice (e.g. a unique-key collision on retry). Fixed with a narrower, more conservative rule than
  client-v2's own: the new `RetryPolicy` (`transport-http`) retries a query only when the failure
  happened *strictly before* the request had been fully sent — reusing the exact same
  `doAfterRequest`-driven `requestSent` signal the `KILL QUERY`-on-cancel feature above already
  relies on, rather than classifying by exception type the way client-v2 does. Since the server
  never received any bytes of a pre-send-failed attempt, retrying it cannot make the query run
  twice server-side by construction — no per-query idempotency flag, no SQL-shape guessing (`SELECT`
  vs. `INSERT`), and no risk of the exact collision scenario raised during design. A failure after
  the request was sent (including a mid-write socket timeout, which client-v2 *does* retry by
  default — a deliberate divergence, chosen to be conservative) is never retried here. Configurable
  via `RetryPolicy(maxAttempts, delay)` — `RetryPolicy.defaultPolicy()` (3 attempts, 50ms fixed
  delay, matching client-v2's attempt count) applied via every `ClickHouseHttpTransport` constructor
  that doesn't take one explicitly; `RetryPolicy.disabled()` turns it off. Wired into the R2DBC
  bootstrap path as two independently-defaulted `ConnectionFactoryOptions`,
  `ClickHouseConnectionFactoryProvider.RETRY_MAX_ATTEMPTS`/`RETRY_DELAY` (`retryMaxAttempts=0`
  disables). Implemented via `Flux.retryWhen(Retry.fixedDelay(...).filter(...))`, gated on the same
  `requestSent` flag; verified hermetically both ways —
  `ClickHouseHttpTransportTest.shouldNotRetryAFailureThatHappensAfterTheRequestWasSent` (a
  connection reset mid-response must not trigger a second request) and
  `.shouldRetryAConnectionLevelFailureThatHappensBeforeTheRequestWasSentUntilItSucceeds` (a
  real "nobody listening yet" port that only starts accepting after a delay, proving retries
  actually bridge that gap). As a side effect, the best-effort `KILL QUERY` on cancel — which
  reuses this same code path — now also benefits from pre-send retry (see the "known, documented"
  entry on it below). Silent risk — fixed, deliberately narrower in scope than client-v2's own
  default to close off the exact collision risk raised during design.
- **Follow-up question raised directly: what if a retried `INSERT` hits a row that already exists
  — does that surface as a key-collision error, and could retry make it worse?** Researched rather
  than assumed: ClickHouse's `MergeTree` family (the standard, non-experimental engine family) has
  **no unique-key/primary-key constraint enforced at insert time at all** — a duplicate `ORDER BY`
  key is silently accepted, not rejected. `ReplacingMergeTree` deduplicates matching keys, but only
  lazily during background merges, never synchronously on `INSERT`. A true `UNIQUE KEY` constraint
  is a proposed, not-yet-shipped ClickHouse feature (tracked upstream as
  [ClickHouse/ClickHouse#70589](https://github.com/ClickHouse/ClickHouse/issues/70589)). So the
  specific failure mode the question describes — an `INSERT` bouncing off a key-collision error —
  does not exist for ClickHouse's standard engines the way it would for Postgres/MySQL; there is no
  new server-side error case for `RetryPolicy` to special-case. The real risk this question was
  actually pointing at — retrying an `INSERT` that already reached the server and silently
  duplicating the row (ClickHouse would just accept it twice, no error either way) — is exactly
  what `RetryPolicy`'s pre-send-only scope already prevents by construction (see the bullet above),
  and `ClickHouseStatement.execute()`/`ClickHouseBatch` route through the identical
  `ClickHouseHttpTransport.queryWithSummary` path as every other query with no `INSERT`-specific
  branching, so the existing
  `ClickHouseHttpTransportTest.shouldNotRetryAFailureThatHappensAfterTheRequestWasSent` guarantee
  already covers `INSERT` — it isn't a separate code path that could silently diverge.

### Known, documented, safe limitations (not fixed — deliberate)

- **`ColumnMetadata.getJavaType()`/`Type.getJavaType()` don't predict a Java class ahead of
  decoding a row.** client-v2's `BinaryStreamReader` decode switch is the actual source of truth;
  duplicating it here risks silent drift. A caller that needs a Java type per column derives one
  from an actually-decoded row's value instead. Fails loudly (no wrong guess), documented on both
  classes.
- **`Connection.setStatementTimeout(Duration)` — resolved in Phase 7.** Implemented as
  ClickHouse's per-query, server-side `max_execution_time` setting, distinct from the transport-wide
  client-side `responseTimeout`. Statements snapshot the connection's configured value when they
  are created; hermetic and real-ClickHouse tests cover the contract.
- **Transactions/savepoints are unimplemented**, matching ClickHouse's HTTP interface having no real
  session affinity for its experimental transaction feature (see `ClickHouseConnection`'s class
  Javadoc for the full, checked-against-docs reasoning). Fails loudly.
- **The best-effort `KILL QUERY` on cancellation is not retried if it fails *after* being sent** —
  e.g. the kill request reached the server but the response was lost, or the server itself rejects
  it for a non-connection reason. Logged at `WARN` either way. Note this is now narrower than it
  used to be: since `killQueryBestEffort` goes through the same `queryWithSummary` path as any other
  query (see "Fixed this pass" above, `RetryPolicy`), a kill request that fails *before* being sent
  — a transient pool/connect hiccup at the exact moment of cancellation — is now retried
  automatically like any other query. Only the post-send case remains an intentionally
  non-retried, best-effort cleanup path.
- **`ssl=true` was completely untested — both the TLS auto-negotiation half and the trust half are
  now verified.** `ClickHouseConnectionFactory.from` builds an `https://` base URL and hands it to
  Reactor Netty's `HttpClient`, which its own docs say auto-applies a default `SslProvider` when the
  URI scheme is `https`, with no explicit `.secure(...)` call needed — previously trusted on faith,
  not checked. Fixed: `ClickHouseHttpTransportTlsTest` starts a real (self-signed-cert) TLS server via
  Reactor Netty and connects `ClickHouseHttpTransport` to it unmodified over `https://`; since the
  client doesn't trust that certificate, a successful handshake attempt is provable by the *kind* of
  failure it gets — an `SSLException` somewhere in the cause chain, categorically different from what
  would happen if the `https://` scheme were silently ignored and plaintext HTTP sent to a TLS-only
  port instead (a hang, reset, or garbled response, not a clean TLS alert). A second test in the same
  class, `shouldSucceedTheHandshakeWhenTheServerCertificateIsExplicitlyTrusted`, then proves the
  positive case now that `sslRootCert`/`trustedCertificatePem` exists (see "Fixed this pass" above):
  the same self-signed certificate, supplied as a trusted certificate, lets the handshake succeed and
  a real response come back.
- **Every `INSERT`'s data went through the URL query string, same as any `SELECT`** —
  `ClickHouseHttpTransport.queryWithSummary` never attached a request body (`RequestSender.send`
  was never called), so a batch `INSERT ... VALUES (...), (...), ...` had its entire row data
  URL-encoded into `?query=...`. Correct for small inserts, but not what ClickHouse's own HTTP
  docs recommend for anything large (clickhouse.com/docs/interfaces/http describes `POST
  /?query=INSERT INTO t FORMAT TabSeparated` with the data streamed as the request body, e.g.
  `curl --data-binary @-`, specifically to avoid URL length limits and loading the whole payload
  into memory) — a real risk for "wiele insertów"/large-batch workloads. Fixed: new
  `ClickHouseHttpTransport.insertWithSummary(ClickHouseQuery, Publisher<ByteBuffer>)` streams
  `data` straight onto the socket via `RequestSender.send(Publisher<ByteBuf>)`, no aggregation, no
  copying beyond `Unpooled.wrappedBuffer`. Deliberately **never retried**, unconditionally, even
  when `RetryPolicy` is configured: `queryWithSummary`'s pre-send-only retry safety relies on
  `requestSent` (`doAfterRequest`/`REQUEST_SENT`) meaning "zero bytes of this attempt reached the
  server" — true for a bodyless request, since flushing the headers *is* sending the whole request,
  but not provably true once a body is being streamed (there is a window where part of `data` may
  have already reached the server while `requestSent` still reads `false`); retrying there could
  silently resend a partially-delivered insert. Proven hermetically:
  `shouldStreamTheRequestBodyToTheServerForAnInsert` (body arrives byte-for-byte via a new
  `ControlledClickHouseServer.startAcceptingInsertsAndRespondingWithSummary` body-capturing
  factory) and `shouldNeverRetryAnInsertEvenOnAConnectionLevelFailureBeforeTheRequestWasSent`
  (proves no retry happens even with `RetryPolicy(20, 200ms)` configured, by asserting the failure
  surfaces in well under the time 20 retries would take). Wired to the connector/R2DBC level as a
  vendor extension, `ClickHouseConnection.insertStreaming(String, Publisher<ByteBuffer>)` — `Result`
  carries only `getRowsUpdated()` (`ClickHouseResult.forInsert`, since a plain `INSERT`'s HTTP
  response has no row body to decode, unlike `SELECT`'s `RowBinaryWithNamesAndTypes`), errors are
  mapped through the same `ClickHouseR2dbcException.wrap` as `ClickHouseStatement`/`ClickHouseBatch`.
  Covered hermetically (`ClickHouseConnectionInsertStreamingTest`: streamed body arrives intact,
  server error maps to `R2dbcException`) and against real ClickHouse
  (`ClickHouseConnectionInsertStreamingAgainstRealClickHouseTest`: rows genuinely land in the table,
  not just accepted-and-discarded). `ClickHouseStatement`/`ClickHouseBatch` themselves are
  unchanged and still URL-encode their data — correct for small inserts, `insertStreaming` is the
  opt-in path for large ones. Silent risk — fixed.
- **`JSON` type support (2026-08-13).** Was entirely untested; tracing client-v2's
  `BinaryStreamReader`/`AbstractBinaryFormatReader` showed a `JSON` column decodes either as a
  plain `String` or into a complex `.internal` object tree, controlled by the
  `output_format_binary_write_json_as_string` server setting baked into the reader at construction
  time — off by default, so a `JSON` column would have decoded into the `.internal` representation
  with no supported way to read it back out. Fixed by having `ClickHouseHttpTransport` append
  `output_format_binary_write_json_as_string=1` to every `queryWithSummary` request
  unconditionally (a no-op for a result set with no `JSON` column, so this needed no new
  `ConnectionFactoryOptions`/`Option` — a caller such as Spring's `DatabaseClient` gets working
  `JSON` columns with zero extra configuration), matched on the decode side by `core`'s
  `RowBinaryDecoder#newReader` setting the same flag on its local `QuerySettings` so client-v2's
  reader expects the same wire shape it's actually getting. `JSON` is GA since ClickHouse 25.3, so
  no `allow_experimental_json_type` setting is needed (and was deliberately not added, since a
  removed experimental flag risks an "Unknown setting" error against a current server). Proven by
  `shouldDecodeJsonTypeAsAPlainString` in `RealWorldTableAgainstRealClickHouseTest`. `Dynamic`/
  `Variant` remain untested — newer, less settled experimental types. Silent risk — fixed.

### Open gaps, not yet addressed

- **Retryable server errors — transport mechanism resolved, R2DBC exposure still open.** Core and
  transport now support explicit per-query opt-in through
  `ClickHouseQuery.withServerErrorRetryEnabled()`, gated on `ServerException.isRetryable()` and no
  response bytes having been emitted. The standard R2DBC `Statement` path has no vendor extension
  to enable that flag yet, so R2DBC callers retain pre-send-only retry behavior.
- **Transport pool configuration through R2DBC — resolved in Phase 7.** Driver-specific
  `ConnectionFactoryOptions` cover max connections, pending-acquire count/timeout, idle time, and
  connection lifetime, including URL parsing and validation tests.
- **`Statement.add()` bound-parameter batching — resolved in Phase 7.** Each complete binding set
  is snapshotted and executed sequentially via `concatMap`, emitting one `Result` per set. Wire-level
  coalescing into one multi-row `INSERT` remains deliberately separate; `insertStreaming` is still
  the large-insert path.

**Confirmed plan and order (2026-08-13), before Maven Central publishing:** raised directly —
INSERT correctness was already implemented and tested against real ClickHouse
(`ClickHouseStatementAgainstRealClickHouseTest`, `ClickHouseBatchAgainstRealClickHouseTest`); this
pass additionally closed the *performance* gap for large inserts, both at the transport level
(streamed request body) and the connector/R2DBC level (`ClickHouseConnection.insertStreaming`
vendor extension) — see "Fixed this pass" above. Order agreed for what's left:

1. Fill in the realistically-closable type-coverage gaps first: a dedicated real-ClickHouse test
   for `Nested` (mechanism already works via `Array`'s `ListDecodingRowBinaryReader`, just
   untested directly), and a first attempt at `LowCardinality` (currently not attempted at all).
2. A short, mostly-documentation Phase 4 sign-off pass — the tests already exist, this is writing
   down which named test proves which "fully reactive" property.
3. `examples/spring-boot-webflux-demo` (Phase 6) — a real Spring Boot + WebFlux app through a
   hand-built `DatabaseClient`/`ConnectionPool` (`R2dbcEntityTemplate` turned out not to work
   against this driver — see Phase 6's write-up), explicitly requested as part of "this has to be
   really well tested" rather than left for after publishing.
4. Phase 5 (benchmarks) — still gated behind Phase 4 per CLAUDE.md's Performance testing section;
   the sign-off pass in step 2 is what unlocks it, not a calendar date.
5. `JSON` type support (2026-08-13) — see "Fixed this pass" above. Done ahead of Phase 5 at the
   user's explicit request ("Najpierw JSON").
6. Phase 5 (benchmarks) — next, still gated behind Phase 4 (already unlocked) per CLAUDE.md's
   Performance testing section. **Confirmed order: benchmarks before Maven Central, not after** —
   re-confirmed directly (2026-08-13). No JMH/Gatling infrastructure and no baseline exist yet;
   this is genuinely not started.
7. Maven Central publication — last, once the above hold.

**Final review pass before Phase 5 (2026-08-13).** A fresh, independent review of `core`,
`transport-http`, `connector`, and `testkit` main-source (task tracked as "Full code review pass"
in the working task list) found nothing severe — the codebase holds up against every CLAUDE.md rule
checked (Javadoc on public members, final-by-default, no `Utils` grab-bags, no null-as-silent-
default, records/sealed types where the shape fits, package-private-by-default). Two small, real,
non-blocking findings, both closed or logged:

- **Closed:** the JSON query-string change (`ClickHouseHttpTransport.queryWithSummary` appending
  `output_format_binary_write_json_as_string=1`) had real-ClickHouse coverage
  (`shouldDecodeJsonTypeAsAPlainString`) but no fast hermetic contract-test assertion that the query
  parameter actually lands on the wire, unlike the existing `param_<name>` case
  (`shouldSendBoundParametersAsParamQueryParameters`). Added
  `shouldAskForJsonColumnsAsPlainStringsOnEveryQuery` to `ClickHouseHttpTransportTest`, asserting
  `server.receivedUri()` via `ControlledClickHouseServer` — matches this project's own test-strategy
  tiering (fast hermetic contract test first, real-ClickHouse test proves the end-to-end decode).
- **Logged, not fixed now:** `testkit`'s `ControlledClickHouseServer` repeats an ~15-line block
  (the `Atomic*` request-tracking field declarations and route wiring) across its 7 static factory
  methods, differing only in response-body logic — real duplication risk (the field declarations
  could silently drift), but not a `Utils`-grab-bag violation and not touched in this pass to avoid
  destabilizing every test class that depends on it right before a benchmark phase. Worth a small
  private-helper extraction (shared state holder + a `wireTracking(...)` method) as a low-risk
  cleanup, whenever `testkit` is next touched for another reason.

**`integration-tests` module: deleted (2026-08-13), not left as a placeholder.** Raised directly —
an empty module sitting in the build forever reads as dead scaffolding, not deliberate design. See
the [module map](#module-map) at the top of this file for the full reasoning and what now covers
its originally-intended role instead.

## Non-functional requirements: logging, metrics, leaks

Named explicitly after a direct question ("najważniejsze aby dobrze działało, logowało, odkładało
metryki, nie było wycieków pamięci i blokowania") — these aren't a separate phase, they're
cross-cutting properties every phase from here on must not silently skip, the same "no silent
gaps" discipline already applied to R2DBC semantics and the mid-stream-error question.

- **No blocking.** Already the whole point of Phases 1–4 — tracked there, not repeated here.
- **No memory/resource leaks.** Two concrete mechanisms, not just an aspiration: (a) Netty
  `ByteBuf` reference counting — enable Netty's leak detector at `paranoid` level
  (`-Dio.netty.leakDetection.level=paranoid`) in `transport-http`'s and `testkit`'s test JVM args,
  so a forgotten `.release()` fails CI instead of showing up as a slow leak in production —
  **deferred out of `0.2.0`**, see Phase 7 item 6's entry below for the current status and the
  parked partial pilot; (b) bounded, deterministic cleanup on cancellation — already an explicit
  Phase 4 checklist item (`FluxInputStreamBridge.close()`/`ControlledClickHouseServer`'s
  `activeConnectionCount()` are the first two places this is already tested), shipped and not
  affected by (a)'s deferral. The step-6 `WeakReference` finding above is a related but different
  concern (a lifetime bug, not a leak) — noted there, not here.
- **Logging.** Not started yet. When it starts: `slf4j-api` only (never a concrete binding) as a
  dependency of `core`/`transport-http`/`connector`, so we never force a logging backend on a
  consumer — same reasoning as R2DBC drivers universally do this. Log at query lifecycle
  boundaries (start, complete, error, cancel) and connection-pool events (acquire, release,
  saturation) once `core`'s query/`query_id` representation exists in Phase 2 — logging a query
  without its `query_id` correlator isn't worth doing, so this naturally follows Phase 2's
  `query_id` work rather than preceding it.
- **Metrics.** Not started, and no vendor decided (Micrometer is the obvious default for anything
  meant to plug into Spring, given Phase 6's WebFlux demo, but not committed to yet). What's
  decided now: expose an extension point (counts/timers for queries issued/completed/failed,
  connection-pool size/saturation, bytes streamed) rather than hardcoding a specific metrics
  library's types into `core`. Concrete shape (an SPI `core` owns vs. a `connector`-level hook) is
  a Phase 3/4 design decision, not a Phase 1 one — named here so it isn't forgotten, not designed
  now.

---

## Phase 5 (later) — Load and performance testing

> [!NOTE]
> **Moved to [docs/performance/](../docs/performance/index.md) (2026-08-14, further split into
> index/methodology/results/running-benchmarks 2026-08-23 as part of Phase 9).** This section had grown to
> ~950 lines (design, every benchmark run, the H0/H1/H2 optimization investigation, the ongoing
> compact-row redesign) and made the rest of this roadmap hard to navigate. Content is unchanged,
> only relocated — same headings, same section anchors, just prefixed with `docs/PERFORMANCE.md#`
> instead of `ROADMAP.md#` wherever something links into a specific subsection.

**Where things stand right now** (see `docs/PERFORMANCE.md` for the full evidence):

| | |
| --- | --- |
| Regression found | `StreamingScanBenchmark`: this driver up to 80% slower than client-v2 at 1M rows (streaming scan) |
| Root causes | H1 — per-row `LinkedHashMap` copy (~576 B/row, ~77% of decode latency); H0 — a smaller `byte[1]` allocation bug (~24 B/row) |
| Fix | Production redesign: per-row `Map` replaced with a compact `DecodedRow` (`Object[]`) |
| Status | **Build-verified (2026-08-14)** — a real, single-fork `StreamingScanBenchmark` run against the redesigned code compiled and passed, with `ourDriver` beating `clientV2` on mean latency at all three tiers: 10k 3.00ms vs 3.82ms (~21% faster), 100k 19.3ms vs 22.2ms (~13% faster), 1M 152.3ms vs 155.0ms (~1.8% faster), plus a tighter tail at 1M (max 177.7ms vs 253.8ms) |

> [!WARNING]
> Not yet final: needs a multi-fork re-run (`-Pjmh.forks=3 -Pjmh.warmupIterations=3`, per this
> project's own established rigor rule) and a full `./gradlew spotlessCheck clean build` to confirm
> the unit test suite — not just compilation — is green.

---

## Phase 6 — Spring WebFlux interop demo (2026-08-13, reworked after a genuine BindMarkersFactory finding — pending green confirmation)

Goal: prove the driver works through Spring's own R2DBC integration (`DatabaseClient`,
`ReactiveTransactionManager`) under the current Spring Boot/Spring Framework line — confirmed
against Spring Boot 4.1.0 (Framework 7)'s own managed dependency coordinates page at implementation
time: Boot 4.1.0 manages `r2dbc-spi` at exactly `1.0.0.RELEASE`, the same version this driver is
pinned to, and `r2dbc-pool` at `1.0.2.RELEASE` — no version conflict to work around. Spring
discovers our driver purely via the standard R2DBC `ConnectionFactoryProvider` SPI (`META-INF/
services`), the same mechanism every other R2DBC driver uses — no Spring dependency belongs in
`core`/`transport-http`/`connector` themselves; the new module depends on
`clickhouse-r2dbc-reactive-connector` as `runtimeOnly`, and its own code never imports a single
class from it.

The original design (first pass, since superseded) had `EventController` going through
`R2dbcEntityTemplate`'s object mapping. Building that surfaced a genuine, driver-wide gap rather
than a demo bug — worth recording in detail since it isn't obvious and will bite the next person
who tries the same thing:

- Spring R2DBC resolves a `BindMarkersFactory` for `DatabaseClient` bean construction via
  `BindMarkersFactoryResolver`, which only recognizes a small hardcoded driver list (Postgres,
  MySQL, MariaDB, SQL Server, H2 — see Spring Framework's own `r2dbc.adoc`, "Currently supported
  databases"). ClickHouse isn't on it, so left to Spring Boot's own auto-configuration,
  `DatabaseClient` bean creation fails outright with `IllegalStateException: Cannot determine a
  BindMarkersFactory for ClickHouse` before a single query ever runs.
- First fix attempt: a `ClickHouseBindMarkersFactoryProvider` implementing
  `BindMarkersFactoryResolver.BindMarkerFactoryProvider`, registered via `META-INF/
  spring.factories`. This unblocked bean construction, but not the deeper problem below — and was
  later removed once `R2dbcConfiguration` started building `DatabaseClient` explicitly, which
  sidesteps `BindMarkersFactoryResolver` entirely (a self-defined bean means Spring Boot's own
  `@ConditionalOnMissingBean(DatabaseClient.class)` bean method never runs).
- The deeper, still-unresolved problem: even with a constructible `DatabaseClient`, Spring's
  `.bind()`/named-parameter/`R2dbcEntityTemplate` machinery generates SQL text using whatever
  placeholder syntax the `BindMarkersFactory` emits (e.g. `"?"`), then calls `Statement.bind(index,
  value)`. This driver's `ClickHouseStatement.bind(int index, Object value)` maps `index` to the
  Nth **declared `{name:Type}` parameter** found via `ClickHouseQuery.parameterNamesIn(sql)`.
  Spring-generated SQL containing `"?"` placeholders has zero such declared parameters, so
  `nameAt(index)` throws `IndexOutOfBoundsException` at query-*execution* time. This is a genuine
  architecture mismatch: ClickHouse's own parameterized-query mechanism requires the parameter's
  type declared inline in the SQL text, which `BindMarkersFactory` has no way to supply (it only
  ever produces placeholder text, with no type information available at marker-generation time).
  **Conclusion**: Spring Data R2DBC's `.bind()`/named-parameter/`R2dbcEntityTemplate` object-mapping
  layer is not currently usable with this driver, independent of the `BindMarkersFactory`
  construction issue above. Closing this gap "for real" would mean either a driver-side rewrite of
  bound SQL to inline literal values before sending (defeats the purpose of parameterized queries,
  reintroduces injection-shaped code paths) or a Spring-side extension point this driver doesn't
  currently have access to (Spring's binding SPI has no hook for a driver to supply type
  information back to the marker factory). Left as a documented limitation, not attempted further,
  per this phase's scope.

Current design, after two rounds of rework — first replacing `R2dbcEntityTemplate` with raw
`DatabaseClient` (the finding above), then restructuring the whole module into a small
Ports & Adapters layout around a more realistic "order events" domain instead of a generic
string-bag CRUD table, explicitly requested as part of "this should be a real project, not dumb
CRUD, covering as much of the driver as reasonably fits":

- New module, `examples/spring-boot-webflux-demo` (not published, added to both
  `settings.gradle.kts` and `build.gradle.kts`'s `nonPublishedModules`) — same role as
  [`spring-reactive-transaction-boundary`'s demo module](https://github.com/CamilYed/spring-reactive-transaction-boundary/tree/main/examples/spring-boot-webflux-r2dbc-ddd-demo)
  (that repo's exact contents weren't fetchable from this session — `github.com`/
  `raw.githubusercontent.com` requests came back empty — so this module's structure is this
  project's own take on the same Ports & Adapters idea already documented in CLAUDE.md's
  Architecture section, applied one layer up from the driver itself, rather than a line-for-line
  mirror).
- Three packages, mirroring the driver's own `core`/`transport-http`/`connector` split one layer up:
  `domain` (`OrderEvent`, `OrderStatus`, `CategoryTotal`, and the `OrderEventRepository` port — an
  interface, no Spring/SQL types in sight), `infrastructure` (`DatabaseClientOrderEventRepository`,
  the port's only adapter and the sole place SQL/`Row.get` decoding happens;
  `OrderEventsSchemaInitializer`; `R2dbcConfiguration`), `api` (`OrderEventController`, depending
  only on the `OrderEventRepository` port, never on `DatabaseClient`/SQL directly).
- `R2dbcConfiguration` (in `infrastructure`): builds `ConnectionFactory`, `DatabaseClient`, and
  `ReactiveTransactionManager` beans by hand rather than through Spring Boot's R2DBC
  auto-configuration — modeled on a hand-rolled `ConnectionFactory`/pool wiring class from one of
  the user's own production Spring Boot services, adapted to this driver and this project's style
  (no Lombok, `final`-by-default, package-private, Javadoc on the class and each `@Bean` method).
  Wraps the base `ConnectionFactory` in an `r2dbc-pool` `ConnectionPool` unless
  `spring.r2dbc.pool.enabled=false`, fail-fast validates the pool settings
  (`initial-size`/`max-size`/`min-idle` sanity checks) before constructing anything, rejects a
  `driver=pool` URL (this configuration already builds its own pool), and logs a startup summary of
  what was configured. This is what makes `DatabaseClient` bean construction succeed at all (see
  the finding above) and is also, independently, a more realistic demo of what an application's
  actual R2DBC wiring tends to look like than relying entirely on Boot's defaults.
- `order_events` table/`OrderEvent` domain type: `id`/`customer_id` (`UUID`), `category`
  (`LowCardinality(String)`), `tags` (`Array(String)`), `amount` (`Decimal(18,4)`, not nullable),
  `discount` (`Nullable(Decimal(18,4))`), `status` (`Enum8('PLACED'=1,'PAID'=2,'CANCELLED'=3)`),
  `client_ip` (`IPv4`), `occurred_at` (`DateTime64(3)`) — chosen specifically to exercise a broad,
  realistic slice of this driver's decode surface through Spring's `DatabaseClient`/`Row`, not the
  trivial string/timestamp pair the first pass had. The full type matrix (every integer width,
  `Map`, `Tuple`, `Nested`, `Nullable` in general, ...) is already covered at the wire level by
  `transport-http`'s `RealWorldTableAgainstRealClickHouseTest`, so this demo isn't re-proving that;
  it's proving a realistic slice survives the extra `DatabaseClient`/`Row` hop, both present and
  absent (`OrderEventControllerAgainstRealClickHouseTest` covers both a fully-populated event and
  one with an empty tags array and `NULL` discount).
- Two real, driver-wide decode gotchas surfaced building `DatabaseClientOrderEventRepository`,
  documented in its own Javadoc rather than hidden behind a silently-"working" helper:
  - `count()`'s `UInt64` result is explicitly cast to `UInt32` server-side before decoding: this
    driver decodes ClickHouse `UInt64` as `BigInteger` (verified against client-v2's pinned
    `BinaryStreamReader` source, same as core's other type-coverage findings), and
    `ClickHouseRow.get` only casts the already-decoded value rather than widening it — asking for
    `Long.class` against a raw `UInt64` column throws `ClassCastException`.
  - `status` (`Enum8`) decodes as client-v2's own internal `EnumValue` type, not a plain `String` —
    `Row.get(name, String.class)` throws `ClassCastException` the same way `Long.class` does for
    `UInt64`; reading it means asking for `Object.class` and calling `toString()` on the result
    (confirmed: `EnumValue.toString()` returns the member name, e.g. `"PLACED"`), then parsing that
    into a domain enum. **Worth flagging as a possible future driver improvement** — decoding
    `Enum8`/`Enum16` as a plain `String` directly, rather than leaking an internal client-v2 type
    through the public R2DBC `Row` surface, would be a small, real quality-of-life fix for any
    consumer, not just Spring ones. Not attempted in this pass; noted here so it isn't lost.
- `GET /order-events/analytics/by-category`: a real `GROUP BY category`/`sum(amount)` aggregation
  query, not a per-row lookup — the point of this demo not being "just CRUD." ClickHouse is built
  for exactly this kind of query, and it round-trips through `DatabaseClient` the same way a plain
  `SELECT *` does; `sum(Decimal(18,4))` still decodes as `BigDecimal`, same decode path as any
  other `Decimal`-family type.
- `TransactionManagerDivergenceTest`: proves, with a real test rather than only prose, that wiring
  `R2dbcTransactionManager`/`TransactionalOperator` over this driver fails clearly with
  `UnsupportedOperationException` — `ClickHouseConnection.beginTransaction()` always errors (see
  that class's own Javadoc). The application *does* register a real `ReactiveTransactionManager`
  bean (`R2dbcConfiguration.transactionManager`), and the test autowires it rather than constructing
  one by hand — proving the actually-configured app fails this way, not a scratch instance built
  inside the test. Directly fulfills this phase's "documenting explicitly where ClickHouse's own
  transaction semantics diverge from a typical RDBMS" goal with proof, not just words.
- `OrderEventControllerAgainstRealClickHouseTest`: full round trip through a real embedded WebFlux
  server (`@SpringBootTest(webEnvironment = RANDOM_PORT)` + `WebTestClient`) against a real
  ClickHouse server (Testcontainers, container started eagerly in a `static` initializer rather
  than via `@Container`/`@Testcontainers` — see the test's own Javadoc for why relying on JUnit
  extension ordering between `@Testcontainers` and `@SpringBootTest`'s `SpringExtension` would be
  exactly the kind of fragile-by-accident setup this project avoids elsewhere). Covers: a
  fully-populated event round trip, an event with empty tags/`NULL` discount, the count endpoint,
  and the by-category analytics endpoint.
- Not a substitute for Phase 4's sign-off — this is a demo/consumer-proof, not where "fully
  reactive" gets verified.
- Test support code (`CreateOrderEventRequestTestBuilder` in `api.builders`, `OrderEventAssert` in
  `api.assertions`) follows the same Test Data Builder / Custom Assertion building blocks CLAUDE.md
  defines for the driver's own tests — `CreateOrderEventRequest` was promoted from a record nested
  inside `OrderEventController` to a top-level public record specifically so the builder (living in
  its own sub-package, per CLAUDE.md's "Package layout for test support code") can construct it
  from outside `api`'s package-private types.
- **Not yet confirmed green** — this module touches genuinely new integration surface (Spring's
  R2DBC binding/pool machinery, WebFlux JSON codecs, `Enum8` decoding through `DatabaseClient`) this
  project's own tests never exercised before, so it's realistically more likely than the core
  driver work to need a round or two of fixes once actually compiled/run — already true twice so
  far: (1) a `.map(MethodRef)` call needed to target R2DBC's `Readable` supertype, not `Row`, to
  match `GenericExecuteSpec.map`'s actual overloads; (2) `spring-boot-starter-data-r2dbc` pulls in
  `DataR2dbcAutoConfiguration`, which eagerly resolves a Spring Data R2DBC `Dialect` via
  `DialectResolver` — a *second*, separate hardcoded driver list from the `BindMarkersFactoryResolver`
  one, unrelated to and not fixed by `R2dbcConfiguration`'s own `DatabaseClient` bean (that
  auto-configuration class isn't gated on `@ConditionalOnMissingBean(DatabaseClient.class)`).
  Switched to `spring-boot-starter-r2dbc` (the lean starter, confirmed to exist as a real Boot
  4.1.0 artifact on Maven Central) instead, since this demo never uses
  `R2dbcEntityTemplate`/Spring Data repositories anyway — removes `DataR2dbcAutoConfiguration` from
  the classpath entirely rather than trying to satisfy or bypass its `Dialect` requirement.

## Phase 7 — Operational control & R2DBC correctness (0.2.0)

Starts 2026-08-17. Scope comes from an external review of the `0.1.0` codebase (kept as
`docs/REVIEW_0.2.0_PLAN_SOURCE.md` for the full original text) — each claim below was individually
re-verified against this repo's actual source before being copied in here as a roadmap commitment,
not taken on faith: `ClickHouseResult`'s own Javadoc already documents the `filter()`-derived
consumption-state gap (2.3 below); `ClickHouseRow.get`'s own Javadoc already documents the
cast-only, no-widening-conversion behavior (2.4); `ClickHouseConnection.setStatementTimeout` and
`ClickHouseStatement.add()` both still throw `UnsupportedOperationException` today, confirmed by
reading the source, not the Javadoc's word for it.

**The single most important call in this phase, if only one thing shipped:** expose and test the
real transport admission-control boundary. `io.r2dbc.pool`'s `ConnectionPool` is not this driver's
physical HTTP connection pool — `ClickHouseHttpTransport`'s Reactor Netty `ConnectionProvider` is,
and it currently has no R2DBC-option-level contract at all (see [Connection
pooling](../docs/operations/connection-pooling.md) in the README). A driver that's only "reactive" at the
`Publisher` type level but has an invisible, uncontrolled transport queue underneath is exactly the
failure mode [Why](../README.md#why) names as this project's origin — closing that gap is more
valuable to production users than any new ClickHouse type or a second transport.

Same TDD/black-box/no-Mockito rules as always (CLAUDE.md), plus phase-specific ground rules that
matter more here because this phase is concurrency- and resource-lifecycle-heavy:

- One issue/branch/PR per item below — no unrelated cleanup, renaming, or speculative refactoring
  riding along on a focused PR.
- Before touching production code: identify the existing contract, write a focused failing test for
  the missing behavior, then implement the smallest change that makes it pass.
- Never reach for `block()`/`join()`/`.get()`/`Thread.sleep()`/unbounded buffering/a blocking HTTP
  client to make a test or a fix easier — if that temptation shows up, the design is wrong, not the
  test.
- Any change touching a Netty `ByteBuf`: test cancellation, the error path, and resource release
  explicitly — don't assume GC/finalizers cover it.
- Any change touching concurrency/queueing: make queue/limit ownership explicit in the code (not
  just in a comment), and test saturation and cancellation-while-pending, not just the happy path.
- Run `spotlessCheck`/unit/integration/verification tasks before calling an item done — same bar as
  every other phase.

### Must have (P0)

1. **Expose the Reactor Netty transport pool as R2DBC options.** New options prefixed
   `transport...` (not `pool...`, so they're never confused with `spring.r2dbc.pool.*`, which
   configures the *other* pool layer): `transportMaxConnections`, `transportPendingAcquireMaxCount`,
   `transportPendingAcquireTimeout`, `transportMaxIdleTime`, `transportMaxLifeTime`. Every
   `ClickHouseHttpTransport` construction path routes through one config object/builder instead of
   growing more constructor overloads (see item 8 below — this is also the trigger for the
   config-object refactor, not refactoring for its own sake). Acceptance: available through both
   `ConnectionFactoryOptions` and the R2DBC URL query string; invalid values fail fast at factory
   creation, no silent fallback; a test that proves an acquire is actually rejected once
   `pendingAcquireMaxCount` is exceeded; a `pendingAcquireTimeout` test; a max-active-connections
   test; README's existing "Connection pooling" section extended to document these options
   (structure already there — Reactor Netty defaults table, the "is it worth setting" guidance —
   this item makes the table's "no R2DBC option" gap it already calls out no longer true).
2. **`Connection.setStatementTimeout(Duration)`** backed by ClickHouse's `max_execution_time`
   server setting, distinct from `responseTimeout` (transport-level, HTTP response wait) — a set
   timeout is inherited by statements created from that connection. `Duration.ZERO` gets an
   explicit, documented meaning (no timeout) rather than an accidental one. Acceptance: a real-
   ClickHouse test with a deliberately slow query that actually gets cut off by the server-side
   limit; correct R2DBC exception mapping for the resulting ClickHouse error; docs distinguishing
   statement timeout (query execution limit) from transport `responseTimeout` (network-level).
3. **Shared `Result` consumption state across `filter()`-derived views.** Today `filter()` returns a
   `ClickHouseResult` with its own independent `AtomicBoolean` — consuming both the original and a
   filtered view is a misuse this class doesn't currently catch (already stated in its own Javadoc).
   Fix: extract the consumption guard into its own small type, shared by an original `Result` and
   every `filter()` view derived from it. Acceptance: `result.map(...); result.getRowsUpdated();`
   throws on the second call (already true); `var f = result.filter(...); f.map(...); result.map(...);`
   also throws (currently doesn't); `result.map(...); result.filter(...);` — filtering after
   consumption throws too. All per R2DBC's single-consumption contract, not an invented rule.
4. **Controlled typed conversions for `Row.get(..., Class<T>)`.** Today it's a bare `type.cast(...)`
   — asking for `Long.class` against a decoded `Integer` throws `ClassCastException` instead of
   converting (already stated in `ClickHouseRow`'s own Javadoc). First scope, deliberately not
   "convert anything to anything": a tested numeric matrix (`Byte`/`Short`/`Integer`/`Long`/
   `Float`/`Double`/`BigInteger`/`BigDecimal`, with explicit range checking — no silent overflow —
   and a predictable conversion-failure exception) plus the unambiguous non-numeric cases
   (`String`, `UUID`, `LocalDate`, `LocalDateTime`, `Instant`/`OffsetDateTime` per the existing
   ClickHouse-type-to-Java-type mapping). Lives in its own class (a converter, not scattered
   `instanceof` checks inside `ClickHouseRow`) with a tabular test matrix.
5. **Correctness-first `Statement.add()`.** Still throws `UnsupportedOperationException`. First
   implementation: each `bind(...).add()` snapshots the current binding set; `execute()` runs the
   snapshotted sets sequentially via `Flux.fromIterable(bindingSets).concatMap(...)`, emitting one
   `Result` per set, per the R2DBC contract. `concatMap` deliberately, not a concurrent operator, for
   this first pass: predictable ordering, simple error semantics, no surprise concurrency increase.
   Batched/coalesced multi-row `INSERT` SQL is explicitly deferred — a large insert should keep using
   `insertStreaming` (already the documented fast path), not wait on this.
6. **Netty leak-detection test lane — deferred out of `0.2.0`, completed as follow-up work.** A
   dedicated test task/lane run with an aggressive Netty leak detector, specifically covering
   cancellation, disconnect mid-response, decoder failure, timeout, retry, and downstream
   cancellation after a few records — the shapes most likely to strand a `ByteBuf`. Already named as
   important in [Non-functional
   requirements](#non-functional-requirements-logging-metrics-leaks); this is where it was meant to
   get built. Not implemented for `0.2.0` — see
   [CHANGELOG.md](../CHANGELOG.md#020--2026-08-20-phase-7-operational-control--r2dbc-correctness)'s
   `0.2.0` Deferred section — but finished afterward on the rebased
   `feature/183-leak-detection-rebased` branch (paranoid-level detector, all six target shapes now
   covered: the original pilot's cancellation and reset-mid-response, plus timeout and retry added to
   two already-existing scenario tests, and two new tests for decoder failure — a truncated
   multi-byte value, not simply an empty result — and downstream cancellation after a few records,
   the latter driven through the real production wiring (`RowBinaryDecoder#decode` +
   `RowDecodingScheduler`), not the raw `decodeRows()` entry point).
7. **An R2DBC compatibility/TestKit lane**, using official R2DBC test tooling where it applies, with
   ClickHouse's intentional non-support explicitly documented rather than silently skipped:
   transactions, savepoints, generated keys (where they don't make sense for this model), batch
   semantics scoped to wherever item 5 lands. Goal isn't 100% green at any cost — it's a precise,
   written answer to "where does this driver match the SPI, and where does ClickHouse deliberately
   not have that semantic."
8. **Document the double-pool behavior.** Largely already done — README's "Connection pooling"
   section (mermaid diagram of both layers, Reactor Netty defaults table, "is it worth tuning"
   guidance) predates this phase and already covers this; item 1 above is what removes the one gap
   that section itself calls out (no R2DBC option for the transport pool). Re-check the wording once
   item 1 ships so the "gap" language gets updated to "how to configure it," not left stale.

### Should have (P1)

9. **A neutral driver observability SPI** (`DriverObservationListener`: `queryStarted`/
   `queryCompleted`/`queryFailed`/`queryCancelled`), no hard Micrometer dependency in `core`. Minimum
   data per event: `query_id`, operation kind, connection-acquire wait time, total request time,
   time-to-first-row, row/byte counts, retries, cancellation/timeout flags, active/pending transport
   connections. Never logs full SQL, bind values, or credentials by default — a query
   hash/fingerprint is the safe default for correlating log lines.
10. **Basic lifecycle logging keyed on `query_id`**, using the SPI above.
11. **Explicit ownership of the `RowBinaryDecoder`'s scheduler.** It currently uses
    `Schedulers.boundedElastic()` because the bridge to client-v2's blocking reader interface needs
    somewhere off the event loop to run. The open question worth a real test, not an assumption:
    does every row read actually happen off the Netty event loop, including when downstream requests
    from a different thread? Target shape: a driver-owned, bounded scheduler (configurable worker
    count and queue), shut down together with the resource that owns it — not incidental reliance on
    the globally-shared `boundedElastic`. Add a test that fails if a blocking read runs on a
    `reactor-http-nio-*` thread, plus slow-subscriber, cancel-during-decode, many-parallel-large-
    result, and bounded-memory tests.
12. **A CI compatibility matrix** — split the current single CI lane into a fast PR lane (Java 21,
    Spotless, unit, integration, leak checks) and a heavier nightly/scheduled lane (minimum
    supported ClickHouse version, current stable/LTS, newest supported, an extra JDK if compatibility
    is ever claimed there, JMH smoke/regression, larger concurrency tests). JMH as a full run stays
    out of the PR gate — measurement noise would produce too many false positives as a hard gate.
13. **Release tag + GitHub Release automation**, wired onto the existing publish workflow: after a
    successful Central publish, create the Git tag, the GitHub Release, and release notes from a
    changelog, so the Maven Central version, the tag, and the GitHub Release always point at the same
    commit. (`0.1.0` currently has none of these — the first thing to backfill once this item ships.)

### Could have (P2)

14. Optional `clickhouse-r2dbc-reactive-micrometer` adapter for item 9's SPI.
15. Query fingerprinting for logs/metrics (pairs with item 9/10).
16. Benchmark-driven tuning of the RowBinary response chunk demand value.

### Explicitly out of scope for 0.2.0

Named up front so the phase doesn't drift into a rewrite: native ClickHouse TCP transport, HTTP/2
"because it might be faster" without a profiler-identified bottleneck forcing it, a full fix for
Spring `DatabaseClient.bind()` against ClickHouse's typed `{name:Type}` placeholders (a separate,
optional `clickhouse-r2dbc-reactive-spring` module is the right shape for that, and it's a `0.3.x`
conversation), `Dynamic`/`Variant` types, automatic retry for writes based on server error codes
(needs an explicit `RetrySafety`/idempotency model first — not just "retry more"), transaction
emulation, a large module-boundary refactor, or a driver-owned ORM/query DSL. `0.2.0` is about
production predictability, not surface area.

### PR 0 — bump client-v2 to 0.9.8 first

Do this before PR 4 (statement timeout) and before any Phase 7 work that touches error/retry
handling, ideally before PR 1 so nothing else is built against the stale API. This repo is
currently pinned to `com.clickhouse:client-v2:0.9.0` (see the version catalog); a Dependabot PR
bumping to `0.9.8` has been open, unreviewed, for over a week (PR #6) while it fixes exactly a gap
this codebase's own comments flag:

- `ClickHouseHttpTransport.queryWithSummary`'s Javadoc says outright: *"Our pinned client-v2
  version predates that class's `queryId`/`isRetryable()` fields — re-check this Javadoc if
  client-v2 is ever upgraded."* `0.9.8`'s changelog adds exactly those: `ClickHouseException`
  becomes a shared root for `ServerException`/`ClientException`, and `isRetryable()` is added so a
  caller can tell whether an exception happened in a retryable state.
- HTTP `503 Service Unavailable` responses are now classified as connection-style failures and
  retried by client-v2 by default, instead of being wrapped as a `ServerException`/`ServerRetryable`
  fault — worth re-checking against this driver's own `isError`/`exceptionCode` handling in
  `ClickHouseHttpTransport`, even though we only reuse client-v2's `ServerException` type and
  row-decoders, never its HTTP client or its retry behavior.

Scope: bump the version catalog entry, run the full test suite (this pulls in a newer
`clickhouse-client`/`clickhouse-data` transitively too — check nothing in the RowBinary decode path
regressed), regenerate `gradle/verification-metadata.xml` for the new artifact checksums, then
re-check and update the `queryId`/`isRetryable()` Javadoc note above now that it's stale in the
other direction (the fields exist now — decide whether folding `isRetryable()` into
`ClickHouseR2dbcException.wrap` is worth a follow-up issue, without scope-creeping this PR into
item 4.3's retry-safety work, which stays explicitly out of 0.2.0). Also merge or close the other
five open Dependabot PRs (`org.sonarqube`, `actions/upload-artifact`, `actions/checkout`,
`actions/setup-java`, `reactor-netty-http`, `reactor`) while in here — they're all green (CI
checkmarks) and just sitting unreviewed, not something to solve mid-Phase-7 later.

### PR sequence

One issue/branch/PR per item, in this order (each is independently mergeable; later ones don't
block on earlier ones except where noted):

| PR | Scope | Depends on |
| --- | --- | --- |
| 0 | Bump client-v2 to 0.9.8 + merge outstanding Dependabot PRs | — |
| 1 | Result consumption correctness (item 3) | — |
| 2 | Row conversions (item 4) | — |
| 3 | `Statement.add()` (item 5) | — |
| 4 | Statement timeout (item 2) | PR 0 (needs `isRetryable`/exception hierarchy from client-v2 0.9.8 for clean exception mapping) |
| 5 | Transport pool options (item 1) — includes the config-object refactor for `ClickHouseHttpTransport` | — |
| 6 | Decoder scheduler contract (item 11) | — |
| 7 | Observability SPI (item 9/10) | benefits from PR 5's pool metrics being available, not blocked by it |
| 8 | R2DBC compatibility lane + CI matrix (items 7, 12) | — |
| 9 | Release/documentation sync (item 13, README/ROADMAP/CHANGELOG) | all of the above, since it documents what shipped |

### Definition of done for 0.2.0

- [x] Users control the physical transport pool without writing their own Java constructor call —
      PR 5, the `transport...` `ConnectionFactoryOptions`/URL query options.
- [x] The transport's pending-acquire queue is bounded and has a timeout, both configurable — PR 5,
      `transportPendingAcquireMaxCount`/`transportPendingAcquireTimeout`.
- [x] Statement timeout works against a real ClickHouse server — PR 4,
      `Connection.setStatementTimeout` backed by `max_execution_time`.
- [x] `Result` has unambiguous single-consumption semantics, including across `filter()` views —
      PR 1, the shared consumption guard.
- [x] Typed `Row.get` has controlled, tested conversions for the P0 type matrix — PR 2.
- [x] `Statement.add()` works correctly (sequential, one `Result` per binding set) — PR 3.
- [x] Cancellation/timeout/error paths leave no `ByteBuf` leaks (leak-detector lane passes) — deferred
      out of `0.2.0` itself (not implemented for that release, see item 6's own entry above and
      [CHANGELOG.md](../CHANGELOG.md#020--2026-08-20-phase-7-operational-control--r2dbc-correctness)'s
      Deferred section), completed afterward: all six target shapes covered on the rebased
      `feature/183-leak-detection-rebased` branch, pending a local `./gradlew spotlessCheck clean
      build` confirmation before merge.
- [x] A test actively protects the Netty event loop from a blocking decode call — PR 6, the
      driver-owned `RowDecodingScheduler` plus its ownership/threading tests.
- [x] It's written down which R2DBC compatibility cases are supported vs. deliberately unsupported —
      PR 8, `docs/R2DBC_COMPATIBILITY.md` + `ClickHouseR2dbcSpiCompatibilityTest`.
- [x] README documents the outer R2DBC pool and inner transport pool as one coherent story —
      re-verified after PR 5; the "no R2DBC option" gap the section used to call out is closed.
- [ ] The release has a changelog entry, a Git tag, and a GitHub Release pointing at the same commit
      as the Maven Central artifact — `CHANGELOG.md`'s `[0.2.0]` section is now finalized and dated
      on `main`; the `workflow_dispatch` run against `release.yml` (tag + GitHub Release + Maven
      Central publish) hasn't happened yet. This box checks once that run completes end to end.
- [x] A benchmark baseline is recorded (docs/PERFORMANCE.md) — re-confirmed for Phase 7: full-scan
      regression found, root-caused, and mostly addressed (chunk coalescing); 10k/100k results are
      solid, 1M rows is an open, honestly-documented measurement question rather than a blocker (see
      [docs/performance/results.md](../docs/performance/results.md#why-the-1m-number-wont-sit-still)) — not a flaky PR
      gate either way.

## Phase 8 — Post-0.2.0 hardening (0.2.1+)

Starts 2026-08-21. Scope comes from a fresh, independent external review of the repository done
after `0.2.0` published and after the follow-up factory-lifecycle/leak-detection/connection-reuse
work — kept in full as
[REVIEW_POST_0.2.0_FRESH_REPO_SOURCE.md](REVIEW_POST_0.2.0_FRESH_REPO_SOURCE.md), same
pattern as Phase 7's `docs/REVIEW_0.2.0_PLAN_SOURCE.md`. The review explicitly credits everything
Phase 7 already shipped and told Claude not to re-do it — see that file's section 1 — so this phase
starts from what's actually still open, not a re-litigation of `0.2.0`.

Four items below were independently re-verified against current `main` before being copied in here
as roadmap commitments (not taken on faith, same discipline as Phase 7):

- `Enum8`/`Enum16` still decode as client-v2's internal `EnumValue`, confirmed in
  `RealWorldTableAgainstRealClickHouseTest`'s own Javadoc (`shouldDecodeEnumTypes`).
- The demo's `DatabaseClientOrderEventRepository.count()` workaround
  (`SELECT toUInt32(count()) AS total`) is stale: `ClickHouseValueConverter`'s numeric matrix
  (confirmed in source) already converts `BigInteger` ↔ `Long` exactly, range-checked — Phase 7 item
  4 fixed the underlying gap this workaround was written around, but the demo was never updated
  afterward.
- `ClickHouseQuery.parameterNamesIn`'s own Javadoc admits the regex-based placeholder scan
  (`PARAMETER_PLACEHOLDER`) cannot distinguish a real `{name:Type}` parameter from the same text
  inside a SQL string literal or comment.
- `ClickHouseConnectionFactoryProvider` confirmed to expose `sslRootCert`/`retryMaxAttempts`/
  `retryDelay`/`transport...`/`observationListener` as R2DBC options, but no `responseTimeout`
  option — `TransportOptions`/`ClickHouseHttpTransport` support a response timeout, but it's only
  reachable by constructing the transport directly, not through `ConnectionFactories.get(...)` or
  Spring's `spring.r2dbc.url`.

The remaining items below were not independently re-run against a real server or re-read line by
line this pass — treat them as a starting hypothesis to confirm with a real test before fixing, per
CLAUDE.md's TDD rule, same as every other phase.

**Performance/benchmark items from the review are deliberately excluded from the P0–P2 lists below**
and tracked separately (see "Deferred — performance/benchmark work"), per the user's standing
direction: finish the non-performance work first; performance/benchmark work waits for a proper
(cloud) benchmark environment. This includes the review's own single-most-emphasized finding
(benchmark response-compression parity) — real finding, but a benchmarking-methodology question that
needs JMH re-runs, not a production-code defect blocking anything else in this phase.

### Should have (P1) — correctness / production-readiness gaps

1. **Done in driver source (2026-08-21). Normalize `Enum8`/`Enum16` to a stable public `String` at
   the driver boundary**, instead of leaking client-v2's internal `EnumValue` through `Row`. See
   "`Enum8`/`Enum16` resolved" under [Phase 2](#phase-2--core-protocol--testkit-contract-tests) for
   the implementation (`core.rowbinary.ListDecodingRowBinaryReader`) and test coverage
   (`RealWorldTableAgainstRealClickHouseTest.shouldDecodeEnumTypes`,
   `ClickHouseRowAssert.hasEnumName` now asserting the runtime type, not just `toString()`). **The
   demo still uses the `Object.class`/`.toString()` workaround, on purpose** — it depends on the last
   published Maven Central release, which doesn't contain this fix yet; that same section documents
   catching this the hard way (a green core unit test, a red demo integration test, both correct for
   what each module actually runs). Removing the workaround from the demo is a separate step, once a
   release containing this fix is published and the demo's dependency is bumped.
2. **Done (2026-08-21). Removed the demo's stale `toUInt32(count())` workaround.**
   `DatabaseClientOrderEventRepository.count()` now issues a plain `SELECT count() AS total` and
   reads it as `Row.get("total", Long.class)` directly — unlike item 1 above, this one *is* safe for
   the demo to use immediately: the underlying fix (`ClickHouseValueConverter`'s numeric conversion
   matrix, `BigInteger` ↔ `Long` widening/narrowing) shipped in `0.2.0`, already published, which is
   exactly what the demo depends on — unlike item 1's `Enum8`/`Enum16` normalization, which is only
   in unreleased driver source. Javadoc and README updated to match; pending the user's real-
   ClickHouse build run to confirm.
3. **Replace the regex-only parameter-placeholder scan with a small SQL-literal/comment-aware
   scanner.** Doesn't need to become a full SQL parser — just skip single-quoted string literals and
   line/block comments while looking for `{name:Type}`. Add failing tests first:
   `SELECT '{not_a_parameter:String}'` and `SELECT 1 -- {not_a_parameter:UInt32}` should not be
   treated as bind parameters today (confirmed) and must not be after the fix either — the fix is
   about literals/comments the scan currently gets wrong, not about changing today's understood
   behavior for genuine placeholders.

   **Done in driver source (2026-08-21), not yet released.** New package-private
   `ClickHouseSqlPlaceholderScanner` (single left-to-right character pass, tracks quote/comment
   context directly) replaces the `Pattern`/`Matcher` scan inside `ClickHouseQuery.parameterNamesIn`.
   Verifying against clickhouse.com/docs/sql-reference/syntax's "Comments" section before implementing
   (rather than assuming `--`/`/* */` was the whole story) surfaced two things the original triage
   above didn't anticipate: ClickHouse also accepts `#!`, `# ` (hash-space), and `//` as line-comment
   starters, and — the one that actually mattered for correctness, not just completeness — ClickHouse
   block comments **nest** (`/* outer /* inner */ still commented */`), unlike standard SQL's. Both
   are now handled; 30 tests in the new `ClickHouseSqlPlaceholderScannerTest` cover every
   context-skipping rule, the backslash/doubled-character escaping rules within quoted spans
   (verified against the same docs page), malformed placeholder shapes that must not match, and
   boundary/unterminated-input cases. Performance was checked, not just claimed: both the old regex
   and the new scanner are O(sql length) with no pathological worst case, and a throwaway (non-JMH,
   per this phase's standing benchmark-work deferral) timing sanity check found the two within noise
   of each other on a typical short query and the scanner ~8% faster on a large SQL text with 200
   placeholders — not a meaningful win either way, which is why the scanner's own Javadoc is explicit
   that correctness, not speed, is the reason it exists. See `ClickHouseSqlPlaceholderScanner`'s
   Javadoc for the full write-up.
4. **Add an R2DBC-configurable `responseTimeout` option**, e.g. `responseTimeout=PT30S`, distinct
   from `connectTimeout` (TCP connect), `statementTimeout`/`max_execution_time` (server-side query
   execution limit), and `transportPendingAcquireTimeout` (pool queue wait) — document the four
   clearly against each other, since they're easy to conflate.

   **Done (2026-08-21).** `TransportOptions`/`ClickHouseHttpTransport` already carried
   `responseTimeout` end to end down to Reactor Netty's `HttpClient.responseTimeout(...)`, and its
   real timeout-firing behavior was already proven at the transport level
   (`ClickHouseHttpTransportTest.shouldFailWithinAnExplicitlyConfiguredTimeoutWhenTheServerNeverResponds`,
   hermetic, against a controlled server that never responds) — what was actually missing was the
   R2DBC-facing option. Added `ClickHouseConnectionFactoryProvider.RESPONSE_TIMEOUT`
   (`responseTimeout`), wired through `ClickHouseConnectionFactory.from` the same way as every other
   custom `Duration` option (typed value or ISO-8601 string from a URL query string). Its Javadoc is
   the actual "document the four clearly" write-up this item asked for, cross-linked from
   `TRANSPORT_PENDING_ACQUIRE_TIMEOUT` and `ClickHouseConnection.setStatementTimeout`; README's
   connection-options table corrected too — it had `connectTimeout`'s row describing
   `responseTimeout`'s behavior, exactly the kind of conflation this item called out. Tests added to
   `ClickHouseConnectionFactoryTest` mirroring the existing `connectTimeout`/transport-pool-option
   coverage: typed-option acceptance, URL-query-string parsing, and rejection of an unparseable
   duration string.
5. **Characterize mid-stream ClickHouse failure semantics with a named real-server test**: some rows
   already received, then the query fails server-side, then the response stream terminates. No
   existing test proves what this driver actually does today. Required behavior, stated explicitly
   rather than left to fall out of whatever happens to occur: never silently complete as if the
   result were whole; surface a useful `R2dbcException`/transport error; preserve whatever rows were
   already emitted per Reactive Streams semantics (no retroactive un-emitting). Also worth confirming
   whether `wait_end_of_query=1` is already reachable through the generic settings API as a documented
   opt-in correctness-vs-streaming tradeoff.

   **Done (2026-08-21) — confirmed against a real ClickHouse server, and the finding is worse than
   the pre-run analysis predicted.** New `connector`-module
   `MidStreamQueryFailureAgainstRealClickHouseTest`, two tests.
   `shouldFailRatherThanSilentlyCompletingWhenTheFailureHappensAfterStreamingHasStarted` forces a
   `DB::Exception` partway through a `system.numbers` scan via `throwIf(...)`, with `max_block_size`
   set small enough that many blocks are flushed to the client before the failing one, under this
   driver's default (non-`wait_end_of_query`) settings. The real run showed the response's trailing
   error text gets misdecoded by client-v2's RowBinary reader as further `UInt64` values —
   large/garbage numbers, structurally indistinguishable from genuine row data — handed to the
   subscriber as if they were real rows, before decoding eventually fails on a later unparseable byte
   and the stream terminates with an error. So the test only asserts what's actually guaranteed:
   never silently completing as if the result were whole, and a fixed-size genuine prefix (the first
   1000 rows) arriving intact and in order — not "every emitted row is genuine", which the first
   version of this test wrongly assumed and a real run promptly disproved.
   `shouldSurfaceACleanServerExceptionWhenWaitEndOfQueryIsEnabled` answers the `wait_end_of_query`
   question directly: yes, already reachable today, zero code changes — `ClickHouseQuery.of(sql)
   .withSettings(Map.of("wait_end_of_query", "1"))` is enough, since `withSettings` already passes
   arbitrary `<name>=<value>` request parameters through untouched (see its Javadoc) — and this real
   run confirmed it surfaces a clean `ServerException`, with none of the garbage-row risk above.

   **Why this shape of corruption happens, verified against ClickHouse's own HTTP interface behavior:**
   HTTP requires headers before body. Once `ClickHouseHttpTransport` has already received response
   headers with no `X-ClickHouse-Exception-Code` (true here — the failure happens after streaming has
   already started) and rows have started arriving, ClickHouse has no HTTP-legal way to retroactively
   attach that header for a later failure. Per ClickHouse's own documented behavior
   (github.com/ClickHouse/ClickHouse/pull/36884, and github.com/ClickHouse/ClickHouse/issues/16207/#33630
   it closed), it instead appends the error as plain text directly into the response body, after
   whatever rows were already written — independent of output format, so this corrupts the RowBinary
   byte stream this transport is decoding rather than cleanly signalling failure at the transport
   level. `ClickHouseHttpTransport.isError`/`receiveOrFail` only inspect the *initial* response
   headers (see that class's own Javadoc on error handling), so this failure shape slips past that
   check entirely and surfaces instead as whatever exception client-v2's RowBinary reader eventually
   throws — but only after silently emitting the misdecoded garbage rows in between, which is the part
   worth calling out loudly rather than burying in a test comment. Written up caller-facing in
   README's [Known limitations](../docs/reference/known-limitations.md) section, since this is a real
   data-integrity risk a caller needs to know about, not just an internal implementation detail.
   `wait_end_of_query=1` sidesteps the whole problem by having ClickHouse buffer the entire response
   server-side (up to its own `http_response_buffer_size`) before sending anything, so a failure is
   always known before headers go out — the second test's `500`-row failure point over a `10000`-row
   `LIMIT` is deliberately tiny to stay well under that buffer regardless of its exact default.
6. **Build a real-server parameter-binding type matrix beyond null/integer/string-escaping.**

   **Done (2026-08-21) — confirmed against a real ClickHouse server**, same honest pattern as item 5
   above: the encoder changes below are reasoned from ClickHouse's own documented parameter literal
   formats (cited inline in `ClickHouseQuery.withParameters`'s Javadoc), then verified first with
   hermetic unit tests in `core`'s `ClickHouseQueryTest` (string-level, verifiable without a server)
   and then end to end against a real server in the new `connector`-module
   `ParameterBindingTypeMatrixAgainstRealClickHouseTest` (bind → real table column → decode → assert
   equal to what was bound). One real finding surfaced only by the actual run, not predicted upfront:
   the numeric-array round trip initially asserted `List<Integer>` back for a bound `Array(UInt32)`
   column and failed — the decoded elements were `Long`, not `Integer`. Not a driver bug: `Array(T)`
   decodes each element through the identical per-`ClickHouseDataType` reader a scalar column of type
   `T` uses (see `ListDecodingRowBinaryReader`'s Javadoc), and `UInt32` already decodes as `Long` for
   scalar columns too (confirmed in `RealWorldTableAgainstRealClickHouseTest#shouldDecodeNumericTypes`,
   `.hasValue("uint32_val", 4000000000L)`) — the array case was just never round-tripped against a
   real server before. Fixed by correcting the test's expected values, and documented the underlying
   rule (`Array(T)` element type always mirrors scalar `T`'s decode type) in
   `ClickHouseValueConverter`'s Javadoc, since that class's existing numeric conversion matrix
   deliberately does not extend to `List` elements — a caller asking for `List.class` back always gets
   exactly the element types the decoder produced, with no widening/narrowing, so this was worth
   spelling out rather than leaving as an implicit, easy-to-assume-wrong detail.

   `UUID`, `BigDecimal`, `LocalDate`, `Boolean`, and any `enum` constant needed **no encoder change**
   — Java's own `toString()` already matches ClickHouse's literal format for each (`LocalDate`'s
   ISO-8601 `yyyy-MM-dd` matches `Date` exactly; `Boolean`'s lowercase `true`/`false` matches `Bool`'s
   documented accepted text, confirmed via a web search of ClickHouse's own Bool-type docs, not
   assumed).

   `LocalDateTime`/`Instant`/`OffsetDateTime`/`ZonedDateTime` **did** need a real fix: Java's
   `toString()` uses an ISO `T` separator (`2024-01-15T10:30:15`), but ClickHouse's own "Queries with
   Parameters" documentation gives a space-separated form (`2024-01-15 10:30:15`) as the `DateTime`
   literal — confirmed via web search of that documentation before writing the encoder, not assumed.
   `Instant`/`OffsetDateTime`/`ZonedDateTime` are normalized to UTC first, since a `DateTime`
   parameter carries no timezone of its own; sub-second precision is deliberately truncated to whole
   seconds (a caller needing `DateTime64` fractional precision should bind an already-formatted
   `String` instead — this driver can't know a placeholder's declared type from the Java value alone).

   `List` is now encoded as a ClickHouse `Array` literal (`[e1,e2,...]`), with `String` elements
   single-quoted and escaped — the one case that needed genuinely new logic beyond a format-string
   fix, since the top-level `String` encoding (no quotes) isn't valid inside an array literal (quotes
   are required there). Nested arrays (`List` of `List`) fail fast with
   `UnsupportedOperationException` rather than silently producing a wrong literal.

   `IPv4`/`IPv6` needed no change either — already reachable today by binding a plain `String`
   through the existing escaped-string path, proven end to end in the real-server test.

   **Deliberately still out of scope, not silently skipped:** `Map` and `Tuple` bound-parameter
   values — neither has an unambiguous single Java type to bind the way `List` does for `Array`, and
   ClickHouse's own parameter support for composite literals is less settled than the scalar/array
   cases covered here. Left as an explicit follow-up, documented in the new test class's own Javadoc.
7. **Design an opt-in, server-error-code-aware retry mode**, on top of the existing safe
   pre-send-only default (`RetryPolicy`/`RETRY_MAX_ATTEMPTS`/`RETRY_DELAY` stay the default — this is
   additive, not a replacement). client-v2 0.9.8's `ServerException.isRetryable()` (available now,
   wasn't at our old `v0.9.0` pin) can identify ClickHouse server error codes considered retryable;
   this driver deliberately doesn't act on it yet.

   **Design + `core`/`transport-http` implementation done (2026-08-21), pending the user's local
   build for compile/test verification (no `javac` in this sandbox — see the honesty pattern used
   throughout this Phase 8 section). `connector`-level exposure through the R2DBC `Statement` API is
   an explicit, tracked follow-up, not yet done — see "Not yet done" at the end of this item.**

   *The core problem this design has to solve.* `ClickHouseHttpTransport.queryWithSummary` is the one
   method that carries both `SELECT`s and literal-SQL `INSERT`s (`INSERT INTO t VALUES (...)`, as
   opposed to the separate streaming-body `insertWithSummary` path, which already never retries and
   stays that way — see that method's own Javadoc, untouched by this design). The transport has no
   reliable way to tell which kind of statement `query.sql()` is: it is opaque SQL text, and sniffing
   it (checking for a leading `SELECT` keyword, say) is exactly the kind of fragile heuristic this
   codebase avoids elsewhere (CTEs like `WITH x AS (...) SELECT`, multi-statement text, a `SELECT`
   nested inside an `INSERT ... SELECT`, etc. would all defeat a naive check). A blanket
   connection-level "retry on retryable server error" flag would therefore be unsafe: a single
   `ClickHouseConnection` is routinely used for both reads and writes, so a connection-scoped flag
   would silently start retrying `INSERT`s too, risking duplicated writes on exactly the class of
   transient server error this feature is meant to smooth over.

   *Rejected shape: connection/transport-level blanket flag.* Simplest to implement, but unsafe for
   the mixed read/write connection case above — rejected for that reason, not effort.

   *Rejected shape: SQL-text sniffing.* Fragile per the CTE/multi-statement/nested-`SELECT` cases
   above; a false "this is a read" classification is a silent data-duplication bug, not a loud one —
   rejected.

   *Chosen shape: per-query opt-in, since only the caller reliably knows whether a given query is
   read-only.* Add a boolean to `ClickHouseQuery` (default `false`, so existing callers are
   unaffected), e.g. `withServerErrorRetryEnabled()` alongside the existing `withParameters`/
   `withSettings` builder-style methods — a caller building a `SELECT`-only query opts it in
   explicitly; a caller building an `INSERT` simply never calls it. This deliberately puts the
   decision where the domain knowledge actually lives (the caller wrote the SQL and knows its shape)
   rather than trying to infer it, the same "explicit over inferred" preference `ClickHouseQuery`
   already applies to `parameters()`/`settings()`. `RetryPolicy` itself needs **no shape change** —
   confirming, more precisely than the note already in its Javadoc anticipated, that operation-scoped
   growth happens on the query, not the connection-level policy record.

   *Retry condition, once opted in* (all four must hold, not just the flag):
   1. `query.serverErrorRetryEnabled()` is `true` (the opt-in above).
   2. The failure is a `ServerException` (not a connection-level failure — those already retry via
      the existing pre-send path, or don't, based on `requestSent`).
   3. `ServerException.isRetryable()` is `true` (ClickHouse's own classification, not a guess about
      which error codes are "probably fine").
   4. **Zero response bytes/rows have been emitted downstream yet** — the same kind of signal
      `requestSent` already provides for the pre-send case, but observed at the body-emission end
      instead of the request-send end (e.g. an `AtomicBoolean` flipped in a `doOnNext` on the raw
      `ByteBuf` flux, checked before allowing a retry). This is the guard that makes retrying a
      `SELECT` safe even after the request was fully sent and ClickHouse started processing it: if no
      bytes reached the subscriber yet, a retry re-executing the query from scratch cannot produce
      duplicate or out-of-order rows for the caller, the same reasoning `MidStreamQueryFailureAgainstRealClickHouseTest` (item 5 above) already established the *opposite* case for — once bytes
      have started flowing, this transport does not retry, full stop, `serverErrorRetryEnabled()` or
      not.
   5. `retryPolicy.isEnabled()` still gates this the same as the existing pre-send retry — this is an
      additional condition under which a retry is attempted within the existing policy's
      `maxAttempts`/`delay` budget, not a second independent retry budget.

   *Required test coverage before this ships* (real-server, since `ServerException.isRetryable()`'s
   actual classification per error code is ClickHouse's own behavior, not something to assume):
   retryable server error surfacing before any rows were emitted, with the flag enabled → retried,
   eventually succeeds; the same retryable error with the flag left at its default `false` → **not**
   retried, surfaces immediately, proving the opt-in actually gates the behavior rather than the mere
   presence of `isRetryable()` support; a non-retryable server error, flag enabled → never retried
   regardless; a retryable-looking server error arriving only after some rows were already streamed to
   the caller → **not** retried (guard 4 above), partial rows preserved and the error still surfaces,
   mirroring `MidStreamQueryFailureAgainstRealClickHouseTest`'s `startsWith`-prefix assertion pattern;
   `insertWithSummary`'s streaming-body path is completely unaffected by this flag regardless of its
   value — an explicit test for this, since it is exactly the kind of thing a later "let's unify the
   two retry paths" refactor could silently break; cancellation during a would-be-retryable window
   still never triggers a retry (mirrors the existing pre-send `Retry.fixedDelay(...).filter(...)`
   behavior, which is cancellation-transparent already — confirm it stays that way once this second
   condition is added to the filter).

   *Implementation status.* `core`'s `ClickHouseQuery.withServerErrorRetryEnabled()` and
   `transport-http`'s `ClickHouseHttpTransport.canRetry`/`queryWithSummary` wiring are both written
   and hermetically tested — `ClickHouseQueryTest` for the flag itself, `ClickHouseHttpTransportTest`
   both with direct unit tests of `canRetry`'s branches (using real `ServerException` instances
   constructed with ClickHouse's own documented retryable/non-retryable codes, `202`
   `TOO_MANY_SIMULTANEOUS_QUERIES` and `60` `TABLE_NOT_FOUND`, checked against client-v2 0.9.8's own
   `ServerException.discoverIsRetryable` source rather than assumed) and with
   `ControlledClickHouseServer`-backed tests proving the actual `retryWhen` wiring engages correctly.
   Pending the user's local build for compile/test verification (this sandbox has no `javac`).

   *Not yet done — explicit follow-up, not silently skipped:* none of this is reachable by an actual
   R2DBC caller yet. `ClickHouseStatement` (the `connector` module's `io.r2dbc.spi.Statement`
   implementation) always builds its `ClickHouseQuery` via the plain `ClickHouseQuery.of(sql)` path
   in `executeOneBindingSet` and never calls `withServerErrorRetryEnabled()` — there is currently no
   way for a caller going through `Connection#createStatement` to reach this opt-in at all. Exposing
   it requires its own deliberate design (most likely a small public vendor-extension interface a
   caller can cast `io.r2dbc.spi.Statement` to, the pattern other R2DBC drivers use for
   driver-specific capabilities not covered by the SPI — `ClickHouseStatement` itself is currently
   package-private with no public surface at all), which is a separate decision from the retry
   semantics themselves and deliberately not rushed alongside them.
8. **Evaluate a POST-body path for long analytical `SELECT`s**, currently sent entirely via
   `?query=<url-encoded-sql>`. Add a real integration/proxy-boundary test with deliberately long SQL
   first (large CTEs/JOIN graphs/many parameters — real request-line/URI limits some
   proxies/load-balancers impose) to prove there's an actual problem before redesigning the request
   shape. If a body-based path is built, treat the existing pre-send-only retry-safety reasoning as
   invalidated for that path until re-proven — the `requestSent`/`doAfterRequest` signal currently
   means "zero bytes reached the server," which stops being straightforwardly true once a body is
   being streamed (see `insertWithSummary`'s own "never retried" reasoning in [Production readiness
   review](#production-readiness-review) for why that distinction already matters elsewhere in this
   codebase).

   **Problem confirmed (2026-08-22), redesign not yet started.** Researched first, not assumed:
   ClickHouse's own HTTP server is generous (`http_max_uri_size` defaults to 1 MiB —
   clickhouse.com/docs/interfaces/http), but a real deployment commonly sits behind a reverse
   proxy/load balancer with a far smaller request-line limit — Netty's own `HttpRequestDecoder`,
   which both Reactor Netty's `HttpServer` and most Netty-based proxies use, defaults
   `maxInitialLineLength` to 4096 bytes (confirmed against Reactor Netty's own
   `HttpRequestDecoderSpec.DEFAULT_MAX_INITIAL_LINE_LENGTH`). New hermetic test
   `ClickHouseHttpTransportLongQueryUriTest` (transport-http module) stands in for that whole class
   of intermediary with a plain Reactor Netty server explicitly left at that exact default (no Docker
   in this sandbox to run an actual nginx/ALB instance, but the underlying mechanism — an
   HTTP/1.1-compliant request-line length limit — is the same either way) and sends SQL long enough
   to comfortably exceed it through the existing `query()` path. Written to prove the driver fails
   loudly (a connection-level error) rather than silently truncating the SQL or hanging — pending the
   user's local build to confirm which specific exception surfaces, same honest
   written-but-unverified pattern used throughout this Phase 8 section (no `javac` in this sandbox).

   **Not yet done:** deciding whether to actually build a POST-body path is a separate follow-up,
   deliberately not rushed alongside confirming the problem exists — a body-based path invalidates
   the pre-send-only retry-safety reasoning (per this item's own text above) and would need its own
   careful design, mirroring how item 7's retry-mode design was kept a distinct step from its
   implementation.
9. **Fix the demo's false HTTP-streaming claim.** `OrderEventController#all()`'s Javadoc claims the
   response is written as it arrives, but with no streaming media type, Spring WebFlux/Jackson
   collects an ordinary `Flux<OrderEvent>` into one JSON array before writing it — the R2DBC/database
   side may stream correctly while the HTTP layer buffers. Add `GET /order-events/stream` producing
   `application/x-ndjson` (or SSE), and prove with a real assertion that the first HTTP element
   arrives before the source completes — not `expectBodyList(...)`, which waits for the whole
   response and proves nothing about streaming. This matters more than most items here: end-to-end
   streaming is a core reason this driver exists, and the demo currently doesn't actually demonstrate
   it at the HTTP boundary.

   **Done (2026-08-22), pending the user's local build for compile/test verification.**
   `OrderEventController#all()`'s Javadoc was rewritten to stop claiming the HTTP response streams
   (it doesn't — Spring WebFlux's default `application/json` writer buffers the whole `Flux` first)
   and now points to a new `GET /order-events/stream` endpoint
   (`produces = MediaType.APPLICATION_NDJSON_VALUE`) for callers who actually need HTTP-layer
   streaming. The new endpoint is proven, not just asserted: `OrderEventStreamingControllerTest`
   binds the controller directly (`WebTestClient.bindToController`, no `ApplicationContext`, no real
   ClickHouse — a real query's own timing is fast and unpredictable, so it can't reliably prove
   incremental delivery either way) to a new `InMemoryOrderEventRepository` test fake whose
   `findAll()` stream the test fully controls via `Flux#delayElements`, then asserts via
   `StepVerifier#expectNoEvent(Duration)` that the second of two events does not arrive within the
   whole delay window after the first one does — a buffered response would deliver both together,
   immediately, once the source completed, and would fail this assertion. A second, real-ClickHouse
   test (`OrderEventControllerAgainstRealClickHouseTest#shouldServeTheSameEventsThroughTheNdjsonStreamingEndpoint`)
   only confirms the endpoint serves the same data with the right content type; it deliberately does
   not attempt to prove streaming timing against a real query. New test-support files:
   `api.fakes.InMemoryOrderEventRepository` and `api.builders.OrderEventTestBuilder` (a Test Data
   Builder for the domain `OrderEvent` record — no such builder existed before this).
10. **Prove Spring shutdown actually disposes factory-owned resources.** `0.2.1`'s new
    `ClickHouseConnectionFactory.dispose()`/`isDisposed()` (see [Connection
    pooling](../docs/operations/connection-pooling.md)'s "Shutting it down" note) is not called automatically by
    anything — `io.r2dbc.pool`'s `ConnectionPool.dispose()` only tears down pooled `Connection`
    handles, not the factory underneath. Add a demo-level shutdown test proving that when the Spring
    context closes, the underlying transport/decoder-scheduler are actually disposed too, not just
    the outer pool — currently nothing in the demo calls `factory.dispose()` at all.

    **Attempted (2026-08-22), reverted after the user's real build run — genuinely blocked, not just
    unverified.** Confirmed the exact gap directly against `io.r2dbc.pool`'s own `ConnectionPool`
    source (fetched from GitHub, not assumed): `ConnectionPool` does implement `java.io.Closeable`,
    so Spring's default `@Bean` destroy-method inference already calls `close()`/`disposeLater()` on
    it automatically — but `disposeLater()` only tears down the pool's own
    `InstrumentedPool<Connection>`, never the delegate `ConnectionFactory` it wraps, so the driver's
    own transport connection pool and decoder scheduler thread pool leak silently on every context
    shutdown regardless. That part of the analysis holds.

    The attempted fix — splitting the driver's raw `ConnectionFactory` into its own
    `@Bean(destroyMethod = "dispose")` in `R2dbcConfiguration`, string-named so the demo still never
    imports the driver's type — does not actually work today, and the user's real build proved it:
    `BeanDefinitionValidationException: Could not find a destroy method named 'dispose' on bean with
    name 'baseConnectionFactory'`, thrown eagerly at bean-creation time (Spring's explicit,
    non-inferred `destroyMethod` enforces the method exists; it does **not** just warn at shutdown as
    an earlier version of this note incorrectly claimed). Root cause, confirmed by re-reading
    `build.gradle.kts`: the demo's `runtimeOnly` dependency deliberately pins the **published**
    `io.github.camilyed:clickhouse-r2dbc-reactive-connector:0.2.0` — and `ClickHouseConnectionFactory
    .dispose()` was added afterward, as part of the still-unreleased `0.2.1` work this whole
    changelog section is under. The demo's classpath at test/runtime genuinely does not contain a
    `dispose()` method to call; there is no reflection trick or defensive coding that fixes this,
    since the method does not exist in the jar the demo actually depends on. This is the same
    published-release-vs-current-`main` gap item 11 below already names — this item was accidentally
    attempted before that one, not after.

    **Done (2026-08-22), re-applied after `0.2.1` was published to Maven Central.** Once `0.2.1`
    was live and `build.gradle.kts`'s `runtimeOnly` bump landed
    (`clickhouse-r2dbc-reactive-connector:0.2.1`), the exact fix described above became applicable
    as written: `R2dbcConfiguration` now declares `baseConnectionFactory()` as its own
    `@Bean(destroyMethod = "dispose")`, and `connectionFactory(ConnectionFactory
    baseConnectionFactory)` takes it as a method parameter rather than a local variable — which also
    means Spring destroys the pool bean before this one, since Spring destroys a bean's dependents
    before the bean itself. `ConnectionFactoryShutdownDisposalAgainstRealClickHouseTest` (new, real
    ClickHouse via Testcontainers) proves it end to end: run a trivial query through the raw
    `baseConnectionFactory` bean, close the Spring context, then assert the same query now fails —
    proving the driver's own transport pool was actually torn down, not just the outer
    `io.r2dbc.pool` wrapper.

    **The underlying fix (the two-bean split + `destroyMethod = "dispose"`) is confirmed correct
    against a real build.** The user's first real `./gradlew` run against this test showed the test
    method body itself completed cleanly — the query-through-raw-factory assertion before close,
    and the query-fails-after-close assertion after it, both passed with no assertion failure
    reported — so `context.close()` genuinely and synchronously disposed the driver's
    `ConnectionFactory` in time for the very next query attempt to observably fail, not a race.
    What actually failed was a **test-harness interaction**, not the fix: the failure's entire
    stack trace was inside Spring's own `EventPublishingTestExecutionListener`/`DefaultContextCache`
    machinery (`IllegalStateException: LifecycleProcessor not initialized`), thrown after the test
    method returned, while the framework tried to publish an `AfterTestExecutionEvent` against the
    `@SpringBootTest`-managed, cached context this test had already manually `.close()`-d.
    `AbstractApplicationContext.restart()` (Spring Framework 7/Boot 4.1's newer context-cache
    restart support) isn't the same operation as a fresh `refresh()` and can't resurrect a context
    whose `close()` already tore down its `LifecycleProcessor`.

    Fixed by no longer using `@SpringBootTest`'s shared, cached context for this specific test: it
    now builds its own standalone context via `SpringApplicationBuilder(DemoApplication.class)
    .web(WebApplicationType.NONE).properties(...).run()`, entirely outside the TestContext
    framework's cache, so closing it mid-test triggers no framework interaction at all —
    `.web(...)`/`.properties(String...)` verified directly against Spring Boot's own
    `SpringApplicationBuilder` source (fetched from GitHub, not assumed) before use. Re-handed back
    for a second real `./gradlew` run to confirm this specific fix.
11. **Add a current-`main` demo integration lane, alongside the existing published-release lane.**
    The demo intentionally depends on
    `io.github.camilyed:clickhouse-r2dbc-reactive-connector:0.2.0` from Maven Central — good as a real
    consumer proof, but it means a regression on current `main` can go unnoticed until the next
    release, since the demo never tests unreleased code. Add a second lane using
    `project(":clickhouse-r2dbc-reactive-connector")` without replacing the published-release one —
    both questions ("does the last release work for a consumer" and "does current `main` still work")
    are worth answering separately.

### Could have (P2) — documentation / policy gaps, cheap and low-risk

12. State current TLS scope explicitly and unambiguously in docs: server TLS verification and custom
    CA (`sslRootCert`) are supported; mTLS client-certificate authentication and HTTP proxy
    (routing/auth) are not. mTLS is the most likely of the two to become a real ask.
13. Define and document a minimum-supported-ClickHouse-version policy. `nightly.yml` currently tests
    `latest` and one pinned version but the project has never stated a minimum; don't guess one — pick
    a small, intentional compatibility matrix (minimum tested / current stable / latest) and document
    per-feature minimums separately where they differ (e.g. `JSON` needs 25.3+ GA, already documented
    elsewhere).
14. Decide and document the project's intended multi-host/failover contract: single endpoint only
    (load balancing is the caller's job, e.g. an external LB/ClickHouse Cloud endpoint/DNS), or
    driver-owned multi-endpoint failover. Leaving it ambiguous is worse than picking the conservative
    option and stating it. If multi-host is ever chosen, it's a real feature (per-endpoint pooling,
    failover/retry semantics, `query_id`/cancellation routed to the right node, health/selection
    strategy, TLS/SNI per endpoint) — not a quick patch, and not started here.
15. Characterize semicolon-separated compound statements (`SELECT 1; SELECT 2`) with a small test —
    `docs/R2DBC_COMPATIBILITY.md` already marks this "untested." Fail fast with a clear error if
    ClickHouse's HTTP interface is fundamentally one-statement-per-request, rather than leaving actual
    behavior to depend on server quirks nobody's checked.
16. Add a "client-v2 upgrade checklist" to `CONTRIBUTING.md`. This driver never reuses client-v2's
    HTTP transport, but does reuse its RowBinary reader/decoder classes, including a small number of
    `.internal` hooks — a real version-upgrade risk worth a named checklist (full real-world type
    matrix, fragmented-buffer tests, cancellation/leak tests, decoder-only + public-API benchmark
    smoke runs, `.internal` API diff) rather than relying on memory next time the version catalog
    entry changes.

### Deferred — performance/benchmark work (stays out of scope until a proper benchmark environment exists)

Per the user's standing direction, performance/benchmark work waits for a real (cloud) benchmark
environment rather than continuing to iterate on a local/loopback one. These items from the review
are real findings, not dismissed — they're staged here alongside the existing performance backlog
(task list items covering `RowDecodingScheduler` worker-count tuning, the JDK 21 virtual-thread
scheduler experiment, mixed heavy-workload benchmarks, and the `StreamingScanBenchmark` chunk-handoff
regression at 1M rows) rather than acted on now:

- **Benchmark response-compression parity** — the review's single most emphasized finding, and the
  reason the *production feature* below existed as a separate, deliberately-sequenced-after item.
  That sequencing was overridden by explicit user direction on 2026-08-22 ("let's go with this
  compression, preserve v2's behavior, do it properly") — the driver feature is now implemented (see
  the "Response LZ4 compression as a driver feature" entry below, now done, and `CHANGELOG.md`'s
  `0.2.2` entry). What's still deferred here, unchanged, is the *benchmark re-run itself*: the
  existing large-result transport/scan numbers in `docs/PERFORMANCE.md`/README were captured before
  this driver sent `compress=1` at all (client-v2's default was ON, this driver's was effectively
  OFF, an unmatched comparison), and re-measuring both sides at matched compression settings (ON/ON
  and OFF/OFF) still needs the real (cloud) benchmark environment this whole section waits for. Until
  then, treat the existing large-result numbers as unmatched-compression and unconfirmed.
- **Benchmark-harness resource-lifecycle bug**: `OurDriverPointQueryClient` (and potentially other
  benchmark helpers) stores only the logical `Connection` it creates via
  `ClickHouseConnectionFactory.from(options)`, not the factory itself — `close()` closes the
  connection but never calls `factory.dispose()`, so the factory-owned transport/connection-pool/
  decoder-scheduler it created are never released. A benchmark-code bug, not a production-code one
  (production callers are expected to hold and dispose their own factory, which this benchmark
  adapter just isn't doing) — fix is to store `ClickHouseConnectionFactory` alongside `Connection` and
  dispose both symmetrically with how the client-v2 benchmark adapter closes its own resources.
- **Verify `@OperationsPerInvocation(4096)`'s interpretation** in
  `PublicApiMatchedPoolThroughputBenchmark` against a manual logical-QPS calculation
  (`REQUESTS_PER_INVOCATION / measured invocation duration` vs. JMH's reported `ops/s`); update the
  Javadoc's "not yet empirically verified" note once confirmed either way. JMH's documented semantics
  already say the design is sound — this is a sanity check, not a redesign.
- **Cloud JMH JSON/artifact/report pipeline** (`.github/workflows/benchmark.yml` +
  `scripts/benchmarks/analyze.py`, producing `summary.md`/charts as a GitHub Actions artifact,
  `workflow_dispatch`-triggered). `nightly.yml` already says JMH regression tracking isn't wired up
  because there's no baseline-comparison strategy yet — this is the natural next step once there's a
  real environment to run it against, but no performance-gate thresholds until multiple paired runs
  establish actual variance.
- **Response LZ4 compression as a driver feature — done (2026-08-22), per explicit user direction
  overriding the original "measure first" sequencing above.** `core.ResponseCompression` (`NONE`/
  `LZ4`, default `LZ4`) threads through `TransportOptions`/`ClickHouseHttpTransport` (sends
  `compress=1`) and `RowBinaryDecoder` (wraps the response body in the new
  `core.rowbinary.ClickHouseLz4InputStream` before decoding, when compressed) — no new Reactor operators or
  schedulers, reusing `FluxInputStreamBridge`/`RowDecodingScheduler` unchanged, so streaming,
  bounded memory, backpressure, and cancellation semantics are inherited rather than reimplemented.
  New `responseCompression` R2DBC connection option (default `true`, opt out with
  `responseCompression=false`). Proven against a real server (not just hand-built fixtures) by
  `ResponseCompressionAgainstRealClickHouseTest` — a 100,000-row `system.numbers` query, comfortably
  spanning multiple LZ4 blocks, decoded both with and without compression. What's still deferred is
  only the benchmark re-run itself — see the entry directly above.
- **Mid-stream server failure under compression, not yet independently characterized.**
  `MidStreamQueryFailureAgainstRealClickHouseTest` deliberately runs with
  `ResponseCompression.NONE` (see that class's own Javadoc) because it was written to characterize
  ClickHouse's *uncompressed* body writer specifically. A real run with compression left at its
  default (2026-08-22) instead produced zero decoded rows and a hard failure for the same scenario
  (`max_block_size = 1000`, `throwIf` at row 50000) — consistent with the compressed HTTP writer
  buffering far more coarsely than the uncompressed one, so nothing crosses its flush threshold
  before the query fails and the connection tears down with no complete block ever sent. Real,
  but not yet independently confirmed (no ClickHouse source/docs cross-check the way the
  uncompressed narrative got) or turned into its own regression test — worth a dedicated follow-up:
  pin down the actual flush trigger (buffer size vs. explicit flush call) and decide whether this
  is worth documenting as an additional Known limitation for compression's default-on behavior.
- **The richer streaming-analytics demo** (event generator, large NDJSON scan with visible
  time-to-first-row, slow-consumer/backpressure demonstration, 128-logical-queries-over-8-physical-
  connections, cancellation + `KILL QUERY`, live metrics) — valuable for showing *why* this driver
  exists, but a substantial, separate build-out; not started. If built, keep `/lab/*`
  driver-behavior-demonstration endpoints separate from the realistic application endpoints, and keep
  the demo from growing into its own DDD project.

### Later (P3) — adoption features, not started

- **Optional `clickhouse-r2dbc-reactive-spring` module** — a typed binding DSL for `DatabaseClient`,
  a ClickHouse-aware `BindMarkersFactory` strategy, and (only if technically honest) a Spring Data
  R2DBC dialect, plus Spring Boot auto-configuration. This is the largest remaining adoption gap
  (Spring's generic bind markers can't carry ClickHouse's `{name:Type}` syntax, and Spring Data's
  dialect resolver doesn't know ClickHouse at all — both already documented in the demo). Keep it a
  separate module rather than polluting `core`/`connector`; don't claim full Spring Data support
  until insert/select/typed-binding/pagination/mapping/startup/shutdown are all proven against a real
  app, and transactions must keep failing loudly either way.
- **Optional `clickhouse-r2dbc-reactive-micrometer` adapter** for the existing
  `DriverObservationListener` SPI — query duration, time-to-first-row, rows/bytes, success/failure/
  cancel, active queries, transport pool acquired/pending/timeout, `KILL QUERY` attempt/failure, retry
  count. No high-cardinality tags (no full SQL, `query_id`, or arbitrary parameter values); no
  Micrometer dependency in `core`/`transport-http`/`connector`.
- **Cancellation-outcome observability** — expose "kill attempted"/"kill failed" through the existing
  observation SPI rather than only a `WARN` log, useful for expensive analytical workloads. Don't make
  cancellation synchronously wait for server confirmation by default; that would change cancellation
  latency.
- **Type coverage, ordered by realistic likelihood of use**: `Time`/`Time64` and parameter-binding for
  already-readable types first (ties into item 6 above); `Dynamic`/`Variant`/`Geo`/vector types later,
  since they're newer and less settled. Don't expand type coverage just to move a checklist
  percentage.
- **OSS/maintenance housekeeping**: `SECURITY.md`, GitHub issue templates (bug report requiring driver
  version/ClickHouse version/JDK/URL options with secrets removed/reproducer/stack trace; performance
  regression requiring JMH JSON), and converting a handful of the most actionable items above into
  real GitHub issues so contributors don't have to read the entire ROADMAP to find something to work
  on. Don't create dozens of speculative issues at once.
- **A short "what's actually next" pointer doc**, separate from this ever-growing historical
  ROADMAP — superseded as a standalone idea by the much fuller plan in
  [Phase 9](#phase-9--documentation--website-redesign): a dedicated `docs/project/production-readiness.md`
  is one of that phase's own deliverables, not a separate item to track here.

## Phase 9 — Documentation & website redesign

**Not started.** Plan captured 2026-08-22 from a detailed information-architecture review the user
provided, covering README/ROADMAP restructuring and a new GitHub Pages documentation website. This
is deliberately scoped as a documentation/information-architecture task, not a code-refactor task —
production Java code should not change as part of this phase, beyond a tiny path/link fix a docs
build genuinely requires.

### Problem

`README.md` currently doubles as landing page, design manifesto, install guide, full configuration
reference, Spring guide, performance report, known-limitations document, pooling tutorial,
architecture doc, module reference, testing-strategy doc, and roadmap summary — one file with far
too many jobs. `ROADMAP.md` is broader still: historical engineering notebook, ADR, implementation
diary, completed-phase history, production-readiness review, current roadmap, backlog, and Claude
handoff material all at once. The content is good; it just isn't sorted by what a reader is actually
looking for when they open either file.

### Target shape

Three layers, each answering a different question:

```text
GitHub README        -> understand the project in 60-90 seconds, install, first query
Documentation website -> the actual user manual (concepts, reference, operations, performance)
engineering/ archive  -> deep investigation notes, old phases, design archaeology (not deleted)
```

`README.md` target: ~150-250 lines. It keeps project purpose, badges, install, one working
R2DBC example, an honest status line, a high-level architecture sketch, and links out — not the full
option reference, not the full pooling story, not the full performance report.

A new `docs/` tree (`guide/`, `concepts/`, `reference/`, `operations/`, `performance/`,
`architecture/`, `project/`, `internals/`, `images/`) holds everything that currently over-stuffs
README/ROADMAP: `docs/reference/configuration.md` (full connection-options table, grouped by
concern — endpoint, TLS, timeouts, retry, physical transport pool, advanced/programmatic — instead
of one flat table), `docs/reference/known-limitations.md`, `docs/project/production-readiness.md`
(✅/⚠️/❌/🧪 adoption-decision matrix), a split `docs/performance/` (`index.md`/`methodology.md`/
`results.md`/`running-benchmarks.md`, replacing the single `docs/PERFORMANCE.md`), and moved copies
of `docs/R2DBC_COMPATIBILITY.md`/`docs/CLIENT_V2_HTTP_REFERENCE.md`. Superseded/historical material
(`docs/CURRENT_WORK.md`, `docs/REVIEW_*.md`, the current sprawling `ROADMAP.md` itself) moves to a
new `engineering/` tree rather than being deleted — `engineering/roadmap-archive.md` keeps every
phase already written here; the new root `ROADMAP.md` becomes a short (2-5 KB) "what's released /
what's next / what's later / what's explicitly not planned" pointer, with completed work delegated
to `CHANGELOG.md` and engineering reasoning delegated to `engineering/`.

### One content decision worth calling out explicitly

The plan recommends the docs stop presenting `io.r2dbc.pool`'s logical pool as co-equal with this
driver's own physical Reactor Netty transport pool. The primary story becomes: one factory-owned
physical transport pool, R2DBC `Connection` objects are cheap logical handles over it, most
applications don't need `io.r2dbc.pool` at all. `io.r2dbc.pool` moves to
`docs/operations/optional-r2dbc-pool.md` as an advanced/optional layer (validation, eviction,
framework-integration reasons to reach for it) — **not removed, and its compatibility tests stay**;
this is a documentation emphasis change, not a feature deprecation. The demo's default
`application.yml` should reflect the recommended default (plain `ConnectionFactory` + driver's own
transport pool), with `io.r2dbc.pool` wiring kept as an explicit opt-in profile/example rather than
the default shape.

### Website

Recommended stack: **VitePress** (Markdown-source, static-HTML-then-SPA-navigation, strong
code-block rendering, local search, GitHub Pages deployment, no content duplication since it renders
the same `docs/` tree rather than being a second source of truth). Astro + Starlight noted as the
fallback if the site later needs much more custom product UI than VitePress's theming supports.
Deploys to `https://camilyed.github.io/clickhouse-r2dbc-reactive/` via a new
`.github/workflows/docs.yml` (build-check on PRs, build-and-deploy on `main`); no custom domain
purchase upfront. Visual direction: near-black/graphite background, ClickHouse-inspired yellow
primary accent, cool cyan secondary accent, restrained gradients — technical/clean/fast/serious, not
a copy of any specific existing site's exact layout. Homepage: hero + one-sentence pitch, "why it
exists" before/after architecture comparison, six feature cards (non-blocking I/O, streaming
results, bounded backpressure, cancellation propagation, R2DBC SPI, Spring WebFlux), one performance
chart with explicit benchmark context next to it (not three unlabeled charts), and a
production-readiness panel using the same ✅/⚠️ vocabulary as the dedicated doc. Benchmark charts on
the site are generated artifacts (`docs/performance/generated/*.svg`) the site renders — the website
itself never runs JMH.

### Sequencing (small PRs, not one rewrite)

1. **Information architecture** — archive old `ROADMAP.md` content, write the short new one, move
   superseded review/current-work docs into `engineering/`, create the `docs/` skeleton, fix links.
   No Java changes.
2. **README rewrite** — short landing page; remove the full configuration table, full pooling
   reference, full performance report; link out to `docs/`.
3. **Split existing docs** into the new `docs/` tree (configuration, known limitations, pooling,
   Spring, architecture, performance, production readiness) — move existing facts, don't
   unnecessarily rewrite already-verified technical claims.
4. **VitePress scaffold** — `package.json`, `docs/.vitepress/*`, `docs/index.md`, the GitHub Pages
   workflow, dark/light theme, local search, navigation.
5. **Homepage styling** — hero, architecture component, feature cards, performance preview,
   production-readiness panel.

Before touching content, the working session that picks this up should first return: the exact
proposed file-move map, the content-ownership matrix (which doc owns which fact), the proposed
README outline, the proposed new `ROADMAP.md` outline, and which pages are published vs.
`engineering/`-archive-only — then implement in the small PRs above, not one enormous diff.

## Phase 10 — Cloud benchmark pipeline

**Stage 1 built, not yet run for real** — see [ROADMAP.md's Phase
10](../ROADMAP.md#phase-10--cloud-benchmark-pipeline) for current status. Plan captured 2026-08-22,
directly answering the blocker every entry in
[Phase 8's deferred performance/benchmark section](#deferred--performancebenchmark-work-stays-out-of-scope-until-a-proper-benchmark-environment-exists)
has been waiting on: a repeatable benchmark environment off the local MacBook M3 Pro (whose 6P+6E
core split already produced the unresolved 1M-row inter-fork variance documented in
`docs/PERFORMANCE.md`). Building this pipeline is CI/tooling infrastructure — a Python analysis
script and a GitHub Actions workflow — not benchmark iteration itself, so it doesn't need an
exception to the standing "performance work waits for a proper environment" rule; it's the
prerequisite that rule has been naming all along. **Actual benchmark re-runs stay deferred until
this pipeline exists and its results have been validated as stable** — building it doesn't
retroactively unblock e.g. the response-compression-parity re-run on its own.

### Non-negotiable constraint

`ourDriver` and `client-v2` **must** run in the same job, on the same VM, against the same
ClickHouse process, every time. Splitting them into separate CI jobs would compare two different
machines under two different sets of momentary noise, not two drivers — the whole reason
`PublicApiMatchedPoolThroughputBenchmark`/`docs/PERFORMANCE.md`'s existing fairness work exists.

### Trust model for cloud numbers

Don't trust an absolute cloud number on its own (`ourDriver = 9000 qps`) — a shared/noisy
GitHub-hosted runner varies run to run. Trust the **`ourDriver / client-v2` ratio**, repeated across
several runs: a ratio that stays close (e.g. 1.07-1.09 across five runs) is a strong signal even if
the absolute numbers swing; a ratio that itself swings wildly (0.91, 1.15, 1.02, 1.20, 0.94) means
the runner is too noisy for this comparison and it's time to escalate to a dedicated VM, not to trust
the number anyway.

### Stage 1 — GitHub Actions only (start here)

New `.github/workflows/benchmark.yml`, triggered by `workflow_dispatch` (optionally also a weekly/
nightly `schedule`), on one standard GitHub-hosted Ubuntu runner — the repo is public, so no custom
runner infrastructure is needed to start. Two profiles: a **fast** sanity-check (1 fork, short
warmup, "did it run / no catastrophic regression", not for public performance claims) that can run
more often, and a **trusted** benchmark (3-5 forks, 5 warmup iterations, `-prof gc`,
`workflow_dispatch`/pre-release only) starting with exactly
`PublicApiMatchedPoolThroughputBenchmark` at `poolSize=8`, `concurrency=8/32/128` — the same
benchmark and parameters the existing local results already use, so cloud and local numbers are
comparable in shape even if not in absolute value.

Preserve JMH's own JSON output (`build/results/jmh/results.json`) as the source of truth rather than
parsing stdout, alongside `metadata.json` (commit SHA, branch, JDK version, OS/arch, ClickHouse
image version, `client-v2` version, driver version, fork/warmup/pool-size counts) and the raw
stdout. A new `scripts/benchmarks/analyze.py` parses the JMH JSON and produces `summary.md` (a
readable commit/environment header plus a queries-per-second/percentile/allocation table per
concurrency tier) and simple throughput/latency/allocation charts (PNG/SVG) — three charts to start,
not a dozen. The whole result directory uploads as a GitHub Actions artifact
(`actions/upload-artifact`); raw JSON is **not** auto-committed to `main`.

Both implementations get an explicit prewarm before measurement (DNS/network init, physical pool
open, `SELECT 1`, one representative point query) so cold-start cost (classloading, first TCP
connect, pool/decoder init) doesn't pollute the first real measurement for either side. ClickHouse
runs a pinned image tag, never `latest`; both clients hit the identical process with identical SQL,
parameter binding, dataset, pool size, compression setting, timeouts, database, and result mapping —
no comparison relying on either library's differing defaults.

### Stage 2 — ephemeral dedicated-vCPU VM (only if Stage 1 proves too noisy)

Not a standing server. Create a VM (e.g. Hetzner Cloud, dedicated vCPU rather than shared — shared
means noisy-neighbor variance, defeating the point), run the benchmark, destroy it — cleanup in an
`always()` step so a failed run can never leave a paid-for VM running. Only build this once Stage 1's
paired ratio has actually shown itself to be too unstable to trust; not before.

### Stage 3 — optional, later

A static GitHub Pages benchmark dashboard (`.../benchmarks/`, latest result plus a `history/` of
past dated JSON snapshots, no React needed — HTML/CSS/JSON plus the same Python generator) once
Stage 1/2 results are stable enough to be worth publishing continuously. Regression-detection
thresholds (e.g. throughput regression > 10%, allocation/query > 15%, p99 > 15%, computed primarily
against the `ourDriver/clientV2` ratio and against several prior runs, not one point) only once
several stable runs exist to calibrate against — don't start with 2-3% thresholds, cloud benchmark
noise will make that a constant false-alarm generator. [Bencher](https://bencher.dev) noted as an
optional later layer for historical tracking/PR comparisons if the homegrown
JSON-artifact-plus-summary approach stops being enough — not needed for the first version.

### Explicitly out of scope for the first PR

No Hetzner VM, no Bencher, no GitHub Pages dashboard, no production driver code changes, no
regression-gate thresholds, and no moving/renaming the existing diagnostic benchmarks in
`clickhouse-r2dbc-reactive-benchmarks` — the new pipeline is an orchestration/reporting layer on top
of them, not a replacement. First PR is exactly: `benchmark.yml` + `analyze.py` + artifact upload +
`summary.md`, run and verified working before being documented as such. See item 30 of the source
planning document for the full Claude task prompt this phase should start from when picked up.

## Working with Claude / IntelliJ

- [CLAUDE.md](../CLAUDE.md) is the source of truth for how code in this repo should look and be
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

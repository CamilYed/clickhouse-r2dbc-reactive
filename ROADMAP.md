# Roadmap

Working plan for turning the current skeleton into a verified, fully reactive R2DBC driver. Phases
are sequential gates, not a fixed calendar — we don't start a phase until the previous one has a
written, checkable answer, in the same spirit as the
[verified execution-path analysis](https://github.com/ClickHouse/ClickHouse/discussions/113638)
proposed to the ClickHouse team.

## Contents

- [Module map](#module-map)
- [docs/CLIENT_V2_HTTP_REFERENCE.md](docs/CLIENT_V2_HTTP_REFERENCE.md) — full HTTP wire-protocol audit (compression, auth, headers, errors, TLS, retries)
- [Phase 0 — client-v2 execution-path finding](#phase-0--client-v2-execution-path-finding)
- [Phase 1 — Transport spike](#phase-1--transport-spike)
- [Phase 2 — Core protocol + testkit contract tests](#phase-2--core-protocol--testkit-contract-tests)
- [Phase 3 — Connector (R2DBC SPI surface)](#phase-3--connector-r2dbc-spi-surface)
- [Phase 4 — "Fully reactive" sign-off](#phase-4--fully-reactive-sign-off)
- [Production readiness review](#production-readiness-review)
- [Non-functional requirements: logging, metrics, leaks](#non-functional-requirements-logging-metrics-leaks)
- [Phase 5 (later) — Load and performance testing](#phase-5-later--load-and-performance-testing)
- [Phase 6 (later) — Spring WebFlux interop demo](#phase-6-later--spring-webflux-interop-demo)
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
| `benchmarks` (`clickhouse-r2dbc-reactive-benchmarks`, Phase 5, in progress) | `core`, `transport-http`, `connector`, `testkit`, client-v2 (as the comparison baseline) | JMH throughput/latency/allocation measurement, this driver vs client-v2, at multiple levels (raw transport, public R2DBC SPI) — see Phase 5 below for the full design. Not published: it's measurement tooling, not a library. | No |
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
  - **Concrete instance found in step 6 (real ClickHouse):** the `Map<String, Object>` returned by
    `next()` (client-v2's `RecordWrapper`) stores its values behind a `WeakReference` to the
    reader's internal state, not directly. Reading from it after the reader itself is no longer
    strongly reachable — which happened here simply from `Flux.generate`/`blockFirst()`
    cancelling the subscription after one element — threw a `NullPointerException` from inside
    client-v2, not from our code. Fixed in `core.RowBinaryDecoder` by copying each row into a
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
[docs/CLIENT_V2_HTTP_REFERENCE.md](docs/CLIENT_V2_HTTP_REFERENCE.md), with file:line citations and
a mapping of what each concern means for Phase 1 through Phase 3. Read it before building anything
in `transport-http`/`core` beyond the Phase 1 spike so nothing gets silently skipped — this is the
answer to "we can't skip anything" for the HTTP surface specifically.

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

**Deliberate reordering:** rather than a shallow pass through steps 3–6 first and looping back for
edge cases in step 7, `transport-http`'s full contract-test matrix is being built out immediately
after step 3's happy path — still strictly TDD, one scenario at a time, but front-loaded because
these are transport-only concerns (`testkit`'s fake server, no `core`/decode involved) and hardening
the transport boundary before building on top of it catches problems earlier. `core`'s bridge
(step 4) waits until this matrix is green. The scenario list — request not sent before subscription,
streamed not aggregated, cancellation tears down the connection, delayed headers, delayed body,
fragmented chunks, slow subscriber/backpressure, no response/timeout, connection reset mid-response,
cancellation before/while-queued/during-receive, pool saturation, pending-acquire timeout — is the
same one already named in [README's testing strategy](README.md#testing-strategy); nothing new, just
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

## Phase 2 — Core protocol + testkit contract tests

Once the transport spike proves the transport contract, fill in:

- `core`: query request/settings/`query_id` representation, response metadata + row decoding for
  the first chosen wire format, cancellation state — transport-independent.
- `testkit`: the full contract-test matrix from [CLAUDE.md](CLAUDE.md#test-types-and-tools) —
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
| Date and time | ⚠️ `Date`/`Date32`/`DateTime`/`DateTime64` covered; `Time`/`Time64` **not supported by our pinned client-v2 at all** — confirmed no case for them in `BinaryStreamReader.readValue`'s switch. A real client-v2-version gap, not a test gap. |
| Nullable and optional | ✅ `Nullable` covered (including an actual `NULL` value across rows); `LowCardinality` covered too — confirmed against client-v2's own test suite (`DataTypeTests`, our pinned `v0.9.0`) that it's a "virtual type" there (wrapper stripped, dispatches to the underlying type, same shape as `Nullable`), then proven end to end through our own pipeline, not just assumed from client-v2's tests |
| Specialized | ⚠️ `UUID` and `Enum8`/`Enum16` covered; geo types, vector-search (`QBit`), domains not attempted |
| Composite (`Array`/`Tuple`/`Map`/`Nested`) | ✅ `Map`/`Tuple`/`Array`/`Nested` all covered — `Nested` flattens into one `Array(...)` column per sub-field by default (`flatten_nested=1`), so on the wire it's indistinguishable from ordinary `Array` columns, same mechanism, confirmed directly |
| Semi-structured (`JSON`/`Dynamic`/`Variant`) | ⚠️ `JSON` covered (GA since ClickHouse 25.3, no `allow_experimental_json_type` needed) — decoded as a plain `String`: `ClickHouseHttpTransport` sends `output_format_binary_write_json_as_string=1` unconditionally on every query (a no-op when there's no JSON column, so no opt-in `ConnectionFactoryOptions` needed), and `core`'s `RowBinaryDecoder#newReader` sets the matching local `QuerySettings` flag so client-v2 decodes it the same way instead of into its complex `.internal` JSON object representation; `Dynamic`/`Variant` still not attempted, newer and less settled |
| Aggregate function (`AggregateFunction`/`SimpleAggregateFunction`) | 🚫 not attempted — these hold intermediate aggregation state, not literal-insertable values, so proving them needs insert-via-aggregate-query, not `INSERT ... VALUES` |
| Special Data Types (`Expression`/`Interval`/`Nothing`/`Set`) | N/A — query-intermediate constructs, not column/storage types |

**Corrected finding (previous version of this table overstated the Composite block).** Reading
`BinaryStreamReader` directly: `readMap()`/`readTuple()` return a plain `LinkedHashMap`/`Object[]`
regardless of type hints, for element types that aren't themselves `Array`/`Nested`. `Enum8`/
`Enum16` return an `.internal` `EnumValue`, but it publicly overrides `toString()` to the member
name — calling `toString()` on the opaque `Object` reads the value without importing or casting to
the internal type (a dependency on current `toString()` behavior, not a documented contract, but no
compile-time coupling).

**`Array`/`Nested` resolved** — both route through `BinaryStreamReader.convertArray()`, which only
returns a plain `List` when a `List.class` type hint is supplied, and the public `next()`/
`readRecord()` path this driver used never passed one. Resolved by `core.ListDecodingRowBinaryReader`
(new): overrides the reader's `protected readRecord(Object[])` hook — exposed by client-v2
specifically for subclassing — to supply `List.class` as the type hint for `Array`/`Nested` columns
only, leaving every other type's decoding untouched. This is a deliberate, narrow, tested dependency
on client-v2's `.internal` package (the `BinaryStreamReader binaryStreamReader` protected field, and
its public `readValue(column, typeHint)` method), the same shape of compromise as the Phase 0
`InputStream` bridge: one documented seam, not a general dependency. Covered by a hermetic unit test
in `core` (`RowBinaryDecoderTest.shouldDecodeAnArrayColumnAsAPlainList`, hand-built wire bytes, no
ClickHouse needed) and a real-ClickHouse test (`RealWorldTableAgainstRealClickHouseTest.
shouldDecodeArrayType`).

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
`core.RowBinaryDecoder.decode(Flux<ByteBuffer>)` (new) returns a `Mono<DecodedResult>` pairing the
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

### Sign-off (2026-08-13)

Every property in [README.md's table](README.md#what-fully-reactive-means-here) mapped to a named
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
- **`Connection.setStatementTimeout(Duration)` still throws `UnsupportedOperationException`.**
  `ClickHouseHttpTransport` now supports a transport-wide `responseTimeout`, but per-statement
  timeouts would need that value threaded per-request rather than baked into the `HttpClient`
  instance at construction — a real architectural change, not a small fix, and out of scope for this
  pass. Fails loudly (throws) rather than silently ignoring the call.
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

- **No server-error-code-aware retry** — `RetryPolicy` (see "Fixed this pass" above) only retries
  failures that happened before the request was sent; it does not retry based on ClickHouse's own
  retryable server error codes the way client-v2's `ServerRetryable` cause (added after our pinned
  `v0.9.0`) does. Deliberately deferred: our pinned client-v2's `ServerException` has no
  `isRetryable()` yet to model this against, and doing it well would need to distinguish
  retry-safe-regardless-of-idempotency server errors from ones that aren't — a genuine design
  question, not a small addition. `RetryPolicy`'s record shape was chosen to leave room to grow a
  second mode later without a breaking change.
- **No way to configure the transport's HTTP connection-pool size (`maxConnections`) through the
  standard R2DBC `ConnectionFactoryOptions` bootstrap path** — only through constructing
  `ClickHouseHttpTransport` directly. `CONNECT_TIMEOUT` is now wired (this pass); pool sizing isn't a
  well-known R2DBC `Option`, so this would need a driver-specific custom `Option`. Configurability
  gap, not a correctness bug.
- **`Statement.add()` (bound-parameter batching) is still `UnsupportedOperationException`** in
  `ClickHouseStatement` — related to, but not solved by, `insertStreaming` (see "Fixed this pass"
  above): making `add()` genuinely performant would mean coalescing N bound-parameter sets into one
  multi-row `INSERT` (ClickHouse's `{name:Type}` mechanism only binds one value per name per
  request) rather than N sequential round-trips. Deliberately deferred — `insertStreaming` covers
  the "I already have my data encoded, stream it efficiently" case; `add()`-based batching is a
  different, not-yet-designed problem (building the multi-row SQL text safely).

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
  so a forgotten `.release()` fails CI instead of showing up as a slow leak in production; (b)
  bounded, deterministic cleanup on cancellation — already an explicit Phase 4 checklist item
  (`FluxInputStreamBridge.close()`/`ControlledClickHouseServer`'s `activeConnectionCount()` are the
  first two places this is already tested). The step-6 `WeakReference` finding above is a related
  but different concern (a lifetime bug, not a leak) — noted there, not here.
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

## Phase 5 (later) — Load and performance testing

**Design finalized 2026-08-13; implementation starting.** Phase 4 is signed off, `integration-tests`
is deleted (see the module map), so this is now unblocked and starting. Explicit request driving this design: measure **this driver vs client-v2** (the same
library `core` already reuses for row decoding, and the one anyone evaluating this driver will
naturally compare it against), at **multiple levels**, professionally enough to trust the numbers —
not a couple of ad hoc `SELECT 1` timings.

**Prior art read before designing this**, not designed from scratch: `ClickHouse/clickhouse-java`'s
own `performance` module (mounted at `/Users/kamil/Projects/clickhouse-java/performance`) is a real,
maintained JMH suite comparing their own client V1 vs V2 vs JDBC, with a scheduled (not per-PR) GitHub
Actions job. Reused directly: JMH itself (not Gatling — see "Tooling" below), `GCProfiler` for
allocation, `SampleTime` mode for percentiles, `@Threads(N)` for one shape of concurrency, and the
"don't run this on every PR" cadence. Deliberately *not* reused: their CSV-file dataset (generated
client-side, checked into a file, then re-uploaded) — ClickHouse can generate large synthetic
datasets server-side in seconds via `INSERT ... SELECT ... FROM numbers(N)`/`generateRandom(...)`,
which is both faster and more "pro" than a client-side CSV round-trip.

### Module

New module `clickhouse-r2dbc-reactive-benchmarks` (not published — measurement tooling, not a
library; added to `nonPublishedModules` already). Depends on `core`, `transport-http`, `connector`,
and `testkit` (for `BaseClickHouseIntegrationTest`-style container lifecycle reuse — a benchmark run
still needs a real ClickHouse instance and shouldn't reinvent that), plus `com.clickhouse:client-v2`
(already in the version catalog, pinned `0.9.0` — same version `core` reuses for decoding, so the
comparison is against the exact client-v2 build this project already depends on, not some other
version) as the comparison baseline. Depending on `core`/`transport-http` directly (not just
`connector`'s public SPI) is intentional and doesn't violate the Architecture rule ("a module
should not reach into another module's internals") — `ClickHouseHttpTransport` and
`RowBinaryDecoder` are already `public final class`, part of each module's own public API, not
internals; benchmarking below the R2DBC SPI layer is exactly how "which level costs what" gets
answered.

**Gradle/JMH tooling**: the `me.champeau.jmh` Gradle plugin (the standard, actively-maintained
community JMH plugin — gives a `jmh` task, JSON/text result output, and proper annotation-processor
wiring without hand-rolling what clickhouse-java's Maven `exec-maven-plugin` setup does manually).
**Not Gatling** — Gatling is built for HTTP/network load-testing DSLs (simulate many external
clients hitting an API); what's being measured here is a Java library's in-process call cost and
concurrency behavior, which is exactly JMH's problem, including its own `@Threads(N)` and
custom-harness-inside-`@Benchmark` support for the concurrency scenario below. Nothing added to the
main build's `check`/`build` tasks — a benchmark run is a separate, explicit `./gradlew
:clickhouse-r2dbc-reactive-benchmarks:jmh` invocation, per CLAUDE.md's "don't gate the main loop on
a multi-minute run."

### Comparison levels ("różne poziomy")

Three levels, deliberately kept as separate benchmark classes rather than one giant parameterized
suite, so a regression at one level (e.g. "our decode got slower") isn't masked or conflated with
another (e.g. "our connection pooling got slower"):

1. **Raw transport + decode** — `ClickHouseHttpTransport.query(...)`/`RowBinaryDecoder.decode(...)`
   directly vs client-v2's `Client.query(...)` + `ClickHouseBinaryFormatReader`. Answers: how much
   does *this driver's own* HTTP-adapter-plus-decode pipeline cost relative to client-v2's, with the
   R2DBC SPI layer removed from the comparison entirely.
2. **Public R2DBC SPI** — `ClickHouseConnection`/`ClickHouseStatement`/`ClickHouseResult` (what an
   actual driver consumer calls) vs client-v2's `Client` API directly. Answers: what does a real user
   of this driver actually pay, R2DBC-shape translation included.
3. **Concurrency/burst, reactive vs blocking** — the scenario that originally motivated this whole
   project (see Phase 1: "~11 concurrent queries per user action"). Not a third *code* level so much
   as a third *axis*: this driver's non-blocking pipeline multiplexing many logical queries over a
   small connection pool vs client-v2's blocking `Client`, one thread per in-flight query. This is
   where a fair comparison needs the most care — see below.

A fourth level (through Spring's `DatabaseClient`, or client-v2's JDBC wrapper) is explicitly **out
of scope for the first pass** — it would measure Spring/JDBC overhead on top of both drivers, not
either driver itself, and can be added later as its own separate concern if real usage data ever
asks for it.

### Dataset

Server-side generated, not client-side-generated-then-uploaded: `INSERT INTO t SELECT ... FROM
numbers(N)` (cheap columns) and `generateRandom('col Type, ...', seed)` (for types `numbers()`
can't produce directly — `String`, `UUID`, `Array`, etc.) via a `CREATE TABLE ... ENGINE =
MergeTree` + `INSERT INTO ... SELECT * FROM generateRandom(...) LIMIT N`, run once per benchmark
`@Setup(Level.Trial)`, timed and logged so a slow *setup* never gets misread as a slow *query*.
Table shape reuses `RealWorldTableAgainstRealClickHouseTest`'s wide, multi-type-column shape for the
decode-heavy benchmarks (not a narrow two-column toy table — real decode cost is dominated by column
*variety*, not just row count) plus a narrow two/three-column table for the point-query/burst
benchmarks (isolates connection/protocol overhead from decode cost).

Row-count tiers, run via JMH's `@Param` (same mechanism clickhouse-java uses for `limit`), so a
local dev loop and a "real" run are the same code, different parameters:

| Tier | Rows | Purpose |
| --- | --- | --- |
| `smoke` | 10,000 | Fast local iteration while writing/debugging a benchmark itself — not a real number. |
| `default` | 1,000,000 | The number actually reported/tracked for regressions — large enough that per-row costs dominate one-time connection setup, small enough to run in CI's time budget. |
| `large` | 50,000,000+ | Manual/opt-in only (`-Prows=large` or similar), for a genuine "does this scale" check before a release — not run routinely, disk/time cost is real. |

### Query mix

Not one query shape — this driver's whole value proposition (non-blocking, streaming, backpressure)
shows up differently depending on the shape:

- **Point/trivial** — `SELECT 1`, and a parameterized single-row lookup (`SELECT ... WHERE id =
  {id:UInt32}` for us, `client.query(...)` with the equivalent for client-v2). Measures the
  protocol/connection-overhead floor, where decode cost is negligible and framing/header/network
  cost dominates.
- **Full-table streaming scan** — `SELECT * FROM t` over the `default`/`large` tier table. Measures
  sustained throughput, time-to-first-row, and allocation under long streaming — the scenario
  `GCProfiler` matters most for.
- **Wide multi-type decode** — same query shape as above but against the wide `RealWorldTable`-style
  table, to isolate "cost per row of varied-type decoding" from "cost per row of narrow decoding."
- **Analytical aggregation** — `SELECT category, count(), avg(amount), quantile(0.95)(amount) FROM t
  GROUP BY category ORDER BY count() DESC` (ClickHouse's actual core use case — ClickHouse does the
  heavy lifting server-side; this mostly measures small-result-set decode + round-trip overhead, but
  it's the shape a real analytics query actually has, not a synthetic stand-in).
- **INSERT** — both this driver's literal-embedded small `INSERT` (`ClickHouseStatement`) and its
  streaming `insertStreaming` vendor extension, vs client-v2's `client.insert(...)`.

### Concurrency/burst — the scenario that needs the most care

JMH's `@Threads(N)` (what clickhouse-java uses) models "N platform threads, each independently
calling a blocking client" — a fair, standard way to benchmark a *blocking* client under load. It
does not model what this driver is actually for: **one small connection pool, many logical
concurrent queries multiplexed non-blockingly over it.** Modeling that inside one JMH thread with a
custom harness (`Flux.range(0, N).flatMap(i -> runQuery(), concurrency).blockLast()`, `SampleTime` or
`SingleShotTime` mode, `concurrency` = the connection pool's `maxConnections`) is what proves the
actual claim. Both shapes get benchmarked side by side, deliberately not collapsed into one number,
because they answer different questions:

- **`@Threads(N)` shape** — client-v2 with N platform threads and N connections vs this driver with N
  platform threads each blocking on `.block()` and a pool of N connections. A same-resources
  comparison; expected to be roughly comparable, since neither side's concurrency model gets to use
  its advantage here.
- **Custom reactive-harness shape** — this driver with a *small* pool (e.g. 4–8 connections)
  serving N=~50–100 concurrent logical queries via `flatMap` concurrency, wall-clock time to
  complete all N, vs client-v2 needing N platform threads (or an external executor) to attempt the
  same concurrency with N connections. This is the actual "~11 concurrent queries per user action"
  motivating scenario from Phase 1, scaled up — the one where fewer OS threads/connections doing the
  same logical work is the entire point, and where raw single-query latency numbers alone would
  mislead.

Per-request latency *within* one burst is recorded via `HdrHistogram` inside the benchmark method
(JMH's own percentile machinery works at "one `@Benchmark` invocation = one op" granularity, which
is too coarse for "N sub-operations happened inside one burst") and reported alongside JMH's own
result JSON, not folded into it.

### What's measured, and how

- **Throughput / latency (p50/p95/p99/p99.9)** — JMH `SampleTime` mode for single-query benchmarks
  (built-in percentile reporting, same as clickhouse-java's own choice); `HdrHistogram` for
  intra-burst percentiles in the concurrency scenario (see above).
- **Time to first row** — not something JMH measures natively: record `System.nanoTime()` at
  subscribe and again at the first `onNext`, collect into a histogram, report as a custom metric
  alongside the JMH result file.
- **Allocation / retained memory under streaming** — JMH's built-in `GCProfiler`
  (`-prof gc`), exactly as clickhouse-java uses it — `gc.alloc.rate`/`gc.alloc.rate.norm` need no
  custom instrumentation.
- **Connection pool behavior under the burst scenario** — log `r2dbc-pool`'s
  `ConnectionPoolMetrics` (acquired/idle/pending count) at intervals during the benchmark; not a
  pass/fail number, a diagnostic trend to eyeball when interpreting a burst result.
- **Off-heap/Netty buffer usage** — `PooledByteBufAllocator.DEFAULT.metric()`'s `toString()` logged
  periodically during a long streaming benchmark; same status — diagnostic, not an automated gate,
  useful for spotting a leak trend during a `large`-tier run.
- **CPU usage** — deliberately *not* built as an automated numeric pipeline for the first pass
  (real ROI is low relative to the effort of doing it reliably in a shared CI runner). Recommended
  as a manual, qualitative companion tool instead: run a `large`-tier benchmark under
  `async-profiler` locally for a flame graph when investigating a specific regression, rather than
  gating every run on a CPU-percent number.

### Reporting

JMH's own JSON result format (`-rf json`, or the `me.champeau.jmh` plugin's equivalent config),
matching clickhouse-java's own convention. Raw JSON result files are **not committed to git** (they're
noisy binary-ish artifacts, one per run) — treated as CI/local run output, uploaded as a workflow
artifact if/when a scheduled CI job exists (see below), otherwise just inspected locally. A small
JSON→Markdown summary script is a reasonable follow-up once the benchmark classes themselves exist
and there's real output to summarize — not built speculatively ahead of having any numbers.

### When this runs

Not on every PR/push — a multi-minute-to-hour JMH run has no place in the fast feedback loop this
project's CI otherwise protects. Matching clickhouse-java's own practice: a `workflow_dispatch`
(manual trigger, e.g. before a release) plus an optional scheduled (nightly/weekly) job once the
suite is stable enough to trust unattended — decided once the classes exist and a first baseline run
has actually happened, not designed in the abstract now.

### TDD note

This module is measurement tooling, not driver behavior — CLAUDE.md's red-green-refactor workflow
is scoped to production code with behavior to protect, and explicitly carves out "skeleton/plumbing
code with no behavior yet" as the exception. A JMH `@Benchmark` method has no assertion to be red
about; its "correctness" is that it measures the right thing, which is a design/review question
(does this benchmark actually isolate what it claims to), not a TDD one. `BaseClickHouseIntegrationTest`-style
setup code and any real assertions this module *does* need (e.g. "the burst benchmark actually ran N
queries, not N-1") still follow this project's normal testing rules.

### First slice, run for real (2026-08-13)

`clickhouse-r2dbc-reactive-benchmarks` scaffolded (`me.champeau.jmh`, `BenchmarkEnvironment`,
`PointQueryTable`) with one benchmark class, `PointQueryBenchmark` (Level 1 — raw transport, this
driver vs client-v2, point lookup against the `smoke`-tier 10,000-row table). Run against real
Docker/ClickHouse (`./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh`), not just compiled:

- **Numbers (1 fork, 1×10s warmup, 3×10s measurement — a sanity check, not yet a trustworthy
  baseline; JMH's own output explicitly warns against over-reading a single short run like this):**
  this driver mean 1115.8 ± 3.8 µs/op vs client-v2 mean 1235.6 ± 6.6 µs/op; p99 1589 µs vs 2165 µs.
  This driver read faster on this run through p99.9; both had noisy, low-sample tail percentiles
  (p99.99/p100) not worth reading into yet.
- **Real finding, not assumed: JMH forks a fresh JVM per `@Benchmark` method by default.**
  `BenchmarkEnvironment`'s `static` container field is therefore shared only within one fork, not
  across the whole `jmh` run — the log showed two separate `clickhouse/clickhouse-server` containers
  starting on two different ports (one for `clientV2`, one for `ourDriver`), each paying the ~4s
  Testcontainers startup independently. Documented in `BenchmarkEnvironment`'s own Javadoc (the
  original version claimed "one container for the whole run," which this run proved wrong). Accepted
  as a real, known cost for now rather than forcing everything into one fork (which would trade away
  JMH's normal isolation between benchmark methods) — revisit if the `large` tier's repeated
  container-plus-reseed cost per fork turns out to dominate a real run's wall time.

Remaining query shapes (full table scan, wide multi-type decode, aggregation, INSERT) and the
concurrency/burst scenario are designed above but not yet built — next.

### Fairness/reproducibility review of `PointQueryBenchmark` (2026-08-13)

An external review of the first run (a written benchmark-strategy document, checked against this
project's own code rather than taken at face value) confirmed the overall Level 1/2/3 design above
independently arrived at the same shape, and caught four concrete, real problems in
`PointQueryBenchmark` specifically — all fixed:

- **Unpinned ClickHouse image (`:latest`).** A performance baseline that silently tracks whatever
  ClickHouse changes underneath it isn't reproducible run-to-run. Pinned to
  `clickhouse/clickhouse-server:26.7.3.19` in `BenchmarkEnvironment` — confirmed via Docker Hub that
  `latest` currently resolves to exactly this image (same digest), so this changes nothing about
  today's numbers, only future reproducibility.
- **Asymmetric parameterization.** This driver used `{id:UInt64}` + ClickHouse's `param_<name>`
  mechanism; client-v2 received a plain inlined literal. Fixed by reading client-v2's own
  `Client#query(String, Map, QuerySettings)` Javadoc directly (in the mounted client-v2 source) and
  confirming it documents the identical `{name:Type}`/`query_params` contract — both benchmark
  methods now build the exact same SQL text and parameter map.
- **`Math.random()` inside the `@Benchmark` hot path.** Not necessarily a dominant cost next to a
  real network round trip, but non-reproducible across runs and a potential source of different
  access patterns between the two methods. Fixed: `PointQueryTable.deterministicIds(rowCount,
  poolSize, seed)` pre-generates a fixed pool once in `@Setup(Level.Trial)` via `SplittableRandom`;
  the benchmark methods only advance an `AtomicLong` cursor through it.
- **Environment/version metadata not recorded.** `BenchmarkEnvironment.start()` now logs the
  ClickHouse server version (queried from the running container, not assumed from the image tag),
  JDK version, and OS/arch alongside every run.

**One asymmetry deliberately left as-is, not force-fit into false equivalence:** this driver
materializes each row into a `Map<String, Object>` (see `RowBinaryDecoder`'s own Javadoc for the
lifetime-safety reason), while client-v2 reads typed values directly off its reader with no
intermediate map. Documented directly in `PointQueryBenchmark`'s Javadoc rather than silently
accepted — isolating it needs a *separate* transport-only benchmark (raw bytes, checksum, no decode
at all) sitting alongside the transport+decode one, not a rewrite of this one. Named as a concrete
next benchmark, not yet built.

**Re-run after the fixes, confirmed green (2026-08-13).** Same 1 fork/1×10s warmup/3×10s
measurement shape as before — still a sanity check, not a trustworthy baseline (JMH's own output
repeats that warning every run). New numbers, now on equal parameterization and a pinned server:

All latency rows are µs/op (lower = faster). **Reading this table: "this driver is X% FASTER"
means this driver's latency number is X% lower/better than client-v2's — the opposite wording
("SLOWER") means this driver's number is worse. Never read "higher"/"lower" alone as good or bad —
they only describe the raw number, not which direction is better.**

| | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| mean | 1142.8 µs | 1075.1 µs | **this driver 5.9% FASTER** |
| p50 | 1122.3 µs | 1058.8 µs | **this driver 5.7% FASTER** |
| p90 | 1226.8 µs | 1155.1 µs | **this driver 5.8% FASTER** |
| p95 | 1280.0 µs | 1189.9 µs | **this driver 7.0% FASTER** |
| p99 | 1577.0 µs | 1273.9 µs | **this driver 19.2% FASTER** |
| p99.9 | 2564.0 µs | 1904.9 µs | **this driver 25.7% FASTER** |
| sample count (30s) | 26,227 (≈874 ops/s) | 27,884 (≈930 ops/s) | **this driver 6.3% higher throughput** |

Notable shift from the pre-fix run: the gap **widens at the tail** (p99/p99.9) rather than staying
flat — this driver's tail latency held closer to its own median than client-v2's did, even in this
single-connection, zero-concurrency scenario. Consistent with, but not yet proof of, the stability-
under-load claim the architecture is actually designed for — that needs the concurrency/burst
benchmark (not built yet) to test directly, not inferred from a serial point-query benchmark.
Treat this table the same way as before: one short single-fork run, not a published claim.

### `TrivialQueryBenchmark`, run for real (2026-08-13)

`SELECT 1`, no table — isolates protocol/connection overhead from the storage-engine lookup
`PointQueryBenchmark` also pays for. Same fork/warmup/measurement shape as the runs above; run
alongside a repeat of `PointQueryBenchmark` in the same invocation (confirmed still green, same
5–7% mean/median edge as the fairness-fixed run above — not re-tabulated here since nothing about
it changed).

All latency rows are µs/op (lower = faster; verdict column spells out which side wins, see the
reading note on the `PointQueryBenchmark` table above).

| | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| mean | 591.1 µs | 548.5 µs | **this driver ≈7.2% FASTER** |
| p50 | — | — | **this driver ≈6.3% FASTER** |
| p90 | — | — | **this driver ≈10.6% FASTER** |
| p95 | — | — | **this driver ≈10.1% FASTER** |
| p99 | — | — | **this driver ≈7.2% FASTER** |
| p99.9 | — | — | **this driver ≈31.6% FASTER** |
| p99.99 | 2586.8 µs | 4650.7 µs | **this driver SLOWER here** (only percentile where it loses) |
| p100 (max) | 15106.0 µs | 8847.4 µs | **this driver 41.4% FASTER** |
| sample count (30s) | 50,699 (≈1690 ops/s) | 54,652 (≈1822 ops/s) | **this driver ≈7.8% higher throughput** |

Same shape as `PointQueryBenchmark`: this driver reads faster from the mean through p99.9, and the
gap widens sharply at p99.9 (31.6%, the widest yet). Two honest caveats, not smoothed over:

- **p50/p90/p95/p99/p99.9 rows are percentage deltas only** — the absolute µs figures from this run
  weren't retained, only the computed percentages against client-v2. Re-run and capture raw JSON
  output (`build/results/jmh/`) before citing this table anywhere more permanent than this doc.
- **p99.99 flips against this driver, in both benchmarks run so far.** `PointQueryBenchmark`'s
  re-run above already showed this driver worse at p99.99/p100; here it's worse at p99.99 but
  better again at p100/max. With only ~1 sample in 10,000–50,000 landing in that bucket, this is
  far more likely sampling noise than a real regression — but it's now shown up twice, not once, so
  it's a "watch, don't ignore" finding rather than a one-off. Worth a real look once
  `StreamingScanBenchmark`/`ConcurrencyBenchmark` exist and a profiler (JMH's own `-prof gc`/async-
  profiler) can attribute it to GC, connection-pool churn, or something else, rather than guessing
  from percentile tables alone.

One environment detail surfaced in this run's logs, not yet acted on: this driver's Reactor Netty
stack logs `Unable to load io.netty.resolver.dns.macos.MacOSDnsServerAddressStreamProvider` on
macOS (missing `netty-resolver-dns-native-macos`, a native optional dependency). Deliberately not
added yet — this benchmark resolves `localhost` only, which doesn't hit the DNS resolver Netty is
warning about, so there's no concrete reason yet to believe this warning explains the p99.99
finding above. Worth revisiting with actual profiling evidence before spending a dependency-version
matching exercise on a hunch.

### `StreamingScanBenchmark`, run for real (2026-08-13) — first result where this driver is slower

Full scan (`SELECT id, label, amount FROM benchmark_point_query`, `smoke` tier, 10,000 rows), no
`WHERE`. Same run also re-executed `PointQueryBenchmark`/`TrivialQueryBenchmark` (a third
confirmation of both — same shape as the tables above, exact numbers below), from the JSON result
file (`-rf json`), not a console paste, so every figure here is exact:

**Superseded by the confirmed, corrected 3-tier result further below — kept only as the historical
record that first surfaced this as worth investigating.** Latency rows are µs/op (lower = faster):

| | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| mean | 3946.5 µs | 4460.8 µs | **this driver 13.0% SLOWER** |
| p50 | 3846.1 µs | 4366.3 µs | **this driver 13.5% SLOWER** |
| p90 | 4816.9 µs | 5210.1 µs | **this driver 8.2% SLOWER** |
| p95 | 5103.6 µs | 5390.3 µs | **this driver 5.6% SLOWER** |
| p99 | 5513.2 µs | 5857.3 µs | **this driver 6.2% SLOWER** |
| p99.9 | 6363.9 µs | 8266.1 µs | **this driver 29.9% SLOWER** |
| p99.99 / p100 (max) | 21266.4 µs | 26247.2 µs | **this driver 23.4% SLOWER** |
| sample count (30s) | 7,597 (≈253 ops/s) | 6,723 (≈224 ops/s) | **this driver 11.5% lower throughput** |

**This driver is slower here, at every percentile, not just the tail — the first benchmark in this
suite where that's true.** Not glossed over: `PointQueryBenchmark`/`TrivialQueryBenchmark` both
decode exactly *one* row per operation; `StreamingScanBenchmark` decodes 10,000. Leading hypothesis,
not yet confirmed: the per-row `LinkedHashMap` allocation `RowBinaryDecoder.emitNextRow` does for
every row (documented as a known, deliberate asymmetry in `PointQueryBenchmark`'s own Javadoc,
tolerable at n=1 row) stops being negligible at n=10,000 rows per operation — 10,000 map allocations
(plus their internal `Node` entries) is a plausible dominant cost that a single-row benchmark simply
can't surface. `client-v2`'s `ClickHouseBinaryFormatReader` reads typed values off its reader
directly, no intermediate collection per row.

**Not yet confirmed, and deliberately not acted on until it is** — per this project's own testing
discipline (assert on behavior/measurements, not assumptions): the next concrete step is re-running
`StreamingScanBenchmark` with JMH's `GCProfiler` (`-Pjmh.profilers=gc` / `-prof gc` — already named
in this section's own "What's measured, and how" design for exactly this scenario) to check
`gc.alloc.rate.norm` for `ourDriver` vs `clientV2` directly, rather than inferring allocation cost
from latency numbers alone. If allocation rate confirms the hypothesis, a follow-up benchmark
variant that reuses/pools row objects (or a decode path that never materializes a
`Map<String,Object>` at all) becomes a concrete, evidence-backed thing to try — not before.

Time-to-first-row (the `HdrHistogram`-based custom metric `StreamingScanBenchmark` also records)
isn't in this run's JSON — that's logged to the console via SLF4J at trial teardown, not into JMH's
own result file. Not captured this run; worth asking for the console log (or re-running with
`tee`) next time TTFR specifically matters.

### Correction: the "13% slower" result above has a real methodology bug (2026-08-13)

Caught by review, not by re-running: `ourDriver`'s time-to-first-row instrumentation called
`AtomicLong#compareAndSet` unconditionally on **every** row, not just the first —

```java
.doOnNext(row -> {
    firstRowNanos.compareAndSet(-1, System.nanoTime());   // CAS + System.nanoTime() every row
    blackhole.consume(row);
})
```

— while `clientV2`'s equivalent check was already the cheap plain branch:

```java
if (firstRowNanos == -1) {
    firstRowNanos = System.nanoTime();   // only on the first row
}
```

At 10,000 rows that's 10,000 unconditional `System.nanoTime()` calls plus 10,000 CAS operations on
`ourDriver`'s side, against effectively one of each on `clientV2`'s — instrumentation overhead
baked directly into the very numbers the table above reports, not something outside the measured
window. **The table above should not be read as a real performance comparison until re-run** — it
may be measuring this driver's TTFR instrumentation cost as much as its actual decode/transport
cost. Left in place (not deleted) as the historical record of the bug, not as a trustworthy result.

Fixed in `StreamingScanBenchmark.ourDriver`: the first-row check is now a plain array-backed
flag (`long[] firstRowNanos = {-1L}`, checked with a plain `==` and set with a plain assignment),
matching `clientV2`'s shape exactly — no `AtomicLong`, no CAS. Safe because `RowBinaryDecoder
.decodeRows` applies no `publishOn`/`subscribeOn`, so `Flux.generate` emits every row synchronously
on the calling thread; there was never a real cross-thread visibility need the CAS was buying.

**Two more fixes made at the same time, per the same review, before any re-run:**

- **Dataset now scales.** `@Param({"10000", "100000", "1000000"})` instead of a single 10,000-row
  tier. A single small tier can't distinguish fixed per-request overhead (HTTP round trip, query
  startup, first-chunk latency) from genuine per-row streaming cost — if a gap is flat across tiers
  it's the former; if it grows with `rows` it's the latter. `10_000_000`+ ("large" tier) stays a
  manual, opt-in edit for a release-gate run, not routine iteration.
- **Rows/sec, derived, not a new metric.** No new JMH instrumentation added (avoiding stacking
  another "did we get the benchmark code itself right" risk on top of the one just found) — computed
  from a completed run's own numbers: `rows / (mean_us / 1_000_000)`. Recorded per tier the next
  time this table is filled in for real.

**Deliberately not done, per the same review's own sequencing:** no change to
`RowBinaryDecoder`/production code. The per-row `Map<String, Object>` allocation hypothesis from the
section above is still just a hypothesis — confirming or ruling it out needs the fixed benchmark's
own numbers across 10k/100k/1M first (does the gap grow with `rows`, or stay flat?), and `-prof gc`
after that if it does grow. Acting on a number produced by a buggy benchmark would have meant
"fixing" a problem that may not exist.

### `StreamingScanBenchmark`, re-run with the TTFR fix and three tiers (2026-08-13) — confirmed real

**This is the trustworthy result — read this table, not the two above.** Fixed TTFR instrumentation
(no more CAS-per-row), three row-count tiers, from JSON (`-rf json`), exact figures. Latency rows
are µs/op — **lower is faster; the verdict column always names which driver wins:**

| rows | client-v2 mean | this driver mean | verdict | client-v2 rows/s | this driver rows/s |
| --- | --- | --- | --- | --- | --- |
| 10,000 | 4,038.6 µs | 4,575.3 µs | **this driver 13.3% SLOWER** | 2.48 M | 2.19 M |
| 100,000 | 21,948.3 µs | 33,961.3 µs | **this driver 54.7% SLOWER** | 4.56 M | 2.94 M |
| 1,000,000 | 153,972.2 µs | 276,924.2 µs | **this driver 79.9% SLOWER** | 6.49 M | 3.61 M |

p50/p99 confirm the same trend, not just the mean:

| rows | p50 verdict | p99 verdict |
| --- | --- | --- |
| 10,000 | this driver 13.0% SLOWER | this driver 13.1% SLOWER |
| 100,000 | this driver 55.8% SLOWER | this driver 53.4% SLOWER |
| 1,000,000 | this driver 80.0% SLOWER | this driver 68.0% SLOWER |

**Answer to this section's own question ("does the gap grow with rows, or stay flat?"): it grows,
sharply and monotonically — 13% → 55% → 80%.** This rules out fixed per-request overhead (HTTP
round trip, query startup, first-chunk latency) as the explanation — a fixed cost would shrink as a
*percentage* of a longer-running operation, not grow six-fold. **This driver's per-row decode path
is now a confirmed, real, worth-fixing regression, not a benchmark artifact.** At the same time:
`TrivialQueryBenchmark`/`PointQueryBenchmark` (one row decoded) still favor this driver by 5–11%,
unaffected by any of this — the fixed-request path is genuinely good; the *sustained streaming*
path is genuinely not, yet. Both things are true at once, and the README/any external claim must
say both, not average them into one number.

### Optimization phase: what's actually in the code, and the investigation plan

An external review (uploaded findings/optimization-plan document, cross-checked against this
repository's real source below, not taken at face value) independently converged on the same
"per-row decode/materialization" diagnosis and proposed a ranked hypothesis list and a
profile-before-you-touch-anything methodology. Verified against the actual code:

- **H1: per-row `LinkedHashMap` copy — confirmed to exist, and sharper than the external review's
  own framing once client-v2's actual internals are read (the mounted `clickhouse-java` source, not
  assumed).** `RowBinaryDecoder.emitNextRow` does `sink.next(new LinkedHashMap<>(reader.next()))`
  for every row. Traced what `reader.next()` itself costs on client-v2's side
  (`AbstractBinaryFormatReader`): it does **not** allocate a fresh `Map` or array per row either —
  it keeps two reused `Object[]` buffers (`currentRecord`/`nextRecord`) that are swapped, not
  reallocated, each call, and wraps whichever one is current in a lightweight `RecordWrapper` (a
  `Map` facade over the array, name→index lookup, no hash table built). So client-v2's own per-row
  cost is one small wrapper object plus the unavoidable boxed value objects (`Long`/`String`/
  `BigDecimal` — this driver must allocate the same ones). **Our `new LinkedHashMap<>(...)` copy
  constructor then iterates that wrapper's entries and builds a brand-new hash table — real hashing,
  real `Node` allocations, an extra cost client-v2's own path never pays**, on top of the values
  both sides already allocate. This sharpens the "preferred direction": copying each row into a
  plain `Object[]` (with once-per-query shared column-name→index metadata for name-based lookup, not
  a `Map` at all) would drop the per-row rehash entirely while keeping the same lifetime-safety
  reasoning `RowBinaryDecoder`'s own Javadoc already gives for not exposing client-v2's `RecordWrapper`
  directly. Still a hypothesis about *how much* it costs, not a measurement, until an allocation
  profiler says so — but now a precisely-scoped one, not a guess.
- **H0 (new — found by reading `FluxInputStreamBridge`/client-v2's `BinaryStreamReader` directly,
  not in the external review): `InputStream.read()`'s single-byte overload allocates a fresh
  `byte[1]` on every call.**
  ```java
  // FluxInputStreamBridge
  @Override
  public int read() throws IOException {
    final byte[] singleByte = new byte[1];   // new allocation, every call
    final int bytesRead = read(singleByte, 0, 1);
    return bytesRead == -1 ? -1 : singleByte[0] & 0xFF;
  }
  ```
  Traced client-v2's `BinaryStreamReader` (the mounted `clickhouse-java` source, not assumed):
  fixed-width columns (`UInt64`, the `Decimal` this table uses) go through `readNBytes`, which loops
  on the bulk `read(byte[], offset, length)` overload — no extra allocation there. But
  `readVarInt`/`readByteOrEOF` — used for every `String` column's length prefix, called once per row
  for `PointQueryTable`'s `label` column — call the single-byte `read()` directly. At 1,000,000 rows
  that's 1,000,000 avoidable tiny-array allocations, on top of H1's `LinkedHashMap` cost, purely
  from this adapter class's naive single-byte path. A real finding, cheap to fix (a reusable
  one-element buffer field — this class is only ever read by one dedicated worker thread, per its
  own Javadoc, so no thread-safety concern), but **not yet fixed** — same discipline as everything
  else here: measure its actual share of allocations first, don't fix on inspection alone.
- **H2: the blocking bridge itself (`FluxInputStreamBridge` → client-v2's blocking reader).**
  Queue/synchronization/wakeup cost per chunk hand-off — real, but this project's architecture
  accepts a bounded, backpressure-respecting blocking bridge as a deliberate interim design (see
  this class's own Javadoc); the question here is only how much of the *measured* cost it accounts
  for, not whether to remove it outright.
- **H3: `ByteBuf → byte[] → ByteBuffer` in `.asByteArray().map(ByteBuffer::wrap)`.** Verified this
  is real production code, not just the benchmark harness — `ClickHouseResult.java:69` uses the
  identical pattern the benchmarks do. Lower priority than H0/H1: this copies once per network
  chunk (tens to low thousands for a 1M-row response), not once per row.
  H4: `RESPONSE_CHUNK_DEMAND = 4` (already documented in `RowBinaryDecoder` as an unbenchmarked
  placeholder) — plausible for sustained-throughput pipelining, independent of H0/H1/H3.
  H5: `Flux.generate`'s own per-row Reactor machinery — lowest priority, investigate only if a
  meaningful gap remains after H0/H1/H3/H4 are quantified and addressed.

**Investigation plan, in order — profile before touching `RowBinaryDecoder` or
`FluxInputStreamBridge`, per this project's own testing discipline (measure, don't infer):**

1. **JMH's built-in GC profiler** (`-prof gc`, no extra tooling needed) on `StreamingScanBenchmark`
   at `rows=1000000` for both drivers — `gc.alloc.rate.norm` (bytes/op) directly answers "how many
   bytes does each driver allocate to decode 1M rows," and dividing by 1,000,000 gives allocated
   bytes/row, the first number this investigation actually needs.
2. **Two new diagnostic isolation benchmarks** (below), to split "is it transport or is it decode"
   and "is it the `Map` copy or something else in decode" instead of guessing from one combined
   number.
3. **CPU/allocation flame graph** (`async-profiler`, if/when set up — not yet part of this project's
   tooling) only if steps 1–2 don't already make the dominant cost obvious.
4. **Only then**, a benchmark-only prototype of a `Map`-free row representation (e.g. `Object[]` +
   shared column-name-to-index metadata) to quantify how much of the gap it actually closes, before
   any production `RowBinaryDecoder`/`ClickHouseRow` change.

**Two new diagnostic benchmarks written (not yet run):**

- `TransportOnlyStreamingBenchmark` — `ClickHouseHttpTransport.query(...)` consuming raw bytes
  (chunk-length sum only, no `RowBinaryDecoder` involved at all) vs client-v2's own
  `QueryResponse#getInputStream()` drained in a plain byte-counting loop. Answers whether H2/H3
  (bridge, byte-array copying) account for a meaningful share on their own, with decode removed from
  the picture entirely. Same `rows` tiers as `StreamingScanBenchmark`.
- `DecoderOnlyBenchmark` — captures one full response body once per trial (outside the measured
  region, via `.aggregate().asByteArray()`), then benchmarks `RowBinaryDecoder.decodeRows` vs
  client-v2's `RowBinaryWithNamesAndTypesFormatReader` (constructed directly, no `Client`/
  `QueryResponse` — the exact class `RowBinaryDecoder` itself wraps) over the *same* captured bytes,
  no network at all. Answers whether H0/H1 (decode/materialization) account for the gap in isolation
  from transport.

Run both:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes='TransportOnlyStreamingBenchmark|DecoderOnlyBenchmark'
```

### Both isolation benchmarks run for real (2026-08-13) — the question is answered

Same invocation also re-ran `StreamingScanBenchmark`; its combined numbers this run (6.0% / 53.7% /
98.2% slower, 10k/100k/1M) differ slightly from the earlier confirmed run (13.3% / 54.7% / 79.9%) —
normal run-to-run variance, same qualitative shape (grows sharply with rows). This run's own
combined figures are the ones used in the "sum of parts" comparison below, for internal consistency
within one JMH invocation.

**Transport is a genuine strength. Decode is the entire problem.** From JSON, exact figures. All
latency is µs/op (lower = faster):

| rows | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| 10,000 | 2,602.8 | 1,265.1 | **this driver 51.4% FASTER** |
| 100,000 | 12,273.8 | 4,562.4 | **this driver 62.8% FASTER** |
| 1,000,000 | 67,045.4 | 14,605.8 | **this driver 78.2% FASTER (4.6x)** |

`TransportOnlyStreamingBenchmark` (raw bytes, zero decode) — this driver's Reactor Netty transport
beats client-v2's blocking HTTP client outright, and the margin **grows** with response size. This
is a real, previously-invisible advantage that `StreamingScanBenchmark`'s combined number was
masking the whole time.

| rows | client-v2 | this driver | verdict |
| --- | --- | --- | --- |
| 10,000 | 843.1 | 2,111.3 | **this driver 150.4% SLOWER (2.5x)** |
| 100,000 | 7,484.3 | 21,595.6 | **this driver 188.5% SLOWER (2.9x)** |
| 1,000,000 | 81,547.1 | 222,911.7 | **this driver 173.4% SLOWER (2.7x)** |

`DecoderOnlyBenchmark` (same captured bytes, in memory, zero network) — client-v2 decodes
**~12–13M rows/sec** consistently across all three tiers; this driver decodes **~4.5–4.7M rows/sec**,
also consistently across tiers. The ratio (≈2.5–2.9x) barely moves with row count, unlike the
combined benchmark's growing percentage — this is a flat, structural per-row cost, exactly what H0
(the `byte[1]` allocation) and H1 (the `LinkedHashMap` rehash) predict, not a scaling artifact.

**One more real, secondary finding — the sum of the two isolated benchmarks doesn't equal the
combined `StreamingScanBenchmark` number, and the gap itself differs by driver:**

| rows | this driver: transport+decode | this driver: combined | extra | client-v2: transport+decode | client-v2: combined | extra |
| --- | --- | --- | --- | --- | --- | --- |
| 10,000 | 3,376.4 | 4,324.6 | +28.1% | 3,445.9 | 4,078.6 | +18.4% |
| 100,000 | 26,158.1 | 33,488.2 | +28.0% | 19,758.1 | 21,793.7 | +10.3% |
| 1,000,000 | 237,517.5 | 290,081.1 | +22.1% | 148,592.5 | 146,387.1 | **−1.5%** |

client-v2's combined number is close to (even slightly under, at 1M — within sampling noise) the
sum of its own parts, consistent with its simple blocking "read network, then decode" sequencing.
This driver's combined number runs a consistent 22–28% **above** the sum of its own parts — a real,
separate cost the two isolation benchmarks don't capture individually.

**Correction (2026-08-13, caught by a follow-up review before this was overstated further): that
22–28% gap is not proven to be `FluxInputStreamBridge` specifically.** `StreamingScanBenchmark` and
`TransportOnlyStreamingBenchmark`/`DecoderOnlyBenchmark` are not mathematically equivalent —
the live combined pipeline also has network chunking, producer/consumer overlap, decoder demand,
and scheduling effects that summing two separately-run isolated benchmarks doesn't reproduce; on
top of that, `DecoderOnlyBenchmark` decodes one aggregated in-memory payload, not the same fragmented
stream a live network response actually arrives as. Correct framing: this is **additional
live-pipeline integration cost** — `FluxInputStreamBridge`'s queue/wakeup hand-off is a strong
candidate for it, given the class's own design, but it is not yet isolated as *the* cause the way
H0/H1 now are. A future benchmark that replays a captured payload through the same chunk boundaries
and timing a live response would have would be needed to actually attribute this delta.

**Conclusion, stated with the confidence the data actually supports: this driver's non-blocking
transport is not the bottleneck — it's this driver's best-measured strength, and the combined
`StreamingScanBenchmark` number was hiding that fact. The large majority of the streaming regression
lives in decode/materialization (H0 + H1), with a smaller, not-yet-precisely-attributed
live-pipeline integration cost on top.** H3 (`ByteBuf`→`byte[]` copying) is **ruled out as the
primary/dominant cause** — the transport benchmark that pays that exact cost is the one where this
driver wins by up to 4.6x — but not proven to contribute zero cost; `TransportOnlyStreamingBenchmark`
demonstrates it isn't dominant, not that it's free.

**Caveats carried over from JMH's own output, not glossed over:** this run's JMH reported it used
an *experimental* "Compiler Blackholes" mode and explicitly warned to "exercise extra caution when
trusting the results" and to keep Blackhole mode consistent across any numbers being compared —
this run's own four benchmark classes all ran in the same invocation, so mode is consistent
*within* this run, but any future comparison against an older run's numbers should be re-run fresh
rather than diffed against historical figures. One point-in-run wobble, noted not hidden:
`PointQueryBenchmark` in this same run showed this driver and client-v2 essentially tied (1164.0 µs
vs 1159.1 µs, 0.4% apart) rather than this driver's usual 5–7% edge seen in three prior runs —
treated as this run's noise, not a reversal, given the weight of prior evidence; `TrivialQueryBenchmark`
in the same run still showed the usual ≈8.5% edge.

**Next step, sharpened by this result:** `DecoderOnlyBenchmark` is now the right target for `-prof
gc` — it isolates decode from network entirely, so an allocation profile of it directly attributes
bytes/row to H0 vs H1 without any transport noise in the way:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=DecoderOnlyBenchmark -Pjmh.profilers=gc
```

### `-prof gc` on `DecoderOnlyBenchmark`, run for real (2026-08-13) — H1 confirmed dominant, H0 minor

First real payoff of fixing `-Pjmh.includes`/`-Pjmh.profilers` (see the build.gradle.kts fix above,
also committed) — this run took 4m33s and executed only `DecoderOnlyBenchmark`, not the whole suite.
`gc.alloc.rate.norm` (bytes allocated per operation, JMH's own metric) ÷ `rows` gives allocated
bytes/row directly:

| rows | client-v2 B/row | this driver B/row | ratio | extra B/row (this driver) |
| --- | --- | --- | --- | --- |
| 10,000 | 296.1 | 835.7 | 2.82x | +539.6 (high error bars this tier — noisiest sample count) |
| 100,000 | 296.0 | 872.1 | 2.95x | +576.0 |
| 1,000,000 | 296.0 | 872.0 | 2.95x | +576.0 |

**client-v2 allocates a remarkably stable ~296 bytes/row at every tier — this driver allocates a
remarkably stable ~872 bytes/row at 100k/1M (872.1 and 872.0 — agreement to three significant
figures across a 10x row-count difference is strong evidence this is a real, structural per-row
cost, not noise).** The allocation ratio (≈2.95x) tracks the latency ratio measured earlier
(≈2.7–2.9x) closely — allocation pressure is very likely the direct mechanical cause of the latency
gap, not a coincidental correlation with some unrelated CPU cost.

**Attributing the extra ≈576 bytes/row to H0 vs H1, from what the code actually does:**
`RowBinaryDecoder.emitNextRow` calls `reader.next()` (client-v2's own decode — the same ~296
bytes/row client-v2's own benchmark pays) and *then* wraps that result in `new LinkedHashMap<>(...)`
— this driver structurally pays client-v2's entire cost first, plus its own map materialization on
top. A `byte[1]` allocation (H0) is a small, fixed ~24 bytes (a 1-byte array's header/padding on a
typical 64-bit JVM with compressed oops) and fires once per row for this table (one varint length
prefix, for the `label` String column) — accounting for roughly 4% of the extra 576 bytes, at most.

**Correction (2026-08-13, caught before this was overstated further): the remaining ≈552 bytes/row
is not proven to be the `LinkedHashMap` copy specifically — that number was derived by subtracting
H0's estimate from the total, which silently folds in every other difference between the two
decode paths too: `FluxInputStreamBridge`'s own `StreamSignal`/`BlockingQueue` machinery (present in
this driver's path, absent from client-v2's baseline, which reads a plain `ByteArrayInputStream`
with no bridge at all), `Flux.generate`'s per-row Reactor state, and `ListDecodingRowBinaryReader`
vs. the base reader class. Subtraction across a multi-variable difference isn't attribution — it's
a total.** H1 is still the strongest candidate (matches the ranked-hypothesis reasoning and the
external review's own read of the code), but **not yet quantified**. Quantifying it needs a
single-variable-change benchmark — see `ourDriverWithoutMapCopy` below — not more arithmetic on the
existing numbers.

One more supporting observation: `gc.alloc.rate` (MB/sec, not bytes/op) came out similar for both
drivers (≈3.1–3.8 GB/sec, all tiers, both drivers) — consistent with both drivers running against
roughly the same machine-level allocation/GC throughput ceiling, which means bytes-per-row is
functioning as a direct, near-linear lever on this driver's own latency here, not a number that's
disconnected from what's actually slow.

**What this changes about the plan:** H0 is real, cheap, and safe to fix immediately — a reusable
single-element buffer in `FluxInputStreamBridge.read()` instead of `new byte[1]` every call, no
design decision involved, easy to verify in isolation. H1 is not a quick fix — it means either
changing what `RowBinaryDecoder.decodeRows`/`RowBinaryDecoder.decode` hand back (today's public
`Flux<Map<String, Object>>` / `DecodedResult` shape) or adding a leaner internal path underneath it,
and needs to account for every caller of that API (`connector`'s `ClickHouseResult`/`ClickHouseRow`,
this benchmark module) before anything changes — a real design task, not a one-line fix. Sequencing:
fix and verify H0 first (isolated, low-risk, real but modest win — expect `DecoderOnlyBenchmark` to
drop by roughly the ~24-bytes-of-576 share, a few percent, not a game-changer on its own), *then*
scope the `LinkedHashMap`-avoidance design for H1 with the full picture of what depends on the
current row shape.

**H0 fixed and confirmed by a real re-run (2026-08-13).** `FluxInputStreamBridge` now has a
`private final byte[] singleByteReadBuffer = new byte[1]` field, reused by `read()` instead of
allocated per call — safe because this class's own Javadoc already establishes it's read by exactly
one dedicated worker thread, never concurrently. Also closed a real black-box test gap while here: no
existing `FluxInputStreamBridgeTest` test exercised the single-byte `read()` overload at all
(`readAllBytes()`, used by every existing test, only calls the bulk `read(byte[], int, int)` overload
internally) — added `shouldReadOneByteAtATimeViaTheSingleByteReadOverload`.

Re-running `DecoderOnlyBenchmark -Pjmh.profilers=gc` after the fix:

| rows | this driver B/row, before H0 | this driver B/row, after H0 | reduction |
| --- | --- | --- | --- |
| 100,000 | 872.1 | 848.05 | −24.0 |
| 1,000,000 | 872.0 | 848.01 | −24.0 |

A clean, reproducible ~24 bytes/row reduction at both tiers — matches the predicted fixed cost of one
`byte[1]` header exactly. Latency barely moved (≈227ms → ≈226ms at 1M), confirming H0 was real but
never the dominant term. **H0 is closed.**

**`DecoderOnlyBenchmark.ourDriverWithoutMapCopy` run for real (2026-08-13) — H1 confirmed, not just
suspected.** Same `FluxInputStreamBridge` (with the H0 fix), same
`RowBinaryWithNamesAndTypesFormatReader` settings, same `Flux.generate` per-row emission shape as
`ourDriver` — the *only* difference is that `reader.next()` is still called (client-v2's own per-row
cost is still paid, unchanged) but its result is discarded instead of copied into
`new LinkedHashMap<>(...)`. A genuine single-variable-change isolation, not a subtraction.

| rows | `ourDriver` B/row | `ourDriverWithoutMapCopy` B/row | H1 cost (B/row) | `ourDriver` latency | `ourDriverWithoutMapCopy` latency |
| --- | --- | --- | --- | --- | --- |
| 100,000 | 848.05 | 272.05 | 576.0 | 22.71 ms | 5.27 ms |
| 1,000,000 | 848.01 | 272.01 | 576.0 | 224.98 ms | 51.31 ms |

**H1 = 576.0 bytes/row, agreeing to four significant figures across a 10x row-count change — as clean
an isolation result as this investigation has produced.** That's the entire original ≈576 bytes/row
gap this investigation started from: H0's ~24 B/row plus H1's ~576 B/row account for essentially all
of the allocation difference between this driver and client-v2, with nothing large left unattributed.
Latency: removing the map copy cuts `ourDriver`'s decode time by ~77% (4.38x) at 1M rows — H1 is not
just an allocation cost, it's the dominant CPU cost in the decode path.

**Mechanically confirmed against the actual `clickhouse-java` source (read directly, not taken from
the external review at face value) — `AbstractBinaryFormatReader.RecordWrapper`:**

```java
// RecordWrapper — private static nested class
private final WeakReference<Object[]> recordRef;
private final WeakReference<TableSchema> schemaRef;

@Override
public Set<Entry<String, Object>> entrySet() {
  int i = 0;
  Set<Entry<String, Object>> entrySet = new HashSet<>();
  for (ClickHouseColumn column : schemaRef.get().getColumns()) {
    entrySet.add(new AbstractMap.SimpleImmutableEntry(column.getColumnName(), recordRef.get()[i++]));
  }
  return entrySet;
}
```

`reader.next()` itself is cheap — it swaps the reused `currentRecord`/`nextRecord` `Object[]` buffers
(allocated once, not per row) and wraps whichever is current in one `RecordWrapper`. But
`new LinkedHashMap<>(reader.next())` runs `HashMap.putMapEntries`, which iterates
`recordWrapper.entrySet()` — and that method allocates a fresh `HashSet` plus one
`SimpleImmutableEntry` per column on *every single call*, which `LinkedHashMap`'s own constructor
then copies into a second hash table (one `LinkedHashMap.Entry` per column). For this three-column
table, one row emission is: `RecordWrapper` + 2×`WeakReference` + `HashSet` + 3×`SimpleImmutableEntry`
+ `LinkedHashMap` + 3×`LinkedHashMap.Entry` — eight-plus objects to move three already-decoded values
into a map client-v2 never needed to build in the first place. The measured 576 bytes/row is fully
explained by real, read source code, not inference.

**H1 is closed — confirmed both experimentally and mechanically. Full decomposition, now measured end
to end:**

| stage | B/row |
| --- | --- |
| pre-H0 driver | 872.0 |
| − H0 (`byte[1]`) | −24.0 |
| post-H0 driver | 848.0 |
| − H1 (`LinkedHashMap` copy) | −576.0 |
| without map copy | 272.0 |

**One caveat, important not to overstate:** `ourDriverWithoutMapCopy` (272 B/row, ~51.3ms at 1M) comes
in *below* client-v2's own baseline (296 B/row, ~88.1ms) — but the two benchmark bodies aren't doing
equivalent work. `clientV2` calls `getLong`/`getString`/`getBigDecimal` per row (materializing and
consuming three typed values); `ourDriverWithoutMapCopy` calls `reader.next()` and discards the result
without touching any column value. **This does not yet mean "our decode is faster than client-v2"** —
it means the bridge/Reactor/`Flux.generate` machinery isn't itself responsible for the regression,
which is the question this diagnostic was built to answer. A fair speed claim needs the actual
production replacement (compact row, real per-value access) benchmarked against client-v2's
getter-based access, not this discard-only diagnostic.

**Architectural implication, now evidence-backed rather than inspection-only:** the fix is not
`sink.next(reader.next())` — `RecordWrapper` holds its row through `WeakReference`s into client-v2's
*reused* `currentRecord`/`nextRecord` buffers, so exposing it downstream would hand callers a view
that can change or go stale as decoding continues. What this data supports: replace
`Flux<Map<String, Object>>` with a compact per-row `Object[]` snapshot (one copy, no hash table) plus
once-per-result shared column-name→index metadata, matching R2DBC's index/name row-access shape
directly instead of routing through a `Map`. Not yet built — next step is a small production-shaped
prototype benchmarked with real value access (not a discard-only diagnostic) before committing to the
full redesign.

**Guardrail, explicit:** every future change from this investigation reruns
`TrivialQueryBenchmark`/`PointQueryBenchmark` alongside `StreamingScanBenchmark`'s three tiers — a
streaming fix that regresses the fixed-request path (this driver's genuine current strength) is not
an acceptable trade. Concurrency (`ConcurrencyBenchmark`, Level 3) stays queued behind this
investigation — per this project's own priority reasoning, but now the priority is real:
`StreamingScanBenchmark` surfaced an actual, sizable regression concurrency work shouldn't be built
on top of unmeasured.

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

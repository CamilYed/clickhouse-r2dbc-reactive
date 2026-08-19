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
- [Phase 5 (later) — Load and performance testing](docs/PERFORMANCE.md)
- [Phase 6 (later) — Spring WebFlux interop demo](#phase-6-later--spring-webflux-interop-demo)
- [Phase 7 — Operational control & R2DBC correctness (0.2.0)](#phase-7--operational-control--r2dbc-correctness-020)
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
| `benchmarks` (`clickhouse-r2dbc-reactive-benchmarks`, Phase 5, in progress) | `core`, `transport-http`, `connector`, `testkit`, client-v2 (as the comparison baseline) | JMH throughput/latency/allocation measurement, this driver vs client-v2, at multiple levels (raw transport, public R2DBC SPI) — see [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for the full design. Not published: it's measurement tooling, not a library. | No |
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

---

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

---

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

---

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

---

## Phase 5 (later) — Load and performance testing

> [!NOTE]
> **Moved to [docs/PERFORMANCE.md](docs/PERFORMANCE.md) (2026-08-14).** This section had grown to
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
pooling](README.md#connection-pooling) in the README). A driver that's only "reactive" at the
`Publisher` type level but has an invisible, uncontrolled transport queue underneath is exactly the
failure mode [Why](README.md#why) names as this project's origin — closing that gap is more
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
6. **Netty leak-detection test lane.** A dedicated test task/lane run with an aggressive Netty leak
   detector, specifically covering cancellation, disconnect mid-response, decoder failure, timeout,
   retry, and downstream cancellation after a few records — the shapes most likely to strand a
   `ByteBuf`. Already named as important in [Non-functional
   requirements](#non-functional-requirements-logging-metrics-leaks); this is where it actually gets
   built.
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
- [ ] Cancellation/timeout/error paths leave no `ByteBuf` leaks (leak-detector lane passes) — **item
      6 was never actually implemented.** Checked directly, not assumed: no
      `-Dio.netty.leakDetection.level=paranoid` JVM arg exists anywhere in the build (`grep` across
      every `build.gradle.kts` finds nothing), and no PR in the [PR sequence](#pr-sequence) table
      above scoped it — the table jumps from item 5 (PR 4) to item 11 (PR 6) with no item-6 row.
      This box cannot honestly be checked yet; either scope a PR 9b for it before cutting `0.2.0`,
      or explicitly move it to a documented follow-up and re-word this line accordingly — but not
      silently claim done.
- [x] A test actively protects the Netty event loop from a blocking decode call — PR 6, the
      driver-owned `RowDecodingScheduler` plus its ownership/threading tests.
- [x] It's written down which R2DBC compatibility cases are supported vs. deliberately unsupported —
      PR 8, `docs/R2DBC_COMPATIBILITY.md` + `ClickHouseR2dbcSpiCompatibilityTest`.
- [x] README documents the outer R2DBC pool and inner transport pool as one coherent story —
      re-verified after PR 5; the "no R2DBC option" gap the section used to call out is closed.
- [ ] The release has a changelog entry, a Git tag, and a GitHub Release pointing at the same commit
      as the Maven Central artifact — PR 9 adds `CHANGELOG.md` and wires tag/Release creation into
      `release.yml`; this box checks once an actual `0.2.0` release runs through it end to end.
- [ ] A benchmark baseline is recorded (docs/PERFORMANCE.md) but is not a flaky PR gate — recorded
      for `0.1.0` already; re-confirm nothing in Phase 7 regressed it before cutting `0.2.0`.

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

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
| `integration-tests` | `connector`, `testkit` | Whole-driver, black-box tests: real ClickHouse via Testcontainers, exercised **only** through the public R2DBC SPI (`ConnectionFactory.create(...)` and onward) — never through `core`/`transport-http` internals. This is "one module where we run the whole thing and test it," as its own module so its (slow, Docker-backed) suite doesn't sit inside `connector`'s fast build loop. Test-only: no `src/main`. | No |
| `benchmarks` *(not built yet — Phase 5)* | `connector` | JMH/Gatling-style throughput and latency measurement against real ClickHouse. Named here so the eventual module boundary is decided in advance; not scaffolded until Phase 5 actually starts, per [CLAUDE.md](CLAUDE.md#performance-testing). | No |
| `examples/spring-boot-webflux-demo` *(not built yet — Phase 6)* | `connector` | A runnable Spring Boot + WebFlux app proving the driver works end to end through Spring's R2DBC integration, mirroring [`spring-reactive-transaction-boundary`](https://github.com/CamilYed/spring-reactive-transaction-boundary)'s demo. Not scaffolded until Phase 3 (R2DBC SPI) is far enough along to have something to demo. | No |

Why `integration-tests` is its own module and not just more tests inside `connector`: two
different things were both called "integration tests" before, and conflating them was the actual
confusion. `connector`'s own `src/test` still gets small, targeted tests for its own classes
(parameter binding, exception mapping) — some of those may use a real ClickHouse too. But the
suite that proves "the whole driver, used exactly the way a consumer would use it, against a real
server" belongs in its own module: it can only ever import `connector`'s public API (never an
internal class from `core`, because it's not even a dependency), it makes the slow Docker-backed
suite easy to run/skip independently (`./gradlew :clickhouse-r2dbc-reactive-integration-tests:test`),
and it's the natural home for the data-setup/cleanup Ability DSL requested below, since that DSL is
about ClickHouse-the-database, not about any one module's internals.

Why `testkit` keeps its name despite the expanded scope: the fake `ControlledClickHouseServer`
and the real-ClickHouse Testcontainers DSL are still one thing — "the shared support code so no
other module's tests need Mockito or ad-hoc infrastructure of their own" — which is exactly what
"testkit" means in other ecosystems (`kotlinx-coroutines-test`, `spring-kafka-test`). Note this is
a different concept from Gradle's built-in `java-test-fixtures` plugin feature (a source-set
convention for sharing test code *within* one module) — we don't need that here, a whole module is
the right unit because multiple *other* modules (`connector`, `integration-tests`) consume it.

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
| Nullable and optional | ⚠️ `Nullable` covered (including an actual `NULL` value across rows); `LowCardinality` not attempted |
| Specialized | ⚠️ `UUID` and `Enum8`/`Enum16` covered; geo types, vector-search (`QBit`), domains not attempted |
| Composite (`Array`/`Tuple`/`Map`/`Nested`) | ✅ `Map`/`Tuple`/`Array` covered; `Nested` uses the same mechanism as `Array` but has no dedicated real-ClickHouse test yet (low-risk follow-up) |
| Semi-structured (`JSON`/`Dynamic`/`Variant`) | 🚫 not attempted, still experimental in ClickHouse itself |
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
`ColumnMetadata`/`ReadableMetadata`/`Type` source, not `main`. Deliberate, documented gaps rather
than guesses: `ColumnMetadata.getJavaType()`/`Type.getJavaType()` don't attempt to predict the Java
class client-v2 will decode a column into ahead of reading any row — that mapping is an internal
decision of client-v2's `BinaryStreamReader` decode switch, not a static function of
`ClickHouseColumn`, and duplicating it here risked silently drifting from it; `getRowsUpdated()`
always completes empty, since ClickHouse's HTTP interface doesn't return update counts the way
R2DBC's `Result` assumes and this driver hasn't built an INSERT/DDL round trip that would need one
yet. `Result.filter`/`flatMap` are implemented (both are abstract in `r2dbc-spi:1.0.0.RELEASE`, no
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
  `RealWorldTableAgainstRealClickHouseTest`) already extend it. `connector`'s own tests and
  `integration-tests` extend it too once they exist — no per-module reimplementation. A richer
  Ability-pattern DSL for creating tables/rows (beyond the `RealClickHouseQueryAbility` already in
  `transport-http`'s tests) can grow here once `connector` actually needs one — TDD, same as
  everything else: the first test that needs it drives the method into existence, don't design the
  whole DSL up front.
- `testkit`: `ClickHouseRowAssert` (custom assertion over a decoded row) also moved here from
  `transport-http`'s test sources, so `connector`/`integration-tests` get it for free too. Grew
  `hasList`/`hasTuple`/`hasMap`/`hasEnumName`/`hasBigInteger`/`hasFloatCloseTo`/`hasInetAddress`/
  `hasUuid` alongside the original `hasValue`/`hasDecimal`/`hasNullAt`/`hasTypeAt`, so
  `RealWorldTableAgainstRealClickHouseTest` reads its assertions as domain statements instead of
  raw `assertThat((SomeType) row.get(...))` casts scattered through the test body.
- `connector`'s own `src/test`: small, targeted tests for its own classes, extending
  `BaseClickHouseIntegrationTest` where real ClickHouse is the simplest way to prove behavior.
- `integration-tests`: whole-driver black-box suite through the public R2DBC SPI only — see the
  [module map](#module-map) for why this is separate from `connector`'s own tests.
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

Explicitly deferred, per [CLAUDE.md](CLAUDE.md#performance-testing). Not started until Phase 4 is
signed off. When it starts:

- Throughput, p50/p95/p99 latency, time to first row.
- Allocation/retained memory under streaming.
- Many-small-request workloads (the original production burst scenario — ~11 concurrent queries
  per user action) vs. large single streaming results.
- Comparison against the existing `ClickHouse/clickhouse-java` R2DBC driver as a baseline.
- Tooling choice (JMH for micro-benchmarks, Gatling/similar for load scenarios) decided at that
  point — nothing added to the build now.

## Phase 6 (later) — Spring WebFlux interop demo

Explicitly deferred until Phase 3 has a working R2DBC SPI to demo. Goal: prove the driver works
unmodified through Spring's own R2DBC integration (`DatabaseClient`/`R2dbcEntityTemplate`/
`ReactiveTransactionManager`) under a current Spring Boot/Spring Framework line (Boot 4 / Framework
7 at the time of writing — reconfirm the exact version when this phase actually starts, since it's
a moving target). Spring discovers our driver purely via the standard R2DBC
`ConnectionFactoryProvider` SPI (`META-INF/services`), the same mechanism every other R2DBC driver
uses — no Spring dependency belongs in `core`/`transport-http`/`connector` themselves.

- New module, `examples/spring-boot-webflux-demo` (not published, not part of the driver's public
  surface) — same role as
  [`spring-reactive-transaction-boundary`'s demo module](https://github.com/CamilYed/spring-reactive-transaction-boundary/tree/main/examples/spring-boot-webflux-r2dbc-ddd-demo).
- Proves: connection pooling via `R2dbcPoolAutoConfiguration`-style setup, transaction-manager
  wiring (documenting explicitly where ClickHouse's own transaction semantics diverge from a
  typical RDBMS — this is exactly the kind of gap Phase 3 already commits to documenting rather
  than silently glossing over), and a real WebFlux endpoint streaming query results end to end.
- Not a substitute for Phase 4's sign-off — this is a demo/consumer-proof, not where "fully
  reactive" gets verified.

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

# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions correspond to the Maven Central
releases of `clickhouse-r2dbc-reactive-{core,transport-http,connector,testkit}`.

`.github/workflows/release.yml` reads this file to build GitHub Release notes: before triggering
that workflow, rename the `## [Unreleased] — X.Y.Z (...)` heading below to `## [X.Y.Z] — YYYY-MM-DD`
and commit it to `main` first — the workflow looks for a `## ` heading containing `[X.Y.Z]` matching
the version it was given and fails the release if it can't find one. See
[CONTRIBUTING.md](CONTRIBUTING.md#cutting-a-release) for the full release checklist.

## [Unreleased] — 0.2.2

### Added

- **HTTP response compression, on by default** — this driver now sends `compress=1` and decodes
  ClickHouse's own custom LZ4 block framing (distinct from standard HTTP `Content-Encoding`),
  matching client-v2's own default of `COMPRESS_SERVER_RESPONSE=true`. New `core.ResponseCompression`
  (`NONE`/`LZ4`) threads the setting through `TransportOptions`/`ClickHouseHttpTransport` (which
  query parameter to send) and `RowBinaryDecoder` (whether to unwrap the response body through the
  new `core.rowbinary.ClickHouseLz4InputStream` before decoding) — zero new Reactor operators or schedulers,
  the existing `FluxInputStreamBridge`/`RowDecodingScheduler` machinery handles it unchanged. New
  R2DBC connection option `responseCompression` (default `true`), e.g.
  `r2dbc:clickhouse://host?responseCompression=false` to opt out. `ClickHouseCityHash`/`LZ4Factory`
  (already transitive via `client-v2`, no new dependency) verify each block's checksum and
  decompress it; wire format verified byte-for-byte against client-v2 0.9.8's own
  `ClickHouseLZ4InputStream`/`ClickHouseLZ4OutputStream`. Proven against a real server, not just
  hand-built fixtures, by `ResponseCompressionAgainstRealClickHouseTest` (100,000-row multi-block
  response, both compressed and explicitly disabled) — closes the compression-parity gap noted in
  [ROADMAP archive's Phase 8, item 12](engineering/roadmap-archive.md#phase-8--post-020-hardening-021).

### Fixed

- **The bundled demo now disposes the driver's `ClickHouseConnectionFactory` at Spring shutdown**,
  resolving the known gap noted below under `0.2.1`. `R2dbcConfiguration` now registers the raw
  factory as its own `baseConnectionFactory()` bean with `@Bean(destroyMethod = "dispose")`, and
  the pooled `connectionFactory(ConnectionFactory baseConnectionFactory)` bean takes it as a
  method parameter — Spring destroys a bean's dependents before the bean itself, so the outer
  `io.r2dbc.pool` `ConnectionPool` always finishes closing every pooled connection first. Proven,
  not just asserted, by a new real-ClickHouse test,
  `ConnectionFactoryShutdownDisposalAgainstRealClickHouseTest`: run a query through the raw factory,
  close the Spring context, then confirm the same query now fails. Required bumping the demo's
  `runtimeOnly` dependency from `0.2.0` to `0.2.1`, since `ClickHouseConnectionFactory.dispose()`
  didn't exist on the previously-pinned release — an earlier attempt at this exact fix failed
  against a real build for that reason (see [ROADMAP archive's Phase 8, item
  10](engineering/roadmap-archive.md#phase-8--post-020-hardening-021) for the full account of both attempts).
- **The demo's `Enum8`/`Enum16` `status` column workaround is gone.** Now that the demo depends on
  `0.2.1` (which decodes `Enum8`/`Enum16` as a plain `String`, see below), `DatabaseClientOrder
  EventRepository` reads it via `row.get("status", String.class)` directly instead of the previous
  `Object.class`/`toString()` indirection.

## [0.2.1] — 2026-08-22

### Added

- **`ClickHouseConnectionFactory.dispose()`/`isDisposed()`**, and the two resources it now actually
  releases: `ClickHouseHttpTransport.dispose()`/`disposeLater()` (the underlying Reactor Netty
  `ConnectionProvider`, previously never explicitly disposed — construction created a dedicated
  named pool with no way to release it short of GC/JVM exit) and `RowDecodingScheduler.dispose()`
  (already existed, but the factory that owns each scheduler instance had no lifecycle hook to call
  it from). See [Connection pooling](docs/operations/connection-pooling.md) "Shutting it down" note.
- **Netty `ByteBuf` leak-detection test lane**, completed as follow-up to the pilot deferred out of
  `0.2.0` (see that release's Deferred section below). Runs `transport-http` and `testkit` tests with
  `-Dio.netty.leakDetection.level=paranoid` and a custom `LeakRecordingResourceLeakDetector`, and now
  covers all six target shapes from [ROADMAP archive](engineering/roadmap-archive.md#phase-7--operational-control--r2dbc-correctness-020)'s
  item 6: cancellation, disconnect mid-response, decoder failure, timeout, retry, and downstream
  cancellation after a few records.
- **`responseTimeout` R2DBC connection option** (`ClickHouseConnectionFactoryProvider.RESPONSE_TIMEOUT`),
  e.g. `r2dbc:clickhouse://host?responseTimeout=PT30S`. The transport already supported it end to end
  (`TransportOptions`/`ClickHouseHttpTransport`, proven against a hermetic controlled server since
  `0.2.0`); it just wasn't reachable from R2DBC connection options until now. Its Javadoc documents
  how it relates to the other three timeouts this driver has (`connectTimeout`, server-side
  `statementTimeout`/`max_execution_time`, `transportPendingAcquireTimeout`) — README's connection
  options table corrected too, since it previously had `connectTimeout`'s row describing
  `responseTimeout`'s actual behavior. See [ROADMAP archive's Phase 8, item
  4](engineering/roadmap-archive.md#phase-8--post-020-hardening-021).
- **Test coverage characterizing a query failing mid-stream** (some rows already delivered, then
  ClickHouse fails server-side) — `MidStreamQueryFailureAgainstRealClickHouseTest` — plus
  confirmation that ClickHouse's own `wait_end_of_query=1` opt-in (buffer the whole response
  server-side for a clean error instead of streaming) is already reachable today with zero code
  changes via `ClickHouseQuery.withSettings(Map.of("wait_end_of_query", "1"))`. **Confirmed against a
  real server: under default settings, a mid-stream failure's trailing error text can be misdecoded
  as spurious garbage rows mixed in with genuine ones before the stream terminates with an error** —
  a real data-integrity risk, now called out in README's
  [Known limitations](docs/reference/known-limitations.md) with `wait_end_of_query=1` as the mitigation. See
  [ROADMAP archive's Phase 8, item 5](engineering/roadmap-archive.md#phase-8--post-020-hardening-021) for the full analysis.
- **Correct parameter-binding wire encoding for `LocalDateTime`/`Instant`/`OffsetDateTime`/
  `ZonedDateTime` and `List` (as ClickHouse `Array` literals)**, replacing the previous unqualified
  `value.toString()` fallback that would have sent an ISO `T`-separated datetime (wrong format for
  ClickHouse's `DateTime`) or a Java `List.toString()` (`[1, 2, 3]`, not a valid `Array` literal).
  `UUID`, `BigDecimal`, `LocalDate`, `Boolean`, `enum` constants, and `IPv4`/`IPv6` (bound as
  `String`) needed no change — confirmed correct via new tests, not just left alone. `Map`/`Tuple`
  bound-parameter values remain deliberately out of scope. **Confirmed against a real server**, which
  also surfaced a real, previously-undocumented finding: `Array(T)` always decodes each element as
  exactly the same Java type a scalar column of type `T` would (e.g. `Array(UInt32)` → `List<Long>`,
  not `List<Integer>`) — now spelled out in `ClickHouseValueConverter`'s Javadoc, since that class's
  numeric conversion matrix deliberately does not extend to `List` elements. See
  [ROADMAP archive's Phase 8, item 6](engineering/roadmap-archive.md#phase-8--post-020-hardening-021) for the full matrix and
  the reasoning behind each type's encoding.
- **Opt-in, per-query retry after a retryable ClickHouse server error** —
  `ClickHouseQuery.withServerErrorRetryEnabled()`. By default a query is only ever retried for a
  failure that happens *before* the request was fully sent (unchanged); this widens retry eligibility,
  for callers who explicitly opt in, to also cover a server error `ServerException.isRetryable()`
  classifies as retryable, as long as no response bytes have been delivered downstream yet. Deliberately
  a per-query opt-in rather than a connection-wide setting or SQL-text sniffing — a single connection is
  routinely used for both reads and writes, and there is no reliable way to tell a `SELECT` from a
  literal-SQL `INSERT` by inspecting the SQL text alone (CTEs, multi-statement text, `INSERT ... SELECT`
  would all defeat a naive check). Not yet reachable through the R2DBC `Statement` API — that's a
  tracked follow-up, not part of this change. See [ROADMAP archive's Phase 8, item
  7](engineering/roadmap-archive.md#phase-8--post-020-hardening-021) for the full design.
- **`GET /order-events/stream`** in the bundled demo, producing `application/x-ndjson` — each event is
  written to the HTTP response as soon as it arrives, proven (not just asserted) by a new
  `OrderEventStreamingControllerTest` that controls source-element timing directly rather than relying
  on a real query's own fast, unpredictable timing. See [ROADMAP archive's Phase 8, item
  9](engineering/roadmap-archive.md#phase-8--post-020-hardening-021).

### Known gap, not yet fixed

- **The bundled demo does not dispose the driver's `ClickHouseConnectionFactory` at Spring
  shutdown.** Fixed in `0.2.2` (demo-only change, see that section above) — confirmed directly
  against `io.r2dbc.pool`'s own `ConnectionPool` source that its
  `disposeLater()` never touches the delegate factory it wraps, so the driver's transport connection
  pool and decoder scheduler thread pool leak silently on every context shutdown. A fix was attempted
  (`@Bean(destroyMethod = "dispose")` on the driver's raw `ConnectionFactory`) and reverted after a
  real build proved it inapplicable: the demo's `runtimeOnly` dependency pins the *published*
  `0.2.0` connector, which predates `ClickHouseConnectionFactory.dispose()` — that method only exists
  in this still-unreleased `0.2.1` work. Genuinely blocked until either `0.2.1` is published and the
  demo's pinned version bumped, or [ROADMAP archive's Phase 8, item
  11](engineering/roadmap-archive.md#phase-8--post-020-hardening-021) (a current-`main` demo lane) lands first. See
  [item 10](engineering/roadmap-archive.md#phase-8--post-020-hardening-021) for the full account.

### Fixed

- **`Enum8`/`Enum16` columns now decode as a plain `String` of the member name, not client-v2's
  internal `EnumValue`.** `Row.get(name, String.class)` works directly — previously a caller had to
  ask for `Object.class` and call `toString()` on the result to read the value without depending on
  the internal type. See [ROADMAP archive's Phase 8, item
  1](engineering/roadmap-archive.md#phase-8--post-020-hardening-021) and the "`Enum8`/`Enum16` resolved" note under
  [Phase 2](engineering/roadmap-archive.md#phase-2--core-protocol--testkit-contract-tests). (The bundled demo used the
  old workaround while still pinned to `0.2.0`; removed once it was bumped to `0.2.1` — see the
  `0.2.2` section above.)
- **Cancelling a query via `Flux.next()`-style single-element consumption no longer forfeits
  connection-pool reuse.** `RowBinaryDecoder`'s disposal hook (added in `0.2.0` to fix a real
  resource-cleanup gap on downstream cancellation) called `FluxInputStreamBridge#close()`
  unconditionally, which unconditionally cancelled the underlying transport subscription — correct
  for a caller that genuinely abandons a large, still-streaming response early, but wasteful for
  `Flux.next()`, which cancels its upstream the instant it has one element, before the response has
  necessarily been observed as fully received. That unconditional cancel closed the connection (and
  triggered a best-effort `KILL QUERY` for an already-finished query) even for small, effectively
  fully-arrived responses — the exact access pattern `PointQueryBenchmark`/
  `BoundedPoolConcurrencyBenchmark` and `ClickHouseHttpTransportConnectionReuseTest` use.
  `FluxInputStreamBridge#close()` now first tries a short, bounded drain toward the upstream's
  natural terminal signal (50ms / 64KB budget) before falling back to a hard cancel — see that
  class's "Cancellation" Javadoc section for the full reasoning.
- **`ClickHouseQuery.parameterNamesIn(String)` no longer mistakes placeholder-shaped text inside a
  string literal, a quoted identifier, or a comment for a real `{name:Type}` bind parameter.**
  Replaced the single `Pattern`/`Matcher` scan (which has no way to track "are we currently inside a
  quote" while matching) with `ClickHouseSqlPlaceholderScanner`, a single-pass character scanner that
  tracks that context directly — skipping single-quoted string literals, double-quoted/backtick-quoted
  identifiers (both with backslash- and doubled-character escaping), and every comment form
  ClickHouse's own lexer accepts (`--`, `#!`, `# `, `//`, and `/* */` block comments, which — verified
  against ClickHouse's docs, not assumed — nest, unlike standard SQL's). See [ROADMAP archive's Phase 8,
  item 3](engineering/roadmap-archive.md#phase-8--post-020-hardening-021).
- **The demo's `OrderEventController#all()` Javadoc no longer claims the HTTP response streams.** It
  didn't — with no streaming media type, Spring WebFlux's default `application/json` writer collects
  the whole `Flux` into one array before writing anything, even though the underlying ClickHouse query
  itself streams correctly. The Javadoc now says so plainly and points callers who need a response
  actually proven to stream at the HTTP layer to the new `GET /order-events/stream` endpoint above. See
  [ROADMAP archive's Phase 8, item 9](engineering/roadmap-archive.md#phase-8--post-020-hardening-021).

### Also confirmed, not yet redesigned

- **Long analytical `SELECT`s can exceed a reverse proxy's/load balancer's HTTP request-line length
  limit** before they exceed ClickHouse's own generous `http_max_uri_size` (1 MiB default) — this
  transport sends the whole SQL text via `?query=<url-encoded-sql>`, never a request body. Confirmed
  with a new hermetic test (`ClickHouseHttpTransportLongQueryUriTest`) against a plain Reactor Netty
  server left at its default 4096-byte `maxInitialLineLength`, the same default most Netty-based
  proxies use: the query fails loudly (a connection-level error), not silently truncated or hung —
  a real, understood problem, not a driver defect, and not unique to this project's use of Reactor. A
  POST-body redesign is deliberately scoped as separate, not-yet-started follow-up work; also noted:
  bound parameter values currently ride the same query string as the SQL text, so binding parameters
  instead of inlining literals does not currently sidestep this. See [ROADMAP archive's Phase 8, item
  8](engineering/roadmap-archive.md#phase-8--post-020-hardening-021).

## [0.2.0] — 2026-08-20 (Phase 7: operational control & R2DBC correctness)

See [ROADMAP archive's Phase 7 section](engineering/roadmap-archive.md#phase-7--operational-control--r2dbc-correctness-020)
for the full scoping and acceptance criteria this release was built against.

### Added

- **Transport pool R2DBC options** (`transportMaxConnections`, `transportPendingAcquireMaxCount`,
  `transportPendingAcquireTimeout`, `transportMaxIdleTime`, `transportMaxLifeTime`) — the physical
  Reactor Netty connection pool underneath `ClickHouseHttpTransport` is now configurable through
  `ConnectionFactoryOptions` and the R2DBC URL query string, not just a Java constructor call. See
  [Connection pooling](docs/operations/connection-pooling.md) section.
- **`Connection.setStatementTimeout(Duration)`**, backed by ClickHouse's `max_execution_time`
  server setting, inherited by every statement created from that connection afterward.
  `Duration.ZERO` explicitly means "no timeout."
- **Correctness-first `Statement.add()`** — batched statements now execute sequentially
  (`concatMap`), one `Result` per bound set, instead of throwing `UnsupportedOperationException`.
- **A neutral driver observability SPI** (`DriverObservationListener`:
  `queryStarted`/`queryCompleted`/`queryFailed`/`queryCancelled`) with a reference
  `Slf4jDriverObservationListener` implementation, keyed on `query_id`. No hard Micrometer
  dependency, no full SQL/bind values/credentials logged by default.
- **`docs/R2DBC_COMPATIBILITY.md`** and `ClickHouseR2dbcSpiCompatibilityTest` — the official R2DBC
  SPI Technology Compatibility Kit (`io.r2dbc:r2dbc-spi-test`) now runs against a real ClickHouse
  server, with every deliberate non-support (transactions, savepoints, BLOB/CLOB, generated keys,
  duplicate column aliases in one `SELECT`) documented with a verified, specific reason instead of
  silently skipped.
- **Nightly CI lane** (`.github/workflows/nightly.yml`) — the same full build now also runs on a
  schedule against multiple ClickHouse server versions, split out from the fast per-PR lane.

### Changed

- **`Result` consumption state is now shared across `filter()`-derived views** — consuming an
  original `Result` and a `filter()` view of it (in either order) is now correctly rejected as
  double-consumption, closing a gap the class's own Javadoc used to just document as a known issue.
- **`Row.get(name, Class<T>)` now performs controlled, tested numeric/temporal conversions**
  (`Byte`/`Short`/`Integer`/`Long`/`Float`/`Double`/`BigInteger`/`BigDecimal` with explicit range
  checking, plus `String`/`UUID`/`LocalDate`/`LocalDateTime`/`Instant`/`OffsetDateTime`) instead of
  a bare `Class#cast` that threw `ClassCastException` on any type mismatch.
- **`RowBinaryDecoder`'s blocking bridge to client-v2 now runs on an explicitly driver-owned,
  bounded `RowDecodingScheduler`** (shut down together with the resource that owns it), replacing
  incidental reliance on the process-wide shared `Schedulers.boundedElastic()`. Closes a real gap
  where per-row reads after the first could run on the Netty event-loop thread.
- **`com.clickhouse:client-v2` bumped `0.9.0` → `0.9.8`** — brings a shared exception root
  (`ClickHouseException`) and `isRetryable()` for `ServerException`/`ClientException`, and
  reclassifies HTTP `503` as a connection-style retry case.
- Outstanding Dependabot PRs merged (`org.sonarqube`, `actions/upload-artifact`,
  `actions/checkout`, `actions/setup-java`, `reactor-netty-http`, `reactor`).
- **`DriverObservationListener.NOOP` (the default when no listener is configured) is now a genuine
  fast path, not just a set of empty method bodies** — added `DriverObservationListener#isEnabled()`
  (defaults to `true`, `NOOP` overrides it to `false`); `QueryObservation.start` returns a stateless
  no-op instance when the configured listener reports itself disabled, skipping `SqlFingerprint`
  (SHA-256) computation and `Instant.now()` timestamping entirely instead of computing them and
  discarding the result. `ClickHouseResult.decode` and `Connection.insertStreaming` skip their own
  per-chunk/per-row byte- and row-counting wiring the same way. Behavior is unchanged for any
  listener that doesn't override `isEnabled()` (the default stays `true`).
- **`FluxInputStreamBridge` now opportunistically coalesces already-buffered network chunks** before
  crossing from Reactor Netty's event loop to `RowDecodingScheduler`'s worker thread, instead of one
  blocking hand-off per chunk. Found and root-caused via `StreamingScanBenchmark`: chunk count scales
  linearly with row count, and each cross-thread hand-off has a real, measurable cost. Clear win at
  10k/100k-row scans; the 1M-row tier's result is not yet a settled single number — see
  [performance results](docs/performance/results.md#why-the-1m-number-wont-sit-still) and
  [Known limitations](docs/reference/known-limitations.md) for the honest, still-open measurement
  question. `RowBinaryDecoder.RESPONSE_CHUNK_DEMAND` raised `4` → `16` alongside this fix, so more
  chunks can be outstanding for the coalescing loop to work with.

### Deferred

- **Netty `ByteBuf` leak-detection test lane** (Phase 7 P0 item 6) — scoped in
  [ROADMAP archive](engineering/roadmap-archive.md#phase-7--operational-control--r2dbc-correctness-020) but not implemented
  for this release. A partial pilot exists on the unmerged `feature/183-netty-leak-detection-lane`
  branch (paranoid-level detector, covering cancellation and reset-mid-response — two of the six
  target shapes), not finished or merged. Moved out of `0.2.0` scope explicitly rather than left
  silently undone; tracked as follow-up work for a future release.

### Fixed

- **`ConnectionFactoryOptions.DATABASE` is now honored** — `ClickHouseConnectionFactory.from(...)`
  previously read `host`/`port`/`ssl`/`user`/`password`/etc. but silently ignored a configured
  `database`, so `r2dbc:clickhouse://host:8123/analytics`-style URLs (or the equivalent typed
  option) ran every query against the connecting user's default database instead. Now sent as
  `X-ClickHouse-Database` on every request once configured.
- **A `user` option with no accompanying `password` no longer authenticates with the literal
  four-character password `"null"`** — `String.valueOf(password)` on a `null` `CharSequence`
  produces the string `"null"`, which `ClickHouseConnectionFactory.from(...)` was sending as the
  actual HTTP Basic password. Now sends an empty password in that case, matching what a user
  configured with no password on the server side actually expects.
- Real-ClickHouse TCK runs surfaced and fixed two correctness bugs introduced while wiring up the
  compatibility lane: `Statement.bind(name, value)` didn't unwrap `io.r2dbc.spi.Parameter` (from
  `Parameters.in(...)`), sending a garbage literal instead of the bound value;
  `Connection.validate(depth)` read closed-connection state eagerly at call time instead of at
  subscription time, so a `Publisher` obtained before `close()` could wrongly report "still open."
- **`Authentication.Basic`/`Authentication.UserKey` no longer leak credentials through `toString()`**
  — the generated record `toString()` used to print the password/key in plain text, and
  `TransportOptions.toString()` embeds `authentication` directly, so logging a `TransportOptions`
  value (e.g. a driver startup summary) could expose them. Both now redact.
- **`FluxInputStreamBridge#read(byte[], int, int)` now honors `InputStream`'s zero-length-read
  contract** — a `length == 0` call used to return `-1` after the stream ended (instead of the
  required `0`) and, worse, could block waiting for the next chunk even when zero bytes were ever
  going to be copied. Checked first now, before any end-of-stream/fill logic runs.

## [0.1.0] — 2026-08-14

First published release to Maven Central (`io.github.camilyed`): `clickhouse-r2dbc-reactive-core`,
`clickhouse-r2dbc-reactive-transport-http`, `clickhouse-r2dbc-reactive-connector`,
`clickhouse-r2dbc-reactive-testkit`.

A fully reactive R2DBC driver for ClickHouse, reusing ClickHouse Java Client V2's public
row-decoding classes only — never its (confirmed blocking) HTTP transport. See
[ROADMAP archive's Phase 0 finding](engineering/roadmap-archive.md#phase-0--client-v2-execution-path-finding) and
[README's Why](README.md#why) for the full rationale.

Functional highlights: the complete R2DBC SPI surface (connection lifecycle,
`SELECT`/`INSERT`/parameterized statements, batches, row/column metadata, `getRowsUpdated()`,
R2DBC exception mapping) exercised end to end against a real ClickHouse server; a non-blocking,
streaming, backpressure-aware transport built on Reactor Netty with a documented, tested
"fully reactive" property matrix (see [ROADMAP archive Phase 4](engineering/roadmap-archive.md#phase-4--fully-reactive-sign-off));
cancellation that tears down the connection and issues a best-effort `KILL QUERY` server-side;
`ssl=true` TLS support including a custom trust store (`sslRootCert`); a pre-send-only retry
policy; a Spring Boot + WebFlux demo module
([`examples/spring-boot-webflux-demo`](examples/spring-boot-webflux-demo)); recorded performance
benchmarks vs. baseline (see [docs/performance/](docs/performance/index.md)).

See [ROADMAP archive's Production readiness review](engineering/roadmap-archive.md#production-readiness-review) for the
detailed, honestly-triaged list of what shipped fixed vs. documented-as-a-limitation for this
release.

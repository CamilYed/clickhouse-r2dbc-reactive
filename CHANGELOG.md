# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions correspond to the Maven Central
releases of `clickhouse-r2dbc-reactive-{core,transport-http,connector,testkit}`.

`.github/workflows/release.yml` reads this file to build GitHub Release notes: before triggering
that workflow, rename the `## [Unreleased] — X.Y.Z (...)` heading below to `## [X.Y.Z] — YYYY-MM-DD`
and commit it to `main` first — the workflow looks for a `## ` heading containing `[X.Y.Z]` matching
the version it was given and fails the release if it can't find one. See
[CONTRIBUTING.md](CONTRIBUTING.md#cutting-a-release) for the full release checklist.

## [0.2.0] — 2026-08-20 (Phase 7: operational control & R2DBC correctness)

See [ROADMAP.md's Phase 7 section](ROADMAP.md#phase-7--operational-control--r2dbc-correctness-020)
for the full scoping and acceptance criteria this release was built against.

### Added

- **Transport pool R2DBC options** (`transportMaxConnections`, `transportPendingAcquireMaxCount`,
  `transportPendingAcquireTimeout`, `transportMaxIdleTime`, `transportMaxLifeTime`) — the physical
  Reactor Netty connection pool underneath `ClickHouseHttpTransport` is now configurable through
  `ConnectionFactoryOptions` and the R2DBC URL query string, not just a Java constructor call. See
  README's [Connection pooling](README.md#connection-pooling) section.
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
  [docs/PERFORMANCE.md](docs/PERFORMANCE.md#why-the-1m-number-wont-sit-still) and
  [README's Known limitations](README.md#known-limitations) for the honest, still-open measurement
  question. `RowBinaryDecoder.RESPONSE_CHUNK_DEMAND` raised `4` → `16` alongside this fix, so more
  chunks can be outstanding for the coalescing loop to work with.

### Deferred

- **Netty `ByteBuf` leak-detection test lane** (Phase 7 P0 item 6) — scoped in
  [ROADMAP.md](ROADMAP.md#phase-7--operational-control--r2dbc-correctness-020) but not implemented
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
[ROADMAP.md's Phase 0 finding](ROADMAP.md#phase-0--client-v2-execution-path-finding) and
[README's Why](README.md#why) for the full rationale.

Functional highlights: the complete R2DBC SPI surface (connection lifecycle,
`SELECT`/`INSERT`/parameterized statements, batches, row/column metadata, `getRowsUpdated()`,
R2DBC exception mapping) exercised end to end against a real ClickHouse server; a non-blocking,
streaming, backpressure-aware transport built on Reactor Netty with a documented, tested
"fully reactive" property matrix (see [ROADMAP.md Phase 4](ROADMAP.md#phase-4--fully-reactive-sign-off));
cancellation that tears down the connection and issues a best-effort `KILL QUERY` server-side;
`ssl=true` TLS support including a custom trust store (`sslRootCert`); a pre-send-only retry
policy; a Spring Boot + WebFlux demo module
([`examples/spring-boot-webflux-demo`](examples/spring-boot-webflux-demo)); recorded performance
benchmarks vs. baseline (see [docs/PERFORMANCE.md](docs/PERFORMANCE.md)).

See [ROADMAP.md's Production readiness review](ROADMAP.md#production-readiness-review) for the
detailed, honestly-triaged list of what shipped fixed vs. documented-as-a-limitation for this
release.

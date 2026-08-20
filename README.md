# ClickHouse R2DBC Reactive

[![CI](https://github.com/CamilYed/clickhouse-r2dbc-reactive/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/CamilYed/clickhouse-r2dbc-reactive/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-functional%20%2F%20pre--release-orange.svg)](#status)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=CamilYed_clickhouse-r2dbc-reactive&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=CamilYed_clickhouse-r2dbc-reactive)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=CamilYed_clickhouse-r2dbc-reactive&metric=coverage)](https://sonarcloud.io/summary/new_code?id=CamilYed_clickhouse-r2dbc-reactive)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=CamilYed_clickhouse-r2dbc-reactive&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=CamilYed_clickhouse-r2dbc-reactive)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=CamilYed_clickhouse-r2dbc-reactive&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=CamilYed_clickhouse-r2dbc-reactive)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=CamilYed_clickhouse-r2dbc-reactive&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=CamilYed_clickhouse-r2dbc-reactive)

[![Java](https://img.shields.io/badge/Java-21-blue.svg)](https://openjdk.org/projects/jdk/21/)
[![Reactor](https://img.shields.io/badge/Reactor-Mono%20%7C%20Flux-blueviolet.svg)](https://projectreactor.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Client%20V2-FFCC01.svg)](https://github.com/ClickHouse/clickhouse-java)

[![Maven Central - connector](https://img.shields.io/maven-central/v/io.github.camilyed/clickhouse-r2dbc-reactive-connector?label=connector)](https://central.sonatype.com/artifact/io.github.camilyed/clickhouse-r2dbc-reactive-connector)
[![Maven Central - core](https://img.shields.io/maven-central/v/io.github.camilyed/clickhouse-r2dbc-reactive-core?label=core)](https://central.sonatype.com/artifact/io.github.camilyed/clickhouse-r2dbc-reactive-core)
[![Maven Central - transport-http](https://img.shields.io/maven-central/v/io.github.camilyed/clickhouse-r2dbc-reactive-transport-http?label=transport-http)](https://central.sonatype.com/artifact/io.github.camilyed/clickhouse-r2dbc-reactive-transport-http)
[![Maven Central - testkit](https://img.shields.io/maven-central/v/io.github.camilyed/clickhouse-r2dbc-reactive-testkit?label=testkit)](https://central.sonatype.com/artifact/io.github.camilyed/clickhouse-r2dbc-reactive-testkit)

Most consumers only need `connector` (it pulls `core`/`transport-http` in transitively) — see
[Installation](#installation). `testkit` is a separate, optional dependency for anyone writing
tests against this driver (a fake ClickHouse HTTP server, a real-ClickHouse Testcontainers DSL).
Badges may briefly show "not found" right after a release — Central Portal sync to the search
index that shields.io reads from can lag a few minutes to a few hours behind publication.

A fully reactive R2DBC driver for ClickHouse. It reuses
[ClickHouse Java Client V2](https://github.com/ClickHouse/clickhouse-java)'s public row-decoding
classes only — its HTTP transport is confirmed blocking (classic Apache HttpClient5 I/O, verified
by reading the source) and is **never used**. Everything that touches the network — connection
handling, request/response streaming, cancellation — is this project's own, small, explicit,
non-blocking transport boundary. See
[ROADMAP.md's Phase 0 finding](ROADMAP.md#phase-0--client-v2-execution-path-finding) for the
verified evidence.

This project exists because implementing the R2DBC interfaces is not, by itself, enough to make
the complete execution path reactive. The goal here is an R2DBC driver where deferred execution,
non-blocking I/O, streaming decoding, bounded backpressure-aware buffering, cancellation, and
deterministic resource cleanup are true end to end, not just at the API surface.

The design direction started as a public design discussion with the ClickHouse team:
[ClickHouse/ClickHouse#113638 — Design discussion: future direction for reactive R2DBC support in the Java client](https://github.com/ClickHouse/ClickHouse/discussions/113638).

## Contents

- [Why](#why)
- [Status](#status)
- [Installation](#installation)
- [Usage](#usage)
- [Performance](#performance)
- [Known limitations](#known-limitations)
- [What "fully reactive" means here](#what-fully-reactive-means-here)
- [Connection pooling](#connection-pooling)
- [Architecture direction](#architecture-direction)
- [Modules](#modules)
- [Requirements](#requirements)
- [Testing strategy](#testing-strategy)
- [Roadmap](#roadmap)
- [What this project is not](#what-this-project-is-not)
- [Relationship to ClickHouse/clickhouse-java](#relationship-to-clickhouseclickhouse-java)
- [Contributing](#contributing)
- [License](#license)

## Why

A production Spring WebFlux application using the existing ClickHouse R2DBC driver showed that
the effective execution path contains two separate resource-management layers: the logical R2DBC
connection pool, and a lower HTTP connection pool and pending-request queue owned by the Java
client. The R2DBC pool could appear healthy while requests were already queued or blocked below
it. Tuning pool sizes and timeouts changed the symptoms but not the architecture.

Returning `Publisher`, `Mono`, `Flux`, or `CompletableFuture` from an API is not the same as being
non-blocking, bounded, cancellable, and streaming end to end. This project is an attempt to build
a driver where those properties are explicit, tested, and owned by a single, well-defined layer.

## Status

Functional, `0.1.0` published to Maven Central. The full R2DBC SPI surface exists and is exercised
against a real ClickHouse server (Testcontainers): connection lifecycle,
`SELECT`/`INSERT`/parameterized statements, batches, row/column metadata, `getRowsUpdated()`, and
R2DBC exception mapping for ClickHouse server errors. **The driver has not been run against a
production workload** — everything above is confirmed by an automated test suite (unit, transport
contract tests against a controlled fake server, and real-ClickHouse integration tests via
Testcontainers, see [Testing strategy](#testing-strategy)), not by production experience yet.

Before relying on this in production, read
[ROADMAP.md's Production readiness review](ROADMAP.md#production-readiness-review) — an explicit,
honestly-triaged list of what's fixed, what's a documented safe limitation, and what's still an
open gap (currently: no retry based on ClickHouse's own server-side retryable error codes — see
[Known limitations](#known-limitations) below for what retry behavior *does* exist, the
cancellation/`KILL QUERY` caveat, which *is* handled but on a best-effort basis, and the
`sslRootCert` custom trust-store option). That page is the actual source of truth for "is this safe
to depend on today", kept up to date as things are found and fixed — treat this README as a summary
of it, not the other way around.

Since `0.1.0`, [ROADMAP.md's Phase 7](ROADMAP.md#phase-7--operational-control--r2dbc-correctness-020)
(`0.2.0`, in progress) has added configurable transport pool options, a real statement-timeout
implementation, correct multi-`Result` `Statement.add()` batching, a driver observability SPI, and
an R2DBC SPI Technology Compatibility Kit lane run against a real server (see
[Connection pooling](#connection-pooling) and [docs/R2DBC_COMPATIBILITY.md](docs/R2DBC_COMPATIBILITY.md)
below) — see [CHANGELOG.md](CHANGELOG.md) for the full, release-by-release list.

Still expect breaking changes at this stage — the SPI surface and options above are exercised by
an automated test suite, not yet by a production workload.

## Installation

Published to Maven Central under `io.github.camilyed`. Check the badge at the top of this file, or
[central.sonatype.com](https://central.sonatype.com/search?q=io.github.camilyed), for the latest
version.

```kotlin
dependencies {
    implementation("io.github.camilyed:clickhouse-r2dbc-reactive-connector:0.1.0")
}
```

Depending on `clickhouse-r2dbc-reactive-connector` alone is enough — it pulls in
`clickhouse-r2dbc-reactive-core` and `clickhouse-r2dbc-reactive-transport-http` on the runtime
classpath (they're `implementation`, not `api`, dependencies of the connector on purpose: they're
this driver's own internals, not part of its public surface — see
[Architecture direction](#architecture-direction)).

### Building from source instead

```bash
git clone https://github.com/CamilYed/clickhouse-r2dbc-reactive.git
cd clickhouse-r2dbc-reactive
./gradlew publishToMavenLocal
```

then depend on it with `mavenLocal()` in your `repositories { }` block and the version from
`gradle.properties`/`-PreleaseVersion` (defaults to `0.1.0-SNAPSHOT`).

## Usage

The driver registers itself with the standard R2DBC `ServiceLoader` bootstrap path under the driver
identifier `clickhouse` — no direct dependency on this driver's own classes is needed to obtain a
`ConnectionFactory`.

```java
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;

ConnectionFactory connectionFactory =
    ConnectionFactories.get("r2dbc:clickhouse://localhost:8123");
// or, with authentication: "r2dbc:clickhouse://user:password@host:8123?ssl=true"

Flux<String> names =
    Flux.usingWhen(
        connectionFactory.create(),
        connection ->
            Flux.from(
                    connection
                        .createStatement("SELECT name FROM users WHERE id = {id:UInt32}")
                        .bind("id", 42)
                        .execute())
                .flatMap(result -> result.map((row, meta) -> row.get("name", String.class))),
        Connection::close);
```

Named parameters (`{name:Type}`, ClickHouse's own binding syntax) are bound with
`.bind("name", value)` — the type annotation in the SQL is what ClickHouse itself uses to interpret
the bound value, this driver does not reinterpret or validate it.

### Connection options

Set through the R2DBC URL's query string or `ConnectionFactoryOptions.builder()` directly:

| Option | Default | Meaning |
| --- | --- | --- |
| `host` | *(required)* | ClickHouse server host |
| `port` | `8123` | ClickHouse HTTP interface port |
| `ssl` | `false` | Use HTTPS |
| `user` / `password` | none (anonymous) | HTTP basic auth against ClickHouse |
| `database` | connecting user's default | Database selected via `X-ClickHouse-Database` on every request, e.g. `r2dbc:clickhouse://host:8123/analytics` |
| `connectTimeout` | none | See [`ClickHouseHttpTransport`](clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/ClickHouseHttpTransport.java)'s Javadoc for why there's no implicit response timeout |
| `sslRootCert` | none (JVM default trust store) | Classpath resource or filesystem path to a PEM-encoded trusted certificate, for self-signed/internal-CA servers — only meaningful with `ssl=true` |
| `retryMaxAttempts` | `3` | Retries for failures before any request bytes reached the server — see [`RetryPolicy`](clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/RetryPolicy.java) for exactly what qualifies |
| `retryDelay` | `50ms` | Fixed delay between retry attempts |

Spring Boot users configuring `spring.r2dbc.url=r2dbc:clickhouse://...` get all of the above for
free through Spring's own R2DBC auto-configuration — see
[`examples/spring-boot-webflux-demo`](examples/spring-boot-webflux-demo) for a complete, runnable
reference application (hexagonal layering, `io.r2dbc.pool` wiring, a real ClickHouse schema with
`LowCardinality`/`Enum8`/`IPv4` columns, and an analytics endpoint). Wrapping the connection factory
in `io.r2dbc.pool`'s `ConnectionPool` — what `spring.r2dbc.pool.*` configures — is covered in
[Connection pooling](#connection-pooling) below.

### Using with Spring Boot

```kotlin
dependencies {
    implementation("io.github.camilyed:clickhouse-r2dbc-reactive-connector:<version>")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("io.r2dbc:r2dbc-pool")
}
```

```yaml
spring:
  r2dbc:
    url: r2dbc:clickhouse://localhost:8123
    username: default
    password: ""
    pool:
      enabled: true
      initial-size: 5
      min-idle: 5
      max-size: 20
      max-idle-time: 30m
      validation-depth: local
```

`spring.r2dbc.url`/`username`/`password`/`pool.*` are all standard Spring Boot R2DBC properties —
nothing ClickHouse-specific about the property names, only about the `r2dbc:clickhouse://` scheme
they configure. `spring.r2dbc.pool.*` is what builds and tunes the R2DBC-SPI-level pool described
in [Connection pooling](#connection-pooling) below; see that section for what each property does.

**One thing Spring Boot's own auto-configuration does *not* get right for this driver, on its
own:** building a `DatabaseClient` bean fails outright with `IllegalStateException: Cannot
determine a BindMarkersFactory for ClickHouse`, because Spring's `BindMarkersFactoryResolver` only
recognizes a small hardcoded list of drivers (Postgres, MySQL, MariaDB, SQL Server, H2) and
ClickHouse isn't on it. This only affects `DatabaseClient`/`R2dbcEntityTemplate`-style usage — a
plain `ConnectionFactory` (or `Connection`, `Statement`, `Result`, as in the [Usage](#usage) example
above) autowires and works with no extra configuration. If your application does need
`DatabaseClient`, copy
[`R2dbcConfiguration`](examples/spring-boot-webflux-demo/src/main/java/io/github/camilyed/clickhouse/r2dbc/examples/webfluxdemo/infrastructure/R2dbcConfiguration.java)
from the reference demo — it builds `ConnectionFactory`, pooled `ConnectionPool`, and
`DatabaseClient` beans by hand, reading the same `spring.r2dbc.*`/`spring.r2dbc.pool.*` properties
Spring Boot itself would, plus fail-fast validation of the pool settings and a startup log line
showing exactly what was configured. See that class's own Javadoc for the full reasoning, including
what still doesn't work even with this fix (`DatabaseClient.sql(...).bind(...)` — ClickHouse's
`{name:Type}` parameter syntax has no equivalent in R2DBC's generic `BindMarkersFactory`
abstraction, so the demo's repository issues fully pre-formed SQL instead).

## Performance

Every number below is from a real ClickHouse server, against `com.clickhouse:client-v2:0.9.8`, this
driver's real production decode path (`RowDecodingScheduler`, not a benchmark-only shortcut). Latest
run is single-fork (a first signal, not yet a 3-fork-confirmed number — see the confidence warning
at the top of [docs/PERFORMANCE.md](docs/PERFORMANCE.md), which has full methodology, every
benchmark's description, and every open caveat.

<p align="center">
  <img src="docs/images/2026-08-20-throughput.png" width="32%" alt="Real point-query throughput through the public R2DBC SPI, matched 8-connection pool, this driver vs client-v2">
  <img src="docs/images/2026-08-20-isolation-trio.png" width="32%" alt="Isolation: transport only vs decode only vs the full production pipeline, this driver vs client-v2, at 1,000,000 rows">
  <img src="docs/images/2026-08-20-streaming-scan.png" width="32%" alt="StreamingScanBenchmark mean latency by row count, this driver vs client-v2">
</p>

| Scenario | Result |
| --- | --- |
| Non-blocking, matched 8-connection pool, real throughput via the public R2DBC SPI | 🟢 **~4x more queries/sec** at every concurrency level tested (8/32/128) — the scenario this project is built for |
| Full table scan (10k/100k/1M rows) | 🟢 **9.8% lower latency at 10k, 11.5% lower at 100k**, 🟡 11.8% higher at 1M — see below, a real regression was found and mostly fixed |
| Transport alone (bytes, no decode) | 🟢 43–45% lower latency at every tier, including 1M rows |
| Decode alone, no network | 🟢 7–14% lower latency at every tier |
| Single-row point lookup / `SELECT 1` floor | 🟡 essentially tied (within ±2%) |
| Blocking `.block()`-per-query calling style, matched pool | 🔴 ~5–8% higher latency — don't call it this way, see below |

**The full-scan regression was real, root-caused, and mostly fixed.** Once every benchmark started
going through the real production decode path (`RowBinaryDecoder.decode` + `RowDecodingScheduler`,
replacing an earlier scheduler-free shortcut that never paid its real cost), a genuine regression
showed up: transport won big on its own, decode won on its own, but the combination — the actual
shipped code path — lost, worse as results grew. Root cause: `FluxInputStreamBridge` did one
cross-thread `queue.take()` per network chunk, and chunk count scales linearly with row count.
Fixed by opportunistically merging already-queued chunks before crossing threads — 100k rows now
wins outright (was 19.5% slower), 1M rows is down to an 11.8% gap (was 56.9%), not fully closed
yet. See [docs/PERFORMANCE.md](docs/PERFORMANCE.md#full-table-scan-found-and-mostly-fixed) for the
full investigation and [How to use this driver
well](docs/PERFORMANCE.md#how-to-use-this-driver-well) for what this means for choosing this driver
today.

## Known limitations

> [!IMPORTANT]
> **Cancelling a subscription stops the query on the ClickHouse server via a best-effort `KILL
> QUERY` this driver sends itself — not via ClickHouse's own connection-close detection, which
> doesn't work.** Verified against a real server, not assumed. See
> [ROADMAP.md's writeup](ROADMAP.md#production-readiness-review) (search "Cancelling a client-side
> subscription") and the regression test that proves it,
> [`QueryCancellationAgainstRealClickHouseTest`](clickhouse-r2dbc-reactive-transport-http/src/test/java/io/github/camilyed/clickhouse/r2dbc/transport/http/QueryCancellationAgainstRealClickHouseTest.java).

Disposing a `Flux`/`Mono` subscription (`.take(n)`, a timeout operator, an upstream cancel) makes
this driver stop reading the response and closes its HTTP connection — that part is fully reactive
and tested. On its own, though, that would **not** stop ClickHouse itself from continuing to
execute the query: ClickHouse's own HTTP interface docs say so directly, *"Running requests don't
stop automatically if the HTTP connection is lost"* (clickhouse.com/docs/interfaces/http), and the
one setting meant to close that gap, `cancel_http_readonly_queries_on_client_close`, is itself
unreliable — a query kept running after client disconnect even with that setting enabled was
reproduced against a recent ClickHouse release and closed by ClickHouse's own maintainers as
["not planned"](https://github.com/ClickHouse/ClickHouse/issues/92786).

**What this driver does about it:** on cancellation, `ClickHouseHttpTransport` sends an explicit
`KILL QUERY WHERE query_id = '<id>' ASYNC` over a separate request — but only once the original
request had actually reached the server (cancelling before that means there's nothing to kill yet).
It reuses the same authenticated user as the original query, since ClickHouse lets a user stop
their own queries without a separate `KILL QUERY` privilege grant. This is genuinely **best-effort,
not a guarantee**: if the kill request itself fails — most plausibly the connecting user turns out
to lack the privilege after all under a restricted RBAC setup, or the server is simply unreachable
at that moment — the failure is logged at `WARN` and otherwise swallowed; it never surfaces on the
caller's already-cancelled subscription. Under a pattern like "timeout and retry" against a server
where this occasionally fails, that residual risk (a query that keeps running despite being
cancelled) still exists, just far less often than before this was implemented — watch for the
`WARN` log if that matters to you.

> [!NOTE]
> **Every query is automatically retried, but only for failures that happen before any bytes were
> sent to the server.** A connection-level hiccup (connect refused, a momentary pool exhaustion) is
> retried up to `retryMaxAttempts` times (default 3, matching client-v2's own default retry count for
> the same class of failure — verified against our pinned client-v2 version's source), each separated
> by `retryDelay` (default 50ms). Deliberately scoped to pre-send failures only, regardless of whether
> the query is a `SELECT` or an `INSERT`: since the server never received any bytes of a pre-send-
> failed attempt, retrying it cannot make the query run twice server-side, so no idempotency guessing
> is needed. A failure *after* the request was sent — a connection reset mid-response, a server error
> — is never retried, precisely to avoid the risk of applying a non-idempotent statement twice. Set
> `retryMaxAttempts=0` to disable entirely. See
> [`RetryPolicy`](clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/RetryPolicy.java)'s
> Javadoc for the full reasoning.

> [!NOTE]
> **`ssl=true` supports a custom trust store via `sslRootCert`, for self-signed/internal-CA
> certificates.** TLS auto-negotiation from the `https://` scheme is verified (see
> [`ClickHouseHttpTransportTlsTest`](clickhouse-r2dbc-reactive-transport-http/src/test/java/io/github/camilyed/clickhouse/r2dbc/transport/http/ClickHouseHttpTransportTlsTest.java)),
> and connecting to a server presenting a self-signed or internal-CA certificate — common for a
> database that usually isn't exposed to the public internet — no longer requires importing anything
> into the JVM's own default trust store. Set the `sslRootCert` connection option
> (`ClickHouseConnectionFactoryProvider.SSL_ROOT_CERT`) to either a classpath resource path or a
> filesystem path pointing at a PEM-encoded certificate; it's resolved as a classpath resource first,
> then as a filesystem path, mirroring r2dbc-postgresql's own `sslRootCert` option — convenient for a
> Kubernetes/Tanzu-style deployment where the certificate is mounted from a `Secret`/`ConfigMap`.
> Requires `ssl=true`; setting it without `ssl=true` fails fast with `IllegalArgumentException`.

> [!NOTE]
> **Large/batch `INSERT`s can stream their data as the request body instead of the URL, via
> `ClickHouseConnection.insertStreaming(String, Publisher<ByteBuffer>)`.** Standard
> `Statement`/`Batch` still send `INSERT` data URL-encoded, same as any `SELECT` — correct, but not
> what large payloads want (URL length limits, the whole payload held in memory). ClickHouse's own
> HTTP docs describe the request-body form as the preferred pattern for exactly this reason. This is
> a ClickHouse-specific vendor extension (R2DBC's `Statement`/`Batch` have no concept of a streamed
> request body), and it is **never retried**, even with `retryMaxAttempts` configured — once bytes
> of the payload may have already reached the server mid-stream, retrying on a connection-level
> failure risks silently duplicating rows. See
> [`ClickHouseHttpTransport.insertWithSummary`](clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/ClickHouseHttpTransport.java)'s
> Javadoc for the full reasoning.

> [!NOTE]
> **`JSON` columns are supported, decoded as a plain `String`, with zero extra configuration.**
> GA since ClickHouse 25.3 — no `allow_experimental_json_type` needed. This driver sends
> `output_format_binary_write_json_as_string=1` unconditionally on every query (harmless when a
> table has no `JSON` column), so a `JSON` column just works whether queried directly or through
> something like Spring's `DatabaseClient`. `Dynamic`/`Variant` remain unsupported — newer, less
> settled experimental types.

> [!NOTE]
> **Transactions, savepoints, generated keys, and `Blob`/`Clob` binding are deliberately
> unsupported** — ClickHouse itself has no equivalent concept for any of them, so this driver fails
> loudly (`UnsupportedOperationException`) rather than silently pretending otherwise. See
> [docs/R2DBC_COMPATIBILITY.md](docs/R2DBC_COMPATIBILITY.md) for the full, written-up answer to
> "where does this driver match the R2DBC SPI" — backed by
> [`ClickHouseR2dbcSpiCompatibilityTest`](clickhouse-r2dbc-reactive-connector/src/test/java/io/github/camilyed/clickhouse/r2dbc/connector/ClickHouseR2dbcSpiCompatibilityTest.java)
> actually running the official R2DBC SPI Technology Compatibility Kit against a real ClickHouse
> server, not just a claim.

## What "fully reactive" means here

Returning a reactive type is a necessary but insufficient condition. A driver is treated as
reactive end to end only if it satisfies all of the following:

| Property | Meaning |
| --- | --- |
| Deferred execution | The query is not sent before subscription. |
| Non-blocking I/O | No `Future#get()`, `CompletableFuture#join()`, `block()`, blocking semaphore, or thread-per-request wrapper on the query path. |
| Stream-oriented consumption | Responses are decoded incrementally; large results are not aggregated in memory before rows are emitted. |
| Backpressure-aware delivery | Downstream demand influences upstream work and buffering; intermediate queues stay bounded and documented. |
| Cancellation propagation | Cancelling a subscription removes a queued request or aborts an active exchange, releases buffers and connections, and — once the request has reached the server — sends a best-effort `KILL QUERY` so the server stops too; see [Known limitations](#known-limitations) for why that's "best-effort" rather than a hard guarantee. |
| Bounded concurrency | Maximum active requests, maximum pending requests, and pending-acquire timeout are explicit; overload produces a predictable error, not an invisible queue. |
| Deterministic cleanup | Connections, response bodies, buffers, and decoder state are released on completion, error, timeout, and cancellation. |
| Reactive error signalling | Transport and ClickHouse errors surface through `onError` with proper R2DBC exception mapping. |
| No scheduler workaround | Moving blocking I/O to `boundedElastic`/`publishOn`/`subscribeOn` does not count as making the path reactive. |

## Connection pooling

There are **two separate pools**, at two separate layers — understanding which one does what
matters for tuning either driver correctly, and it's exactly the confusion the [Why](#why) section
above names as the original motivation for this project.

```mermaid
flowchart LR
    APP["Your code<br/>(Mono/Flux, DatabaseClient...)"] --> L1

    subgraph L1["Layer 1 - optional<br/>io.r2dbc.pool"]
        POOL["ConnectionPool<br/>pools ClickHouseConnection handles"]
    end

    L1 --> CONN["ClickHouseConnection<br/>thin, disposable, no real resource"]
    CONN --> TRANSPORT["ClickHouseHttpTransport<br/>one instance, shared by every Connection"]

    subgraph L2["Layer 2 - always on<br/>Reactor Netty"]
        TRANSPORT --> NETTY["ConnectionProvider<br/>pools the real TCP connections"]
    end

    NETTY --> CH[("ClickHouse server<br/>HTTP interface")]
```

Layer 1 is optional and, in this driver, mostly a lifecycle/API-contract concern — not a scarce
resource guard. Layer 2 is the one actually holding physical sockets and is always present,
whether you configure it or not.

1. **R2DBC-SPI-level pool** — `io.r2dbc.pool`'s `ConnectionPool`, the standard R2DBC pooling
   implementation (not something this driver builds itself; the same one every other R2DBC driver
   plugs into the same way), wrapping this driver's `ClickHouseConnectionFactory`. What it actually
   pools here is cheap: each `ClickHouseConnection` is a thin, disposable `AtomicBoolean`-guarded
   handle over a *shared* transport — constructing one costs essentially nothing (see
   `ClickHouseConnectionFactory`'s own Javadoc). `Connection#validate(ValidationDepth)` is real, not
   a stub, at `REMOTE` depth: it round-trips a `SELECT 1` against the server; `LOCAL` only checks the
   connection's own `closed` flag. Verified end to end (query execution, remote validation, and
   `maxSize` actually bounding concurrent acquires — a second concurrent acquire waits, it doesn't
   error or silently exceed the bound) in
   [`ClickHouseConnectionFactoryR2dbcPoolAgainstRealClickHouseTest`](clickhouse-r2dbc-reactive-connector/src/test/java/io/github/camilyed/clickhouse/r2dbc/connector/ClickHouseConnectionFactoryR2dbcPoolAgainstRealClickHouseTest.java)
   against a real server.

   **How to configure it** — two ways, depending on whether Spring Boot is involved:

   - **Spring Boot**: `spring.r2dbc.pool.*` (see [Using with Spring
     Boot](#using-with-spring-boot) above for a full `application.yml` example). Maps onto
     `ConnectionPoolConfiguration.Builder` as follows:

     | Property | Builder method | Meaning |
     | --- | --- | --- |
     | `enabled` | *(whether a pool is built at all)* | `false` returns the unpooled `ConnectionFactory` directly |
     | `initial-size` | `initialSize` | Connections eagerly created at pool startup |
     | `min-idle` | `minIdle` | Minimum idle connections kept around, even under low load |
     | `max-size` | `maxSize` | Hard cap on concurrent connections; further acquires wait |
     | `max-idle-time` | `maxIdleTime` | How long an idle connection sits before eviction |
     | `max-life-time` | `maxLifeTime` | Max total lifetime of a connection, regardless of idle activity |
     | `max-acquire-time` | `maxAcquireTime` | How long an acquire waits before failing, once `max-size` is saturated |
     | `max-create-connection-time` | `maxCreateConnectionTime` | Timeout for actually establishing a new connection |
     | `max-validation-time` | `maxValidationTime` | Timeout for a `validate(...)` check during acquire |
     | `validation-depth` | `validationDepth` | `LOCAL` (cheap, checks the handle's own state) or `REMOTE` (round-trips `SELECT 1`) |
     | `acquire-retry` | `acquireRetry` | How many times a failed acquire is retried before giving up |

     Plain Spring Boot auto-configuration builds this pool correctly on its own (it's the
     `DatabaseClient` bean, not the pool, that needs the workaround described above) — but this
     project's `R2dbcConfiguration` reference still builds it explicitly, adding fail-fast
     validation (e.g. `initial-size > max-size` fails startup immediately, not on the first
     confusing pool-exhaustion timeout) and a startup log line stating exactly what was configured.

   - **Without Spring Boot**: wrap the URL with `io.r2dbc.pool`'s own `pool:` driver prefix — no
     extra code needed, since `io.r2dbc.pool` registers its own `ConnectionFactoryProvider` that
     wraps whatever driver the inner URL resolves to:

     ```java
     ConnectionFactory pooled =
         ConnectionFactories.get("r2dbc:pool:clickhouse://localhost:8123");
     ```

     Requires `io.r2dbc:r2dbc-pool` on the classpath; pool tuning through this URL form uses
     `io.r2dbc.pool`'s own query-parameter conventions — see [r2dbc-pool's own
     documentation](https://github.com/r2dbc/r2dbc-pool) for the exact parameter names. Building a
     `ConnectionPoolConfiguration` programmatically (as `R2dbcConfiguration` does) gives the same
     result with the properties table above, just without the URL-string indirection.

2. **Transport-level pool** — `ClickHouseHttpTransport`'s own Reactor Netty `ConnectionProvider`,
   pooling the actual TCP connections. This is the pool this project's non-blocking architecture is
   actually built around, and it's shared by *every* `ClickHouseConnection` this factory produces,
   regardless of how many R2DBC-SPI-level connections layer 1 hands out above it. Confirmed (not
   assumed) to correctly reuse one TCP connection across sequential queries — for both a
   `.next()`-style single-row consumption pattern and a fully-drained stream — in
   [`ClickHouseHttpTransportConnectionReuseTest`](clickhouse-r2dbc-reactive-transport-http/src/test/java/io/github/camilyed/clickhouse/r2dbc/transport/http/ClickHouseHttpTransportConnectionReuseTest.java),
   including against Reactor Netty's own debug-level pool logging (`Channel acquired`/`Releasing
   channel`/`Channel cleaned` against the same channel ID across requests) as independent
   confirmation — an earlier draft of that investigation suspected a bug here and was wrong; see the
   test's own Javadoc for why the first diagnostic signal was misleading.

   **Configuring it.** Five `transport...` R2DBC connection options — deliberately not `pool...`, so
   they're never confused with `spring.r2dbc.pool.*` above, which configures the *other*,
   R2DBC-SPI-level pool — map directly onto Reactor Netty's own `ConnectionProvider.Builder`:

   | R2DBC option | Reactor Netty `ConnectionProvider.Builder` method | Meaning |
   | --- | --- | --- |
   | `transportMaxConnections` | `maxConnections` | Physical TCP connections open to ClickHouse at once, per remote host |
   | `transportPendingAcquireMaxCount` | `pendingAcquireMaxCount` | How many acquire requests queue once `transportMaxConnections` is saturated, before new ones fail fast |
   | `transportPendingAcquireTimeout` | `pendingAcquireTimeout` | How long a queued acquire waits before failing with a pool-timeout error |
   | `transportMaxIdleTime` | `maxIdleTime` | How long an idle pooled connection sits before it's closed and evicted |
   | `transportMaxLifeTime` | `maxLifeTime` | Max total lifetime of a pooled connection, regardless of idle activity |

   All five default to Reactor Netty's own default (see the table below) when not set — this driver
   never invents a default of its own. Available both via `ConnectionFactoryOptions` (typed values,
   e.g. `.option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, 8)`) and via the
   R2DBC URL query string (e.g. `r2dbc:clickhouse://host?transportMaxConnections=8`, Duration values
   as ISO-8601 text, e.g. `transportPendingAcquireTimeout=PT2S`) — so a Spring Boot app configuring
   `spring.r2dbc.url=r2dbc:clickhouse://...` (the path almost everyone actually uses) can size this
   pool the same way it configures everything else, no separate Java-only path needed. An invalid
   value (e.g. a negative duration, a non-positive connection count) fails fast at factory creation,
   never silently falling back to a default. Every construction path through `ClickHouseHttpTransport`
   routes through one `TransportOptions` config object internally, so these five options and every
   other transport-level setting (`responseTimeout`, a custom trusted certificate, `RetryPolicy`,
   ...) compose freely instead of each needing its own constructor overload.

   **Reactor Netty's own defaults, since nothing here is required reading to use this driver** —
   this is what `ConnectionProvider.create(name)` gives you when nothing above is set:

   | Setting | Default | Meaning |
   | --- | --- | --- |
   | `maxConnections` | `max(availableProcessors, 8) * 2` (at least 16) | Physical TCP connections open to ClickHouse at once, per remote host |
   | `pendingAcquireMaxCount` | `500` | How many acquire requests queue once `maxConnections` is saturated, before new ones fail fast |
   | `pendingAcquireTimeout` | `45s` | How long a queued acquire waits before failing with a pool-timeout error |
   | `maxIdleTime` / `maxLifeTime` | unset (no eviction) | An idle or long-lived pooled connection is never proactively closed by the pool itself — it's held until the server closes it or the app shuts down |
   | Leasing strategy | `fifo` | Idle connections are handed out oldest-released-first |

   **Is it worth setting `maxConnections` yourself?** Usually not, and that's the point of this
   project: because the pipeline is non-blocking end to end, many more *logical* concurrent queries
   can be in flight than there are physical connections — `Flux.flatMap(..., concurrency)` submits
   all of them immediately, and whichever don't fit under `maxConnections` simply queue inside
   Reactor Netty with no thread blocked waiting, up to `pendingAcquireMaxCount`/
   `pendingAcquireTimeout`. `BoundedPoolConcurrencyBenchmark` (below) measures exactly this
   headroom at an artificially small 8-connection pool against 8/32/128 concurrent queries and this
   driver still wins on latency — the default pool (≥16 connections) already has more headroom than
   that benchmark's deliberately tight one. Reach for a higher `maxConnections` only if you've
   measured real pending-acquire waits under your own load (Reactor Netty logs these at `DEBUG`),
   or you deliberately want more physical parallelism against ClickHouse itself (e.g. ClickHouse's
   own `max_concurrent_queries` server-side limit becomes the real ceiling before this pool would).

**Why this is the point of the whole project, not an implementation detail:** client-v2's `Client`
is blocking — serving *N* logical concurrent queries needs *N* platform threads each blocked
waiting on a connection, one way or another. This driver's non-blocking pipeline lets many more
logical queries than physical connections be *in flight* at once — `Flux.flatMap(..., concurrency)`
subscribes to all of them immediately; whichever don't fit in the physical pool queue inside Reactor
Netty itself, with no blocked thread paying for each one. Measured directly:
`BoundedPoolConcurrencyBenchmark` and `PublicApiMatchedPoolThroughputBenchmark` (see
[docs/PERFORMANCE.md](docs/PERFORMANCE.md)) configure both this driver and client-v2 with the *same*
8-connection pool and drive 8/32/128 logical concurrent point queries at once (async on both sides,
not one blocking thread per query) — this driver wins decisively, **~4x the real throughput** through
the public R2DBC SPI at every concurrency level tested. Latest run is single-fork, not yet
multi-fork confirmed — still the strongest measured evidence so far for the architectural property
this project set out to provide. Still one pool size / three concurrency levels, not yet a full
scalability sweep — see that doc for the full caveats and what's still open.

> [!IMPORTANT]
> **Call this driver reactively (`Flux`/`Mono`/R2DBC), not wrapped in `.block()` per query.**
> `ConcurrencyBenchmark` and `MatchedPoolThreadsConcurrencyBenchmark` (see
> [docs/PERFORMANCE.md](docs/PERFORMANCE.md)) both drive this driver through
> `@Threads(N)`-blocking-callers — one platform thread blocked on `.block()` per in-flight query,
> the shape you get if you call this driver like a classic blocking JDBC driver. Both show this
> driver ~5–8% slower on mean than client-v2 under that calling style, reproducible whether the
> connection pool is matched to client-v2's or not — so it is not a pool-sizing problem and setting
> `transportMaxConnections` will not fix it. It is specifically the blocking calling style that
> forfeits this driver's advantage: `BoundedPoolConcurrencyBenchmark` above uses the identical pool
> size but drives concurrency through `Flux.flatMap` instead, and wins by 3.5–4x. If your application
> calls this driver through `.block()`/`.toFuture().get()` under real concurrent load, don't expect
> this driver's non-blocking design to pay off — and per the
> [Testing strategy](#testing-strategy)/["fully reactive" definition](#what-fully-reactive-means-here)
> below, that calling style is also the one thing this whole project is built to avoid you doing.

## Architecture direction

```mermaid
flowchart TD
    RSPI[R2DBC SPI] --> CONN["connector<br/>R2DBC SPI adapter"]
    CONN --> CORE["core<br/>query / settings / query_id<br/>owns the Transport port"]
    CORE -. public decoder classes only .-> V2DEC[("client-v2<br/>RowBinary decoders")]
    CORE --> PORT{{"Transport port<br/>(interface owned by core)"}}
    PORT --> HTTP["transport-http<br/>Reactor Netty, non-blocking"]
    HTTP --> CH[("ClickHouse<br/>HTTP interface")]
```

Only the dashed edge touches `client-v2`, and only for its public row-decoding classes
(`RowBinaryWithNamesAndTypesFormatReader` and friends). `client-v2`'s own HTTP client
(`internal.HttpAPIClientHelper`, classic Apache HttpClient5 I/O) is confirmed blocking by reading
its source and is **never called anywhere in this project** — `transport-http` owns its own
Reactor Netty client, from the socket up, independent of `client-v2` entirely. Full evidence in
[ROADMAP.md's Phase 0 finding](ROADMAP.md#phase-0--client-v2-execution-path-finding); a complete
audit of what `client-v2` actually sends on the wire (compression, auth, headers, error semantics)
is in [docs/CLIENT_V2_HTTP_REFERENCE.md](docs/CLIENT_V2_HTTP_REFERENCE.md) — useful background even
though none of that code is reused, since `transport-http` has to solve the same wire-protocol
problems independently.

Responsibility boundaries:

- **Connector** — R2DBC SPI discovery, URL/option parsing, connection lifecycle, statement
  creation and parameter binding, deferred execution, result/metadata adaptation, R2DBC exception
  mapping, cancellation propagation, explicit unsupported-transaction-semantics handling. No
  dependency on Spring.
- **Core** — query request representation, ClickHouse settings and `query_id`, protocol encoding,
  response metadata and row decoding, cancellation state, transport-independent lifecycle rules,
  and the `Transport` port interface that `transport-http` implements. Reuses `client-v2`'s public
  row-decoding classes only — never its transport; this is not a second general-purpose client.
- **Transport** — non-blocking connection acquisition, active/pending-request limits, connect/
  acquire/response/idle timeouts, streaming response chunks, aborting active requests, connection
  reuse, metrics. Built on Reactor Netty, entirely independent of `client-v2`.

A particular networking library is a swappable adapter behind the transport boundary, not a
public architectural dependency.

## Modules

The four modules below exist as Gradle modules today; whole-driver black-box coverage (through the
public R2DBC SPI only, against real ClickHouse) lives inside `connector`'s own
`*AgainstRealClickHouseTest` classes — a once-planned separate `integration-tests` module for this
was scaffolded, sat empty, and was later deleted rather than kept as a placeholder (see
[ROADMAP.md's module map](ROADMAP.md#module-map)). Full responsibilities and the reasoning behind
each boundary live there too — treat it as authoritative and this table as a quick summary only, to
avoid the two drifting apart.

| Module | Purpose |
| --- | --- |
| `clickhouse-r2dbc-reactive-core` | Transport-independent domain: queries/settings/`query_id`, the `Transport` port, row decoding (`client-v2`'s public decoder classes only — never its HTTP client). |
| `clickhouse-r2dbc-reactive-transport-http` | Non-blocking HTTP adapter (Reactor Netty), implementing `core`'s `Transport` port. No `client-v2` code. |
| `clickhouse-r2dbc-reactive-connector` | The R2DBC SPI implementation (`ConnectionFactoryProvider`, `Connection`, `Statement`, `Result`, metadata). |
| `clickhouse-r2dbc-reactive-testkit` | Shared test infrastructure: a fake wire-level server for deterministic transport contract tests, plus a real-ClickHouse Testcontainers DSL. |

Module boundaries may change before the first release; this table reflects current intent, not a
committed API.

## Requirements

| Requirement | Version |
| --- | --- |
| JDK | 21 |
| Reactive Streams | via Project Reactor |
| ClickHouse Java Client | `client-v2`, its public row-decoding classes only — its transport is confirmed blocking and is not used (see [Architecture direction](#architecture-direction)) |
| Verified database | ClickHouse (local Testcontainers instance for integration tests) |

## Testing strategy

1. **Static execution-path analysis** — map exact classes/methods in `clickhouse-java`, locate
   blocking calls, `CompletableFuture` boundaries, `InputStream` usage, pool/queue defaults, and
   identify Client V2 components safe to reuse.
2. **Transport contract tests** against a controlled local server: immediate response, delayed
   headers, delayed body, fragmented metadata/rows, partial final record, slow subscriber, no
   response, connection reset, error after partial data, cancellation before/during acquire,
   cancellation while queued, cancellation during body receive, pool saturation, pending-acquire
   timeout.
3. **ClickHouse integration tests** against a real instance: `SELECT 1`, large result sets,
   metadata, nullable values, arrays, parameter binding, timeout, cancellation, active-query
   verification, parallel requests, slow subscriber, bounded-memory verification.
4. **R2DBC tests**: provider discovery, URL parsing, connection creation, deferred execution,
   statement binding, result consumption, row access, metadata, error mapping, unsupported
   transaction operations, repeat-subscription behaviour, cleanup after cancellation.
5. **Performance and dependency impact**: throughput, p50/p95/p99 latency, time to first row,
   allocations/retained memory, cancellation latency, many-small-request workloads, large
   streaming-result workloads, dependency size and startup impact.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the detailed, gated working plan, and its
[Production readiness review](ROADMAP.md#production-readiness-review) for the current, up-to-date
list of what's fixed, safe-and-documented, or still an open gap — that section is updated far more
often than this one and is the one to check before depending on this driver. [CHANGELOG.md](CHANGELOG.md)
lists what shipped in each release.

`0.1.0` (execution-path analysis, transport spike, the full first R2DBC connector surface, Maven
Central publication) and [Phase 7/`0.2.0`](ROADMAP.md#phase-7--operational-control--r2dbc-correctness-020)
(configurable transport pool, statement timeout, correct `Statement.add()` batching, an
observability SPI, the R2DBC compatibility lane) are both done or in their final PR. What's next:

- Native TCP transport / HTTP multiplexing, evaluated as a separate track, not assumed to be faster
  without a profiler-identified bottleneck forcing it (see
  [What this project is not](#what-this-project-is-not))
- Whatever [ROADMAP.md's Production readiness review](ROADMAP.md#production-readiness-review) still
  lists as an open gap once `0.2.0` ships

## What this project is not

- Not a fork of `ClickHouse/clickhouse-java` or a replacement for its JDBC/Client V2 artifacts.
- Not a second general-purpose ClickHouse client — it reuses Client V2's public row-decoding
  classes only, not its transport (confirmed blocking, never used).
- Not a claim that reactive I/O alone fixes inefficient query patterns; application-level read
  model design is a separate concern from transport architecture.
- Not committed to Reactor Netty as a permanent dependency — it is the first transport candidate,
  evaluated against JDK HTTP client and other options.

## Relationship to ClickHouse/clickhouse-java

This is an independent project, not a fork. It depends on `com.clickhouse:client-v2` as a regular
Maven dependency, reusing its public row-decoding classes only — its HTTP transport is confirmed
blocking (classic Apache HttpClient5 I/O) and is not used; this project owns its own non-blocking
transport instead. See [ROADMAP.md's Phase 0 finding](ROADMAP.md#phase-0--client-v2-execution-path-finding)
for the verified evidence. If the design direction proves useful, parts of it may later be
proposed back to `ClickHouse/clickhouse-java` as a module or connector, following up on
[ClickHouse/ClickHouse#113638](https://github.com/ClickHouse/ClickHouse/discussions/113638).

## Contributing

Issues and discussion are welcome, especially around the open gaps tracked in
[ROADMAP.md's Production readiness review](ROADMAP.md#production-readiness-review). Formal
contribution guidelines (`CONTRIBUTING.md`) exist and cover the PR checklist; see there for the
current process.

## License

This project is licensed under the Apache License 2.0.

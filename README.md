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

A fully reactive R2DBC driver for ClickHouse. It reuses
[ClickHouse Java Client V2](https://github.com/ClickHouse/clickhouse-java)'s public row-decoding
classes only — its HTTP transport is confirmed blocking (classic Apache HttpClient5 I/O, verified
by reading the source) and is **never used**. Everything that touches the network — connection
handling, request/response streaming, cancellation — is this project's own, small, explicit,
non-blocking transport boundary. See
[docs/architecture/overview.md](docs/architecture/overview.md) for the full design and the
verified evidence behind that claim.

This project exists because implementing the R2DBC interfaces is not, by itself, enough to make
the complete execution path reactive. The goal here is an R2DBC driver where deferred execution,
non-blocking I/O, streaming decoding, bounded backpressure-aware buffering, cancellation, and
deterministic resource cleanup are true end to end, not just at the API surface — see
[docs/concepts/fully-reactive.md](docs/concepts/fully-reactive.md) for exactly what that means and
how it's verified.

The design direction started as a public design discussion with the ClickHouse team:
[ClickHouse/ClickHouse#113638 — Design discussion: future direction for reactive R2DBC support in the Java client](https://github.com/ClickHouse/ClickHouse/discussions/113638).

Most consumers only need `connector` (it pulls `core`/`transport-http` in transitively) — see
[Installation](#installation). `testkit` is a separate, optional dependency for anyone writing
tests against this driver. Badges may briefly show "not found" right after a release — Central
Portal sync to the search index shields.io reads from can lag behind publication.

## Contents

- [Architecture at a glance](#architecture-at-a-glance)
- [Why](#why)
- [Status](#status)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Learn more](#learn-more)
- [Project](#project)

## Architecture at a glance

```mermaid
flowchart TD
    RSPI[R2DBC SPI] --> CONN["connector<br/>R2DBC SPI adapter"]
    CONN --> CORE["core<br/>query / settings / query_id<br/>owns the Transport port"]
    CORE -. public decoder classes only .-> V2DEC[("client-v2<br/>RowBinary decoders")]
    CORE --> PORT{{"Transport port<br/>(interface owned by core)"}}
    PORT --> HTTP["transport-http<br/>Reactor Netty, non-blocking"]
    HTTP --> CH[("ClickHouse<br/>HTTP interface")]
```

Only the dashed edge touches `client-v2`, and only for its public row-decoding classes —
`client-v2`'s own HTTP client is confirmed blocking and is **never called anywhere in this
project**; `transport-http` owns its own non-blocking Reactor Netty client, from the socket up.
Full responsibility boundaries and reasoning: [docs/architecture/overview.md](docs/architecture/overview.md).

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

Functional, `0.2.1` published to Maven Central. The full R2DBC SPI surface exists and is exercised
against a real ClickHouse server (Testcontainers): connection lifecycle,
`SELECT`/`INSERT`/parameterized statements, batches, row/column metadata, `getRowsUpdated()`, and
R2DBC exception mapping for ClickHouse server errors. **The driver has not been run against a
production workload** — everything above is confirmed by an automated test suite (unit, transport
contract tests against a controlled fake server, and real-ClickHouse integration tests via
Testcontainers, see [docs/internals/testing-strategy.md](docs/internals/testing-strategy.md)), not
by production experience yet.

Before relying on this in production, read
[docs/reference/known-limitations.md](docs/reference/known-limitations.md) and
[engineering/roadmap-archive.md's Production readiness
review](engineering/roadmap-archive.md#production-readiness-review) — an explicit,
honestly-triaged list of what's fixed, what's a documented safe limitation, and what's still an
open gap. Those pages are the actual source of truth for "is this safe to depend on today"; treat
this README as a summary, not the other way around. See [ROADMAP.md](ROADMAP.md) for what's
released, in progress, next, and explicitly not planned, and [CHANGELOG.md](CHANGELOG.md) for what
shipped in each release. Still expect breaking changes at this stage.

## Requirements

| Requirement | Version |
| --- | --- |
| JDK | 21 |
| Reactive Streams | via Project Reactor |
| ClickHouse Java Client | `client-v2`, its public row-decoding classes only — its transport is confirmed blocking and is not used (see [docs/architecture/overview.md](docs/architecture/overview.md)) |
| Verified database | ClickHouse (local Testcontainers instance for integration tests) |

## Installation

Published to Maven Central under `io.github.camilyed`. Check the badge at the top of this file, or
[central.sonatype.com](https://central.sonatype.com/search?q=io.github.camilyed), for the latest
version.

```kotlin
dependencies {
    implementation("io.github.camilyed:clickhouse-r2dbc-reactive-connector:0.2.1")
}
```

Depending on `clickhouse-r2dbc-reactive-connector` alone is enough — it pulls in
`clickhouse-r2dbc-reactive-core` and `clickhouse-r2dbc-reactive-transport-http` on the runtime
classpath (they're `implementation`, not `api`, dependencies of the connector on purpose: they're
this driver's own internals, not part of its public surface).

Building from source instead:

```bash
git clone https://github.com/CamilYed/clickhouse-r2dbc-reactive.git
cd clickhouse-r2dbc-reactive
./gradlew publishToMavenLocal
```

then depend on it with `mavenLocal()` in your `repositories { }` block and the version from
`gradle.properties`/`-PreleaseVersion` (defaults to `0.2.1-SNAPSHOT`).

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
the bound value, this driver does not reinterpret or validate it. `String`, numeric types, `null`,
`UUID`, `BigDecimal`, `LocalDate`, `Boolean`, `enum` constants, temporal types, and `List` (as an
`Array` literal) are all supported — see
[docs/reference/configuration.md](docs/reference/configuration.md) for every connection option, and
[`ClickHouseQuery.withParameters`](clickhouse-r2dbc-reactive-core/src/main/java/io/github/camilyed/clickhouse/r2dbc/core/ClickHouseQuery.java)'s
Javadoc for exactly how each parameter type is encoded.

### Using DatabaseClient (Spring)

Spring's own `DatabaseClient` works too, once wired past one auto-configuration gap Spring has for
this driver (see the callout below):

```java
import org.springframework.r2dbc.core.DatabaseClient;

Mono<Long> count =
    databaseClient
        .sql("SELECT count() AS total FROM users")
        .map(row -> row.get("total", Long.class))
        .one();

Flux<String> names =
    databaseClient.sql("SELECT name FROM users ORDER BY name").map(row -> row.get("name", String.class)).all();
```

> [!IMPORTANT]
> Spring Boot's own auto-configuration cannot build a `DatabaseClient` bean for this driver on its
> own — it fails with `IllegalStateException: Cannot determine a BindMarkersFactory for
> ClickHouse`, because Spring's driver-detection list doesn't include ClickHouse. A plain
> `ConnectionFactory`/`Connection` (the example above) is unaffected. See
> [docs/guide/spring-boot.md](docs/guide/spring-boot.md) for the few lines of `application.yml`
> needed, the `DatabaseClient` fix (copy `R2dbcConfiguration` from the reference demo), and why
> `.bind(...)`/`R2dbcEntityTemplate` still don't work even with that fix.

## Learn more

This README covers "does this work and how do I add it to a build." Everything past that —
internals, tuning, and the honest limitations — lives in `docs/`:

| Topic | Where |
| --- | --- |
| Every connection option | [docs/reference/configuration.md](docs/reference/configuration.md) |
| Known limitations, honestly triaged | [docs/reference/known-limitations.md](docs/reference/known-limitations.md) |
| What "fully reactive" means, and how it's verified | [docs/concepts/fully-reactive.md](docs/concepts/fully-reactive.md) |
| Connection pooling (this driver's own, and the optional `io.r2dbc.pool` layer) | [docs/operations/connection-pooling.md](docs/operations/connection-pooling.md) |
| Spring Boot guide | [docs/guide/spring-boot.md](docs/guide/spring-boot.md) |
| Architecture direction and module responsibilities | [docs/architecture/overview.md](docs/architecture/overview.md) |
| Testing strategy | [docs/internals/testing-strategy.md](docs/internals/testing-strategy.md) |
| Performance measurements | [docs/performance/](docs/performance/index.md) |
| R2DBC SPI compatibility (TCK results) | [docs/reference/r2dbc-compatibility.md](docs/reference/r2dbc-compatibility.md) |
| What client-v2 actually sends on the wire | [docs/internals/client-v2-http-reference.md](docs/internals/client-v2-http-reference.md) |

## Project

### Roadmap

See [ROADMAP.md](ROADMAP.md) for what's released, in progress, next, and explicitly not planned.
[CHANGELOG.md](CHANGELOG.md) lists what shipped in each release. For the full phase-by-phase
history, engineering rationale, and the [Production readiness
review](engineering/roadmap-archive.md#production-readiness-review) (the ✅/⚠️/❌ matrix of what's
fixed, safe-and-documented, or still an open gap), see
[engineering/roadmap-archive.md](engineering/roadmap-archive.md).

### What this project is not

- Not a fork of `ClickHouse/clickhouse-java` or a replacement for its JDBC/Client V2 artifacts.
- Not a second general-purpose ClickHouse client — it reuses Client V2's public row-decoding
  classes only, not its transport (confirmed blocking, never used).
- Not a claim that reactive I/O alone fixes inefficient query patterns; application-level read
  model design is a separate concern from transport architecture.
- Not committed to Reactor Netty as a permanent dependency — it is the first transport candidate,
  evaluated against JDK HTTP client and other options.

### Relationship to ClickHouse/clickhouse-java

This is an independent project, not a fork. It depends on `com.clickhouse:client-v2` as a regular
Maven dependency, reusing its public row-decoding classes only — its HTTP transport is confirmed
blocking (classic Apache HttpClient5 I/O) and is not used; this project owns its own non-blocking
transport instead. See [docs/architecture/overview.md](docs/architecture/overview.md) for the
verified evidence. If the design direction proves useful, parts of it may later be proposed back to
`ClickHouse/clickhouse-java` as a module or connector, following up on
[ClickHouse/ClickHouse#113638](https://github.com/ClickHouse/ClickHouse/discussions/113638).

### Contributing

Issues and discussion are welcome, especially around the open gaps tracked in
[docs/reference/known-limitations.md](docs/reference/known-limitations.md). Formal contribution
guidelines (`CONTRIBUTING.md`) exist and cover the PR checklist; see there for the current process.

### License

This project is licensed under the Apache License 2.0.

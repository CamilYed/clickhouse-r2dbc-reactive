# ClickHouse R2DBC Reactive

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-design%20%2F%20pre--alpha-orange.svg)](#status)
[![Java](https://img.shields.io/badge/Java-21-blue.svg)](https://openjdk.org/projects/jdk/21/)
[![Reactor](https://img.shields.io/badge/Reactor-Mono%20%7C%20Flux-blueviolet.svg)](https://projectreactor.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Client%20V2-FFCC01.svg)](https://github.com/ClickHouse/clickhouse-java)

A fully reactive R2DBC driver for ClickHouse, built as a thin connector on top of the
[ClickHouse Java Client V2](https://github.com/ClickHouse/clickhouse-java), with a small,
explicit, non-blocking transport boundary.

This project exists because implementing the R2DBC interfaces is not, by itself, enough to make
the complete execution path reactive. The goal here is an R2DBC driver where deferred execution,
non-blocking I/O, streaming decoding, bounded backpressure-aware buffering, cancellation, and
deterministic resource cleanup are true end to end, not just at the API surface.

The design direction started as a public design discussion with the ClickHouse team:
[ClickHouse/ClickHouse#113638 — Design discussion: future direction for reactive R2DBC support in the Java client](https://github.com/ClickHouse/ClickHouse/discussions/113638).

## Contents

- [Why](#why)
- [Status](#status)
- [What "fully reactive" means here](#what-fully-reactive-means-here)
- [Architecture direction](#architecture-direction)
- [Planned modules](#planned-modules)
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

Early design and validation phase. No production implementation exists yet.

The current focus is:

1. verifying the existing ClickHouse Java client execution path (blocking boundaries,
   `CompletableFuture` usage, streaming vs. aggregation, pool and queue ownership);
2. a small transport spike proving non-blocking I/O, streaming decoding, bounded buffering,
   and cancellation with `SELECT 1` and a streamed multi-row result;
3. only after that, building out the R2DBC SPI surface.

Expect breaking changes at every stage before a `0.1.0` release.

## What "fully reactive" means here

Returning a reactive type is a necessary but insufficient condition. A driver is treated as
reactive end to end only if it satisfies all of the following:

| Property | Meaning |
| --- | --- |
| Deferred execution | The query is not sent before subscription. |
| Non-blocking I/O | No `Future#get()`, `CompletableFuture#join()`, `block()`, blocking semaphore, or thread-per-request wrapper on the query path. |
| Stream-oriented consumption | Responses are decoded incrementally; large results are not aggregated in memory before rows are emitted. |
| Backpressure-aware delivery | Downstream demand influences upstream work and buffering; intermediate queues stay bounded and documented. |
| Cancellation propagation | Cancelling a subscription removes a queued request or aborts an active exchange, releases buffers and connections, and can cancel the server-side query via `query_id`. |
| Bounded concurrency | Maximum active requests, maximum pending requests, and pending-acquire timeout are explicit; overload produces a predictable error, not an invisible queue. |
| Deterministic cleanup | Connections, response bodies, buffers, and decoder state are released on completion, error, timeout, and cancellation. |
| Reactive error signalling | Transport and ClickHouse errors surface through `onError` with proper R2DBC exception mapping. |
| No scheduler workaround | Moving blocking I/O to `boundedElastic`/`publishOn`/`subscribeOn` does not count as making the path reactive. |

## Architecture direction

```text
R2DBC SPI
  -> thin ClickHouse R2DBC connector
       -> reusable query/protocol components (ClickHouse Java Client V2)
            -> small transport boundary
                 -> non-blocking HTTP adapter (Reactor Netty candidate)
                 -> possible future native TCP adapter
```

Responsibility boundaries:

- **Connector** — R2DBC SPI discovery, URL/option parsing, connection lifecycle, statement
  creation and parameter binding, deferred execution, result/metadata adaptation, R2DBC exception
  mapping, cancellation propagation, explicit unsupported-transaction-semantics handling. No
  dependency on Spring.
- **Core** — query request representation, ClickHouse settings and `query_id`, protocol encoding,
  response metadata and row decoding, cancellation state, transport-independent lifecycle rules.
  Reuses stable Client V2 components where they fit; this is not a second general-purpose client.
- **Transport** — non-blocking connection acquisition, active/pending-request limits, connect/
  acquire/response/idle timeouts, streaming response chunks, aborting active requests, connection
  reuse, metrics. The chosen HTTP library (Reactor Netty is the current candidate) stays an
  implementation detail and must not leak into the public API.

A particular networking library is a swappable adapter behind the transport boundary, not a
public architectural dependency.

## Planned modules

| Module | Purpose |
| --- | --- |
| `clickhouse-r2dbc-reactive-core` | Transport-independent query/protocol core, reused where possible from Client V2. |
| `clickhouse-r2dbc-reactive-transport-http` | Non-blocking HTTP transport adapter (Reactor Netty candidate). |
| `clickhouse-r2dbc-reactive-connector` | The R2DBC SPI implementation (`ConnectionFactoryProvider`, `Connection`, `Statement`, `Result`, metadata). |
| `clickhouse-r2dbc-reactive-testkit` | Controlled local server and contract tests for transport behaviour (delayed headers/body, fragmented rows, slow subscriber, pool saturation, cancellation at every stage). |

Module boundaries may change before the first release; this table reflects current intent, not a
committed API.

## Requirements

| Requirement | Version |
| --- | --- |
| JDK | 21 |
| Reactive Streams | via Project Reactor |
| ClickHouse Java Client | `client-v2` (reused components, not forked) |
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

Near-term:

- Publish verified execution-path analysis of the existing ClickHouse Java client
- Transport spike: `SELECT 1` through non-blocking I/O, incremental decoding, deterministic
  cleanup, against a controlled local server
- Define the small transport SPI (active/pending limits, timeouts, cancellation, streaming chunks)

Later:

- First R2DBC connector surface reusing Client V2 core components
- ClickHouse integration test matrix (Testcontainers)
- Benchmarks vs. the existing R2DBC driver
- Maven Central publication (`io.github.camilyed`), following the same release process used in
  [`spring-reactive-transaction-boundary`](https://github.com/CamilYed/spring-reactive-transaction-boundary)
- Evaluate HTTP multiplexing / native TCP transport as a separate track

## What this project is not

- Not a fork of `ClickHouse/clickhouse-java` or a replacement for its JDBC/Client V2 artifacts.
- Not a second general-purpose ClickHouse client — it reuses Client V2 core components.
- Not a claim that reactive I/O alone fixes inefficient query patterns; application-level read
  model design is a separate concern from transport architecture.
- Not committed to Reactor Netty as a permanent dependency — it is the first transport candidate,
  evaluated against JDK HTTP client and other options.

## Relationship to ClickHouse/clickhouse-java

This is an independent project, not a fork. It depends on `com.clickhouse:client-v2` as a regular
Maven dependency and reuses its stable components instead of duplicating them. If the design
direction proves useful, parts of it may later be proposed back to `ClickHouse/clickhouse-java`
as a module or connector, following up on
[ClickHouse/ClickHouse#113638](https://github.com/ClickHouse/ClickHouse/discussions/113638).

## Contributing

Issues and discussion are welcome, especially around the execution-path analysis and transport
spike. Contribution guidelines will be added once the first module skeleton lands.

## License

This project is licensed under the Apache License 2.0.

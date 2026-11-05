# Connection configuration reference

Every option this driver accepts, set through the R2DBC URL's query string or
`ConnectionFactoryOptions.builder()` directly. See the [main README](../../README.md#usage) for a
minimal working example, and [../operations/connection-pooling.md](../operations/connection-pooling.md)
for the two connection-pooling options tables (`transport...` and, if you opt into it,
`io.r2dbc.pool`'s own).

## Endpoint and authentication

| Option | Default | Meaning |
| --- | --- | --- |
| `host` | *(required)* | ClickHouse server host |
| `port` | `8123` | ClickHouse HTTP interface port |
| `ssl` | `false` | Use HTTPS |
| `user` / `password` | none (anonymous) | HTTP basic auth against ClickHouse |
| `database` | connecting user's default | Database selected via `X-ClickHouse-Database` on every request, e.g. `r2dbc:clickhouse://host:8123/analytics` |
| `sslRootCert` | none (JVM default trust store) | Classpath resource or filesystem path to a PEM-encoded trusted certificate, for self-signed/internal-CA servers — only meaningful with `ssl=true` |

## Timeouts

| Option | Default | Meaning |
| --- | --- | --- |
| `connectTimeout` | none | How long to wait for the underlying TCP connection to establish, before any request is even sent — R2DBC's standard `ConnectionFactoryOptions.CONNECT_TIMEOUT` |
| `responseTimeout` | none | How long to wait for response bytes once a request has been sent. See [`ClickHouseHttpTransport`](../../clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/ClickHouseHttpTransport.java)'s Javadoc for why there's no implicit limit by default — ClickHouse is analytical, a legitimate query can run far longer than a typical OLTP request |

`connectTimeout`, `responseTimeout`, `transportPendingAcquireTimeout` (see the pooling doc), and the
server-side `statementTimeout` (`ClickHouseConnection.setStatementTimeout`, `max_execution_time`)
each bound a different phase of a request and are easy to conflate — see
[`ClickHouseConnectionFactoryProvider.RESPONSE_TIMEOUT`](../../clickhouse-r2dbc-reactive-connector/src/main/java/io/github/camilyed/clickhouse/r2dbc/connector/ClickHouseConnectionFactoryProvider.java)'s
Javadoc for how all four relate.

## Response compression and retry

| Option | Default | Meaning |
| --- | --- | --- |
| `responseCompression` | `true` | Ask ClickHouse to use its native checksummed response-compression blocks (`compress=1`, not standard HTTP `Content-Encoding`) and transparently decompress LZ4 blocks from older servers or ZSTD blocks from ClickHouse 26.9+. Matches client-v2's own default (`COMPRESS_SERVER_RESPONSE=true`); set to `false` to send/receive uncompressed. See [`ResponseCompression`](../../clickhouse-r2dbc-reactive-core/src/main/java/io/github/camilyed/clickhouse/r2dbc/core/ResponseCompression.java)'s Javadoc for the wire format |
| `retryMaxAttempts` | `3` | Retries for failures before any request bytes reached the server — see [`RetryPolicy`](../../clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/RetryPolicy.java) for exactly what qualifies |
| `retryDelay` | `50ms` | Fixed delay between retry attempts |

The core/transport API also supports opt-in retry of a retryable ClickHouse server error through
`ClickHouseQuery.withServerErrorRetryEnabled()`, provided no response bytes were emitted. That
opt-in is not currently exposed by the R2DBC `Statement` API; queries executed through standard
R2DBC therefore use the pre-send-only behavior described in the table.

Rows are decoded through the client-v2-backed reader. The experimental `rowDecoder` option was
removed after its isolated decoder improvement did not produce a meaningful end-to-end gain;
configurations that still set it fail fast instead of being silently ignored.

## Per-statement ClickHouse settings

R2DBC has no standard API for database-specific server settings. This driver exposes a deliberately
small vendor extension without changing the standard `Statement` contract:

```java
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseStatement;

ClickHouseStatement statement =
    (ClickHouseStatement) connection.createStatement("SELECT * FROM events");
statement
    .setClickHouseSetting("max_threads", "2")
    .setClickHouseSetting("log_comment", "monthly-report");
```

`ClickHouseConnection.createStatement(...)` has a covariant return type, so code that already holds
the concrete `ClickHouseConnection` does not need the cast. Calling the method again with the same
name replaces the previous value. A statement-specific `max_execution_time` overrides the snapshot
inherited from `Connection.setStatementTimeout(...)`.

Names must use letters, digits, and underscores and cannot start with a digit. The driver rejects
request parameters it owns (`query`, `query_id`, `compress`, `database`, response-format controls,
and every `param_*` name). Those are protocol invariants, not server execution settings; allowing a
statement to replace them could break decoding, parameter binding, cancellation, or resource
ownership.

The transport remains Reactor Netty. This extension forwards ClickHouse server settings only; it
does not mirror client-v2's Apache HTTP client or socket options. In particular, client-v2 0.11.0's
socket-buffer default change does not apply to this driver.

The response format is also driver-owned. Normal `Statement` queries always request
`RowBinaryWithNamesAndTypes` using `X-ClickHouse-Format`; do not add an explicit SQL `FORMAT` clause.
ClickHouse changed precedence in 26.8, so mixing the header and SQL clause is not portable across
the server versions in this project's compatibility matrix. Explicit input formats remain valid for
`ClickHouseConnection.insertStreaming(...)`, where the caller supplies the encoded request body.

Spring Boot users configuring `spring.r2dbc.url=r2dbc:clickhouse://...` get all of the above for
free through Spring's own R2DBC auto-configuration — see [../guide/spring-boot.md](../guide/spring-boot.md)
for the full Spring Boot guide, including the one thing Spring's auto-configuration does *not* get
right for this driver on its own (`DatabaseClient`).

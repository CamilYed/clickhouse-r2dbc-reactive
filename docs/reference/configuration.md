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
| `responseCompression` | `true` | Ask ClickHouse to compress response bodies with its own custom LZ4 block framing (`compress=1`, not standard HTTP `Content-Encoding`) and transparently decompress them. Matches client-v2's own default (`COMPRESS_SERVER_RESPONSE=true`); set to `false` to send/receive uncompressed. See [`ResponseCompression`](../../clickhouse-r2dbc-reactive-core/src/main/java/io/github/camilyed/clickhouse/r2dbc/core/ResponseCompression.java)'s Javadoc for the wire format |
| `retryMaxAttempts` | `3` | Retries for failures before any request bytes reached the server — see [`RetryPolicy`](../../clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/RetryPolicy.java) for exactly what qualifies |
| `retryDelay` | `50ms` | Fixed delay between retry attempts |

The core/transport API also supports opt-in retry of a retryable ClickHouse server error through
`ClickHouseQuery.withServerErrorRetryEnabled()`, provided no response bytes were emitted. That
opt-in is not currently exposed by the R2DBC `Statement` API; queries executed through standard
R2DBC therefore use the pre-send-only behavior described in the table.

## Row decoder

| Option | Default | Meaning |
| --- | --- | --- |
| `rowDecoder` | `clickhouse` | `clickhouse` uses client-v2's reader. `native` uses this driver's native scalar RowBinary reader when every result column is supported and falls back to client-v2 for the whole result otherwise. Both modes produce the same values and Java types; `native` remains opt-in. |

Spring Boot users configuring `spring.r2dbc.url=r2dbc:clickhouse://...` get all of the above for
free through Spring's own R2DBC auto-configuration — see [../guide/spring-boot.md](../guide/spring-boot.md)
for the full Spring Boot guide, including the one thing Spring's auto-configuration does *not* get
right for this driver on its own (`DatabaseClient`).

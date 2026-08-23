# Optional: `io.r2dbc.pool`'s `ConnectionPool`

**Most applications don't need this page.** This driver already pools its physical TCP connections
itself — see [connection-pooling.md](connection-pooling.md) — so `io.r2dbc.pool` is not needed to
guard a scarce resource here. It's a genuinely optional, advanced layer: read on only if you
specifically need its `Connection#validate(ValidationDepth)`/eviction behavior, or a framework
integration that expects to manage R2DBC-SPI-level pooling itself.

`io.r2dbc.pool`'s `ConnectionPool` is the standard R2DBC pooling implementation (not something this
driver builds itself; the same one every other R2DBC driver plugs into the same way), wrapping this
driver's `ClickHouseConnectionFactory`. What it actually pools here is cheap: each
`ClickHouseConnection` is a thin, disposable `AtomicBoolean`-guarded handle over a *shared*
transport — constructing one costs essentially nothing (see `ClickHouseConnectionFactory`'s own
Javadoc). `Connection#validate(ValidationDepth)` is real, not a stub, at `REMOTE` depth: it
round-trips a `SELECT 1` against the server; `LOCAL` only checks the connection's own `closed`
flag. Verified end to end (query execution, remote validation, and `maxSize` actually bounding
concurrent acquires — a second concurrent acquire waits, it doesn't error or silently exceed the
bound) in
[`ClickHouseConnectionFactoryR2dbcPoolAgainstRealClickHouseTest`](../../clickhouse-r2dbc-reactive-connector/src/test/java/io/github/camilyed/clickhouse/r2dbc/connector/ClickHouseConnectionFactoryR2dbcPoolAgainstRealClickHouseTest.java)
against a real server.

## How to configure it

Two ways, depending on whether Spring Boot is involved:

### Spring Boot

`spring.r2dbc.pool.*` (see [../guide/spring-boot.md](../guide/spring-boot.md) for a full
`application.yml` example). Maps onto `ConnectionPoolConfiguration.Builder` as follows:

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
`DatabaseClient` bean, not the pool, that needs the workaround described in the Spring Boot guide)
— but this project's `R2dbcConfiguration` reference still builds it explicitly, adding fail-fast
validation (e.g. `initial-size > max-size` fails startup immediately, not on the first confusing
pool-exhaustion timeout) and a startup log line stating exactly what was configured.

### Without Spring Boot

Wrap the URL with `io.r2dbc.pool`'s own `pool:` driver prefix — no extra code needed, since
`io.r2dbc.pool` registers its own `ConnectionFactoryProvider` that wraps whatever driver the inner
URL resolves to:

```java
ConnectionFactory pooled =
    ConnectionFactories.get("r2dbc:pool:clickhouse://localhost:8123");
```

Requires `io.r2dbc:r2dbc-pool` on the classpath; pool tuning through this URL form uses
`io.r2dbc.pool`'s own query-parameter conventions — see [r2dbc-pool's own
documentation](https://github.com/r2dbc/r2dbc-pool) for the exact parameter names. Building a
`ConnectionPoolConfiguration` programmatically (as `R2dbcConfiguration` does) gives the same result
with the properties table above, just without the URL-string indirection.

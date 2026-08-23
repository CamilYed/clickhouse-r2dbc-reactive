# Using with Spring Boot

```kotlin
dependencies {
    implementation("io.github.camilyed:clickhouse-r2dbc-reactive-connector:<version>")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
}
```

```yaml
spring:
  r2dbc:
    url: r2dbc:clickhouse://localhost:8123
    username: default
    password: ""
```

That's it — `spring.r2dbc.url`/`username`/`password` are standard Spring Boot R2DBC properties,
nothing ClickHouse-specific about the names, only about the `r2dbc:clickhouse://` scheme they
configure. No `io.r2dbc:r2dbc-pool` dependency and no `pool:` block above: this driver's own
transport already pools physical connections underneath (see
[../operations/connection-pooling.md](../operations/connection-pooling.md)), so there's nothing
extra to add to get a working, production-usable setup. Add `io.r2dbc:r2dbc-pool` and
`spring.r2dbc.pool.*` only if you've decided you specifically want R2DBC-SPI-level pooling on top
of that — [../operations/optional-r2dbc-pool.md](../operations/optional-r2dbc-pool.md) explains
what that second layer actually buys you and when it's worth reaching for.

## The one thing Spring Boot's auto-configuration does *not* get right for this driver, on its own

Building a `DatabaseClient` bean fails outright with `IllegalStateException: Cannot determine a
BindMarkersFactory for ClickHouse`, because Spring's `BindMarkersFactoryResolver` only recognizes a
small hardcoded list of drivers (Postgres, MySQL, MariaDB, SQL Server, H2) and ClickHouse isn't on
it. This only affects `DatabaseClient`/`R2dbcEntityTemplate`-style usage — a plain
`ConnectionFactory` (or `Connection`, `Statement`, `Result`, as in the [main README's Usage
example](../../README.md#usage)) autowires and works with no extra configuration.

If your application does need `DatabaseClient`, copy
[`R2dbcConfiguration`](../../examples/spring-boot-webflux-demo/src/main/java/io/github/camilyed/clickhouse/r2dbc/examples/webfluxdemo/infrastructure/R2dbcConfiguration.java)
from the reference demo — it builds `ConnectionFactory`, pooled `ConnectionPool`, and
`DatabaseClient` beans by hand, reading the same `spring.r2dbc.*`/`spring.r2dbc.pool.*` properties
Spring Boot itself would, plus fail-fast validation of the pool settings and a startup log line
showing exactly what was configured. See that class's own Javadoc for the full reasoning, including
what still doesn't work even with this fix (`DatabaseClient.sql(...).bind(...)` — ClickHouse's
`{name:Type}` parameter syntax has no equivalent in R2DBC's generic `BindMarkersFactory`
abstraction, so the demo's repository issues fully pre-formed SQL instead).

See [`examples/spring-boot-webflux-demo`](../../examples/spring-boot-webflux-demo) itself for a
complete, runnable reference application (hexagonal layering, `io.r2dbc.pool` wiring, a real
ClickHouse schema with `LowCardinality`/`Enum8`/`IPv4` columns, and an analytics endpoint).

# Spring Boot + WebFlux demo — order events

A small, realistic-ish order-events service, not a toy CRUD sample — proves
`clickhouse-r2dbc-reactive` works through Spring's own R2DBC integration, laid out with the same
Ports & Adapters (Hexagonal Architecture) split the driver itself uses between `core` and its
transport/connector adapters (see the root project's [CLAUDE.md](../../CLAUDE.md), Architecture
section). Not published, not part of the driver's public surface.

This module never imports a single class from `clickhouse-r2dbc-reactive-*` — the driver is only
on the runtime classpath. Spring discovers it purely through `spring.r2dbc.url`'s
`r2dbc:clickhouse://...` scheme, resolved by the standard R2DBC `ConnectionFactoryProvider`
`ServiceLoader` mechanism, the same path every other R2DBC driver is discovered through.

## Layout

```
domain/          OrderEvent, OrderStatus, CategoryTotal, OrderEventRepository (a port - an
                  interface, no Spring/SQL types in sight)
infrastructure/   DatabaseClientOrderEventRepository (the port's only adapter - all the SQL and
                  R2DBC-specific decoding lives here), OrderEventsSchemaInitializer,
                  R2dbcConfiguration (ConnectionFactory/ConnectionPool/DatabaseClient/
                  ReactiveTransactionManager beans)
api/              OrderEventController - depends only on OrderEventRepository, never on
                  DatabaseClient/SQL directly
```

`OrderEventController` never touches SQL. `DatabaseClientOrderEventRepository` never touches HTTP.
Swapping the persistence adapter (a different driver, an in-memory fake for a unit test) would
never require touching `api` or `domain` — the same seam the driver's own `core`↔`transport-http`
boundary demonstrates, one layer up.

## What it proves

- **`R2dbcConfiguration`** — builds `ConnectionFactory` (wrapped in an `r2dbc-pool` `ConnectionPool`
  unless `spring.r2dbc.pool.enabled=false`, with fail-fast pool-setting validation and a startup log
  summary), `DatabaseClient`, and `ReactiveTransactionManager` by hand instead of through Spring
  Boot's own R2DBC auto-configuration. Building `DatabaseClient` explicitly (with an explicit
  anonymous `"?"` `BindMarkersFactory`) is what lets the bean construct at all: Spring Boot's own
  auto-configured `DatabaseClient` bean resolves its `BindMarkersFactory` from a small hardcoded
  driver list that doesn't include ClickHouse, and fails outright with `IllegalStateException:
  Cannot determine a BindMarkersFactory for ClickHouse` before a single query ever runs. See
  `R2dbcConfiguration`'s own Javadoc for the full write-up, and
  [ROADMAP.md's Phase 6](../../ROADMAP.md#phase-6-later--spring-webflux-interop-demo) for why this
  still doesn't make `.bind(...)`/`R2dbcEntityTemplate` actually usable against this driver
  (ClickHouse's `{name:Type}` parameter syntax needs the type inline in the SQL text, which
  `BindMarkersFactory` has no way to supply) — `DatabaseClientOrderEventRepository` therefore only
  ever issues fully pre-formed SQL with escaped literal values.
- **`spring-boot-starter-r2dbc`, not `spring-boot-starter-data-r2dbc`** — the latter pulls in
  `DataR2dbcAutoConfiguration`, whose constructor eagerly resolves a Spring Data R2DBC `Dialect` via
  `DialectResolver`, a *second*, separate hardcoded driver list from the `BindMarkersFactoryResolver`
  one above, and fails the same way (`NoDialectException: Cannot determine a dialect for
  ClickHouse`) regardless of any bean this module defines, since it isn't gated on
  `@ConditionalOnMissingBean(DatabaseClient.class)`. Since this demo never uses
  `R2dbcEntityTemplate`/Spring Data repositories anyway, the lean starter removes the problem
  entirely rather than working around it.
- **`DatabaseClientOrderEventRepository`** — the only place SQL and `Row.get(name, Class)` decoding
  happen. Two real, driver-wide decode gotchas surfaced here and are documented in its own Javadoc
  rather than hidden: `count()`'s `UInt64` result needs a server-side `toUInt32(...)` cast (this
  driver decodes `UInt64` as `BigInteger`, not `Long`, and `Row.get` doesn't widen); `status`
  (`Enum8`) decodes as client-v2's own internal `EnumValue`, not a plain `String`, so it has to be
  read via `Object.class` + `toString()` rather than `String.class` directly.
- **Broad type coverage through the extra `DatabaseClient`/`Row` hop** — `order_events` carries
  `UUID`, `LowCardinality(String)`, `Array(String)`, `Decimal(18,4)`, `Nullable(Decimal(18,4))`,
  `Enum8(...)`, `IPv4`, and `DateTime64(3)` columns. The full type matrix (every integer width,
  `Map`, `Tuple`, `Nested`, `Nullable` in general, ...) is already covered at the wire level by
  `transport-http`'s `RealWorldTableAgainstRealClickHouseTest` — this demo isn't re-proving that,
  just proving a realistic slice survives the extra hop through Spring's own abstractions.
- **`GET /order-events/analytics/by-category`** — a real `GROUP BY`/`sum(...)` aggregation query,
  not a per-row lookup. This is the point of not making this "just CRUD": ClickHouse is built for
  exactly this kind of query, and it round-trips through `DatabaseClient` the same way a simple
  `SELECT *` does.
- **`TransactionManagerDivergenceTest`** — proves, rather than just documents in prose, that wiring
  Spring's standard declarative-transaction machinery (`R2dbcTransactionManager`/
  `TransactionalOperator`) over this driver fails clearly with `UnsupportedOperationException`,
  because ClickHouse's own transaction feature is experimental, native-protocol-only, and this
  driver only speaks the stateless HTTP interface (see `ClickHouseConnection`'s own Javadoc). Uses
  the application's actual `ReactiveTransactionManager` bean (autowired), not a scratch instance, so
  the test proves the real app wiring fails this way.

## API

| Method | Path                                | What                                          |
| ------ | ----------------------------------- | ---------------------------------------------- |
| POST   | `/order-events`                     | Record a new order event                       |
| GET    | `/order-events`                     | Stream every event, oldest first               |
| GET    | `/order-events/count`               | Total event count                              |
| GET    | `/order-events/analytics/by-category` | Total `amount` per `category`, highest first |

## Running it

Point it at a real ClickHouse server before running `bootRun`:

```bash
SPRING_R2DBC_URL=r2dbc:clickhouse://localhost:8123 \
SPRING_R2DBC_USERNAME=default \
./gradlew :examples:spring-boot-webflux-demo:bootRun
```

Tests don't need this — they wire `spring.r2dbc.url` dynamically against a Testcontainers-started
ClickHouse server (see `OrderEventControllerAgainstRealClickHouseTest`'s Javadoc for why the
container is started eagerly in a `static` initializer rather than via
`@Container`/`@Testcontainers`).

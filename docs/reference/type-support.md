# ClickHouse type support

This page distinguishes types proved against a real server by this driver from types merely
recognized by the underlying client-v2 decoder. A dependency release note is useful evidence, but
it is not a driver contract: values still pass through this project's
`RowBinaryWithNamesAndTypes` reader, decoded-row model, backpressure path, and tested ClickHouse
server matrix. Requested `Row.get(..., type)` conversions are an additional connector concern; the
table below records the actual decoded representation.

## Verified on a real server

The following families have real-ClickHouse integration tests through the production transport and
decoder:

| ClickHouse types | Java representation |
| --- | --- |
| `Int8`…`Int256`, `UInt8`…`UInt256` | matching boxed primitive where possible; `BigInteger` for values wider than signed `long` |
| `Float32`, `Float64` | `Float`, `Double` |
| `BFloat16` | `Float` |
| `Decimal`, including 128/256-bit tiers | `BigDecimal` |
| `Bool` | `Boolean` |
| `String`, `FixedString` | `String` |
| `Date`, `Date32` | `LocalDate` |
| `DateTime`, `DateTime64` | `ZonedDateTime` |
| `IPv4`, `IPv6` | `InetAddress` |
| `Array`, `Nested` | `List<?>` |
| `Tuple` | `Object[]` |
| `Map` | `Map<?, ?>` |
| `Nullable`, `LowCardinality` | underlying type, or `null` for an absent nullable value |
| `UUID` | `UUID` |
| `Enum8`, `Enum16` | enum member name as `String` |
| `JSON` | JSON text as `String` |

The executable source of truth is
[`RealWorldTableAgainstRealClickHouseTest`](../../clickhouse-r2dbc-reactive-transport-http/src/test/java/io/github/camilyed/clickhouse/r2dbc/transport/http/RealWorldTableAgainstRealClickHouseTest.java).
The nightly matrix runs the real-server suite against both the newest image and the pinned 26.7
compatibility reference.

## Not yet guaranteed

These types do not yet have driver-owned end-to-end coverage and therefore are not claimed as
supported:

| Types | Status |
| --- | --- |
| `Time`, `Time64` | Recognized by client-v2, but not verified through this driver. |
| `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString`, `MultiLineString`, `Geometry`, `MultiPoint` | Geo boundary representations are not verified. `MultiPoint` also requires ClickHouse 26.8+, newer than the pinned 26.7 compatibility reference. |
| `QBit` | Added to client-v2 in 0.11.0 and experimental on the server; driver integration test still required. |
| `Dynamic`, `Variant` | No driver-owned compatibility contract yet. |
| `AggregateFunction`, `SimpleAggregateFunction` states | Need a dedicated aggregate-state fixture rather than literal inserts. |

Each row is intentionally a separate future TDD step. Adding support means first writing a
real-server test that proves both the value and its public Java representation; it does not mean
copying client-v2's release notes into this table.

## Version boundary

The driver currently tests the newest ClickHouse image in the fast CI lane and ClickHouse 26.7 as a
pinned nightly compatibility reference. The formal minimum supported version remains a separate
open policy decision. A type introduced after 26.7 needs a version-scoped test lane or that explicit
minimum-version decision; it must not make the 26.7 build fail accidentally.

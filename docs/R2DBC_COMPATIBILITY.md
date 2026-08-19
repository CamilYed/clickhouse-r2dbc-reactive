# R2DBC SPI compatibility

A precise, written answer to "where does this driver match the R2DBC SPI, and where does
ClickHouse deliberately not have that semantic" — the goal ROADMAP.md Phase 7 item 7 sets, not
100% green at any cost.

The source of truth is code, not this table: `ClickHouseR2dbcSpiCompatibilityTest` in
`clickhouse-r2dbc-reactive-connector` runs the official R2DBC SPI Technology Compatibility Kit
(`io.r2dbc:r2dbc-spi-test`'s `TestKit<T>`) against a real ClickHouse server via Testcontainers.
Every gap below is a `@Disabled`-annotated test override in that class, each with its own
one-line reason at the point of disablement; this document is the same information written up as
a table, so it can be read without opening the test file.

## Supported (TCK passes as-is)

Everything not listed in the tables below passes the TCK unmodified: connection creation and
`isAutoCommit()`/`validate()` (including lazily, at subscription time — see `ClickHouseConnection`),
statement creation and its null-argument failure mode, positional and named parameter binding
(including `bindNull` and values wrapped via `io.r2dbc.spi.Parameters.in(...)`), the full
`Statement.add()`/batching contract (including its failure modes — incomplete binding, incomplete
batch, a trailing `add()` with no following bind), `Result.filter`/`flatMap` segment handling for
both `SELECT` and `INSERT`, case-insensitive column name lookup (`Row.get(String)`,
`RowMetadata.contains`/`getColumnMetadata` by name), and out-of-range `Row`/`RowMetadata` accessor
failure modes (`IndexOutOfBoundsException`/`NoSuchElementException`).

## Deliberately unsupported

ClickHouse itself has no equivalent concept — this driver fails loudly (`UnsupportedOperationException`) rather than silently pretending otherwise.

| SPI behavior | TCK test(s) | Why not |
| --- | --- | --- |
| Transactions (`beginTransaction`, `commitTransaction`, `rollbackTransaction`, `setAutoCommit(false)`) | `changeAutoCommitCommitsTransaction`, `sameAutoCommitLeavesTransactionUnchanged`, `transactionCommit`, `transactionRollback` | ClickHouse's stateless HTTP interface has no session-scoped transaction state to implement against — see `ClickHouseConnection`'s own class Javadoc, checked directly against ClickHouse's "Transactional (ACID) support" docs. |
| Savepoints (`createSavepoint`, `rollbackTransactionToSavepoint`) | `savePoint`, `savePointStartsTransaction` | Savepoints only make sense inside a transaction, which this driver doesn't implement (above). |
| BLOB binding/extraction (`io.r2dbc.spi.Blob`) | `blobInsert`, `blobSelect` | ClickHouse has no distinct large-object type — binary data is just a `String`/`FixedString` column, and a plain string bind/read already covers it; this driver doesn't implement the `Blob` streaming API. |
| CLOB binding/extraction (`io.r2dbc.spi.Clob`) | `clobInsert`, `clobSelect` | Same reasoning as BLOB — ClickHouse text is just `String`. |
| Generated keys (`Statement.returnGeneratedValues()`) | `returnGeneratedValues`, `returnGeneratedValuesFails` | ClickHouse has no autoincrement/`RETURNING`-style mechanism; this driver doesn't override the method, so it inherits the R2DBC SPI's own default (`UnsupportedOperationException`) — itself the honest answer, not a gap to work around. |
| Duplicate column names in one `SELECT` (`RowMetadata`/`Row` access to two columns aliased identically) | `columnMetadata`, `duplicateColumnNames` | Confirmed against a real ClickHouse 26.7.3.19 server: `SELECT col1 AS test_value, col2 AS test_value FROM ...` is rejected outright — `Code: 179. DB::Exception: Multiple expressions col2 AS test_value and col1 AS test_value for alias test_value ... (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. Unlike Postgres/MySQL, ClickHouse never produces a result set with two identically-named columns in the first place, so there's nothing for `Row`/`RowMetadata` to disambiguate by position. |

## Untested / not verified against this driver

Not a deliberate non-support decision — genuinely not exercised, documented rather than silently
skipped.

| SPI behavior | TCK test(s) | Why not verified |
| --- | --- | --- |
| Multiple result sets from one semicolon-separated multi-statement request | `compoundStatement` | ClickHouse's HTTP interface executes one statement per request; this driver has never been run against a multi-statement query, and the outcome (error vs. some partial behavior) hasn't been characterized. |

## Architectural differences from the TCK's assumptions

Not a missing feature — a different, still R2DBC-conformant design choice the TCK's specific
assertions don't anticipate.

| SPI behavior | TCK test(s) | The difference |
| --- | --- | --- |
| Eager bind-value type validation | `bindFails` | This driver defers all bind-value handling to query encoding at `execute()` time (see `ClickHouseQuery#withParameters`) rather than type-checking a value inside `bind()` itself. `bind(identifier, Class.class)` does not synchronously throw `IllegalArgumentException` the way the TCK expects — an unsupported value only surfaces once ClickHouse rejects the encoded parameter server-side. |
| `Result.UpdateCount` segments for `INSERT` via `Statement.execute()` | `segmentInsertEmitsUpdateCount` | ClickHouse's HTTP `INSERT` response carries no response body at all, so there's no segment stream to emit a `Result.UpdateCount` from — `writtenRows` is only ever available via `Result#getRowsUpdated()` (read from the `X-ClickHouse-Summary` response header), never as a segment. See `ClickHouseResult`'s own Javadoc. |

## Running it

```
./gradlew :clickhouse-r2dbc-reactive-connector:test --tests ClickHouseR2dbcSpiCompatibilityTest
```

Requires Docker (Testcontainers spins up a real `clickhouse-server` container, shared with every
other `BaseClickHouseIntegrationTest`-derived test in the same JVM run).

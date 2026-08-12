# Current work — handoff notes

Snapshot for continuing this session with a fresh model. Only what CLAUDE.md/ROADMAP.md don't
already say. Phase 1 and Phase 2's type-coverage pass are effectively done; the user has chosen
**Phase 3 (connector/R2DBC SPI)** as the next real work, but this session detoured first into
finishing type coverage properly and refactoring the real-ClickHouse test infrastructure — both
now done, both committed. Phase 3 itself has not started yet (no `connector` code written beyond
the existing skeleton).

## Exact current implementation state

- `core`: `ClickHouseQuery`, `FluxInputStreamBridge`, `StreamSignal`, `RowBinaryDecoder` — plus new
  `ListDecodingRowBinaryReader` (package-private): subclasses client-v2's public
  `RowBinaryWithNamesAndTypesFormatReader`, overrides its `protected readRecord(Object[])` hook to
  supply a `List.class` type hint for `Array`/`Nested` columns only, so they decode as a plain
  `List` instead of client-v2's `.internal` `ArrayValue`. One deliberate, documented, tested
  `.internal` dependency — same shape as the Phase 0 `InputStream` bridge compromise, not a general
  one. `RowBinaryDecoder` now builds this reader instead of the base class directly; every other
  type's decoding is unaffected. Covered by a hermetic unit test in `core`
  (`RowBinaryDecoderTest.shouldDecodeAnArrayColumnAsAPlainList`, hand-built wire bytes, no
  ClickHouse needed) — the fixture-building helper lives in `core`'s own test
  `fakes.RowBinaryFixtures`.
- `transport-http`: unchanged core transport (`ClickHouseHttpTransport`, `Authentication`) from
  before. Its `RealWorldTableAgainstRealClickHouseTest` now has **ten** tests, one per ClickHouse
  docs category actually exercised, including the three added this session:
  `shouldDecodeMapType`/`shouldDecodeTupleType`/`shouldDecodeEnumTypes`/`shouldDecodeArrayType`.
  Full, current, honest type-coverage status is written in that class's Javadoc and in
  ROADMAP.md's Phase 2 section (search "First real pass at type coverage") — **read that before
  re-deriving type-coverage status from scratch**, it's already exhaustive: Numeric/String/Network
  fully covered; Date-time (missing `Time`/`Time64` — genuine client-v2 0.9.0 gap, not a test gap),
  Nullable (missing `LowCardinality`), Specialized (`UUID`+`Enum8`/`16` covered; geo/vector/domains
  not attempted) partially covered; Composite now **covered** (`Map`/`Tuple`/`Array`; `Nested` uses
  the same mechanism but has no dedicated test yet — cheap follow-up); Semi-structured and
  Aggregate-function categories not attempted at all (JSON/Dynamic/Variant still experimental in
  ClickHouse itself; AggregateFunction needs a different test shape, not literal `INSERT`).
- `testkit`: **new** `BaseClickHouseIntegrationTest` (top-level package) — the ClickHouse container
  is now a single `static` field here (Testcontainers singleton-container pattern: every subclass,
  in every module, shares one running container per JVM; `@Testcontainers`/`@Container` on a base
  class is picked up by JUnit5's own annotation search on subclasses without redeclaring it). A
  `@BeforeEach protected void dropAllTables()` runs before every test, discovering and dropping
  every table via `SHOW TABLES`/`DROP TABLE IF EXISTS` sent over a plain synchronous
  `java.net.http.HttpClient` — deliberately **not** through this project's own transport, so
  cleanup can't depend on (or mask a bug in) the driver code under test. `ClickHouseRowAssert` also
  moved here from `transport-http`'s test sources (package `testkit.assertions`), so
  `connector`/`integration-tests` get it for free later, and grew new methods:
  `hasList`/`hasTuple`/`hasMap`/`hasEnumName`/`hasBigInteger`/`hasFloatCloseTo`/`hasInetAddress`/
  `hasUuid` alongside the original four. All three of `transport-http`'s real-ClickHouse test
  classes now `extend BaseClickHouseIntegrationTest` instead of each declaring their own
  `@Container` field — no behavior change, just removed duplication and added the automatic
  cleanup they didn't have before (tests previously all created differently-named tables so
  cross-test pollution never actually bit, but it was implicit, not enforced).
- `connector`, `integration-tests`: still skeleton only — this is Phase 3's actual work, not
  started.

## What was completed this session (this pass, after the "wszystko zielono" checkpoint)

In order: (1) corrected an overly-broad ROADMAP claim that all of "Composite" was blocked — read
client-v2 0.9.0 source directly and found `Map`/`Tuple` already decode cleanly with zero code
changes, added tests for both plus `Enum8`/`Enum16` (readable via `toString()` on the opaque
`Object`, no internal-type import needed); (2) at the user's explicit go-ahead, unblocked
`Array`/`Nested` for real via `ListDecodingRowBinaryReader` (see above); (3) at the user's explicit
request, refactored all real-ClickHouse tests onto a shared `BaseClickHouseIntegrationTest` with a
singleton container and automatic per-test table cleanup, and moved+expanded the custom row
assertion into `testkit` for reuse.

## Current branch / unfinished work

- Branch `main`. Commits this pass (in order): `66f9889` (Map/Tuple/Enum correction),
  `a76f7cb` (Array/Nested unblock) — both pushed to this point. **The
  `BaseClickHouseIntegrationTest` + `ClickHouseRowAssert` move + three-test-class refactor is
  finished but NOT YET COMMITTED** — do that first thing on resume, before anything else, once the
  user confirms the refactored tests are still green (they haven't re-run since this refactor
  landed).
- Task list: tasks through #41 are `completed`. No task yet created for Phase 3's actual opening
  move (that's the next thing to create once this refactor's commit lands).

## Failing or pending tests

Unknown for the very latest refactor (base class + assertion move + three test classes rewritten)
— not yet run by the user. Everything before that refactor was last confirmed green including
Map/Tuple/Enum/Array.

## Exact next smallest TDD step

Two things, in order:

1. Commit the `BaseClickHouseIntegrationTest`/`ClickHouseRowAssert`/three-test-class refactor once
   the user confirms it's green.
2. Start Phase 3: `connector`'s `ConnectionFactoryProvider` + `ConnectionFactoryOptions` first —
   the R2DBC discovery entry point. Already verified the exact `r2dbc-spi:1.0.0.RELEASE` source for
   `ConnectionFactoryProvider`/`ConnectionFactoryOptions`/`ConnectionFactory`/`Connection`/
   `Statement`/`Result` (fetched directly from `github.com/r2dbc/r2dbc-spi` at tag
   `v1.0.0.RELEASE`, not `main` — same "pin the exact version" discipline as client-v2). Key
   signatures worth remembering so they don't need re-fetching: `ConnectionFactoryProvider.create(
   ConnectionFactoryOptions)`/`supports(...)`/`getDriver()`; `ConnectionFactoryOptions.DRIVER`/
   `PROTOCOL`/`HOST`/`PORT`/`DATABASE`/`USER`/`PASSWORD`/`SSL`/`CONNECT_TIMEOUT` well-known
   `Option`s plus arbitrary custom ones via `Option.valueOf(name)`; `ConnectionFactory.create()`
   returns `Publisher<? extends Connection>`; `Connection.createStatement(sql)` returns
   `Statement`; `Statement.execute()` returns `Publisher<? extends Result>`; `Result.map(BiFunction<
   Row,RowMetadata,T>)`/`map(Function<Readable,T>)`. TDD as always: first failing test drives
   `ConnectionFactoryProvider` into existence (e.g. "supports options with driver=clickhouse"),
   don't design the whole `connector` module's shape up front.

## Known risks or unresolved questions

- Same recurring `.git` lock-file friction as always — this session it got bad enough that a stale
  `.git/index.lock` from days earlier could not be `rm`'d at all (mount-level delete restriction,
  not just git's own lock contention). Worked around via `GIT_INDEX_FILE`-based plumbing commits
  plus `allow_cowork_file_delete` (a Cowork tool that enables deletion for the folder after explicit
  user-visible approval) to actually clear the stale lock files. If commits start failing with
  "Unable to create .../.git/*.lock: File exists" again, try that combination before assuming the
  repo is broken.
- `Time`/`Time64`, `LowCardinality`, geo types, vector-search (`QBit`), domains, JSON/Dynamic/
  Variant, AggregateFunction/SimpleAggregateFunction all remain genuinely untested — see
  `RealWorldTableAgainstRealClickHouseTest`'s Javadoc and ROADMAP's type-coverage table for the
  full, current, honest list. None of these block Phase 3.

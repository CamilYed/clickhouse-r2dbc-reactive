# Current work — handoff notes

Snapshot for continuing this session with a fresh model. Only what CLAUDE.md/ROADMAP.md don't
already say. Phase 1 is closed; this session pushed well into Phase 2 territory (auth, error
handling, real-world type coverage) without yet formally opening a "Phase 2" task in ROADMAP.

## Exact current implementation state

- `core`: unchanged from last snapshot (`FluxInputStreamBridge`, `StreamSignal`, `RowBinaryDecoder`)
  **plus** new `ClickHouseQuery` record (`sql`, `queryId`; `of(sql)` generates a UUID, `of(sql,
  queryId)` uses a given one) — TDD-built, 4 tests, committed.
- `transport-http`: `ClickHouseHttpTransport` now has **four** public constructors: `(baseUrl)`,
  `(baseUrl, maxConnections)`, `(baseUrl, user, password)` (Basic), `(baseUrl, Authentication)`
  (any mode). `Authentication` is now a **public** sealed interface (promoted from
  package-private once `UserKey` made it real public API surface) with static factories
  `none()`/`basic(user,password)`/`userKey(user,key)`. `query(ClickHouseQuery)` (was
  `query(String)`) sends `X-ClickHouse-Query-Id` as a request header (empirically verified
  sufficient against real ClickHouse — see `QueryIdHeaderAgainstRealClickHouseTest` and
  `docs/CLIENT_V2_HTTP_REFERENCE.md`) and detects server errors via
  `X-ClickHouse-Exception-Code` header or HTTP status ≥ 400, surfacing them as client-v2's own
  `ServerException` (3-arg ctor — pinned version is 0.9.0, **not** master, see class Javadoc).
  `ClickHouseHttpTransportTest` — 13 green tests (added query-id-header, server-error,
  userKey-auth cases; the other 11 call sites mechanically updated to `ClickHouseQuery.of(...)`).
- `testkit`: `ControlledClickHouseServer` — `receivedQueryId` field replaced with
  `receivedHeaders: AtomicReference<HttpHeaders>` + generic `receivedHeader(name)`; new
  `startRespondingWithClickHouseError(code, message, httpStatus)` factory.
- New test-support classes (module-local to `transport-http`, per CLAUDE.md's Ability/Custom
  Assertion pattern): `abilities.RealClickHouseQueryAbility` (`execute(sql)`/`queryRows(sql)`
  against a real server through the full pipeline), `assertions.ClickHouseRowAssert`
  (`hasValue`/`hasDecimal`/`hasNullAt`/`hasTypeAt`).
- `RealWorldTableAgainstRealClickHouseTest` (new, **not yet committed** — see below): six tests
  against real ClickHouse, one per ClickHouse docs type-category actually exercised
  (`shouldDecodeAMultiTypeMultiRowTable` for Nullable/multi-row, plus
  `shouldDecodeNumericTypes`/`StringTypes`/`DateAndTimeTypes`/`NetworkTypes`/`SpecializedTypes`).
  User confirmed green. Full category-by-category coverage status is now written up in both the
  class Javadoc and ROADMAP.md (search "First real pass at type coverage") — **do not re-derive
  this list from scratch**, it's already exhaustive and honest: 3 categories fully covered
  (Numeric, String, Network), 3 partially covered (Date/time — `Time`/`Time64` missing from
  pinned client-v2 itself; Nullable — `LowCardinality` untried; Specialized — only `UUID`), 3 not
  attempted at all (Composite, Semi-structured, Aggregate function), 1 N/A (Special Data Types).
- `connector`, `integration-tests`: still skeleton only.

## What was completed this session

In order: (1) all HTTP auth modes (`UserKey` added alongside `None`/`Basic`, `Authentication`
promoted to public sealed interface with factories); (2) server-side error surfacing via
`ServerException`; (3) `query_id` plumbed end-to-end as `ClickHouseQuery`; (4) a genuinely
realistic real-ClickHouse scenario test, twice-restructured — first as one wide `all_types`
table, then split to mirror ClickHouse's own docs category taxonomy per explicit user request —
and rebuilt on Ability + Custom Assertion instead of ad hoc code; (5) an honest, exhaustive
type-coverage audit against that taxonomy, written into both the test's Javadoc and ROADMAP.md,
in response to the user explicitly not being satisfied that "all types" were covered until shown
proof.

Two genuine (not test-gap) findings surfaced and documented:

1. **`Time`/`Time64`** have no decode path at all in pinned client-v2 0.9.0 — confirmed by
   reading `BinaryStreamReader.readValue`'s switch directly via `git show v0.9.0:...` against the
   user-provided full-history clone at `/Users/kamil/Projects/clickhouse-java`.
2. **Composite types + `Enum8`/`Enum16`** decode to client-v2's own `.internal`-package types
   without a `typeHintMapping` — blocked by this project's own Phase 0 reuse-boundary decision,
   not by test effort. This is now flagged in ROADMAP as an **open design decision**, not a task:
   does `core` supply a `typeHintMapping`, or build its own array/map/enum representation?

## Current branch / unfinished work

- Branch `main`. Auth/error-handling/query_id work already committed in prior turns this session
  (exact SHAs not re-verified here — check `git log`). **`RealWorldTableAgainstRealClickHouseTest`
  and its two new `abilities`/`assertions` support classes are NOT yet committed** — write them up
  as one commit once you resume (they're finished and user-confirmed green; nothing left to
  change on them right now). The `ROADMAP.md` type-coverage table addition (this session, already
  applied to the file) should go in the same or an adjacent commit.

## Failing or pending tests

None reported. Full suite green per user's last "zielono" — including the six-category real-world
type test.

## Exact next smallest TDD step

Two independent open threads, no user go-ahead yet on either:

1. Commit the uncommitted files above.
2. The Composite/Enum design decision (typeHintMapping vs. own representation) — this is real
   engineering work, not a quick add. Needs a deliberate conversation with the user before writing
   any code, since it shapes what `core`'s own type surface looks like going forward.

Do not start either without checking in — same standing practice as every prior snapshot.

## Known risks or unresolved questions

- Same recurring `.git/index.lock` plumbing note as always, nothing new.
- `Time`/`Time64` unsupported by pinned client-v2 0.9.0 is a real, user-facing gap once this
  driver ships — worth deciding (later) whether to special-case it, wait for a client-v2 upgrade,
  or document it as an explicit limitation.
- LowCardinality, geo types, vector-search (`QBit`) types, domains, JSON/Dynamic/Variant,
  AggregateFunction/SimpleAggregateFunction all remain genuinely untested — not because they're
  hard to reach, but because they weren't yet prioritized. See ROADMAP's type-coverage table for
  the full, current, honest list.

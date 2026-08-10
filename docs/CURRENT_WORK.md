# Current work — handoff notes

Snapshot for continuing this session with a fresh model. Only what CLAUDE.md/ROADMAP.md don't
already say. **Phase 1 is now fully closed** — this is the cleanest stopping point of the project
so far.

## Exact current implementation state

- `core`: `FluxInputStreamBridge` (`Flux<ByteBuffer>` → blocking `InputStream`, credit-based
  demand queue, sized `demand + 1`), `StreamSignal` (sealed `Data`/`Complete`/`Error`),
  `RowBinaryDecoder` (wires the bridge to client-v2's `RowBinaryWithNamesAndTypesFormatReader`,
  `Flux<ByteBuffer>` → `Flux<Map<String,Object>>`). All three have thorough Javadoc — the bridge's
  explains the credit/backpressure mechanism in full, not just states it, per an explicit user
  request to make the reasoning legible in the code itself, not only in chat.
- `transport-http`: `ClickHouseHttpTransport` — three public constructors: `(baseUrl)`,
  `(baseUrl, maxConnections)` (bounded pool), `(baseUrl, user, password)` (HTTP Basic auth). Auth
  is modeled as a package-private `sealed interface Authentication` (`None`/`Basic` records, each
  with `addTo(HttpHeaders)`) — **not** an `Optional<String>` field; that was IntelliJ-flagged and
  refactored away, since `Optional` as a field type is a known anti-pattern (intended for return
  types, not state). `query(sql)` always sets `X-ClickHouse-Format: RowBinaryWithNamesAndTypes` —
  real ClickHouse defaults to `TabSeparated` when a bare query has no `FORMAT` clause, which the
  controlled fake server never surfaced since it ignores request headers entirely.
  `ClickHouseHttpTransportTest` — 11 green tests (full contract matrix, unchanged).
  `SelectOneAgainstRealClickHouseTest` (new) — real `ClickHouseContainer` via Testcontainers, full
  pipeline (`query()` → `asByteArray().map(ByteBuffer::wrap)` → `RowBinaryDecoder.decodeRows()`),
  green. Uses `clickhouse/clickhouse-server:latest`.
- `testkit`: unchanged from last snapshot (`ControlledClickHouseServer`/`ClickHouseWireFixtures` in
  `testkit.fakes`, `ToByteArrayAbility` in `testkit.abilities`). `BaseClickHouseIntegrationTest`
  still correctly deferred to Phase 3.
- `connector`, `integration-tests`: still only skeleton/`package-info.java` — genuinely next once
  Phase 2 is far enough along.

## What was completed this session

Finished Phase 1 steps 5 and 6 in one sitting: wired `RowBinaryDecoder` (client-v2's real decoder)
to the bridge for a controlled-server `SELECT 1`, then proved the same pipeline against a real
ClickHouse container. Two real gaps surfaced by going against a real server (exactly what step 6's
own description anticipated) and got fixed, not worked around:

1. **Auth.** Testcontainers' `ClickHouseContainer` sets `CLICKHOUSE_USER=test`, which removes
   anonymous access. Added HTTP Basic auth (see `Authentication` above).
2. **Response format.** Real ClickHouse defaults to `TabSeparated`; forced
   `RowBinaryWithNamesAndTypes` via the request header.
3. **A real client-v2 bug/gotcha, found via `NullPointerException`:** `RecordWrapper` (what
   `reader.next()` returns) holds row data behind a `WeakReference` to the reader's internal state.
   Reading it after the reader is no longer strongly reachable — which happened here simply from
   `blockFirst()` cancelling the subscription after the first element — threw an NPE from inside
   client-v2. Fixed by switching `RowBinaryDecoder` from `Flux.fromIterable` to `Flux.generate` and
   copying each row into a plain `LinkedHashMap` the instant it's read, while the reader is still
   on the stack. Documented in the class Javadoc **and** cross-referenced from ROADMAP's Phase 0
   risk section (this is the first concrete instance of the risk that section already predicted).

Also, per direct user request: refactored `Optional<String>` field → sealed `Authentication`
(IntelliJ had flagged it); substantially expanded `FluxInputStreamBridge`'s Javadoc to actually
explain the credit-based backpressure mechanism instead of just asserting it works; added a new
ROADMAP "Non-functional requirements: logging, metrics, leaks" section (SLF4J-api-only logging
tied to Phase 2's `query_id` work, a metrics extension point with no vendor decided yet, Netty
paranoid leak-detector in tests) — named explicitly per the user's stated priorities, not designed
in detail yet; added a "reads only, writes are future scope" note and a "driver decodes wire types,
doesn't reimplement SQL — the real coverage matrix is types, not SQL features" note to Phase 2,
both from direct user questions about scope.

## Current branch / unfinished work

- Branch `main`. Two commits this session: `9a43b40...` (step 5+6 code) and the ROADMAP-only commit
  right after it (non-functional-requirements + reads/writes/types scope notes) — **check `git log`
  for the exact second SHA, it was made after this file was last read into context.** Not confirmed
  pushed to GitHub — same caveat as every prior snapshot, this sandbox has no reliable persistent
  push path; the user pushes from their own machine.
- Task list: everything through task #29 (Phase 1 step 6) is `completed`. No task yet created for
  Phase 2's opening move.

## Failing or pending tests

None. Full suite green, including the new real-ClickHouse test (needs Docker; the user confirmed
`zielono` after the WeakReference fix).

## Exact next smallest TDD step

Phase 2 (ROADMAP.md): `core`'s query/settings/`query_id` representation, transport-independent.
User explicitly deferred starting this to the next session ("zrobimy to jutro") — **do not start
Phase 2 code without the user's go-ahead**, this snapshot exists so a fresh session has full context
the moment they say go, not so it jumps ahead on its own. `query_id` is the one piece with a stated
hard dependency (Phase 4's cancellation story, and Phase 2's own logging note above), so it's the
natural first slice — but confirm with the user before assuming that's still the intended order.

## Known risks or unresolved questions

- Recurring `.git/index.lock` etc. — same plumbing workaround, still reliable, nothing new.
- Open scope questions raised by the user this session, now named in ROADMAP but **not designed**:
  reactive `INSERT` path (Phase 2/3, unscoped), a wire-type contract-test matrix against real
  ClickHouse tables (Phase 3, needs `BaseClickHouseIntegrationTest` first), logging/metrics
  concrete shape (Phase 2/3/4, vendor/SPI not decided), a possible
  `examples/spring-boot-webflux-demo` module (already Phase 6, JDK/Boot/Framework versions
  deliberately not pinned yet — "reconfirm when this phase actually starts" is already written into
  ROADMAP).
- Whether client-v2 compresses by default: confirmed **no** — ClickHouse's HTTP interface only
  compresses when asked (`compress=1` query param or `Accept-Encoding` header); our transport sends
  neither, so today's real-ClickHouse test received plain uncompressed `RowBinaryWithNamesAndTypes`.
  LZ4 support remains explicitly Phase 2 future work (`docs/CLIENT_V2_HTTP_REFERENCE.md`).

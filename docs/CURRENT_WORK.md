# Current work — handoff notes

Snapshot for continuing this session with a fresh model. Only what CLAUDE.md/ROADMAP.md don't
already say. Delete once Phase 1 fully closes (transport-http's matrix is done; `core`'s bridge —
step 4 — is what's left).

## Exact current implementation state

- `core`, `connector`: only `package-info.java`, no code yet. **This is genuinely next.**
- `integration-tests`: only `build.gradle.kts`, no `src/` yet.
- `transport-http`: `ClickHouseHttpTransport.java` — `query(String sql)` returns `ByteBufFlux`
  (Reactor Netty's own type, per ROADMAP's "don't over-design yet"). Two public constructors:
  `ClickHouseHttpTransport(String baseUrl)` (default `ConnectionProvider`) and
  `ClickHouseHttpTransport(String baseUrl, int maxConnections)` (bounded pool, added this session
  for the pool-saturation test) — both delegate to a private constructor taking a
  `ConnectionProvider`. `.responseTimeout(Duration.ofSeconds(2))` hardcoded. SQL still goes in the
  URL query string, not the POST body (known placeholder, Phase 2 fixes the real request shape).
  Has Javadoc. `ClickHouseHttpTransportTest.java` — **11 green tests, the full contract-test
  matrix**: happy path, deferred execution (two variants — immediately after subscribe, and while
  queued behind a saturated pool), streaming not aggregating, cancellation tearing down the
  connection, delayed headers, delayed body, backpressure, timeout on no response, error on
  mid-response reset, pool saturation.
- `testkit`: `ControlledClickHouseServer` (package `testkit.fakes`) now tracks three things per
  request: `hasReceivedRequest()`, `hasClosedConnection()`, `activeConnectionCount()` (the last one
  added this session, driving the pool-saturation tests). Seven factory methods, each building a
  specific wire scenario — see the class itself, it's short and each method name says what it
  does. `ClickHouseWireFixtures` also in `testkit.fakes`. `testkit.abilities.ToByteArrayAbility`
  holds the one shared `ByteBuf`→`byte[]` conversion. `BaseClickHouseIntegrationTest` (real
  ClickHouse via Testcontainers) still doesn't exist — still correctly deferred to Phase 3.

## What was completed this session (after the previous CURRENT_WORK.md snapshot)

Finished the last two scenarios in transport-http's contract matrix (pool saturation, cancellation
while queued/before acquire) — this needed the one remaining real production change: a
configurable, bounded `ConnectionProvider` on `ClickHouseHttpTransport`. Also did a requested
refactor pass: moved `ControlledClickHouseServer`/`ClickHouseWireFixtures` into a `testkit.fakes`
package, extracted a duplicated `toByteArray` helper into `testkit.abilities.ToByteArrayAbility`,
added Javadoc to `ClickHouseHttpTransport`, and added two new CLAUDE.md rules (no `Utils`/`Helper`
grab-bag classes; a `builders`/`abilities`/`assertions`/`fakes` package convention for test
support code). Synced with a dependabot-merged JUnit BOM bump (6.1.0→6.1.2) from GitHub. Drafted
(not posted — no Slack/GitHub write access from this session) a few outreach messages for the
user: a Slack update, a GitHub Discussion comment, and a Slack #random ask — all pointing back to
this repo, all phrased as "early exploratory POC," not overclaiming finished results.

## Current branch / unfinished work

- Branch `main`, pushed and confirmed synced with GitHub as of the JUnit BOM merge (`bf69577`) —
  the user did the push themselves and confirmed. Everything since then (this session's work) is
  committed locally; **confirm it's actually pushed before assuming so** — this sandbox still has
  no reliable persistent network path to verify push status live.
- Task list: **transport-http's 11-scenario contract matrix is fully done.** Nothing pending in
  that area. The next real task is Phase 1 step 4 from ROADMAP.md: `core`'s
  `Flux<ByteBuffer>`→`InputStream` bridge — brand new module, brand new code, no tests exist there
  yet at all.

## Important decisions made but not yet reflected anywhere else

- None new this session beyond what's already in ROADMAP.md/CLAUDE.md. The compression-disabled-
  for-Phase-1 and mid-stream-error-still-open decisions from earlier snapshots are unchanged and
  still apply — `core`'s bridge work should not silently assume either question is resolved.

## Failing or pending tests

None. Everything in `transport-http` and `testkit` is green. Clean stopping point.

## Exact next smallest TDD step

Phase 1 step 4 (ROADMAP.md): the `Flux<ByteBuffer>`→`InputStream` bridge, in `core`, tested in
complete isolation from HTTP — feed it a `Flux<ByteBuffer>` by hand in a unit test, assert on bytes
read through the bridge, no network/testkit involved at all. This is a genuinely new kind of test
for this codebase (no fake server needed, just a plain `core` unit test) and a real concurrency
concern (bounded internal queue, a small dedicated worker pool — not `Schedulers.boundedElastic()`,
backpressure by pausing the upstream `Flux` when the queue is full, cancellation interrupts the
worker). Read ROADMAP.md's step 4 description again before starting; this is more involved than
anything done in transport-http so far and deserves its own focused session rather than being
squeezed in at the end of one.

## Known risks or unresolved questions

- Recurring `.git/index.lock` etc. — same as every prior snapshot. Plumbing workaround still
  reliable. `git pull` doesn't work at all from this sandbox (SSH remote, no key) — worked around
  this session by fetching anonymously over HTTPS into a throwaway ref and fast-forwarding
  manually. Same trick will work again if needed.
- No code-level risks currently open — this is the cleanest checkpoint of the project so far.

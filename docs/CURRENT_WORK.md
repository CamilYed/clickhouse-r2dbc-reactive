# Current work — handoff notes

Snapshot for continuing this session with a fresh model. Only what CLAUDE.md/ROADMAP.md don't
already say. Delete or fold into ROADMAP.md once Phase 1's transport-http matrix is fully done.

## Exact current implementation state

- `core`, `connector`: only `package-info.java`, no code yet.
- `integration-tests`: only `build.gradle.kts`, no `src/` yet.
- `transport-http`:
  - `ClickHouseHttpTransport.java` — `query(String sql)` returns `ByteBufFlux`, POSTs SQL as a
    URL query param (still a placeholder, not the real request shape — Phase 2). Has
    `.responseTimeout(Duration.ofSeconds(2))` hardcoded on the `HttpClient`, added this session to
    make the "no response" test pass. Uses the default (unbounded) Reactor Netty connection
    provider — no pool size configured yet. Has its Javadoc now (added by the user this session —
    the gap noted in the previous snapshot is closed).
  - `ClickHouseHttpTransportTest.java` — 9 tests, all confirmed GREEN this session:
    `shouldReturnTheConfiguredResponseBody`, `shouldNotSendTheRequestBeforeSubscription`,
    `shouldEmitEachChunkSeparatelyInsteadOfAggregating`,
    `shouldCloseTheConnectionWhenTheSubscriptionIsCancelled`,
    `shouldStillReturnTheBodyWhenHeadersAreDelayed`,
    `shouldStillReturnTheBodyWhenBodyIsDelayedAfterHeaders`,
    `shouldNotDeliverDataBeforeTheSubscriberRequestsIt`,
    `shouldFailWithinItsOwnTimeoutWhenTheServerNeverResponds`,
    `shouldSignalAnErrorWhenTheConnectionIsResetMidResponse`.
- `testkit`:
  - `ControlledClickHouseServer.java` — grew a lot this session. Public factories:
    `startRespondingToSelectOneWith(byte[])`, `startRespondingToSelectOneWithChunks(byte[]...)`,
    `startRespondingWithFirstChunkThenHanging(byte[])`,
    `startRespondingToSelectOneWithDelay(byte[], Duration)` (delays before anything is sent),
    `startRespondingToSelectOneWithBodyDelay(byte[], Duration)` (headers flushed immediately via
    explicit `sendHeaders()`, body delayed separately — the one factory that doesn't go through
    the shared `startRespondingWith` helper, deliberately), `startAcceptingButNeverResponding()`,
    `startRespondingThenResettingConnection(byte[], Duration)` (sends a chunk then force-closes the
    raw channel server-side via a delayed `Mono`, simulating a network reset instead of a clean
    HTTP completion). Exposes `hasReceivedRequest()` and `hasClosedConnection()`.
  - `BaseClickHouseIntegrationTest` (real-ClickHouse Testcontainers DSL) — still doesn't exist,
    still deliberately deferred to Phase 3.

## What was completed this session (after the previous CURRENT_WORK.md snapshot)

Resolved the one loose end from last time (awaitility 4.3.1 doesn't exist on Maven Central —
verified against `maven-metadata.xml` directly, pinned to the real 4.3.0), then worked straight
through 9 of the 11 tasks in `transport-http`'s contract-test matrix, TDD throughout. Two of those
needed a real production change (not just a new fake-server capability + assertion): the timeout
test drove adding `.responseTimeout(...)` to `ClickHouseHttpTransport`; everything else passed
against the existing implementation (Reactor Netty's inherent laziness/backpressure-compliance
already provided the property, so the test just locks it in as a regression guard — expected and
fine, not a smell). One real bug caught and fixed mid-session: the first cancellation test
(`.subscribe().dispose()` immediately) was a race — cancelling before the request had even reached
the server, so nothing was actually being proven. Fixed by using `.take(1).blockLast(...)`, which
guarantees the request landed and data flowed before cancelling.

## Current branch / unfinished work

- Branch `main`, remote `origin` (SSH) unchanged from last snapshot. Confirm push status locally —
  still unverified from this sandbox (no network route to GitHub here).
- Task list: **9 of 11 transport-http contract-test tasks done** (#16–#20, #22–#24; #21 explicitly
  skipped as a duplicate of #17 — same underlying mechanism, would've been a copy-paste test).
  Remaining: **#25 (cancellation before/while-queued/during-receive — "during-receive" is already
  effectively covered by #18, so what's left is specifically "before acquire" and "while queued")**
  and **#26 (pool saturation + pending-acquire timeout)**.

## Important decisions made but not yet reflected anywhere else

- **#25/#26 need `ClickHouseHttpTransport` to gain a configurable, bounded `ConnectionProvider`**
  (currently uses Reactor Netty's default/unbounded pool) — this is genuinely new production
  surface, not just another fake-server variant, which is why the session stopped here rather than
  pushing through. The next session should start by adding something like a `maxConnections`
  constructor parameter (or a small config object — don't over-design, follow whatever the first
  failing test actually demands) before either test can be meaningful: pool saturation can't be
  observed against an effectively-unbounded pool, and "cancel while queued" needs requests to
  actually queue.
- Compression-disabled-for-Phase-1 and the mid-stream-error open question (from the previous
  snapshot) are both still unresolved and still apply unchanged — not touched this session.

## Failing or pending tests

None currently failing. All 9 tests in `ClickHouseHttpTransportTest` are confirmed green by the
user. Nothing is mid-RED right now — safe stopping point.

## Exact next smallest TDD step

Task #26 (pool saturation) is the more natural one to start with, since #25's "while queued" case
depends on the same bounded-pool infrastructure anyway:

1. RED: a test that configures `ClickHouseHttpTransport` with `maxConnections=1` (constructor
   change needed — smallest version: add a second constructor param, don't build a full settings
   object yet), fires two concurrent requests against a slow `ControlledClickHouseServer` response,
   and asserts the second one doesn't start until the first completes/releases its connection
   (e.g. via timing, or via the fake server's `hasReceivedRequest()`-style counter extended to
   count concurrent in-flight requests).
2. GREEN: wire a `ConnectionProvider.builder(...).maxConnections(n)...build()` into the `HttpClient`
   in the constructor.
3. Then #25's "while queued" case: cancel the second (queued) request before it ever gets a
   connection, assert it never reaches the server at all (`hasReceivedRequest()` style check, or a
   request counter) and that cancelling it doesn't affect the first, still-in-flight request.
4. #25's "before acquire" case: same shape as the already-fixed cancellation race bug above, just
   applied intentionally this time — cancel immediately after subscribe, before the connection
   attempt completes, assert no request ever reaches the server.

## Known risks or unresolved questions

- Recurring `.git/index.lock` etc. — same as before, proven plumbing workaround still needed most
  commits this session. Not re-diagnosed further.
- Push status to GitHub still unverified from this sandbox.
- No new risks introduced this session beyond what's listed above.

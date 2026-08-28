# Production readiness

A condensed adoption-decision matrix — what's fixed, what's a deliberate documented limitation, and
what's still an open gap. This page is a summary; the full narrative for every row (why, how it was
verified, which test proves it) lives in
[engineering/roadmap-archive.md's Production readiness review](../../engineering/roadmap-archive.md#production-readiness-review).

Legend: ✅ fixed/verified safe · ⚠️ safe limitation, documented, fails loudly (not silently) ·
❌ open gap, not yet addressed · 🧪 untested / needs more testing.

## Fixed this pass

| Finding | Verdict | Notes |
| --- | --- | --- |
| Hardcoded 2s timeout | ✅ | Replaced with configurable timeouts (connect/response). |
| `ServerException` → `R2dbcException` mapping | ✅ | Errors map through `ClickHouseR2dbcException.wrap`. |
| `getRowsUpdated()` always empty | ✅ | Populated from the ClickHouse response summary. |
| `close()` not preventing reuse | ✅ | A closed connection now rejects further use. |
| No connect-timeout bound | ✅ | `CONNECT_TIMEOUT` wired into the R2DBC bootstrap path. |
| `Connection.setStatementTimeout(Duration)` | ✅ | Implemented as ClickHouse's server-side `max_execution_time`; covered hermetically and against a real server. |
| Transport pool options unavailable through R2DBC | ✅ | `transportMaxConnections`, pending-acquire bounds, idle time, and lifetime are accepted through `ConnectionFactoryOptions` and R2DBC URLs. |
| `RESPONSE_CHUNK_DEMAND` magic number | ✅ | Documented, not removed — a deliberate tuning constant. |
| Statement/Batch thread-safety | ✅ | Documented as not thread-safe (matches R2DBC SPI expectations). |
| Best-effort `KILL QUERY` on cancel | ✅ | Sends `KILL QUERY ... ASYNC`; see the caveats under Known limitations below. |
| `sslRootCert` custom trust store | ✅ | Both TLS auto-negotiation and the trust half are verified against a real self-signed-cert server. |
| Pre-send retry (`RetryPolicy`) | ✅ | Retries only failures strictly before the request was fully sent — cannot duplicate a query server-side by construction. |
| INSERT retried-duplicate-key question | ✅ | Researched, not assumed: ClickHouse's `MergeTree` family enforces no unique-key constraint at insert time, so the specific collision this question raised doesn't exist for standard engines. |
| Large/batch `INSERT`s sent entirely via URL query string | ✅ | `ClickHouseConnection.insertStreaming(String, Publisher<ByteBuffer>)` streams the body instead; deliberately never retried once any bytes may have reached the server. |
| `Statement.add()` bound-parameter batching | ✅ | Snapshots each complete binding set and executes the sets sequentially, emitting one `Result` per set. It does not coalesce them into one wire-level multi-row `INSERT`. |
| `JSON` type support | ✅ | Decodes as a plain `String`, zero extra configuration; GA since ClickHouse 25.3. |

## Known, documented, safe limitations (not fixed — deliberate)

| Finding | Verdict | Notes |
| --- | --- | --- |
| `ColumnMetadata.getJavaType()` doesn't predict a Java class ahead of decoding | ⚠️ | Duplicating client-v2's decode switch risks silent drift; derive the type from an actually-decoded row instead. |
| Transactions/savepoints | ⚠️ | Unimplemented — ClickHouse's HTTP interface has no real session affinity for its experimental transaction feature. Fails loudly. |
| Best-effort `KILL QUERY` not retried if it fails *after* being sent | ⚠️ | Logged at `WARN`. A pre-send failure of the kill request itself is now retried like any other query; only the post-send case stays best-effort. |

## Open gaps, not yet addressed

| Finding | Verdict | Notes |
| --- | --- | --- |
| Retryable server errors not exposed through R2DBC `Statement` | ❌ | Core/transport implement safe per-query opt-in through `ClickHouseQuery.withServerErrorRetryEnabled()`, but the connector has no R2DBC-facing vendor extension for enabling it. Standard R2DBC queries therefore retain pre-send-only retry behavior. |
| Arbitrary per-statement ClickHouse settings not exposed through R2DBC | ❌ | Core supports `ClickHouseQuery.withSettings(...)`; the connector currently uses it only for `setStatementTimeout`/`max_execution_time`. Settings such as `wait_end_of_query` need a vendor extension. |
| `Dynamic`/`Variant` types | 🧪 | Untested — newer, less settled experimental ClickHouse types than `JSON`. |

## See also

- [Known limitations](../reference/known-limitations.md) — the reader-facing summary of the rows a
  caller is most likely to hit day to day.
- [engineering/roadmap-archive.md's Production readiness
  review](../../engineering/roadmap-archive.md#production-readiness-review) — the full narrative,
  including how each fix was verified and which test proves it.

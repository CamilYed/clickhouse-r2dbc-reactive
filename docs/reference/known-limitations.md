# Known limitations

The honest, still-open list of what's a documented safe limitation versus what genuinely isn't
supported yet. For the full ✅/⚠️/❌ triage of every production-readiness finding (not just the
ones below), see [../../engineering/roadmap-archive.md's Production readiness
review](../../engineering/roadmap-archive.md#production-readiness-review) — that page is the actual
source of truth, kept up to date as things are found and fixed; this page is a readable summary of
the parts a caller is most likely to hit.

> [!NOTE]
> **Full-table-scan performance at very large result sets (1M+ rows) is not settled — treat it as a
> range, not a number.** Repeated 3-fork benchmark runs of the identical code and configuration
> produced results anywhere from tied with `client-v2` to ~30% slower, run to run, at the 1M-row
> tier specifically (10k/100k rows do not show this instability). GC pauses and per-fork
> Testcontainers instances were tested and ruled out as the cause; the leading remaining suspects —
> JVM JIT/compilation timing or OS-level thread scheduling differences across separate JVM launches
> — need local profiling tools (`async-profiler`, JFR, `powermetrics`) this investigation didn't have
> available. See [../performance/results.md](../performance/results.md#why-the-1m-number-wont-sit-still) for the
> full breakdown. This is a benchmarking-methodology gap, not a known driver defect — but it means
> the "1M rows" figures on that page should not be read as settled the way the 10k/100k ones are.

> [!IMPORTANT]
> **Cancelling a subscription stops the query on the ClickHouse server via a best-effort `KILL
> QUERY` this driver sends itself — not via ClickHouse's own connection-close detection, which
> doesn't work.** Verified against a real server, not assumed. See
> [../../engineering/roadmap-archive.md's Production readiness
> review](../../engineering/roadmap-archive.md#production-readiness-review) (search "Cancelling a
> client-side subscription") and the regression test that proves it,
> [`QueryCancellationAgainstRealClickHouseTest`](../../clickhouse-r2dbc-reactive-transport-http/src/test/java/io/github/camilyed/clickhouse/r2dbc/transport/http/QueryCancellationAgainstRealClickHouseTest.java).

Disposing a `Flux`/`Mono` subscription (`.take(n)`, a timeout operator, an upstream cancel) makes
this driver stop reading the response and closes its HTTP connection — that part is fully reactive
and tested. On its own, though, that would **not** stop ClickHouse itself from continuing to
execute the query: ClickHouse's own HTTP interface docs say so directly, *"Running requests don't
stop automatically if the HTTP connection is lost"* (clickhouse.com/docs/interfaces/http), and the
one setting meant to close that gap, `cancel_http_readonly_queries_on_client_close`, is itself
unreliable — a query kept running after client disconnect even with that setting enabled was
reproduced against a recent ClickHouse release and closed by ClickHouse's own maintainers as
["not planned"](https://github.com/ClickHouse/ClickHouse/issues/92786).

**What this driver does about it:** on cancellation, `ClickHouseHttpTransport` sends an explicit
`KILL QUERY WHERE query_id = '<id>' ASYNC` over a separate request — but only once the original
request had actually reached the server (cancelling before that means there's nothing to kill yet).
It reuses the same authenticated user as the original query, since ClickHouse lets a user stop
their own queries without a separate `KILL QUERY` privilege grant. This is genuinely **best-effort,
not a guarantee**: if the kill request itself fails — most plausibly the connecting user turns out
to lack the privilege after all under a restricted RBAC setup, or the server is simply unreachable
at that moment — the failure is logged at `WARN` and otherwise swallowed; it never surfaces on the
caller's already-cancelled subscription. Under a pattern like "timeout and retry" against a server
where this occasionally fails, that residual risk (a query that keeps running despite being
cancelled) still exists, just far less often than before this was implemented — watch for the
`WARN` log if that matters to you.

> [!IMPORTANT]
> **A query that fails *after* it has already started streaming rows back can hand you spurious
> garbage rows mixed in with genuine ones, under this driver's default settings.** Confirmed against
> a real server, not assumed: ClickHouse's HTTP interface can only attach its clean
> `X-ClickHouse-Exception-Code` error header before any response bytes go out; once rows are already
> streaming, a server-side failure gets appended as plain error text directly into the response body
> instead — and that text gets misdecoded by the RowBinary reader as further column values
> (indistinguishable in shape from real data) before decoding eventually fails and the `Flux`
> terminates with an error. This driver guarantees the query never silently completes as if the
> result were whole, and that genuine rows already emitted are never retroactively dropped or
> reordered — but it cannot guarantee every emitted row is genuine. If your application cannot
> tolerate that risk, set ClickHouse's own `wait_end_of_query=1` setting via
> [`ClickHouseQuery.withSettings(Map.of("wait_end_of_query", "1"))`](../../clickhouse-r2dbc-reactive-core/src/main/java/io/github/camilyed/clickhouse/r2dbc/core/ClickHouseQuery.java),
> which trades incremental streaming for having ClickHouse buffer the whole response server-side and
> only ever send a clean error. See
> [`MidStreamQueryFailureAgainstRealClickHouseTest`](../../clickhouse-r2dbc-reactive-connector/src/test/java/io/github/camilyed/clickhouse/r2dbc/connector/MidStreamQueryFailureAgainstRealClickHouseTest.java)
> for the regression tests proving both behaviors.

> [!NOTE]
> **Every query is automatically retried, but only for failures that happen before any bytes were
> sent to the server.** A connection-level hiccup (connect refused, a momentary pool exhaustion) is
> retried up to `retryMaxAttempts` times (default 3, matching client-v2's own default retry count for
> the same class of failure — verified against our pinned client-v2 version's source), each separated
> by `retryDelay` (default 50ms). Deliberately scoped to pre-send failures only, regardless of whether
> the query is a `SELECT` or an `INSERT`: since the server never received any bytes of a pre-send-
> failed attempt, retrying it cannot make the query run twice server-side, so no idempotency guessing
> is needed. A failure *after* the request was sent — a connection reset mid-response, a server error
> — is never retried, precisely to avoid the risk of applying a non-idempotent statement twice. Set
> `retryMaxAttempts=0` to disable entirely. See
> [`RetryPolicy`](../../clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/RetryPolicy.java)'s
> Javadoc for the full reasoning.

> [!NOTE]
> **`ssl=true` supports a custom trust store via `sslRootCert`, for self-signed/internal-CA
> certificates.** TLS auto-negotiation from the `https://` scheme is verified (see
> [`ClickHouseHttpTransportTlsTest`](../../clickhouse-r2dbc-reactive-transport-http/src/test/java/io/github/camilyed/clickhouse/r2dbc/transport/http/ClickHouseHttpTransportTlsTest.java)),
> and connecting to a server presenting a self-signed or internal-CA certificate — common for a
> database that usually isn't exposed to the public internet — no longer requires importing anything
> into the JVM's own default trust store. Set the `sslRootCert` connection option
> (`ClickHouseConnectionFactoryProvider.SSL_ROOT_CERT`) to either a classpath resource path or a
> filesystem path pointing at a PEM-encoded certificate; it's resolved as a classpath resource first,
> then as a filesystem path, mirroring r2dbc-postgresql's own `sslRootCert` option — convenient for a
> Kubernetes/Tanzu-style deployment where the certificate is mounted from a `Secret`/`ConfigMap`.
> Requires `ssl=true`; setting it without `ssl=true` fails fast with `IllegalArgumentException`.

> [!NOTE]
> **Large/batch `INSERT`s can stream their data as the request body instead of the URL, via
> `ClickHouseConnection.insertStreaming(String, Publisher<ByteBuffer>)`.** Standard
> `Statement`/`Batch` still send `INSERT` data URL-encoded, same as any `SELECT` — correct, but not
> what large payloads want (URL length limits, the whole payload held in memory). ClickHouse's own
> HTTP docs describe the request-body form as the preferred pattern for exactly this reason. This is
> a ClickHouse-specific vendor extension (R2DBC's `Statement`/`Batch` have no concept of a streamed
> request body), and it is **never retried**, even with `retryMaxAttempts` configured — once bytes
> of the payload may have already reached the server mid-stream, retrying on a connection-level
> failure risks silently duplicating rows. See
> [`ClickHouseHttpTransport.insertWithSummary`](../../clickhouse-r2dbc-reactive-transport-http/src/main/java/io/github/camilyed/clickhouse/r2dbc/transport/http/ClickHouseHttpTransport.java)'s
> Javadoc for the full reasoning.

> [!NOTE]
> **`JSON` columns are supported, decoded as a plain `String`, with zero extra configuration.**
> GA since ClickHouse 25.3 — no `allow_experimental_json_type` needed. This driver sends
> `output_format_binary_write_json_as_string=1` unconditionally on every query (harmless when a
> table has no `JSON` column), so a `JSON` column just works whether queried directly or through
> something like Spring's `DatabaseClient`. `Dynamic`/`Variant` remain unsupported — newer, less
> settled experimental types.

> [!NOTE]
> **Transactions, savepoints, generated keys, and `Blob`/`Clob` binding are deliberately
> unsupported** — ClickHouse itself has no equivalent concept for any of them, so this driver fails
> loudly (`UnsupportedOperationException`) rather than silently pretending otherwise. See
> [r2dbc-compatibility.md](r2dbc-compatibility.md) for the full, written-up answer to
> "where does this driver match the R2DBC SPI" — backed by
> [`ClickHouseR2dbcSpiCompatibilityTest`](../../clickhouse-r2dbc-reactive-connector/src/test/java/io/github/camilyed/clickhouse/r2dbc/connector/ClickHouseR2dbcSpiCompatibilityTest.java)
> actually running the official R2DBC SPI Technology Compatibility Kit against a real ClickHouse
> server, not just a claim.

# ClickHouse HTTP protocol — client-v2 reference audit

This is a from-source audit of everything `com.clickhouse:client-v2`'s
`internal.HttpAPIClientHelper` does on the wire, so `transport-http`/`core` don't silently miss a
concern by only looking at the `SELECT 1` happy path. Not a copy of client-v2's code or design —
our own notes, with file:line citations against the class that owns each concern, translated into
what we actually need and when.

Source: `ClickHouse/clickhouse-java`, commit `601ade16364d667b71a4982bbabf60ed2d941dc1` (cloned
2026-08-07, not vendored into this repo — clone it yourself to check citations:
`git clone https://github.com/ClickHouse/clickhouse-java.git`). All line numbers below are against
`client-v2/src/main/java/com/clickhouse/client/api/internal/HttpAPIClientHelper.java` unless noted.
Re-verify if client-v2 moves past this commit before we build the corresponding feature — this is
a snapshot, not a live contract.

## Contents

- [Compression — two independent layers](#compression--two-independent-layers)
- [Authentication](#authentication)
- [Headers set on every request](#headers-set-on-every-request)
- [Query parameters (settings, query_id, roles, statement params)](#query-parameters-settings-query_id-roles-statement-params)
- [Response handling and error semantics](#response-handling-and-error-semantics)
- [The mid-stream error caveat, concretely](#the-mid-stream-error-caveat-concretely)
- [TLS / mTLS](#tls--mtls)
- [Connection pooling and timeouts](#connection-pooling-and-timeouts)
- [Retry classification](#retry-classification)
- [Explicitly out of scope for this audit](#explicitly-out-of-scope-for-this-audit)
- [Mapping to our phases](#mapping-to-our-phases)

## Compression — two independent layers

Easy to conflate; client-v2 keeps them genuinely separate (lines 848–966):

1. **Standard HTTP compression** (`Content-Encoding`/`Accept-Encoding: gzip`, generic — handled by
   Apache `commons-compress`' `CompressorStreamFactory`). Off by default
   (`USE_HTTP_COMPRESSION` = `client.use_http_compression`, default `"false"` —
   `ClientConfigProperties.java:90`).
2. **ClickHouse's own LZ4 frame compression**, applied to the data block itself, independent of
   HTTP framing (`LZ4Entity`, using `lz4Factory`/`net.jpountz.lz4`). Query params `compress=1`
   (server→client) / `decompress=1` (client→server), plus header `Content-Encoding`/
   `Accept-Encoding: lz4` only when `USE_HTTP_COMPRESSION` is also on. **This is the default
   compression path**, not gzip: `COMPRESS_SERVER_RESPONSE` (`compress`) defaults `"true"`,
   `COMPRESS_CLIENT_REQUEST` (`decompress`) defaults `"false"` (`ClientConfigProperties.java:86,88`).
3. Response wrapping picks LZ4 vs. generic decompression based on which signal is present
   (`wrapResponseEntity`, line 949) — and explicitly skips LZ4-decoding a response on
   `403`/`401` (line 963: error bodies for those statuses aren't LZ4-framed).

**For us:** server-response LZ4 decompression is the one that matters first (default-on) — our
bridge (`Flux<ByteBuffer>` → `InputStream`) needs an LZ4-frame-decoding stage before the bytes ever
reach `RowBinaryWithNamesAndTypesFormatReader`, or we must explicitly send `compress=0` in Phase 1
and defer real LZ4 support to a later step. Recommendation: **explicitly disable it for the Phase 1
spike** (`compress=0`/no `Accept-Encoding`) so the transport spike proves the streaming/backpressure
properties without also debugging frame decoding at the same time; add LZ4 (or plain gzip, simpler
first cut) as its own TDD unit once Phase 1's acceptance criteria are green.

## Authentication

Three mutually exclusive modes, decided by config, all in `addHeaders` (lines 796–866):

- **mTLS / SSL cert auth**: `X-ClickHouse-User` header + `X-ClickHouse-SSL-Certificate-Auth: on`,
  no password sent — the client cert itself authenticates (`SSL_AUTH` flag).
- **HTTP Basic** (`HTTP_USE_BASIC_AUTH`, default `"true"` — `ClientConfigProperties.java:38`):
  standard `Authorization: Basic base64(user:password)`.
- **ClickHouse's own header pair** (used when Basic is off): `X-ClickHouse-User` +
  `X-ClickHouse-Key`.
- Optional `Proxy-Authorization` if a proxy is configured.
- If a caller manually sets `Authorization` **and** the ClickHouse user/password headers, the
  user/password headers are silently dropped (lines 876–881) — "explicit auth header wins."

**For us:** Basic auth is the sane default to implement first (it's also literally what R2DBC's
`ConnectionFactoryOptions.USER`/`PASSWORD` map onto most naturally). SSL cert auth and proxy auth
are real but lower priority — document as supported-later, not silently unsupported (per
[CLAUDE.md](../CLAUDE.md)'s no-silent-gaps rule already stated for R2DBC semantics).

## Headers set on every request

`addHeaders` (line 796 onward):

| Header | Source |
| --- | --- |
| `Content-Type` | fixed `text/plain; charset=UTF-8` for the SQL body |
| `X-ClickHouse-Format` | requested result/input format (e.g. `RowBinaryWithNamesAndTypes`) |
| `X-ClickHouse-Query-Id` | client-supplied `query_id`, if set |
| `X-ClickHouse-Database` | target database |
| auth headers | see above |
| `Accept-Encoding`/`Content-Encoding` | compression, see above |
| arbitrary `X-*` pass-through | any config key prefixed `http_header_` is forwarded verbatim as a header with that prefix stripped — an escape hatch for callers |
| `User-Agent` | corrected/normalized last (`correctUserAgentHeader`, line 866) |

**For us:** `query_id` is the one with a hard dependency elsewhere — Phase 1/Phase 2's cancellation
story (server-side `KILL QUERY` semantics) needs it generated and threaded through from day one,
not bolted on later.

**Verified empirically (Phase 2, `QueryIdHeaderAgainstRealClickHouseTest`):** client-v2's own
`ClickHouseHttpProto.HEADER_QUERY_ID` Javadoc claims "Response only header ... Cannot be used in
request" — yet client-v2's own `addHeaders` code sets it as a request header anyway, and separately
sends a `query_id` URL query parameter too. That's a contradiction inside client-v2 itself, worth
not trusting blindly. Tested directly against a real ClickHouse container, header-only (no query
parameter): the server's response echoed back exactly the `query_id` we sent as a request header.
**The header alone is sufficient** — client-v2's own doc comment is stale/misleading, not something
to imitate defensively by also sending the query parameter unless a future finding says otherwise.

## Query parameters (settings, query_id, roles, statement params)

`addRequestParams`/`addStatementParams` (lines 926–954):

- `query_id` is sent **both** as a header and (per `getQueryId`, line 862 in the earlier excerpt)
  read back from either the server's response header or the client's own request header —
  whichever is present — when building error messages. So server and client agree on one
  `query_id` per request, not two independent ones.
- Session roles (`SESSION_DB_ROLES`) become repeated `role` query params.
- Arbitrary ClickHouse **server settings** (config keys prefixed `SERVER_SETTING_PREFIX`) become
  arbitrary query params 1:1 — this is how `max_execution_time`, `wait_end_of_query`, etc. actually
  reach the server; there's no fixed enum of "supported settings" in the transport layer itself,
  it's an open string-keyed map.
- SQL statement parameters (`param_<name>=<value>` query params) — ClickHouse's own parameterized-query
  mechanism, separate from settings.

**For us:** modeling `QuerySettings` in `core` as an open string-keyed map (not a fixed set of
fields) matches how ClickHouse itself treats settings — mirrors what Phase 2's "core: query
request/settings/query_id representation" already says, just confirms the shape.

## Response handling and error semantics

`executeRequest` (line 688 onward):

- `X-ClickHouse-Exception-Code` header present ⇒ always an error, **regardless of HTTP status**
  (checked before the status-code switch, line 705) — confirms the "HTTP 200 doesn't guarantee
  success" caveat already in our README, but sharpens it: the actual signal to check per-response
  is this header, not just status code.
- Status code switch (line 710): `200` ⇒ success; `407` ⇒ proxy auth; `502`/`503` ⇒ treated as
  connectivity errors (`ConnectException`, i.e. potentially retryable); `400`/`401`/`403`/`500`/`404`
  ⇒ read the ClickHouse error body; anything else ⇒ generic `ClientException`, logged but not
  specially classified.
- Error body itself has two shapes handled separately: a genuine ClickHouse error (parsed by
  `readClickHouseError`, using the exception-code header to build a `ServerException` with
  ClickHouse's own error code) vs. a non-ClickHouse error (`readNotClickHouseError` — e.g. a proxy
  or gateway's own error page).

**For us:** R2DBC exception mapping in `connector` (Phase 3) needs both: a `ServerException`
analog carrying ClickHouse's numeric error code (for `R2dbcException.getErrorCode()`), and a
fallback path for "the response wasn't even from ClickHouse" (proxy/gateway noise).

## The mid-stream error caveat, concretely

Checked whether client-v2 does anything special for an error that happens *after* some rows have
already streamed with `HTTP 200` already sent (the scenario our README already flags as a known
caveat). Finding: **it doesn't, beyond passing through whatever the underlying `HttpEntity`
exposes as trailers** (`LZ4Entity`/`CompressedEntity` both just delegate `getTrailers()` to the
wrapped entity — no ClickHouse-specific trailer parsing found anywhere in `client-v2`). The actual
mechanism ClickHouse exposes for this is the `wait_end_of_query` server setting
(`ServerSettings.WAIT_END_OF_QUERY`, surfaced as `QuerySettings.waitEndOfQuery(boolean)`): when
`1`, the server buffers the whole result server-side and only responds once it's known to have
succeeded (trading the "fully reactive, streamed from the first byte" property for a correctness
guarantee); when `0` (default), you get true streaming but a mid-query failure can simply truncate
the connection with no trailer to explain why.

**For us:** this is a real, load-bearing design decision for Phase 2/4, not a detail — "fully
reactive" (streamed, not buffered) and "every error is signalled reactively, not silently
truncated" are two properties in README's table that are in tension here. Options to evaluate then:
detect an abnormally-closed/truncated stream and surface it as a distinct
"connection closed mid-result, cause unknown" error type (rather than silently completing the
`Flux` short); document `wait_end_of_query=1` as the opt-in "prioritize error-safety over
first-byte latency" mode we expose through `QuerySettings`. Don't resolve this now — flag it as a
named open question for Phase 2's contract-test matrix.

## TLS / mTLS

`createSSLContext` (line 167 onward): configurable trust store, client certificate/key (mTLS,
independent of how the server cert is verified), `SSLMode`, SNI override, cipher suite allowlist,
and an escape hatch to hand in a caller-supplied `SSLContext` directly.

**For us:** Reactor Netty's `HttpClient.secure(...)` covers the equivalent surface natively; this
is "know the shape exists," not "port this code" — Netty's own SSL API is what `transport-http`
should use.

## Connection pooling and timeouts

`createConnectionConfig`/pool builder (lines 212–274): connect timeout, response timeout
(`RequestConfig.setResponseTimeout`, line 531), `PoolingHttpClientConnectionManager` with
per-route connection limits (`HTTP_MAX_OPEN_CONNECTIONS`) — mechanically irrelevant to us (that's
exactly the blocking Apache HttpClient5 pool Phase 0 already ruled out reusing), but the
**concepts** (connect timeout, response/read timeout, bounded per-target connection limit) are
exactly what Phase 1's "active/pending request limits are explicit and enforced" criterion already
names — Reactor Netty's `ConnectionProvider` (max connections, pending-acquire timeout/queue) is
the non-blocking equivalent to design against.

## Retry classification

`shouldRetry` (line 1015 onward): retry causes are an explicit, configurable set
(`ClientFaultCause`: `NoHttpResponse`, `ConnectTimeout`, `ConnectionRequestTimeout`,
`SocketTimeout`, `ServerRetryable`) — `ServerRetryable` specifically checks
`ServerException.isRetryable()`, i.e. ClickHouse itself flags certain error codes as safe to retry
(e.g. resource-contention errors), not just any 5xx.

**For us:** out of scope for Phase 1–4 (no retry logic planned yet per README's "what this project
is not," unless that's been revisited) — noted here so a future retry feature doesn't reinvent
"which errors are safe to retry" from scratch; ClickHouse's own retryable-error-code list is the
right source of truth to mirror, not a guess.

## Explicitly out of scope for this audit

Not reviewed (would need a separate pass if/when relevant): multipart request encoding (used for
some bulk-insert paths, line 549 area — "multipart doesn't support compression right now" per the
code's own comment), session *stickiness* beyond role params, Kerberos/GSS auth, HTTP/2, proxy
auto-detection, and anything in client-v2 outside `internal.HttpAPIClientHelper` (e.g. the
higher-level `Client` façade, insert-path builders) — those weren't read for this pass.

## Mapping to our phases

- **Phase 1 (now):** none of this blocks the transport spike. Recommend explicitly disabling
  compression (`compress=0`) for the spike itself, per the compression section above, so streaming/
  backpressure/cancellation get proven without also debugging LZ4 framing.
- **Phase 2 (core protocol):** `query_id` generation/threading, settings-as-open-map, and the
  mid-stream-error open question all belong here — each should get a named test in `testkit`'s
  contract matrix per [CLAUDE.md](../CLAUDE.md#test-types-and-tools).
  Response compression (LZ4 first, since it's ClickHouse's default) is also natural here, as its
  own small TDD unit once `SELECT 1` works uncompressed.
- **Phase 3 (connector):** auth mapping from R2DBC `ConnectionFactoryOptions` (Basic first), error
  mapping (`ServerException`-equivalent with ClickHouse's numeric code) to `R2dbcException`.
- **Later / explicitly deferred:** mTLS, proxy auth, retry logic, multipart/bulk-insert — track in
  ROADMAP.md if/when they become real requirements rather than speculative scope now.

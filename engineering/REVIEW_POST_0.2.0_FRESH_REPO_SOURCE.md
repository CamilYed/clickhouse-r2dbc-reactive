# Fresh repository review for Claude — `clickhouse-r2dbc-reactive`

> External review, adopted as the source for
> [ROADMAP.md's Phase 8](roadmap-archive.md#phase-8--post-020-hardening-021). Every concrete technical
> claim below that Phase 8 turned into a roadmap item was re-verified against actual current source
> before being copied in — this file is kept in full as the original external text; ROADMAP.md is
> the working, triaged, up-to-date version of the same findings. Items this review flagged as
> benchmark/performance-methodology concerns are deferred in ROADMAP.md until a proper benchmark
> environment exists, per the project's standing "no more performance work without an environment"
> direction — that's a scheduling decision, not a disagreement with the finding.

**Repository:** `CamilYed/clickhouse-r2dbc-reactive`  
**Reviewed HEAD:** `698949ca4c9f98b9b996c68712acd812586054cc`  
**Released version:** `0.2.0`  
**Current development line:** `0.2.1-SNAPSHOT` / `[Unreleased] 0.2.1`  
**Review date:** 2026-08-21

## Purpose

This is a fresh review of the repository **after 0.2.0** and after the recent lifecycle, leak-detection,
connection-reuse and README work.

Do not blindly reuse findings from older reviews. Several old issues are already fixed.

The goal is to identify:

1. current correctness risks;
2. benchmark methodology risks;
3. production-readiness gaps;
4. adoption/Spring gaps;
5. demo gaps;
6. sensible work ordering for the next releases.

---

# 1. Things that are already done — do not implement them again

The repository has moved substantially since the earlier reviews.

Current main already has:

- `0.2.0` published;
- standard R2DBC `ConnectionFactoryProvider` discovery;
- R2DBC TCK coverage against a real ClickHouse server;
- `ConnectionFactoryOptions.DATABASE` mapped to `X-ClickHouse-Database`;
- correct semantics for `USER` without `PASSWORD` — empty password, not literal `"null"`;
- configurable Reactor Netty transport pool:
  - max connections;
  - max pending acquisitions;
  - pending-acquire timeout;
  - max idle time;
  - max life time;
- standard `CONNECT_TIMEOUT`;
- trusted custom CA through `sslRootCert`;
- pre-send retry policy;
- `Connection.setStatementTimeout(Duration)` via `max_execution_time`;
- `Statement.add()` with correctness-first sequential execution;
- query cancellation plus best-effort `KILL QUERY`;
- neutral observability SPI;
- NOOP observability fast path;
- numeric and temporal `Row.get(..., Class<T>)` conversions;
- dedicated driver-owned bounded row-decoding scheduler;
- chunk coalescing in `FluxInputStreamBridge`;
- real ClickHouse type tests for a broad set of types;
- streaming insert vendor extension;
- nightly ClickHouse compatibility matrix;
- Netty paranoid leak-detection lane;
- explicit factory/transport/decoder lifecycle in current `0.2.1` work:
  - `ClickHouseConnectionFactory.dispose()`;
  - `ClickHouseConnectionFactory.isDisposed()`;
  - transport disposal;
  - decoder scheduler disposal;
- bounded-drain fix so `Flux.next()` does not unnecessarily destroy an otherwise reusable connection;
- a public-API matched-pool throughput benchmark already exists:
  `PublicApiMatchedPoolThroughputBenchmark`.

Do not spend another PR “fixing” any of the above unless a new failing test proves a remaining bug.

---

# 2. Highest-priority finding: benchmark compression is not currently apples-to-apples

This should be checked **before using the current scan/transport benchmark numbers as strong public
performance evidence**.

The project's own `docs/CLIENT_V2_HTTP_REFERENCE.md` says client-v2 enables ClickHouse server-response
compression by default.

That is still true in the pinned `client-v2:0.9.8` source:

```text
COMPRESS_SERVER_RESPONSE("compress", Boolean.class, "true")
```

The custom Reactor Netty transport currently does not implement the matching LZ4 response-compression
path and does not send `compress=1`.

At the same time, benchmark client-v2 builders such as:

```text
ClientV2PointQueryClient
StreamingScanBenchmark
TransportOnlyStreamingBenchmark
BoundedPoolConcurrencyBenchmark
```

construct client-v2 with its defaults and do not visibly disable server-response compression.

Therefore some benchmark comparisons are currently closer to:

```text
our driver:
    uncompressed ClickHouse response
    + Reactor Netty

client-v2:
    LZ4 compressed ClickHouse response
    + decompression
    + client-v2 transport
```

than to:

```text
same wire behavior
vs
same wire behavior
```

This matters especially for:

```text
StreamingScanBenchmark
TransportOnlyStreamingBenchmark
large result sets
```

On a local Docker/loopback benchmark compression may **hurt** client-v2 because network bandwidth is
cheap and compression/decompression consumes CPU. On a real remote network compression can instead
help substantially.

So the direction of the bias depends on the environment.

## Required action

Before another headline performance write-up:

1. inspect the exact client-v2 0.9.8 API for disabling server-response compression;
2. create an explicitly named benchmark profile with:

```text
our driver: compression OFF
client-v2:   compression OFF
```

3. verify the actual request/wire settings in a test — do not only trust builder configuration;
4. rerun:
   - `TransportOnlyStreamingBenchmark`;
   - `StreamingScanBenchmark`;
   - `PublicApiMatchedPoolThroughputBenchmark`;
5. label older large-result benchmark results as having unmatched compression unless proven otherwise.

Later, when this driver supports response compression, add a second matrix:

```text
compression OFF / OFF
compression ON  / ON
```

Do not implement compression in the production driver merely to make the benchmark “look fair”.
First make the existing comparison controlled.

This is the most important fresh finding in this review.

---

# 3. Benchmark lifecycle bug after the new factory-dispose work

`OurDriverPointQueryClient` currently does:

```java
final ConnectionFactory factory = ClickHouseConnectionFactory.from(options);
this.connection = Mono.from(factory.create()).block(...);
```

but stores only:

```java
private final Connection connection;
```

and `close()` does only:

```java
Mono.from(connection.close()).block(...);
```

This was less visible before the recent lifecycle work.

Current `ClickHouseConnectionFactory` now explicitly owns:

```text
one ClickHouseHttpTransport
one Reactor Netty ConnectionProvider
one RowDecodingScheduler
```

and the supported lifecycle hook is:

```java
factory.dispose();
```

Closing a logical `Connection` deliberately does **not** dispose those factory-owned resources.

Therefore the benchmark adapter loses the object that owns the resources it created.

## Why this matters

Across benchmark trials/parameters in one fork, this can leave:

```text
old connection providers
old decoder schedulers
old worker threads/resources
```

alive longer than intended and can contaminate later benchmark parameters.

## Minimal fix

Store the concrete factory:

```java
private final ClickHouseConnectionFactory factory;
private final Connection connection;
```

and in `close()`:

```text
close logical connection
dispose factory
```

If deterministic shutdown matters for JMH teardown, consider adding/using an awaitable factory-level
disposal path, but do not redesign production lifecycle only for JMH without evidence.

Also inspect every benchmark helper that creates a factory/transport and confirm ownership is closed
symmetrically with client-v2.

---

# 4. `@OperationsPerInvocation` — fix the stale uncertainty, not the implementation

`PublicApiMatchedPoolThroughputBenchmark` contains a comment saying the interpretation of
`@OperationsPerInvocation(4096)` has not yet been empirically verified.

JMH's documented semantics are specifically that the annotation tells JMH that one benchmark method
invocation contains N logical operations and JMH adjusts the score accordingly.

So the design is sensible.

Still do one deterministic sanity check:

```text
manual logical QPS =
REQUESTS_PER_INVOCATION / measured invocation duration
```

versus JMH's reported `ops/s`.

If they match, remove the “not yet verified” uncertainty from the Javadoc.

Do not replace the benchmark design unnecessarily.

---

# 5. Cloud benchmark/reporting pipeline is still missing

The repository has:

```text
ci.yml
nightly.yml
release.yml
```

but no dedicated:

```text
.github/workflows/benchmark.yml
scripts/benchmarks/analyze.py
benchmark report artifact pipeline
GitHub Pages benchmark history
```

The nightly workflow itself explicitly states that JMH regression work is not wired yet because a
baseline-comparison strategy is missing.

This should now be implemented because the repository has a reasonable headline benchmark.

## First version

Keep it small:

```text
workflow_dispatch
        ↓
one Ubuntu GitHub runner
        ↓
one pinned ClickHouse container
        ↓
ourDriver + client-v2 in SAME job
        ↓
JMH JSON
        ↓
analyze.py
        ↓
summary.md + charts
        ↓
GitHub Actions artifact
```

Artifact:

```text
benchmark-results/
    results.json
    metadata.json
    stdout.txt
    summary.md
    throughput.png
    latency.png
    allocation.png
```

Do not create performance gates based on ±2% changes.

First collect multiple paired runs and measure variance.

The cloud pipeline plan should remain separate from production-driver optimizations.

---

# 6. The demo still claims HTTP streaming that it does not actually demonstrate

Current controller:

```java
@GetMapping("/order-events")
Flux<OrderEvent> all()
```

has Javadoc saying:

> response body is written as it arrives from ClickHouse, not buffered into a single in-memory list

But it does not declare a streaming media type.

With Spring WebFlux/Jackson, an ordinary multi-value publisher rendered as:

```text
application/json
```

is normally collected and serialized as one JSON array.

Real incremental flushing is obtained with a streaming media type such as:

```text
application/x-ndjson
```

or SSE.

So the R2DBC/database side may be streaming correctly while the HTTP demo buffers at the web-codec
boundary.

## Fix

Keep the existing JSON endpoint if useful, but add an explicit endpoint such as:

```text
GET /order-events/stream
Produces: application/x-ndjson
```

and prove:

```text
first HTTP element is received before database/source completion
```

Do not test this with:

```java
expectBodyList(...)
```

because that waits for the whole response and proves nothing about streaming.

This is important because end-to-end streaming is one of the central reasons this driver exists.

---

# 7. The demo still contains a stale `count()` workaround

`DatabaseClientOrderEventRepository` still says raw ClickHouse `UInt64` cannot be read as `Long`, and
therefore executes:

```sql
SELECT toUInt32(count()) AS total
```

Current `ClickHouseValueConverter` already supports numeric conversion between:

```text
Byte
Short
Integer
Long
Float
Double
BigInteger
BigDecimal
```

with range checking.

So the demo documentation and workaround are stale.

The current workaround also unnecessarily caps a real count to UInt32 range.

## Fix

Test against a real server:

```sql
SELECT count() AS total
```

and:

```java
row.get("total", Long.class)
```

If green, remove:

```text
toUInt32(...)
old ClassCastException explanation
```

Do not change anything else in the repository adapter in the same PR.

---

# 8. `Enum8` / `Enum16` still leak a client-v2 internal implementation type

The real-world type test explicitly documents that enums currently decode as client-v2's internal:

```text
EnumValue
```

The demo has to do:

```java
row.get("status", Object.class).toString()
```

This is undesirable for a public R2DBC driver because callers can observe an upstream `.internal`
type through `Row`.

## Recommended fix

Normalize:

```text
Enum8
Enum16
```

to a stable public representation — most naturally `String` member name — at the driver boundary.

Then test:

```java
row.get("status", String.class)
```

against real ClickHouse.

Do not expose `com.clickhouse...internal.EnumValue` in any public API or documentation.

This is a relatively small change with a high ergonomics payoff.

---

# 9. Parameter binding still needs a real type matrix

Current parameter encoding in `ClickHouseQuery` is:

```java
if null -> \N
if String -> escaped String
otherwise -> value.toString()
```

Unit coverage is currently focused on:

```text
null
integer
string escaping
```

That is not enough to claim robust typed R2DBC binding across the type surface the driver can read.

Potentially problematic inputs include:

```text
UUID
BigInteger / UInt128 / UInt256
BigDecimal
LocalDate
LocalDateTime
Instant / OffsetDateTime / ZonedDateTime
Boolean
Array(String)
Array(numeric)
Map
Tuple
Enum
IPv4 / IPv6
Nullable complex values
```

`List.toString()` or arbitrary object `toString()` is not automatically ClickHouse's Escaped
parameter format.

## Required work

Before adding clever conversion code, build a real ClickHouse binding matrix:

```text
Java input
ClickHouse placeholder type
encoded request value
actual returned ClickHouse value
```

Example:

```text
bind UUID        -> {v:UUID}
bind BigDecimal  -> {v:Decimal(18,4)}
bind LocalDate   -> {v:Date}
bind List<String> -> {v:Array(String)}
```

Only after tests show which types fail should the encoder be expanded.

Prefer an explicit small parameter encoder over “everything uses toString”.

---

# 10. Placeholder parsing is regex-based and has a known correctness limitation

`ClickHouseQuery.parameterNamesIn(sql)` uses:

```text
\{([a-zA-Z_]\w*):[^}]+}
```

and its own Javadoc acknowledges that it cannot distinguish a real parameter from matching text
inside:

```text
SQL string literals
comments
```

This can create phantom bind parameters.

Examples to test:

```sql
SELECT '{not_a_parameter:String}'
```

and:

```sql
SELECT 1 -- {not_a_parameter:UInt32}
```

## Recommendation

Replace the regex-only discovery with a very small SQL scanner that understands enough to skip:

```text
single-quoted strings
double/backtick quoted identifiers if relevant
line comments
block comments
```

It does not need to become a full SQL parser.

Keep ClickHouse's `{name:Type}` syntax untouched.

---

# 11. Long analytical SQL is still placed in the URL

Normal query execution builds:

```text
POST /?query=<URL-encoded-SQL>&param_...&settings...
```

The streaming insert path already documents that putting payload in the body avoids URL limits.

But ordinary SELECT/analytics SQL still goes into the URI.

Real ClickHouse analytical queries can contain:

```text
large CTEs
long JOIN graphs
generated expressions
many parameters/settings
```

and proxies/load balancers often impose request-line/URI limits.

## Recommendation

Add a real integration/proxy-boundary test for long SQL first.

Then evaluate a transport shape where SQL is sent in the POST body while parameters/settings remain
query parameters.

Be careful: changing from a bodyless request to a request body changes the current retry-safety
reasoning around `requestSent`.

Do not casually reuse the current pre-send retry condition until body-write semantics are proven.

---

# 12. Mid-stream ClickHouse failure semantics remain unresolved

The protocol audit correctly identifies a hard ClickHouse HTTP tradeoff:

```text
wait_end_of_query=0
    -> true streaming / low TTFR
    -> failure can happen after HTTP 200 and after partial result data

wait_end_of_query=1
    -> stronger error certainty
    -> server buffers before response
```

There is still no named real-server test characterizing what the driver does when ClickHouse fails
after rows have begun streaming.

## Add tests for

```text
some rows received
then server/query failure
then response stream terminates
```

Expected behavior must be explicit:

```text
never silently complete as if result were complete
surface a useful R2dbcException / transport error
preserve already emitted rows according to Reactive Streams semantics
```

Also document `wait_end_of_query=1` as an opt-in correctness-vs-streaming tradeoff if the generic
settings API can already express it.

This is more important than adding exotic data types.

---

# 13. Server-side retryable ClickHouse errors remain the main documented retry gap

Current `RetryPolicy` is conservative:

```text
retry only failures known to happen before the request was fully sent
```

That is safe and should remain the default until a stronger policy is proven.

The repository already uses client-v2 `0.9.8`, whose public `ServerException.isRetryable()` can
identify ClickHouse server error codes considered retryable.

Current transport deliberately does not act on it.

## Recommendation

Do not simply retry every retryable server exception.

First define semantics by operation:

```text
SELECT:
    server-classified retry may be safe if no rows were emitted

INSERT:
    do not retry automatically unless idempotency/dedup semantics are explicit

streaming INSERT:
    current no-retry behavior should remain conservative
```

Potential API:

```text
retryMode = PRE_SEND_ONLY              // current safe default
retryMode = SAFE_SERVER_READ_RETRIES   // opt-in later
```

Tests must cover:

```text
retryable server error before rows
non-retryable server error
retryable-looking error after partial data
INSERT never duplicated
cancellation never causes retry
```

---

# 14. Transport `responseTimeout` exists but is not exposed through normal R2DBC configuration

`TransportOptions` / `ClickHouseHttpTransport` support a response timeout.

`ClickHouseConnectionFactoryProvider` currently exposes:

```text
connectTimeout
transport pool options
retry options
sslRootCert
observation listener
```

but there is no corresponding public custom R2DBC option for transport `responseTimeout`.

Users going through:

```text
r2dbc:clickhouse://...
ConnectionFactories.get(...)
Spring Boot
```

therefore cannot configure that transport capability without bypassing the normal bootstrap path.

## Recommendation

Add a clearly named custom option, for example:

```text
responseTimeout=PT30S
```

while keeping it distinct from:

```text
statementTimeout -> ClickHouse max_execution_time
connectTimeout   -> TCP connect
pendingAcquireTimeout -> pool queue
```

Document the semantic difference carefully.

---

# 15. Response compression should become a real driver feature after benchmark fairness is fixed

The current driver intentionally streams uncompressed RowBinary responses.

For same-host / LAN use this may be excellent.

For:

```text
remote ClickHouse Cloud
cross-region links
large scans
bandwidth-constrained environments
```

ClickHouse LZ4 response compression can be valuable.

## Recommended sequence

Do not start by implementing compression.

First:

```text
benchmark OFF/OFF fairly
```

Then design:

```text
responseCompression = NONE | LZ4
```

with:

```text
streaming decompression
bounded memory
backpressure preserved
no full-response aggregate
fragmented-frame tests
cancellation tests
leak-detection tests
```

Then benchmark:

```text
local loopback
remote/cloud-like network
compression OFF/OFF
compression ON/ON
```

---

# 16. Spring integration is functional but still not ergonomic

The demo proves the driver can be discovered and used through Spring's R2DBC stack.

But the most common Spring experience is still weak:

```text
DatabaseClient.sql(...).bind(...)
R2dbcEntityTemplate
Spring Data R2DBC
```

The demo itself documents why:

- Spring's generic bind markers do not carry ClickHouse's `{name:Type}` type;
- Spring Data's dialect resolver does not know ClickHouse;
- demo queries therefore embed escaped literals manually.

This is one of the largest adoption gaps even if it is not an R2DBC SPI correctness bug.

## Explore as a separate module, not by polluting the core driver

Possible future module:

```text
clickhouse-r2dbc-reactive-spring
```

Goals to investigate:

```text
typed binding DSL for DatabaseClient
ClickHouse-aware BindMarkersFactory strategy
optional Spring Data R2DBC dialect if technically honest
Spring Boot auto-configuration
Micrometer adapter
factory lifecycle integration
```

Do not claim full Spring Data support until a real application test proves:

```text
insert
select
typed binding
pagination/limit
mapping
startup
shutdown
```

Transactions must continue failing clearly.

---

# 17. Demo should also test current source, not only the last published release

The demo intentionally depends on:

```text
runtimeOnly("io.github.camilyed:clickhouse-r2dbc-reactive-connector:0.2.0")
```

This is good as a **consumer proof**.

But it means a change on current `main` can break the demo integration and the demo will still test
0.2.0.

Keep the published-release lane, but add a second current-source integration lane.

Possible shapes:

```text
published consumer demo
    -> Maven Central 0.2.0

current-main demo integration test
    -> project(":clickhouse-r2dbc-reactive-connector")
```

Do not silently replace the consumer-proof lane with the project dependency.

Have both questions tested separately.

---

# 18. Factory lifecycle needs an integration story for Spring

Current `0.2.1` work correctly gives `ClickHouseConnectionFactory` explicit `dispose()` semantics.

But application frameworks need to know who calls it.

The current demo builds:

```text
base ConnectionFactory
then optional r2dbc-pool ConnectionPool
```

and only exposes the final `ConnectionFactory` bean.

Once the demo consumes a version containing the new factory lifecycle, add a shutdown test proving:

```text
Spring context closes
outer r2dbc-pool closes
underlying ClickHouseConnectionFactory is disposed
decoder scheduler is disposed
Reactor Netty provider is disposed
```

Do not assume closing logical R2DBC connections disposes the factory — current code explicitly says
it does not.

Consider whether the public factory should also implement a conventional lifecycle interface such as
`AutoCloseable`/Reactor `Disposable`, but only if that genuinely improves framework integration and
does not create confusing double-close semantics.

---

# 19. Type coverage: prioritize common gaps before exotic types

Current real-world type coverage is already good.

Still-open useful items include:

### High value

```text
Enum8 / Enum16 -> stable String
Time / Time64
parameter binding for supported read types
timezone edge cases for DateTime / DateTime64
```

### Medium/later

```text
Dynamic
Variant
Geo
QBit/vector types
AggregateFunction state
SimpleAggregateFunction
```

Do not expand the type zoo merely to increase a checklist percentage.

Prioritize types users can realistically encounter in ordinary analytical schemas.

---

# 20. Multi-host / failover support is absent

The driver currently builds one:

```text
scheme://host:port
```

transport target.

For many production ClickHouse deployments users instead rely on:

```text
load balancer
ClickHouse Cloud endpoint
DNS
```

which may be sufficient.

But client-v2 can model multiple endpoints/failover more directly.

Before implementing anything, decide and document the project's intended contract:

```text
A. single endpoint only — load balancing belongs outside the driver
or
B. multiple endpoints with failover/load balancing
```

Do not leave it ambiguous.

If B is chosen, it needs:

```text
per-endpoint pool behavior
retry/failover semantics
query_id behavior
cancellation to the correct node
health/selection strategy
TLS/SNI behavior
```

This is a later feature, not a quick patch.

---

# 21. mTLS and proxy support are still intentionally absent

Current TLS supports:

```text
normal JVM trust
custom root certificate
```

The protocol audit identifies additional real enterprise surfaces:

```text
client certificate/key (mTLS)
proxy auth
proxy routing
custom SSLContext/SNI/ciphers
```

Do not implement all of them speculatively.

But for production-readiness documentation, state clearly:

```text
server TLS verification: supported
custom CA: supported
mTLS client authentication: not supported
HTTP proxy: not supported
proxy authentication: not supported
```

mTLS is the most likely next enterprise requirement.

---

# 22. Observability is a good SPI but lacks a ready-to-use production adapter

The neutral `DriverObservationListener` is a good architectural choice.

A consumer still has to implement its own metrics integration.

A future small optional module could provide:

```text
clickhouse-r2dbc-reactive-micrometer
```

Metrics:

```text
query duration
time to first row
rows
bytes
success/failure/cancel
active queries
transport pool acquired
transport pool pending
pool acquire timeout
KILL QUERY attempt/failure
retry count
```

Avoid high-cardinality tags:

```text
full SQL
query_id
customer IDs
arbitrary parameter values
```

Use:

```text
operation kind
SQL fingerprint if carefully bounded/optional
outcome
```

No Micrometer dependency should be added to core/transport/connector.

---

# 23. Cancellation observability can be improved

Cancellation itself is implemented carefully and has real-server coverage.

But operationally there is a difference between:

```text
downstream cancelled
KILL QUERY sent successfully
KILL QUERY failed
server query confirmed gone
```

The current best-effort kill failure is mainly a WARN log.

For expensive analytical workloads it would be useful to expose at least:

```text
kill attempted
kill failed
```

through an optional observation callback or transport metric.

Do not turn cancellation into synchronous “wait until server confirms” behavior by default; that
would alter cancellation latency.

---

# 24. Multiple result sets remain uncharacterized

`docs/R2DBC_COMPATIBILITY.md` correctly marks semicolon-separated compound statements as:

```text
untested / not verified
```

Do a small characterization test:

```sql
SELECT 1; SELECT 2
```

The result can legitimately be:

```text
server rejects it
driver explicitly rejects it
supported in a defined way
```

The key is to stop leaving it unknown.

If ClickHouse HTTP is intentionally one statement/request, fail early with a useful error rather than
letting behavior depend on server quirks.

---

# 25. Define a minimum ClickHouse support policy

Nightly currently tests:

```text
latest
26.7.3.19
```

and explicitly says the project has not established a minimum supported version.

That honesty is good.

The next step is not to guess one.

Build a small compatibility matrix around a few intentionally chosen versions and define:

```text
minimum tested
current stable
latest
```

If some features have later minimums, document them separately, e.g.:

```text
base driver
JSON type support
Time/Time64
Dynamic/Variant
```

Avoid a blanket “supports ClickHouse X+” until tested.

---

# 26. Keep the `client-v2` internal dependency risk visible

The driver correctly does **not** reuse client-v2's blocking HTTP transport.

It does reuse parts of client-v2's RowBinary reader/decoder implementation, including a small number
of `.internal` hooks.

That reduces duplicated protocol code, but it is a version-upgrade risk.

For every future client-v2 upgrade:

```text
run full real-world type matrix
run fragmented-buffer tests
run cancellation/leak tests
run decoder-only benchmark
run public API benchmark
inspect internal API diff
```

Consider adding an explicit “client-v2 upgrade checklist” to CONTRIBUTING.

Do not casually float the client-v2 version.

---

# 27. OSS/project-maintenance improvements

Lower priority than driver correctness, but useful now that 0.2.0 is public:

```text
SECURITY.md
GitHub issue templates
bug-report template requiring:
  driver version
  ClickHouse version
  JDK
  connection URL options with secrets removed
  reproducer
  stack trace
performance-regression template requiring JMH JSON
```

There are currently no open GitHub issues, while much future work exists only in a very large
ROADMAP.

Consider converting the next few actionable items into real issues so contributors can discover and
discuss them without reading the entire roadmap.

Do not create dozens of speculative issues.

---

# 28. ROADMAP is useful but is becoming historical documentation

`ROADMAP.md` is now very large and contains the history of many completed phases.

`docs/CURRENT_WORK.md` is already explicitly historical/superseded.

For a new contributor, it is increasingly difficult to answer:

> what are the five things actually worth working on now?

Consider adding a short:

```text
docs/NEXT.md
```

or:

```text
docs/PRODUCTION_READINESS.md
```

containing only:

```text
current released version
current HEAD goals
P0 gaps
P1 gaps
known limitations
links into ROADMAP for history
```

Keep ROADMAP; do not rewrite its history.

---

# 29. Rich demo is still worth building

The current order-events demo proves basic integration, but it still does not visually demonstrate
why a fully reactive ClickHouse driver is valuable.

The richer demo should eventually show:

```text
high-volume event generator
large NDJSON SELECT stream
time-to-first-row
slow consumer
backpressure
bounded transport pool
128 logical queries / 8 physical connections
cancellation + KILL QUERY
statement timeout
pool-acquire timeout
streaming INSERT
analytics:
  revenue timeline
  quantiles
  unique users
  funnel
live metrics/dashboard
```

Keep a separation:

```text
realistic application endpoints
/lab/* driver-behavior endpoints
```

Do not make the demo a giant DDD project.

Its job is to make the driver architecture visible.

---

# 30. Suggested priority order

## P0 — before stronger performance/public claims

1. Normalize benchmark compression.
2. Fix benchmark factory disposal.
3. Verify/remove stale `@OperationsPerInvocation` uncertainty.
4. Add cloud JMH JSON + artifact pipeline.
5. Fix demo's false HTTP-streaming claim with NDJSON.
6. Remove stale `toUInt32(count())` workaround.

## P1 — correctness / production transport

7. Build real-server parameter binding type matrix.
8. Fix parameter placeholder parsing inside strings/comments.
9. Normalize Enum8/Enum16 to String.
10. Characterize mid-stream failure behavior.
11. Add R2DBC-configurable response timeout.
12. Test long SQL / design POST-body query path.
13. Design safe server-retryable-error mode.
14. Add current-source demo integration lane.
15. Prove Spring shutdown disposes factory resources.

## P2 — production/adoption

16. Add response LZ4 compression.
17. Improve Spring integration.
18. Add optional Micrometer adapter.
19. Test Time/Time64.
20. Decide multi-host/failover contract.
21. mTLS/client cert support if requested.
22. Characterize compound statements.
23. Define tested ClickHouse version support policy.
24. Build the richer reactive analytics demo.

## P3 — later

25. Dynamic/Variant.
26. Geo/vector/exotic types.
27. wire-level batch INSERT optimization for `Statement.add()`.
28. advanced proxy/auth/session features.

---

# 31. Suggested release direction

## `0.2.1`

Keep it focused on hardening the work already underway:

```text
factory lifecycle
leak-detection lane
connection-reuse cancellation fix
benchmark harness correctness if small
documentation corrections
```

Avoid turning a patch/minor hardening release into a large feature release.

## `0.3.0` candidate theme

A coherent theme would be:

```text
production transport + parameter correctness
```

Potential scope:

```text
fair benchmark baseline
typed parameter matrix / encoder fixes
Enum normalization
long-query transport
mid-stream error characterization
responseTimeout option
safe server-retry policy design
```

Compression could fit here if it is done carefully, but should not be rushed merely because
client-v2 defaults to it.

## Later release

```text
Spring ergonomics + observability + rich demo
```

---

# 32. Claude execution rules

When implementing this review:

```text
1. Work from current main, not an old review branch.
2. Verify every claimed gap against current source before editing.
3. One hypothesis / one concern per PR.
4. Do not refactor unrelated code.
5. Add a failing test before changing correctness behavior.
6. Use real ClickHouse when behavior depends on ClickHouse semantics.
7. Use ControlledClickHouseServer for deterministic transport edge cases.
8. No Mockito.
9. No Thread.sleep.
10. Keep JDK 21 compatibility.
11. Do not change production code in a benchmark-methodology PR.
12. Do not change benchmark code in a production optimization PR.
13. Do not update docs with performance claims until the matching benchmark run exists.
14. Preserve existing deliberate unsupported R2DBC semantics.
15. Never make transactions appear supported.
```

---

# 33. First task I would give Claude

Do **not** ask Claude to “implement all missing things”.

Start with this:

```text
Fresh task against current main:

Audit benchmark compression parity and benchmark-owned resource lifecycle only.

1. Verify client-v2 0.9.8's effective server-response compression setting in every comparative JMH
   client.
2. Verify our Reactor Netty transport's effective response-compression setting.
3. Add a deterministic test/probe proving the effective setting rather than relying only on builder
   defaults.
4. Make the baseline comparison explicitly compression OFF on both sides.
5. Do not add LZ4 support to the production driver in this PR.
6. Fix OurDriverPointQueryClient so it retains and disposes the ClickHouseConnectionFactory that
   owns its transport and RowDecodingScheduler.
7. Inspect the other benchmark helpers for the same ownership mistake.
8. Verify @OperationsPerInvocation against a manual logical-QPS calculation and correct its Javadoc.
9. Run the relevant benchmark smoke tests.
10. Do not rewrite historical performance numbers yet.

Return:
- exact files changed;
- why each change is necessary;
- benchmark commands to run next;
- which old performance tables need re-running before they can be treated as controlled comparisons.

No unrelated driver optimization.
```

After that is merged, the second task should be the GitHub Actions benchmark artifact/report pipeline.

---

# 34. Bottom line

The driver is no longer at the “can it work?” stage.

The repository already has a substantial amount of correctness and operational engineering:

```text
real R2DBC TCK
real ClickHouse tests
bounded Reactor Netty transport
cancellation
timeouts
pool control
streaming insert
leak detection
explicit lifecycle
observability
benchmark isolation work
```

The next value is not another large pile of features.

The next value is:

```text
make benchmark evidence unquestionably fair
close the remaining public-API type/binding leaks
characterize hard HTTP streaming failure modes
make production configuration complete
make Spring usage less awkward
show the value through a genuinely streaming demo
```

The single most important new finding from this review is the **compression mismatch in comparative
benchmarks**. Fix that before treating the current large-result transport/scan performance numbers as
an apples-to-apples verdict.

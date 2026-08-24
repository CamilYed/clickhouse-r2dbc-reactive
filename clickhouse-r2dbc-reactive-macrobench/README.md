# clickhouse-r2dbc-reactive-macrobench

Phase 12 (see the root [ROADMAP.md](../ROADMAP.md#phase-12--spring-boot-end-to-end-macrobenchmark-planned-not-started)):
a real Spring Boot WebFlux request path for end-to-end comparison between this driver and
client-v2, complementing (not replacing) the JMH suite in `clickhouse-r2dbc-reactive-benchmarks`.

Depends on this repo's own `clickhouse-r2dbc-reactive-connector` source
(`project(":clickhouse-r2dbc-reactive-connector")`), not the published release - unlike
`examples/spring-boot-webflux-demo`, this module benchmarks the driver as it exists on the
current branch.

## What's here (PR1 - infrastructure only)

- `backend.BenchmarkQueryBackend` - one interface, two implementations: `r2dbc.R2dbcBenchmarkQueryBackend`
  (this driver, via a plain `DatabaseClient`, no outer `io.r2dbc.pool`) and
  `clientv2.ClientV2BenchmarkQueryBackend` (client-v2's `Client`, `useAsyncRequests(true)`).
- Three scenarios: `point` (primary-key lookup), `analytics` (real `JOIN`/`GROUP BY`/aggregation),
  `stream` (NDJSON row stream). A fourth scenario (`cancel` - abort in-flight requests, correlate
  against `system.processes`) is deliberately **not** included yet - see `BenchmarkQueryBackend`'s
  Javadoc for why.
- `config.ConditionalOnBackendEnabled` - gates each backend's beans on `benchmark.backend`
  (`r2dbc` / `client-v2` / `dual`), so an isolated single-backend run never creates the other
  backend's connections/threads at all.
- `dataset.DatasetSeeder` - drops and reseeds the tables both backends read from at startup,
  entirely server-side, never through either driver under comparison.
- `api.BenchmarkController` - `/benchmark/{backend}/point/{id}`, `/benchmark/{backend}/analytics`,
  `/benchmark/{backend}/stream?limit=N`.
- `.github/workflows/macro-benchmark.yml` - manual-dispatch smoke check (boots the app, seeds a
  small dataset, curls each scenario). **Not a load test** - no k6/wrk2, no paired-A/B rounds, no
  resource collector. That's PR2's job per ROADMAP.md's Phase 12 PR sequence.

## Running locally

```bash
# against a ClickHouse instance already listening on localhost:8123
./gradlew :clickhouse-r2dbc-reactive-macrobench:bootRun

curl http://localhost:8081/benchmark/r2dbc/point/1
curl http://localhost:8081/benchmark/client-v2/analytics
curl http://localhost:8081/benchmark/r2dbc/stream?limit=10
```

### Quick local comparison with `ab`

```bash
scripts/run-ab.sh r2dbc point
scripts/run-ab.sh client-v2 point
```

Runs a discarded warmup pass, then a measured [Apache Bench](https://httpd.apache.org/docs/2.4/programs/ab.html)
run against `/benchmark/{backend}/{scenario}` - see `run-ab.sh`'s own header for why it's a
**local iteration tool, not a trusted published number**: `ab` is closed-loop (hides tail latency
under real overload), unlike the open-loop `k6`/`wrk2` methodology ROADMAP.md's Phase 12 PR2 is
for.

To compare both backends across all three scenarios at once instead of reading six separate raw
`ab` reports, use `scripts/ab-summary.sh`, which runs `run-ab.sh` for every backend x scenario
combination and prints one table (RPS, mean/p50/p95/p99/max latency, failed requests):

```bash
scripts/ab-summary.sh              # "quick" profile: 2000 requests, concurrency 10
scripts/ab-summary.sh stress       # "stress" profile: 50000 requests, concurrency 200, warmup 5000
scripts/ab-summary.sh 20000 100 2000  # explicit requests/concurrency/warmup
KEEP_LOGS=1 scripts/ab-summary.sh  # keep each run's full ab output for inspection
```

`stress` is still `ab` (closed-loop, local-only - see `run-ab.sh`'s header), just heavier concurrency
and a proportionally longer warmup than the default smoke-test profile - useful for seeing
tail-latency behavior diverge under real concurrent load instead of an idle 10-connection sanity
check. If `ab` fails with `apr_socket_recv: ... Too many open files`, raise your shell's open-file
limit first: `ulimit -n 4096`.

Configuration (env vars, all optional - see `application.yml` for defaults):

| Variable | Purpose |
| --- | --- |
| `MACROBENCH_PORT` | HTTP port (default `8081`) |
| `MACROBENCH_BACKEND` | `r2dbc` \| `client-v2` \| `dual` (default `dual`) |
| `MACROBENCH_R2DBC_URL` | `r2dbc:clickhouse://...` URL for the `r2dbc` backend |
| `MACROBENCH_CLICKHOUSE_HTTP_URL` | bare `http://host:port` for the `client-v2` backend and dataset seeding |
| `MACROBENCH_CLICKHOUSE_USER` / `MACROBENCH_CLICKHOUSE_PASSWORD` | credentials for both |
| `MACROBENCH_POINT_ROWS` / `MACROBENCH_ANALYTICS_ROWS` | dataset size (default `100000` each - a local-smoke-test size, not the "trusted" ~5M-row sizing ROADMAP.md's Phase 12 describes) |
| `MACROBENCH_POOL_SIZE` | physical connection pool size both backends are pinned to (default `8`) - see `BenchmarkProperties`' Javadoc for why this exists and defaults to a fixed, equal value rather than each backend's own (different) built-in default |

Not published to Maven Central, not part of the driver's public API - see the root
`build.gradle.kts`'s `nonPublishedModules`.

## Local results (informal, not a trusted baseline)

A local `ab-summary.sh stress` run (2026-08-24, `MACROBENCH_POOL_SIZE` not yet implemented at run
time - see the caveat below), `MACROBENCH_POINT_ROWS`/`MACROBENCH_ANALYTICS_ROWS` left at the
default 100k, 50000 requests/concurrency 200/warmup 5000 against both backends:

| backend | scenario | rps | mean (ms) | p50 | p95 | p99 | max | failed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| r2dbc | point | 3516.26 | 56.88 | 55 | 90 | 108 | 202 | 0 |
| r2dbc | analytics | 1483.05 | 134.86 | 133 | 213 | 248 | 342 | 0 |
| r2dbc | stream | 2554.70 | 78.29 | 76 | 127 | 156 | 246 | 0 |
| client-v2 | point | 3266.19 | 61.23 | 60 | 75 | 86 | 143 | 0 |
| client-v2 | analytics | 1491.60 | 134.09 | 131 | 156 | 171 | 217 | 0 |
| client-v2 | stream | 2991.60 | 66.85 | 67 | 76 | 81 | 103 | 0 |

**Known confound in this specific run: pool sizes were not pinned.** At the time this ran, neither
backend had an explicit pool size configured, and their defaults are not equal: this driver fell
back to Reactor Netty's `max(availableProcessors, 8) * 2` formula (24 connections on the machine
this ran on, per its own `decoderWorkerCount=24` startup log - see
[docs/operations/connection-pooling.md](../docs/operations/connection-pooling.md)), while client-v2
defaulted to its own `ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS` of 10. So the table above
compares a 24-connection r2dbc pool against a 10-connection client-v2 pool, not the same physical
resource budget on both sides - r2dbc's tail latency at `analytics` (p99 248ms vs client-v2's
171ms) is plausibly explained by that alone, not by anything architectural. **Not reliable evidence
of either driver being faster or slower.**

This module now pins both backends to an equal, explicit pool size by default (`benchmark.pool-size`
/ `MACROBENCH_POOL_SIZE`, default `8` - see `BenchmarkProperties`' Javadoc) precisely because of this
finding. Rerunning `ab-summary.sh stress` on current `main` reproduces the same request pattern under
a matched pool instead. A genuinely trusted baseline (matched pool, real dataset sizing, k6/wrk2
open-loop load, dedicated CI machine) is still ROADMAP.md's Phase 12 PR2, not this table.

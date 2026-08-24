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

Configuration (env vars, all optional - see `application.yml` for defaults):

| Variable | Purpose |
| --- | --- |
| `MACROBENCH_PORT` | HTTP port (default `8081`) |
| `MACROBENCH_BACKEND` | `r2dbc` \| `client-v2` \| `dual` (default `dual`) |
| `MACROBENCH_R2DBC_URL` | `r2dbc:clickhouse://...` URL for the `r2dbc` backend |
| `MACROBENCH_CLICKHOUSE_HTTP_URL` | bare `http://host:port` for the `client-v2` backend and dataset seeding |
| `MACROBENCH_CLICKHOUSE_USER` / `MACROBENCH_CLICKHOUSE_PASSWORD` | credentials for both |
| `MACROBENCH_POINT_ROWS` / `MACROBENCH_ANALYTICS_ROWS` | dataset size (default `100000` each - a local-smoke-test size, not the "trusted" ~5M-row sizing ROADMAP.md's Phase 12 describes) |

Not published to Maven Central, not part of the driver's public API - see the root
`build.gradle.kts`'s `nonPublishedModules`.

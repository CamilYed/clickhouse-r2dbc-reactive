# Running the benchmarks yourself

The benchmark suite lives in `clickhouse-r2dbc-reactive-benchmarks` — JMH benchmarks comparing this
driver against client-v2, at multiple levels (see [methodology.md](methodology.md) for what each
class measures). This page is the source of truth for execution instructions; the module's
[README](../../clickhouse-r2dbc-reactive-benchmarks/README.md) intentionally remains a short entry
point.

Not published, not part of `./gradlew build`/`check`. Requires Docker and JDK 21. The dependency
versions used by a run come from the checked-out commit; current `main` uses client-v2 `0.11.2`.

## Running

Against a Testcontainers-managed server (default — one container per JMH fork):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh
```

Against a long-lived external server instead (faster iteration — no container startup per fork):

```
./scripts/start-benchmark-clickhouse.sh
export BENCH_CLICKHOUSE_URL=http://localhost:28123
export BENCH_CLICKHOUSE_USER=default
export BENCH_CLICKHOUSE_PASSWORD=

./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh
```

Results are written under `build/results/jmh/` (JSON format). Not committed to git — inspect
locally or upload as a CI artifact.

To run one benchmark class only:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=PointQueryBenchmark
```

To run with a JMH profiler (e.g. the GC profiler for allocation numbers):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=DecoderOnlyBenchmark -Pjmh.profilers=gc
```

To run multiple forks (needed before trusting a result — see [index.md's confidence
warning](index.md); the plain `jmh` task defaults to 1 fork/1 warmup iteration, a sanity check, not
a trustworthy number):

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.forks=3 -Pjmh.warmupIterations=3
```

Flags compose freely, e.g.:

```
./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh -Pjmh.includes=BoundedPoolConcurrencyBenchmark -Pjmh.forks=3 -Pjmh.warmupIterations=3
```

For a long unattended run (the full suite, or any 3-fork run), wrap with `caffeinate -d -i` on
macOS to prevent the machine sleeping mid-run.

## Cloud workflow

The repeatable comparison environment is `.github/workflows/benchmark.yml`. Start it from the
GitHub Actions UI or with the GitHub CLI:

```
gh workflow run benchmark.yml \
  -f profile=trusted-clean \
  -f benchmark=PublicApiMatchedPoolThroughputBenchmark
```

Use `fast` only as a build/sanity signal. Use `trusted` when GC/JFR diagnostics are the purpose of
the run. Use `trusted-clean` for a latency or throughput claim: three forks, five warmups, five
measurements, and no profiler or background sampler adding latency noise.

Both implementations run in the same job, on the same VM, against the same ClickHouse process.
The artifact contains the raw JMH JSON, stdout, environment metadata, and—where supported by the
selected benchmark—an analyzed summary. Record the workflow URL with every published result.

## Interpreting old results

The [historical results](results.md) include local exploratory runs and later cloud runs. Absolute
numbers are specific to the recorded machine, commit, JDK, ClickHouse image, client-v2 version,
and profile. In particular, those pages have not yet been re-baselined after the move from
client-v2 `0.9.8` to `0.11.2`; use them as investigation history, not as current release claims.

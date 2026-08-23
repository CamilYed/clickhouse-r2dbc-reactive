# Testing strategy

1. **Static execution-path analysis** — map exact classes/methods in `clickhouse-java`, locate
   blocking calls, `CompletableFuture` boundaries, `InputStream` usage, pool/queue defaults, and
   identify Client V2 components safe to reuse.
2. **Transport contract tests** against a controlled local server: immediate response, delayed
   headers, delayed body, fragmented metadata/rows, partial final record, slow subscriber, no
   response, connection reset, error after partial data, cancellation before/during acquire,
   cancellation while queued, cancellation during body receive, pool saturation, pending-acquire
   timeout.
3. **ClickHouse integration tests** against a real instance: `SELECT 1`, large result sets,
   metadata, nullable values, arrays, parameter binding, timeout, cancellation, active-query
   verification, parallel requests, slow subscriber, bounded-memory verification.
4. **R2DBC tests**: provider discovery, URL parsing, connection creation, deferred execution,
   statement binding, result consumption, row access, metadata, error mapping, unsupported
   transaction operations, repeat-subscription behaviour, cleanup after cancellation.
5. **Performance and dependency impact**: throughput, p50/p95/p99 latency, time to first row,
   allocations/retained memory, cancellation latency, many-small-request workloads, large
   streaming-result workloads, dependency size and startup impact. See
   [../performance/index.md](../performance/index.md) for the actual measurements.

See also [CLAUDE.md's Test types and tools](../../CLAUDE.md#test-types-and-tools) for the concrete
tooling (JUnit 5, AssertJ, Testcontainers, no Mockito) and where each level of test lives.

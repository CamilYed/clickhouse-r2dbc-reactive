package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 * {@link PointQueryClient} through client-v2's public async {@link Client} API, adapted into
 * Reactor exactly once here rather than inside the benchmark class itself — see {@code
 * CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md} section 7. Pool-sized identically to {@link
 * OurDriverPointQueryClient} via {@code enableConnectionPool}/{@code setMaxConnections}, the same
 * configuration {@code BoundedPoolConcurrencyBenchmark} (in this module's {@code src/jmh/java})
 * already uses for a matched-pool comparison.
 *
 * <p><b>{@code useAsyncRequests(true)} is not optional here.</b> client-v2's {@code
 * ClientConfigProperties.ASYNC_OPERATIONS} defaults to {@code false} — with that default, {@code
 * Client#query(...)} runs the entire blocking HTTP round trip synchronously on the calling thread
 * and only wraps the already-finished result in {@code CompletableFuture.completedFuture(...)}
 * afterward. Left at the default, every {@link #query(long)} call in this benchmark would execute
 * serially on whatever thread drives {@code Flux.flatMap(..., concurrency)}, regardless of the
 * {@code concurrency}/{@code poolSize} parameters — confirmed against a real cloud run
 * (2026-08-23): client-v2's measured throughput and per-query latency were both flat across
 * concurrency=8/32/128, and flat-throughput × flat-latency multiplied out to almost exactly one
 * sequential worker, not eight. With {@code useAsyncRequests(true)}, client-v2 dispatches each
 * query onto its own {@code Executors.newCachedThreadPool()} (unbounded thread count, but the real
 * concurrency ceiling is still {@code setMaxConnections(poolSize)} — the same physical-connection
 * budget both sides are compared under, per this pipeline's non-negotiable constraint).
 *
 * <p>Three constructors, three distinct scenarios: {@link #ClientV2PointQueryClient(int)} matches
 * client-v2 to an explicit pool size for a fair matched-pool comparison; {@link
 * #ClientV2PointQueryClient(double)} instead leaves client-v2 at its own default pool (10, see
 * {@code ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS}) and slows every query down via {@code
 * sleep(...)} — see {@link DefaultPoolSlowQueryThroughputBenchmark}'s Javadoc for why that second
 * scenario exists; {@link #ClientV2PointQueryClient(FixedExecutorPoolSize)} matches an explicit
 * pool size like the first constructor, but additionally swaps client-v2's own default {@code
 * Executors.newCachedThreadPool()} (see this class's own Javadoc above for why async dispatch runs
 * on one at all) for a {@code Executors.newFixedThreadPool(poolSize)} sized to match the connection
 * pool — see Phase 11 PR3 in ROADMAP.md: isolates how much of client-v2's throughput/latency edge
 * over this driver is the cached executor's willingness to spin up as many threads as concurrent
 * requests demand, versus something architectural. All three share the same query/close logic
 * below, only the SQL text, pool configuration, and (for the third) executor differ.
 */
final class ClientV2PointQueryClient implements PointQueryClient {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private final Client client;
  private final String selectSql;

  /**
   * A distinct parameter type (rather than a boolean flag on {@link
   * #ClientV2PointQueryClient(int)}) so a call site self-documents "fixed executor, sized to this
   * pool" instead of an opaque {@code true}/{@code false} — see CLAUDE.md's "no boolean/flag
   * parameters that silently change behavior".
   */
  record FixedExecutorPoolSize(int poolSize) {}

  /**
   * Non-null only when this instance owns a fixed executor it must shut down in {@link #close()}.
   */
  private final @Nullable ExecutorService ownedFixedExecutor;

  /**
   * Builds a client-v2 {@link Client} with a physical connection pool sized to {@code poolSize},
   * async request dispatch enabled (see the class Javadoc for why this is required for a fair
   * comparison), executor left at client-v2's own default (a cached thread pool it creates and
   * shuts down itself).
   */
  ClientV2PointQueryClient(final int poolSize) {
    this.selectSql = SELECT_BY_ID_SQL;
    this.client = baseBuilder().enableConnectionPool(true).setMaxConnections(poolSize).build();
    this.ownedFixedExecutor = null;
  }

  /**
   * Builds a client-v2 {@link Client} left at its own default connection pool size (10, pooling
   * already enabled by default), async request dispatch enabled. Every query additionally selects
   * {@code sleep(sleepSeconds)} (ignored in the mapped result) to give the physical pool something
   * to actually queue behind — see {@link DefaultPoolSlowQueryThroughputBenchmark}'s Javadoc.
   */
  ClientV2PointQueryClient(final double sleepSeconds) {
    this.selectSql =
        "SELECT label, amount, sleep("
            + sleepSeconds
            + ") FROM "
            + PointQueryTable.NAME
            + " WHERE id = {id:UInt64}";
    this.client = baseBuilder().build();
    this.ownedFixedExecutor = null;
  }

  /**
   * Builds a client-v2 {@link Client} with a physical connection pool sized to {@code
   * fixedExecutorPoolSize.poolSize()}, async dispatch running on a {@code
   * Executors.newFixedThreadPool(poolSize)} instead of client-v2's own default cached pool — see
   * this class's own Javadoc for why. Unlike the default executor, a caller-supplied one is never
   * closed by {@code Client#close()} (per client-v2's own {@code setSharedOperationExecutor}
   * Javadoc: "it is application responsibility to close the executor"), so this instance owns and
   * shuts it down itself in {@link #close()}.
   */
  ClientV2PointQueryClient(final FixedExecutorPoolSize fixedExecutorPoolSize) {
    this.selectSql = SELECT_BY_ID_SQL;
    final int poolSize = fixedExecutorPoolSize.poolSize();
    this.ownedFixedExecutor = Executors.newFixedThreadPool(poolSize);
    this.client =
        baseBuilder()
            .enableConnectionPool(true)
            .setMaxConnections(poolSize)
            .setSharedOperationExecutor(ownedFixedExecutor)
            .build();
  }

  private static Client.Builder baseBuilder() {
    return new Client.Builder()
        .addEndpoint(BenchmarkEnvironment.httpUrl())
        .setUsername(BenchmarkEnvironment.username())
        .setPassword(BenchmarkEnvironment.password())
        .setDefaultDatabase("default")
        .useAsyncRequests(true);
  }

  @Override
  public Mono<PointResult> query(final long id) {
    return Mono.fromFuture(
        client
            .query(selectSql, Map.of("id", id))
            .thenApply(response -> mapSingleRow(id, response)));
  }

  private PointResult mapSingleRow(final long id, final QueryResponse response) {
    try (QueryResponse closeable = response;
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(closeable)) {
      if (reader.next() == null) {
        throw new IllegalStateException("Expected exactly one row for id=" + id + " but got none");
      }
      return new PointResult(reader.getString(1), reader.getBigDecimal(2));
    } catch (final Exception e) {
      // Function<QueryResponse, PointResult> (thenApply's functional interface) declares no checked
      // exceptions, but both QueryResponse#close() and ClickHouseBinaryFormatReader#close() declare
      // `throws Exception` (try-with-resources' implicit close calls) - rethrown unchecked so the
      // CompletableFuture surfaces the real failure instead of the caller only ever seeing a
      // generic checked-exception wrapper.
      throw new CompletionException(e);
    }
  }

  @Override
  public void close() {
    client.close();
    if (ownedFixedExecutor != null) {
      ownedFixedExecutor.shutdownNow();
    }
  }
}

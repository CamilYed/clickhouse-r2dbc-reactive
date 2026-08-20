package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import java.util.Map;
import java.util.concurrent.CompletionException;
import reactor.core.publisher.Mono;

/**
 * {@link PointQueryClient} through client-v2's public async {@link Client} API, adapted into
 * Reactor exactly once here rather than inside the benchmark class itself — see {@code
 * CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md} section 7. Pool-sized identically to {@link
 * OurDriverPointQueryClient} via {@code enableConnectionPool}/{@code setMaxConnections}, the same
 * configuration {@code BoundedPoolConcurrencyBenchmark} (in this module's {@code src/jmh/java})
 * already uses for a matched-pool comparison.
 */
final class ClientV2PointQueryClient implements PointQueryClient {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private final Client client;

  /**
   * Builds a client-v2 {@link Client} with a physical connection pool sized to {@code poolSize}.
   */
  ClientV2PointQueryClient(final int poolSize) {
    this.client =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .enableConnectionPool(true)
            .setMaxConnections(poolSize)
            .build();
  }

  @Override
  public Mono<PointResult> query(final long id) {
    return Mono.fromFuture(
        client
            .query(SELECT_BY_ID_SQL, Map.of("id", id))
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
  }
}

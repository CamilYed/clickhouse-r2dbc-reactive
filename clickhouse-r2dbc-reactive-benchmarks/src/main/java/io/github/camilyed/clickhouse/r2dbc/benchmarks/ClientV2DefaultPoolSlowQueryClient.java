package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import java.util.Map;
import java.util.concurrent.CompletionException;
import reactor.core.publisher.Mono;

/**
 * {@link PointQueryClient} through client-v2's public async {@link Client} API, deliberately
 * <b>not</b> given an explicit {@code setMaxConnections} — left at client-v2's own default of 10
 * (see {@code ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS}). See {@link
 * DefaultPoolSlowQueryThroughputBenchmark}'s Javadoc for why an unmatched, out-of-the-box pool is
 * the point here, mirroring {@link OurDriverDefaultPoolSlowQueryClient} on this driver's side.
 *
 * <p>{@code useAsyncRequests(true)} is still required, for the same reason it is everywhere else in
 * this module (see {@link ClientV2PointQueryClient}'s Javadoc) — without it, {@code Client#query}
 * runs synchronously on the calling thread regardless of pool size, which would make this benchmark
 * measure one sequential worker instead of client-v2's real default-pool concurrency.
 */
final class ClientV2DefaultPoolSlowQueryClient implements PointQueryClient {

  private final Client client;
  private final String selectSql;

  /**
   * Builds a client-v2 {@link Client} left at its own default connection pool size (10, pooling
   * already enabled by default), async request dispatch enabled. Every query additionally selects
   * {@code sleep(sleepSeconds)} (ignored in the mapped result) — see the owning benchmark's Javadoc
   * for why.
   */
  ClientV2DefaultPoolSlowQueryClient(final double sleepSeconds) {
    this.selectSql =
        "SELECT label, amount, sleep("
            + sleepSeconds
            + ") FROM "
            + PointQueryTable.NAME
            + " WHERE id = {id:UInt64}";
    this.client =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .useAsyncRequests(true)
            .build();
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
  }
}

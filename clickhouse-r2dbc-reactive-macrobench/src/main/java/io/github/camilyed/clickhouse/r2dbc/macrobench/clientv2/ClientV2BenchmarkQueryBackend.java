package io.github.camilyed.clickhouse.r2dbc.macrobench.clientv2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.BenchmarkQueryBackend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.PointRow;
import io.github.camilyed.clickhouse.r2dbc.macrobench.dataset.BenchmarkDataset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link BenchmarkQueryBackend} through client-v2's public async {@link Client} API, adapted into
 * Reactor exactly once here - same pattern {@code clickhouse-r2dbc-reactive-benchmarks}' {@code
 * ClientV2PointQueryClient} already proved. {@code useAsyncRequests(true)} (set on the {@link
 * Client} bean, see {@link ClientV2BackendConfiguration}) is required for the same reason
 * documented there: left at client-v2's default, {@link Client#query} runs the blocking HTTP round
 * trip synchronously on the calling thread, which under Reactor Netty's event loop would starve
 * every other in-flight WebFlux request on that thread.
 */
final class ClientV2BenchmarkQueryBackend implements BenchmarkQueryBackend {

  private static final String SELECT_POINT_SQL =
      "SELECT id, label, amount FROM " + BenchmarkDataset.POINT_TABLE + " WHERE id = {id:UInt64}";

  private final Client client;

  ClientV2BenchmarkQueryBackend(final Client client) {
    this.client = client;
  }

  @Override
  public Backend kind() {
    return Backend.CLIENT_V2;
  }

  @Override
  public Mono<PointRow> point(final long id) {
    return Mono.fromFuture(
        client.query(SELECT_POINT_SQL, Map.of("id", id)).thenApply(this::mapSinglePointRow));
  }

  @Override
  public Mono<List<CategoryTotal>> analytics() {
    return Mono.fromFuture(
        client.query(BenchmarkDataset.ANALYTICS_SQL).thenApply(this::mapCategoryTotals));
  }

  @Override
  public Flux<PointRow> stream(final long limit) {
    final String sql =
        "SELECT id, label, amount FROM "
            + BenchmarkDataset.POINT_TABLE
            + " ORDER BY id LIMIT "
            + limit;
    return Mono.fromFuture(client.query(sql)).flatMapMany(this::mapPointRows);
  }

  private PointRow mapSinglePointRow(final QueryResponse response) {
    try (QueryResponse closeable = response;
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(closeable)) {
      if (reader.next() == null) {
        throw new IllegalStateException("Expected exactly one row, got none");
      }
      return toPointRow(reader);
    } catch (final Exception e) {
      throw new CompletionException(e);
    }
  }

  private Flux<PointRow> mapPointRows(final QueryResponse response) {
    final List<PointRow> rows = new ArrayList<>();
    try (QueryResponse closeable = response;
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(closeable)) {
      while (reader.next() != null) {
        rows.add(toPointRow(reader));
      }
    } catch (final Exception e) {
      throw new CompletionException(e);
    }
    return Flux.fromIterable(rows);
  }

  private List<CategoryTotal> mapCategoryTotals(final QueryResponse response) {
    final List<CategoryTotal> rows = new ArrayList<>();
    try (QueryResponse closeable = response;
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(closeable)) {
      while (reader.next() != null) {
        rows.add(
            new CategoryTotal(
                reader.getString("category"),
                reader.getLong("order_count"),
                reader.getBigDecimal("total_amount")));
      }
    } catch (final Exception e) {
      throw new CompletionException(e);
    }
    return rows;
  }

  private PointRow toPointRow(final ClickHouseBinaryFormatReader reader) {
    return new PointRow(
        reader.getLong("id"), reader.getString("label"), reader.getBigDecimal("amount"));
  }
}

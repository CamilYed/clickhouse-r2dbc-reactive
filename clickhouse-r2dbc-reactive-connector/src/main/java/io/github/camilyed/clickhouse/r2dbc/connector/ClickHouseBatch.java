package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * A collection of standalone SQL statements executed together, created via {@link
 * ClickHouseConnection#createBatch()}.
 *
 * <p>Unlike {@link ClickHouseStatement}, a batched statement carries no bound parameters — {@link
 * #add(String)} takes a complete, literal SQL string, run exactly as given. {@link #execute()} runs
 * every added statement sequentially, one full round trip per statement in the order {@link
 * #add(String)} was called, and emits one {@link Result} per statement in that same order — later
 * statements are not started until the previous one's request has been sent, which matters for a
 * batch like {@code CREATE TABLE ...} followed by {@code INSERT INTO ...} against it.
 *
 * <p>Any failure obtaining a {@link Result} for one of the statements is mapped onto {@link
 * io.r2dbc.spi.R2dbcException} via {@link ClickHouseR2dbcException} ({@code
 * ClickHouseR2dbcException.wrap}), same as {@link ClickHouseStatement}.
 */
final class ClickHouseBatch implements Batch {

  private final ClickHouseHttpTransport transport;
  private final List<String> statements = new ArrayList<>();

  ClickHouseBatch(final ClickHouseHttpTransport transport) {
    this.transport = transport;
  }

  @Override
  public Batch add(final String sql) {
    if (sql == null) {
      throw new IllegalArgumentException("sql must not be null");
    }
    statements.add(sql);
    return this;
  }

  @Override
  public Publisher<? extends Result> execute() {
    return Flux.fromIterable(statements).concatMap(this::executeOne);
  }

  private Publisher<ClickHouseResult> executeOne(final String sql) {
    final Flux<ByteBuffer> body =
        transport.query(ClickHouseQuery.of(sql)).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decode(body)
        .map(ClickHouseResult::new)
        .onErrorMap(ClickHouseR2dbcException::wrap);
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * A ClickHouse SQL statement, created via {@link ClickHouseConnection#createStatement(String)}.
 *
 * <p>{@link #execute()} runs {@code sql} unmodified through {@link ClickHouseHttpTransport} and
 * decodes the {@code RowBinaryWithNamesAndTypes} response via {@link RowBinaryDecoder#decode},
 * exposing exactly one {@link Result} per call. Parameter binding ({@link #bind}/{@link
 * #bindNull}/{@link #add}) is separately scoped future work (needs a {@code param_<name>}
 * bind-parameter design, not yet decided) and is left unimplemented here for the same reason:
 * nothing in this class silently pretends to support what it doesn't.
 */
final class ClickHouseStatement implements Statement {

  private final ClickHouseHttpTransport transport;
  private final String sql;

  ClickHouseStatement(final ClickHouseHttpTransport transport, final String sql) {
    this.transport = transport;
    this.sql = sql;
  }

  @Override
  public Statement add() {
    throw new UnsupportedOperationException("Batched bindings are not supported yet");
  }

  @Override
  public Statement bind(final int index, final Object value) {
    throw new UnsupportedOperationException("Parameter binding is not supported yet");
  }

  @Override
  public Statement bind(final String name, final Object value) {
    throw new UnsupportedOperationException("Parameter binding is not supported yet");
  }

  @Override
  public Statement bindNull(final int index, final Class<?> type) {
    throw new UnsupportedOperationException("Parameter binding is not supported yet");
  }

  @Override
  public Statement bindNull(final String name, final Class<?> type) {
    throw new UnsupportedOperationException("Parameter binding is not supported yet");
  }

  @Override
  public Publisher<? extends Result> execute() {
    final Flux<ByteBuffer> body =
        transport.query(ClickHouseQuery.of(sql)).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decode(body).map(ClickHouseResult::new);
  }
}

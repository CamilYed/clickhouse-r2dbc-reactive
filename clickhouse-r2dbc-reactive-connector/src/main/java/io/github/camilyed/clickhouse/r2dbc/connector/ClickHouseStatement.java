package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * A ClickHouse SQL statement, created via {@link ClickHouseConnection#createStatement(String)}.
 *
 * <p>{@link #execute()} runs {@code sql} through {@link ClickHouseHttpTransport} (with any bound
 * parameters attached, see below) and decodes the {@code RowBinaryWithNamesAndTypes} response via
 * {@link RowBinaryDecoder#decode}, exposing exactly one {@link Result} per call.
 *
 * <p>Parameter binding maps directly onto ClickHouse's own named parameterized-query mechanism
 * (checked against clickhouse.com/docs/interfaces/http, not assumed): {@code sql} declares each
 * placeholder as {@code {name:Type}}, and {@link #bind(String, Object)}/{@link #bindNull(String,
 * Class)} bind a value to one of those declared names — {@link ClickHouseQuery#parameterNamesIn}
 * parses {@code sql} once, at construction, to know which names are valid to bind, so binding an
 * undeclared name fails fast with {@link NoSuchElementException} rather than silently being
 * ignored or sent as a stray, unused request parameter. {@link #bind(int, Object)}/{@link
 * #bindNull(int, Class)} map {@code index} to the declared name at that position, in first-
 * occurrence order — ClickHouse's own placeholder syntax has no positional form of its own, so
 * this index-to-name mapping is this driver's own convention, not something ClickHouse defines.
 * {@link #add()} (batched bindings, i.e. one {@code Statement} executed once per saved binding
 * set) is separately scoped future work and still throws {@link UnsupportedOperationException}.
 */
final class ClickHouseStatement implements Statement {

  private final ClickHouseHttpTransport transport;
  private final String sql;
  private final List<String> parameterNames;
  private final Map<String, Object> boundValues = new LinkedHashMap<>();

  ClickHouseStatement(final ClickHouseHttpTransport transport, final String sql) {
    this.transport = transport;
    this.sql = sql;
    this.parameterNames = ClickHouseQuery.parameterNamesIn(sql);
  }

  @Override
  public Statement add() {
    throw new UnsupportedOperationException("Batched bindings are not supported yet");
  }

  @Override
  public Statement bind(final int index, final Object value) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    return bind(nameAt(index), value);
  }

  @Override
  public Statement bind(final String name, final Object value) {
    if (name == null || value == null) {
      throw new IllegalArgumentException("name and value must not be null");
    }
    requireDeclaredParameter(name);
    boundValues.put(name, value);
    return this;
  }

  @Override
  public Statement bindNull(final int index, final Class<?> type) {
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    return bindNull(nameAt(index), type);
  }

  @Override
  public Statement bindNull(final String name, final Class<?> type) {
    if (name == null || type == null) {
      throw new IllegalArgumentException("name and type must not be null");
    }
    requireDeclaredParameter(name);
    boundValues.put(name, null);
    return this;
  }

  @Override
  public Publisher<? extends Result> execute() {
    if (boundValues.size() < parameterNames.size()) {
      throw new IllegalStateException(
          "Not all declared parameters have been bound: declared "
              + parameterNames
              + ", bound "
              + boundValues.keySet());
    }
    final ClickHouseQuery query = ClickHouseQuery.of(sql).withParameters(boundValues);
    final Flux<ByteBuffer> body = transport.query(query).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decode(body).map(ClickHouseResult::new);
  }

  private String nameAt(final int index) {
    if (index < 0 || index >= parameterNames.size()) {
      throw new IndexOutOfBoundsException(
          "No declared parameter at index " + index + " (declared: " + parameterNames + ")");
    }
    return parameterNames.get(index);
  }

  private void requireDeclaredParameter(final String name) {
    if (!parameterNames.contains(name)) {
      throw new NoSuchElementException(
          "'" + name + "' is not a parameter declared in this statement's SQL: " + parameterNames);
    }
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.reactivestreams.Publisher;

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
 * undeclared name fails fast with {@link NoSuchElementException} rather than silently being ignored
 * or sent as a stray, unused request parameter. {@link #bind(int, Object)}/{@link #bindNull(int,
 * Class)} map {@code index} to the declared name at that position, in first- occurrence order —
 * ClickHouse's own placeholder syntax has no positional form of its own, so this index-to-name
 * mapping is this driver's own convention, not something ClickHouse defines. {@link #add()}
 * (batched bindings, i.e. one {@code Statement} executed once per saved binding set) is separately
 * scoped future work and still throws {@link UnsupportedOperationException}.
 *
 * <p>Any failure obtaining a {@link Result} — a ClickHouse server error, a transport failure, a
 * local decode bug — is mapped onto {@link io.r2dbc.spi.R2dbcException} via {@link
 * ClickHouseR2dbcException} ({@code ClickHouseR2dbcException.wrap}), so standard R2DBC error
 * handling around {@link #execute()} catches it.
 *
 * <p><b>Not thread-safe</b>, deliberately, same as {@code java.sql.PreparedStatement} and every
 * other R2DBC driver's statement type: {@code bind}/{@code bindNull} mutate this instance's binding
 * state with no synchronization. The expected usage is one logical sequence — bind everything, then
 * call {@link #execute()} once — even though the {@link Result} that sequence eventually produces
 * is consumed asynchronously and may hop threads. Binding concurrently from multiple threads, or
 * mutating bindings while {@link #execute()} is reading them, is undefined behavior this class does
 * not guard against.
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

  // value is declared non-null under this module's @NullMarked contract, but this overrides a
  // plain io.r2dbc.spi.Statement method - external callers of the public R2DBC SPI aren't bound
  // by that static guarantee, so failing fast here beats a confusing NPE deeper in the call chain.
  // @SuppressWarnings("java:S2583") is not honored by SonarCloud's current analyzer for this rule
  // (confirmed: the issue re-appears with it present) - NOSONAR is the mechanism that actually
  // suppresses it.
  @Override
  public Statement bind(final int index, final Object value) {
    if (value == null) { // NOSONAR - see class-level defensive-null-check note above
      throw new IllegalArgumentException("value must not be null");
    }
    return bind(nameAt(index), value);
  }

  // See bind(int, Object) above for why this defensive check is kept despite @NullMarked.
  @Override
  public Statement bind(final String name, final Object value) {
    if (name == null || value == null) { // NOSONAR - see bind(int, Object) above
      throw new IllegalArgumentException("name and value must not be null");
    }
    requireDeclaredParameter(name);
    boundValues.put(name, value);
    return this;
  }

  // See bind(int, Object) above for why this defensive check is kept despite @NullMarked.
  @Override
  public Statement bindNull(final int index, final Class<?> type) {
    if (type == null) { // NOSONAR - see bind(int, Object) above
      throw new IllegalArgumentException("type must not be null");
    }
    return bindNull(nameAt(index), type);
  }

  // See bind(int, Object) above for why this defensive check is kept despite @NullMarked.
  @Override
  public Statement bindNull(final String name, final Class<?> type) {
    if (name == null || type == null) { // NOSONAR - see bind(int, Object) above
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
    return transport
        .queryWithSummary(query)
        .flatMap(ClickHouseResult::decode)
        .onErrorMap(ClickHouseR2dbcException::wrap);
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

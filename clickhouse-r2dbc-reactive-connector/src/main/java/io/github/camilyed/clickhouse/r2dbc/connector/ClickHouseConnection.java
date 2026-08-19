package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A logical connection to ClickHouse over its HTTP interface.
 *
 * <p>Checked directly against ClickHouse's own "Transactional (ACID) support" docs
 * (clickhouse.com/docs/guides/developer/transactional), not assumed: outside of ClickHouse's own
 * {@code BEGIN TRANSACTION}/{@code COMMIT}/{@code ROLLBACK} feature, every statement is its own
 * atomic unit and the client sees <b>read uncommitted</b> isolation — snapshot isolation only
 * applies to clients that are themselves inside one of those transactions. That transaction feature
 * is explicitly <b>experimental</b>, requires ClickHouse Keeper/ZooKeeper, only works with the
 * (default) Atomic database engine and non-replicated MergeTree tables, is off by default (needs
 * {@code allow_experimental_transactions=1} server-side), and — as far as this driver has verified
 * — its documented examples all go through {@code clickhouse client} / the native TCP protocol, not
 * the stateless HTTP interface this driver uses; whether {@code BEGIN}/{@code COMMIT} even work
 * correctly over HTTP without a sticky, connection-affine session is an open question this driver
 * has not tried to answer. Given all of that, this class does not implement transactions: every
 * transaction/savepoint-related method either fails clearly with {@link
 * UnsupportedOperationException} or, where the R2DBC spec explicitly allows it, no-ops — see each
 * method's Javadoc for which. A caller that assumes real transactional guarantees here would be
 * wrong to, and this class fails loudly rather than silently pretending otherwise.
 *
 * <p>{@link #close()} marks this logical connection closed; it does not tear down {@code transport}
 * itself, since the same transport (and its underlying connection pool) is shared by every {@link
 * Connection} a {@link ClickHouseConnectionFactory} produces. It does, however, make this instance
 * unusable afterward: {@link #createStatement} and {@link #createBatch} both throw {@link
 * IllegalStateException} once closed, and {@link #validate} reports {@code false} — a caller (or a
 * connection pool) that closes this connection and then accidentally keeps using it fails loudly
 * instead of silently sending real queries through a "closed" connection.
 */
public final class ClickHouseConnection implements Connection {

  private static final String TRANSACTIONS_NOT_SUPPORTED =
      "ClickHouse does not support transactions";

  private final ClickHouseHttpTransport transport;
  private final RowDecodingScheduler decodingScheduler;
  private final DriverObservationListener observationListener;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private @Nullable Duration statementTimeout;

  /**
   * {@code decodingScheduler} is shared, unchanged, with every {@link Statement}/{@link Batch} this
   * connection creates — owned by, and disposed together with, the {@code
   * ClickHouseConnectionFactory} that created this connection, exactly like {@code transport} and
   * its underlying connection pool already are (see this class's own Javadoc). See {@link
   * RowDecodingScheduler}'s Javadoc for the full ownership contract.
   */
  ClickHouseConnection(
      final ClickHouseHttpTransport transport, final RowDecodingScheduler decodingScheduler) {
    this(transport, decodingScheduler, DriverObservationListener.NOOP);
  }

  /**
   * {@code observationListener} is shared, unchanged, with every {@link Statement}/{@link Batch}
   * this connection creates, and used directly by {@link #insertStreaming} — see {@link
   * DriverObservationListener}'s Javadoc for the full contract.
   */
  ClickHouseConnection(
      final ClickHouseHttpTransport transport,
      final RowDecodingScheduler decodingScheduler,
      final DriverObservationListener observationListener) {
    this.transport = transport;
    this.decodingScheduler = decodingScheduler;
    this.observationListener = observationListener;
  }

  @Override
  public Publisher<Void> beginTransaction() {
    return Mono.error(new UnsupportedOperationException(TRANSACTIONS_NOT_SUPPORTED));
  }

  @Override
  public Publisher<Void> beginTransaction(final TransactionDefinition definition) {
    return Mono.error(new UnsupportedOperationException(TRANSACTIONS_NOT_SUPPORTED));
  }

  @Override
  public Publisher<Void> close() {
    return Mono.fromRunnable(() -> closed.set(true));
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return Mono.error(new UnsupportedOperationException(TRANSACTIONS_NOT_SUPPORTED));
  }

  @Override
  public Batch createBatch() {
    requireOpen();
    return new ClickHouseBatch(transport, decodingScheduler, observationListener);
  }

  @Override
  public Publisher<Void> createSavepoint(final String name) {
    return Mono.error(new UnsupportedOperationException("ClickHouse does not support savepoints"));
  }

  // sql is declared non-null under this module's @NullMarked contract, but this overrides a
  // plain io.r2dbc.spi.Connection method — external callers of the public R2DBC SPI aren't bound
  // by that static guarantee, so failing fast here beats a confusing NPE deeper in the call chain.
  @Override
  public Statement createStatement(final String sql) {
    if (sql == null) { // NOSONAR - see defensive-null-check note above
      throw new IllegalArgumentException("sql must not be null");
    }
    requireOpen();
    return new ClickHouseStatement(
        transport, sql, statementTimeout, decodingScheduler, observationListener);
  }

  @Override
  public boolean isAutoCommit() {
    return true;
  }

  @Override
  public ConnectionMetadata getMetadata() {
    return ClickHouseConnectionMetadata.INSTANCE;
  }

  /**
   * Always {@link IsolationLevel#READ_UNCOMMITTED} — ClickHouse's own docs state plainly that
   * clients outside of an explicit transaction (which this connection never starts, see the class
   * Javadoc) have read uncommitted isolation; snapshot isolation is only for clients inside one.
   */
  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    return IsolationLevel.READ_UNCOMMITTED;
  }

  @Override
  public Publisher<Void> releaseSavepoint(final String name) {
    // The R2DBC spec explicitly allows this: "Calling this for drivers not supporting
    // savepoint release results in a no-op."
    return Mono.empty();
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return Mono.error(new UnsupportedOperationException(TRANSACTIONS_NOT_SUPPORTED));
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(final String name) {
    return Mono.error(new UnsupportedOperationException("ClickHouse does not support savepoints"));
  }

  @Override
  public Publisher<Void> setAutoCommit(final boolean autoCommit) {
    if (!autoCommit) {
      return Mono.error(
          new UnsupportedOperationException("ClickHouse connections are always auto-commit"));
    }
    return Mono.empty();
  }

  @Override
  public Publisher<Void> setLockWaitTimeout(final Duration timeout) {
    return Mono.error(new UnsupportedOperationException("Lock wait timeout is not supported yet"));
  }

  /**
   * Configures a server-side execution time limit — ClickHouse's own {@code max_execution_time}
   * setting — inherited by every {@link Statement} this connection creates <em>after</em> this call
   * (a snapshot at {@link #createStatement} time, not a live link back to this connection; a
   * statement already created keeps whatever limit was in effect when it was created). Distinct
   * from transport-level {@code responseTimeout} (how long to wait for HTTP response bytes) — this
   * bounds how long ClickHouse itself is willing to keep running the query server-side, regardless
   * of how promptly the client reads the response.
   *
   * <p>{@link Duration#ZERO} is an explicit, documented "no timeout" — matching {@code
   * max_execution_time}'s own native meaning of {@code 0} (unlimited), not an accidental "time out
   * immediately". A negative {@code timeout} is rejected with {@link IllegalArgumentException},
   * since no negative duration has a sensible meaning here.
   */
  @Override
  public Publisher<Void> setStatementTimeout(final Duration timeout) {
    if (timeout.isNegative()) {
      return Mono.error(
          new IllegalArgumentException("timeout must not be negative, got: " + timeout));
    }
    return Mono.fromRunnable(() -> statementTimeout = timeout);
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(final IsolationLevel isolationLevel) {
    return Mono.error(
        new UnsupportedOperationException(
            "ClickHouse does not support configurable isolation levels"));
  }

  @Override
  public Publisher<Boolean> validate(final ValidationDepth depth) {
    if (closed.get()) {
      return Mono.just(false);
    }
    if (depth == ValidationDepth.LOCAL) {
      return Mono.just(true);
    }
    return transport
        .query(ClickHouseQuery.of("SELECT 1"))
        .aggregate()
        .asByteArray()
        .thenReturn(true)
        .onErrorReturn(false);
  }

  /**
   * ClickHouse-specific vendor extension: sends {@code sql} (an {@code INSERT}) with {@code data}
   * streamed as the HTTP request body instead of embedded in the URL — see {@code
   * ClickHouseHttpTransport#insertWithSummary} for the full reasoning, including why this path is
   * never retried regardless of how this connection's {@code RetryPolicy} is configured. Not part
   * of the standard R2DBC SPI: {@link Statement}/{@link Batch} have no concept of a streamed
   * request body, so this is exposed directly on the connection rather than wrapped in either of
   * those.
   *
   * <p>{@code sql} is expected to be a complete {@code INSERT} statement, including its input
   * {@code FORMAT} clause (e.g. {@code "INSERT INTO t FORMAT TabSeparated"}); {@code data} is the
   * already-encoded payload in that format, streamed as-is with no transformation. The returned
   * {@link Result} carries only {@link Result#getRowsUpdated()} — ClickHouse's HTTP interface sends
   * back no row data for a plain {@code INSERT}, so {@link Result#map}/{@link Result#flatMap} on it
   * never emit anything (see {@link ClickHouseResult#forInsert}). Any failure, including one that
   * happens mid-stream, is mapped onto {@link io.r2dbc.spi.R2dbcException} via {@link
   * ClickHouseR2dbcException#wrap}, same as {@link ClickHouseStatement#execute()}.
   *
   * <p>Reports one {@link OperationKind#INSERT} attempt to this connection's {@link
   * DriverObservationListener} — {@code byteCount} is the number of bytes actually read off {@code
   * data} before the request completed (not necessarily every byte {@code data} would ever have
   * emitted, if the request fails partway through); {@code rowCount} is {@code
   * response.writtenRows()}; {@code timeToFirstRow} is always {@link Duration#ZERO} (see {@link
   * io.github.camilyed.clickhouse.r2dbc.core.QueryCompletedEvent}'s Javadoc for why).
   */
  // sql/data are declared non-null under this module's @NullMarked contract, but this is a
  // public entry point external callers reach without JSpecify tooling of their own - failing
  // fast here beats a confusing NPE deeper in the call chain.
  public Publisher<Result> insertStreaming(final String sql, final Publisher<ByteBuffer> data) {
    if (sql == null) { // NOSONAR - see defensive-null-check note above
      throw new IllegalArgumentException("sql must not be null");
    }
    if (data == null) { // NOSONAR - see defensive-null-check note above
      throw new IllegalArgumentException("data must not be null");
    }
    requireOpen();
    final ClickHouseQuery query = ClickHouseQuery.of(sql);
    final QueryObservation observation =
        QueryObservation.start(observationListener, query.queryId(), OperationKind.INSERT, sql);
    final AtomicLong sentByteCount = new AtomicLong();
    final Flux<ByteBuffer> observedData =
        Flux.from(data).doOnNext(buffer -> sentByteCount.addAndGet(buffer.remaining()));
    return transport
        .insertWithSummary(query, observedData)
        .flatMap(response -> response.body().aggregate().asByteArray().thenReturn(response))
        .doOnNext(
            response ->
                observation.completed(response.writtenRows().getAsLong(), sentByteCount.get()))
        .map(response -> (Result) ClickHouseResult.forInsert(response.writtenRows().getAsLong()))
        .doOnError(observation::failed)
        .doOnCancel(observation::cancelled)
        .onErrorMap(ClickHouseR2dbcException::wrap);
  }

  private void requireOpen() {
    if (closed.get()) {
      throw new IllegalStateException("This connection is closed");
    }
  }
}

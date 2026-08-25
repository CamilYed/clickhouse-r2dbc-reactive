package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoderMode;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
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
 *
 * <p><b>Not thread-safe</b>, deliberately, same as {@link ClickHouseStatement} — see that class's
 * Javadoc for the expected usage pattern (build up state on one thread, then call {@link
 * #execute()} once).
 */
final class ClickHouseBatch implements Batch {

  private final ClickHouseHttpTransport transport;
  private final RowDecodingScheduler decodingScheduler;
  private final DriverObservationListener observationListener;
  private final RowBinaryDecoderMode rowDecoderMode;
  private final List<String> statements = new ArrayList<>();

  ClickHouseBatch(
      final ClickHouseHttpTransport transport, final RowDecodingScheduler decodingScheduler) {
    this(transport, decodingScheduler, DriverObservationListener.NOOP);
  }

  /**
   * {@code observationListener} is notified of one {@link OperationKind#QUERY} lifecycle per
   * statement this batch's {@link #execute()} runs — see {@link DriverObservationListener}'s
   * Javadoc for the full contract.
   */
  ClickHouseBatch(
      final ClickHouseHttpTransport transport,
      final RowDecodingScheduler decodingScheduler,
      final DriverObservationListener observationListener) {
    this(transport, decodingScheduler, observationListener, RowBinaryDecoderMode.CLICKHOUSE);
  }

  /**
   * {@code rowDecoderMode} decides which {@code RowBinaryReader} decodes every statement this
   * batch's {@link #execute()} runs — see {@code ClickHouseConnectionFactoryProvider#ROW_DECODER}'s
   * Javadoc for the full contract.
   */
  ClickHouseBatch(
      final ClickHouseHttpTransport transport,
      final RowDecodingScheduler decodingScheduler,
      final DriverObservationListener observationListener,
      final RowBinaryDecoderMode rowDecoderMode) {
    this.transport = transport;
    this.decodingScheduler = decodingScheduler;
    this.observationListener = observationListener;
    this.rowDecoderMode = rowDecoderMode;
  }

  // sql is declared non-null under this module's @NullMarked contract, but this overrides a
  // plain io.r2dbc.spi.Batch method - external callers of the public R2DBC SPI aren't bound by
  // that static guarantee, so failing fast here beats a confusing NPE deeper in the call chain.
  @Override
  public Batch add(final String sql) {
    if (sql == null) { // NOSONAR - see defensive-null-check note above
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
    final ClickHouseQuery query = ClickHouseQuery.of(sql);
    final QueryObservation observation =
        QueryObservation.start(observationListener, query.queryId(), OperationKind.QUERY, sql);
    final ResponseCompression compression = transport.responseCompression();
    return transport
        .queryWithSummary(query)
        .flatMap(
            response ->
                ClickHouseResult.decode(
                    response, decodingScheduler, observation, compression, rowDecoderMode))
        .doOnError(observation::failed)
        .doOnCancel(observation::cancelled)
        .onErrorMap(ClickHouseR2dbcException::wrap);
  }
}

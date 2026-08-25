package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoderMode;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseQueryResponse;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A ClickHouse {@link Result}: rows, plus the written-row count ClickHouse itself reports for the
 * query that produced it (see {@link #decode}). {@link #filter}/{@link #flatMap} only ever see
 * {@link Result.RowSegment}s, since no other segment kind is produced yet.
 *
 * <p>Consumption-once is enforced across an instance <em>and</em> every {@link #filter}-derived
 * view of it: a second call to {@link #map}/{@link #getRowsUpdated}/{@link #flatMap} on the same
 * instance, on a filtered view after the original was already consumed, or vice versa, all throw
 * {@link IllegalStateException} (per the R2DBC spec's single-consumption contract), because a
 * {@link #filter} view shares its originating instance's {@link ResultConsumptionGuard} rather than
 * getting its own — see that class's Javadoc. Calling {@link #filter} itself after consumption also
 * throws, even though {@link #filter} is a lazy view and not itself a consuming operation.
 *
 * <p>A failure while consuming rows (e.g. a connection reset mid-stream, a local decode bug) is
 * mapped onto {@link io.r2dbc.spi.R2dbcException} via {@link ClickHouseR2dbcException} ({@code
 * ClickHouseR2dbcException.wrap}), same as {@link ClickHouseStatement}/{@link ClickHouseBatch}.
 */
final class ClickHouseResult implements Result {

  private final ClickHouseRowMetadata metadata;
  private final Flux<DecodedRow> rows;
  private final long writtenRows;
  private final ResultConsumptionGuard consumptionGuard;

  ClickHouseResult(final DecodedResult decoded, final long writtenRows) {
    this(new ClickHouseRowMetadata(decoded.columns()), decoded.rows(), writtenRows);
  }

  private ClickHouseResult(
      final ClickHouseRowMetadata metadata, final Flux<DecodedRow> rows, final long writtenRows) {
    this(metadata, rows, writtenRows, new ResultConsumptionGuard());
  }

  private ClickHouseResult(
      final ClickHouseRowMetadata metadata,
      final Flux<DecodedRow> rows,
      final long writtenRows,
      final ResultConsumptionGuard consumptionGuard) {
    this.metadata = metadata;
    this.rows = rows;
    this.writtenRows = writtenRows;
    this.consumptionGuard = consumptionGuard;
  }

  /**
   * Sends {@code response.body()} through {@link RowBinaryDecoder#decode} and wraps the result into
   * a {@link ClickHouseResult} carrying {@code response.writtenRows()} — the one place both halves
   * of a {@link ClickHouseQueryResponse} (decoded rows from {@code core}, the written-row count
   * from {@code transport-http}) come together, used identically by {@link
   * ClickHouseStatement#execute()} and {@link ClickHouseBatch}.
   *
   * <p>{@code response.writtenRows()} is read inside the {@code map} below, after {@link
   * RowBinaryDecoder#decode} has already resolved — deliberately, not just incidentally: per {@link
   * ClickHouseQueryResponse}'s Javadoc, the count is only known once the response headers have
   * actually arrived, which {@code decode}'s schema read already waits on.
   *
   * <p>{@code decodingScheduler} is forwarded to {@link RowBinaryDecoder#decode} unchanged — see
   * that method's Javadoc for why every blocking decode call must run there rather than on
   * whichever thread ends up consuming the returned {@link Result}.
   *
   * <p>{@code observation}'s {@code queryCompleted}/{@code queryFailed}/{@code queryCancelled} fire
   * based on the returned {@link ClickHouseResult}'s own row {@link Flux} — its terminal signal,
   * not this method's — since that {@link Flux} is only actually subscribed to later, when a caller
   * consumes the {@link Result} via {@link #map}/{@link #flatMap}; see {@link
   * io.github.camilyed.clickhouse.r2dbc.core.QueryCompletedEvent}'s Javadoc for why a caller that
   * never does so never triggers those events at all.
   *
   * <p>When {@code observation.isEnabled()} is {@code false} (the default, unconfigured case), the
   * per-chunk byte-counting and per-row {@code doOnNext}/{@code doOnComplete}/{@code doOnError}/
   * {@code doOnCancel} wiring below is skipped entirely rather than wired and left to feed a call
   * that was never going to happen — see {@link
   * io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener#isEnabled()}'s Javadoc for
   * why that's the point of the flag.
   *
   * <p>{@code compression} — the same {@link ClickHouseHttpTransport#responseCompression()} value
   * that decided whether {@code compress=1} was sent for this request — is forwarded to {@link
   * RowBinaryDecoder#decode} unchanged, so the decode side unwraps ClickHouse's LZ4 block framing
   * exactly when the request side asked for it. See {@link ResponseCompression}'s Javadoc.
   *
   * <p>{@code rowDecoderMode} is likewise forwarded to {@link RowBinaryDecoder#decode} unchanged —
   * see {@code ClickHouseConnectionFactoryProvider#ROW_DECODER}'s Javadoc for the full contract.
   */
  static Mono<ClickHouseResult> decode(
      final ClickHouseQueryResponse response,
      final RowDecodingScheduler decodingScheduler,
      final QueryObservation observation,
      final ResponseCompression compression,
      final RowBinaryDecoderMode rowDecoderMode) {
    return observation.isEnabled()
        ? decodeObserved(response, decodingScheduler, observation, compression, rowDecoderMode)
        : decodePlain(response, decodingScheduler, compression, rowDecoderMode);
  }

  private static Mono<ClickHouseResult> decodePlain(
      final ClickHouseQueryResponse response,
      final RowDecodingScheduler decodingScheduler,
      final ResponseCompression compression,
      final RowBinaryDecoderMode rowDecoderMode) {
    final Flux<ByteBuffer> body = response.body().asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decode(body, decodingScheduler, compression, rowDecoderMode)
        .map(decoded -> new ClickHouseResult(decoded, response.writtenRows().getAsLong()));
  }

  private static Mono<ClickHouseResult> decodeObserved(
      final ClickHouseQueryResponse response,
      final RowDecodingScheduler decodingScheduler,
      final QueryObservation observation,
      final ResponseCompression compression,
      final RowBinaryDecoderMode rowDecoderMode) {
    final AtomicLong byteCount = new AtomicLong();
    final Flux<ByteBuffer> body =
        response
            .body()
            .asByteArray()
            .doOnNext(bytes -> byteCount.addAndGet(bytes.length))
            .map(ByteBuffer::wrap);
    return RowBinaryDecoder.decode(body, decodingScheduler, compression, rowDecoderMode)
        .map(
            decoded ->
                new ClickHouseResult(
                    observed(decoded, observation, byteCount), response.writtenRows().getAsLong()));
  }

  private static DecodedResult observed(
      final DecodedResult decoded, final QueryObservation observation, final AtomicLong byteCount) {
    final AtomicLong rowCount = new AtomicLong();
    final Flux<DecodedRow> observedRows =
        decoded
            .rows()
            .doOnNext(
                row -> {
                  observation.firstRowReceived();
                  rowCount.incrementAndGet();
                })
            .doOnComplete(() -> observation.completed(rowCount.get(), byteCount.get()))
            .doOnError(observation::failed)
            .doOnCancel(observation::cancelled);
    return new DecodedResult(decoded.columns(), observedRows);
  }

  /**
   * A {@link Result} carrying only {@code writtenRows} and no rows/columns at all — what {@link
   * ClickHouseConnection#insertStreaming} returns, since ClickHouse's HTTP interface sends back no
   * body at all for a plain {@code INSERT} (unlike a {@code SELECT}'s {@code
   * RowBinaryWithNamesAndTypes} response, which {@link #decode} expects). {@link #map}/{@link
   * #flatMap} on the returned instance never emit anything; only {@link #getRowsUpdated()} is
   * meaningful.
   */
  static ClickHouseResult forInsert(final long writtenRows) {
    return new ClickHouseResult(new ClickHouseRowMetadata(List.of()), Flux.empty(), writtenRows);
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    consumptionGuard.markConsumedOrFail();
    return Mono.just(writtenRows);
  }

  // mappingFunction is declared non-null under this module's @NullMarked contract, but this
  // overrides a plain io.r2dbc.spi.Result method - external callers of the public R2DBC SPI
  // aren't bound by that static guarantee, so failing fast here beats a confusing NPE deeper in
  // the call chain.
  @Override
  public <T> Publisher<T> map(final BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    if (mappingFunction == null) { // NOSONAR - see defensive-null-check note above
      throw new IllegalArgumentException("mappingFunction must not be null");
    }
    consumptionGuard.markConsumedOrFail();
    final Flux<T> mapped =
        rows.map(row -> mappingFunction.apply(new ClickHouseRow(row, metadata), metadata));
    return mapped.onErrorMap(ClickHouseR2dbcException::wrap);
  }

  // See map(...) above for why this defensive check is kept despite @NullMarked.
  @Override
  public Result filter(final Predicate<Segment> filter) {
    if (filter == null) { // NOSONAR - see map(...) above
      throw new IllegalArgumentException("filter must not be null");
    }
    consumptionGuard.failIfAlreadyConsumed();
    return new ClickHouseResult(
        metadata, rows.filter(row -> filter.test(rowSegment(row))), writtenRows, consumptionGuard);
  }

  // See map(...) above for why this defensive check is kept despite @NullMarked.
  @Override
  public <T> Publisher<T> flatMap(
      final Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    if (mappingFunction == null) { // NOSONAR - see map(...) above
      throw new IllegalArgumentException("mappingFunction must not be null");
    }
    consumptionGuard.markConsumedOrFail();
    final Flux<T> flatMapped = rows.concatMap(row -> mappingFunction.apply(rowSegment(row)));
    return flatMapped.onErrorMap(ClickHouseR2dbcException::wrap);
  }

  private RowSegment rowSegment(final DecodedRow row) {
    return new ClickHouseRowSegment(new ClickHouseRow(row, metadata));
  }

  private record ClickHouseRowSegment(Row row) implements RowSegment {}
}

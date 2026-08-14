package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseQueryResponse;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>Consumption-once is enforced per instance (a second call to {@link #map}/{@link
 * #getRowsUpdated}/{@link #flatMap} on the <em>same</em> instance throws {@link
 * IllegalStateException}, per the R2DBC spec), but not transitively across a {@link #filter}-
 * derived instance — that derived {@link Result} wraps the same underlying row stream and has its
 * own, separate consumption guard. Calling both the original and a filtered view is a misuse this
 * class does not currently detect.
 *
 * <p>A failure while consuming rows (e.g. a connection reset mid-stream, a local decode bug) is
 * mapped onto {@link io.r2dbc.spi.R2dbcException} via {@link ClickHouseR2dbcException} ({@code
 * ClickHouseR2dbcException.wrap}), same as {@link ClickHouseStatement}/{@link ClickHouseBatch}.
 */
final class ClickHouseResult implements Result {

  private final ClickHouseRowMetadata metadata;
  private final Flux<DecodedRow> rows;
  private final long writtenRows;
  private final AtomicBoolean consumed = new AtomicBoolean(false);

  ClickHouseResult(final DecodedResult decoded, final long writtenRows) {
    this(new ClickHouseRowMetadata(decoded.columns()), decoded.rows(), writtenRows);
  }

  private ClickHouseResult(
      final ClickHouseRowMetadata metadata, final Flux<DecodedRow> rows, final long writtenRows) {
    this.metadata = metadata;
    this.rows = rows;
    this.writtenRows = writtenRows;
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
   */
  static Mono<ClickHouseResult> decode(final ClickHouseQueryResponse response) {
    final Flux<ByteBuffer> body = response.body().asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decode(body)
        .map(decoded -> new ClickHouseResult(decoded, response.writtenRows().getAsLong()));
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
    markConsumedOrFail();
    return Mono.just(writtenRows);
  }

  // mappingFunction is declared non-null under this module's @NullMarked contract, but this
  // overrides a plain io.r2dbc.spi.Result method - external callers of the public R2DBC SPI
  // aren't bound by that static guarantee, so failing fast here beats a confusing NPE deeper in
  // the call chain.
  @SuppressWarnings("java:S2583")
  @Override
  public <T> Publisher<T> map(final BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    if (mappingFunction == null) {
      throw new IllegalArgumentException("mappingFunction must not be null");
    }
    markConsumedOrFail();
    final Flux<T> mapped =
        rows.map(row -> mappingFunction.apply(new ClickHouseRow(row, metadata), metadata));
    return mapped.onErrorMap(ClickHouseR2dbcException::wrap);
  }

  // See map(...) above for why this defensive check is kept despite @NullMarked.
  @SuppressWarnings("java:S2583")
  @Override
  public Result filter(final Predicate<Segment> filter) {
    if (filter == null) {
      throw new IllegalArgumentException("filter must not be null");
    }
    return new ClickHouseResult(
        metadata, rows.filter(row -> filter.test(rowSegment(row))), writtenRows);
  }

  // See map(...) above for why this defensive check is kept despite @NullMarked.
  @SuppressWarnings("java:S2583")
  @Override
  public <T> Publisher<T> flatMap(
      final Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    if (mappingFunction == null) {
      throw new IllegalArgumentException("mappingFunction must not be null");
    }
    markConsumedOrFail();
    final Flux<T> flatMapped = rows.concatMap(row -> mappingFunction.apply(rowSegment(row)));
    return flatMapped.onErrorMap(ClickHouseR2dbcException::wrap);
  }

  private RowSegment rowSegment(final DecodedRow row) {
    return new ClickHouseRowSegment(new ClickHouseRow(row, metadata));
  }

  private void markConsumedOrFail() {
    if (!consumed.compareAndSet(false, true)) {
      throw new IllegalStateException("This result has already been consumed");
    }
  }

  private record ClickHouseRowSegment(Row row) implements RowSegment {}
}

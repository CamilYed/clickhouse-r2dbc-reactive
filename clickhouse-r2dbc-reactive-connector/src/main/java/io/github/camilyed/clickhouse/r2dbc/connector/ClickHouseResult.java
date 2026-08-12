package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A ClickHouse {@link Result}: rows only, for now. ClickHouse's HTTP interface doesn't return
 * update counts or {@code OUT} parameters the way {@link #getRowsUpdated()}/{@code OutParameters}
 * assume, and this driver hasn't built an INSERT/DDL round trip that would need one yet ({@code
 * ClickHouseHttpTransport.query}/{@code RowBinaryDecoder} are "shaped for SELECT-style" queries
 * today — see ROADMAP.md). {@link #getRowsUpdated()} therefore always completes empty rather than
 * guessing a count. {@link #filter}/{@link #flatMap} only ever see {@link Result.RowSegment}s,
 * since no other segment kind is produced yet.
 *
 * <p>Consumption-once is enforced per instance (a second call to {@link #map}/{@link
 * #getRowsUpdated}/{@link #flatMap} on the <em>same</em> instance throws {@link
 * IllegalStateException}, per the R2DBC spec), but not transitively across a {@link #filter}-
 * derived instance — that derived {@link Result} wraps the same underlying row stream and has its
 * own, separate consumption guard. Calling both the original and a filtered view is a misuse this
 * class does not currently detect.
 */
final class ClickHouseResult implements Result {

  private final ClickHouseRowMetadata metadata;
  private final Flux<Map<String, Object>> rows;
  private final AtomicBoolean consumed = new AtomicBoolean(false);

  ClickHouseResult(final DecodedResult decoded) {
    this(new ClickHouseRowMetadata(decoded.columns()), decoded.rows());
  }

  private ClickHouseResult(final ClickHouseRowMetadata metadata, final Flux<Map<String, Object>> rows) {
    this.metadata = metadata;
    this.rows = rows;
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    markConsumedOrFail();
    return Mono.empty();
  }

  @Override
  public <T> Publisher<T> map(final BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    if (mappingFunction == null) {
      throw new IllegalArgumentException("mappingFunction must not be null");
    }
    markConsumedOrFail();
    return rows.map(values -> mappingFunction.apply(new ClickHouseRow(values, metadata), metadata));
  }

  @Override
  public Result filter(final Predicate<Segment> filter) {
    if (filter == null) {
      throw new IllegalArgumentException("filter must not be null");
    }
    return new ClickHouseResult(metadata, rows.filter(values -> filter.test(rowSegment(values))));
  }

  @Override
  public <T> Publisher<T> flatMap(
      final Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    if (mappingFunction == null) {
      throw new IllegalArgumentException("mappingFunction must not be null");
    }
    markConsumedOrFail();
    return rows.concatMap(values -> mappingFunction.apply(rowSegment(values)));
  }

  private RowSegment rowSegment(final Map<String, Object> values) {
    return new ClickHouseRowSegment(new ClickHouseRow(values, metadata));
  }

  private void markConsumedOrFail() {
    if (!consumed.compareAndSet(false, true)) {
      throw new IllegalStateException("This result has already been consumed");
    }
  }

  private record ClickHouseRowSegment(Row row) implements RowSegment {}
}

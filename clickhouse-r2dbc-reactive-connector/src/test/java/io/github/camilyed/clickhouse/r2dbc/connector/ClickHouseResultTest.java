package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.r2dbc.spi.Result;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class ClickHouseResultTest {

  @Test
  void shouldMapEveryRowUsingItsRowAndMetadata() {
    // given
    final ClickHouseResult result = resultOf(Map.of("id", 1), Map.of("id", 2));

    // when / then
    StepVerifier.create(result.map((row, rowMetadata) -> row.get("id", Integer.class)))
        .expectNext(1, 2)
        .verifyComplete();
  }

  @Test
  void shouldCompleteEmptyWhenAskedForRowsUpdated() {
    // given
    final ClickHouseResult result = resultOf(Map.of("id", 1));

    // when / then
    StepVerifier.create(result.getRowsUpdated()).verifyComplete();
  }

  @Test
  void shouldRejectConsumingTheSameResultTwice() {
    // given
    final ClickHouseResult result = resultOf(Map.of("id", 1));
    result.getRowsUpdated();

    // when / then
    // Consumption is tracked when map()/getRowsUpdated()/flatMap() is called, not when the
    // returned Publisher is subscribed - so the second call throws directly, synchronously,
    // rather than the returned Publisher signalling onError.
    assertThatThrownBy(() -> result.map((row, rowMetadata) -> row))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldOnlySeeRowsMatchingTheFilterPredicate() {
    // given
    final ClickHouseResult result = resultOf(Map.of("id", 1), Map.of("id", 2), Map.of("id", 3));

    // when
    final var filtered =
        result.filter(
            segment -> ((Result.RowSegment) segment).row().get("id", Integer.class) % 2 == 0);

    // then
    StepVerifier.create(filtered.map((row, rowMetadata) -> row.get("id", Integer.class)))
        .expectNext(2)
        .verifyComplete();
  }

  @SafeVarargs
  private ClickHouseResult resultOf(final Map<String, Object>... rows) {
    final List<ColumnDescriptor> columns = List.of(new ColumnDescriptor("id", "Int32"));
    return new ClickHouseResult(new DecodedResult(columns, Flux.just(rows)));
  }
}

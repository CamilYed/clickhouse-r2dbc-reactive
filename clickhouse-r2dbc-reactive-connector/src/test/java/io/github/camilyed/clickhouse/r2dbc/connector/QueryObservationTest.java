package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.connector.fakes.RecordingDriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;
import io.github.camilyed.clickhouse.r2dbc.core.SqlFingerprint;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class QueryObservationTest {

  private final RecordingDriverObservationListener listener =
      new RecordingDriverObservationListener();

  @Test
  void shouldFireQueryStartedImmediatelyWhenStarted() {
    // when
    QueryObservation.start(listener, "query-1", OperationKind.QUERY, "SELECT 1");

    // then
    assertThat(listener.startedEvents()).hasSize(1);
    // and
    assertThat(listener.startedEvents().getFirst().queryId()).isEqualTo("query-1");
    assertThat(listener.startedEvents().getFirst().operationKind()).isEqualTo(OperationKind.QUERY);
    assertThat(listener.startedEvents().getFirst().sqlFingerprint())
        .isEqualTo(SqlFingerprint.of("SELECT 1"));
  }

  @Test
  void shouldFireQueryCompletedWithZeroTimeToFirstRowWhenNoRowWasEverReceived() {
    // given
    final QueryObservation observation =
        QueryObservation.start(listener, "query-1", OperationKind.QUERY, "SELECT 1");

    // when
    observation.completed(0, 0);

    // then
    assertThat(listener.completedEvents()).hasSize(1);
    // and
    assertThat(listener.completedEvents().getFirst().timeToFirstRow()).isEqualTo(Duration.ZERO);
    assertThat(listener.completedEvents().getFirst().rowCount()).isZero();
    assertThat(listener.completedEvents().getFirst().byteCount()).isZero();
  }

  @Test
  void shouldFireQueryCompletedCarryingTheAccumulatedRowAndByteCount() {
    // given
    final QueryObservation observation =
        QueryObservation.start(listener, "query-1", OperationKind.QUERY, "SELECT 1");
    observation.firstRowReceived();

    // when
    observation.completed(3, 42);

    // then
    assertThat(listener.completedEvents()).hasSize(1);
    // and
    assertThat(listener.completedEvents().getFirst().rowCount()).isEqualTo(3);
    assertThat(listener.completedEvents().getFirst().byteCount()).isEqualTo(42);
    assertThat(listener.completedEvents().getFirst().queryId()).isEqualTo("query-1");
    assertThat(listener.completedEvents().getFirst().operationKind())
        .isEqualTo(OperationKind.QUERY);
  }

  @Test
  void shouldOnlyTimeTheFirstOfMultipleFirstRowReceivedCalls() {
    // given
    final QueryObservation observation =
        QueryObservation.start(listener, "query-1", OperationKind.QUERY, "SELECT 1");

    // when
    observation.firstRowReceived();
    observation.firstRowReceived();
    observation.completed(2, 16);

    // then - no exception, and exactly one completed event, proving the second call was a no-op
    assertThat(listener.completedEvents()).hasSize(1);
  }

  @Test
  void shouldFireQueryFailedCarryingTheGivenCause() {
    // given
    final QueryObservation observation =
        QueryObservation.start(listener, "query-1", OperationKind.QUERY, "SELECT 1");
    final RuntimeException cause = new RuntimeException("boom");

    // when
    observation.failed(cause);

    // then
    assertThat(listener.failedEvents()).hasSize(1);
    // and
    assertThat(listener.failedEvents().getFirst().cause()).isSameAs(cause);
    assertThat(listener.failedEvents().getFirst().queryId()).isEqualTo("query-1");
  }

  @Test
  void shouldFireQueryCancelled() {
    // given
    final QueryObservation observation =
        QueryObservation.start(listener, "query-1", OperationKind.QUERY, "SELECT 1");

    // when
    observation.cancelled();

    // then
    assertThat(listener.cancelledEvents()).hasSize(1);
    // and
    assertThat(listener.cancelledEvents().getFirst().queryId()).isEqualTo("query-1");
  }
}

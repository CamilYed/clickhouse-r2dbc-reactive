package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DriverObservationListenerTest {

  private static final SqlFingerprint A_FINGERPRINT = SqlFingerprint.of("SELECT 1");

  @Test
  void shouldAcceptAQueryStartedEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                DriverObservationListener.NOOP.queryStarted(
                    new QueryStartedEvent("query-1", OperationKind.QUERY, A_FINGERPRINT)))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldAcceptAQueryCompletedEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                DriverObservationListener.NOOP.queryCompleted(
                    new QueryCompletedEvent(
                        "query-1",
                        OperationKind.QUERY,
                        A_FINGERPRINT,
                        Duration.ofMillis(10),
                        Duration.ofMillis(2),
                        1,
                        8)))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldAcceptAQueryFailedEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                DriverObservationListener.NOOP.queryFailed(
                    new QueryFailedEvent(
                        "query-1",
                        OperationKind.QUERY,
                        A_FINGERPRINT,
                        Duration.ofMillis(10),
                        new RuntimeException("boom"))))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldAcceptAQueryCancelledEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                DriverObservationListener.NOOP.queryCancelled(
                    new QueryCancelledEvent(
                        "query-1", OperationKind.QUERY, A_FINGERPRINT, Duration.ofMillis(10))))
        .doesNotThrowAnyException();
  }
}

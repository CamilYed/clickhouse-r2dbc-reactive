package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCancelledEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryCompletedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryFailedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.QueryStartedEvent;
import io.github.camilyed.clickhouse.r2dbc.core.SqlFingerprint;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link Slf4jDriverObservationListener} handles every event type without throwing — this
 * project has no log-content-capturing test infrastructure (see {@code
 * ClickHouseHttpTransport}'s own untested best-effort-KILL-QUERY WARN log for the established
 * precedent), so, same as there, the logging call itself is exercised but its rendered output is
 * not asserted.
 */
class Slf4jDriverObservationListenerTest {

  private static final SqlFingerprint A_FINGERPRINT = SqlFingerprint.of("SELECT 1");
  private final Slf4jDriverObservationListener listener = new Slf4jDriverObservationListener();

  @Test
  void shouldLogAQueryStartedEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                listener.queryStarted(
                    new QueryStartedEvent("query-1", OperationKind.QUERY, A_FINGERPRINT)))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldLogAQueryCompletedEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                listener.queryCompleted(
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
  void shouldLogAQueryFailedEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                listener.queryFailed(
                    new QueryFailedEvent(
                        "query-1",
                        OperationKind.QUERY,
                        A_FINGERPRINT,
                        Duration.ofMillis(10),
                        new RuntimeException("boom"))))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldLogAQueryCancelledEventWithoutThrowing() {
    // when / then
    assertThatCode(
            () ->
                listener.queryCancelled(
                    new QueryCancelledEvent(
                        "query-1", OperationKind.QUERY, A_FINGERPRINT, Duration.ofMillis(10))))
        .doesNotThrowAnyException();
  }
}

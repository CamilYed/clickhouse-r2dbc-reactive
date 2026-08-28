package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.connector.fakes.RecordingDriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Proves {@link ClickHouseConnection#insertStreaming} — the vendor extension wrapping {@code
 * ClickHouseHttpTransport#insertWithSummary} — actually streams the given data and reports back
 * ClickHouse's own written-row count, black-box through the connection rather than the transport
 * directly (already covered at that level by {@code ClickHouseHttpTransportTest}).
 */
class ClickHouseConnectionInsertStreamingTest {

  @Test
  void shouldStreamTheGivenDataAndReportTheWrittenRowCount() {
    // given
    final byte[] rowData = "1\tAda\n2\tGrace\n".getBytes(StandardCharsets.UTF_8);

    // when
    final long writtenRows;
    final byte[] receivedBody;
    try (final var server =
        ControlledClickHouseServer.startAcceptingInsertsAndRespondingWithSummary(
            "{\"written_rows\":\"2\"}")) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()), RowDecodingScheduler.defaults());

      final Result result =
          Mono.from(
                  connection.insertStreaming(
                      "INSERT INTO t FORMAT TabSeparated", Flux.just(ByteBuffer.wrap(rowData))))
              .block(Duration.ofSeconds(5));
      writtenRows = Mono.from(result.getRowsUpdated()).block(Duration.ofSeconds(5));
      receivedBody = server.receivedRequestBody();
    }

    // then
    assertThat(receivedBody).isEqualTo(rowData);
    assertThat(writtenRows).isEqualTo(2L);
  }

  @Test
  void shouldMapAServerErrorToAnR2dbcException() {
    // given
    try (final var server =
        ControlledClickHouseServer.startRespondingWithClickHouseError(
            241, "Memory limit exceeded", 500)) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()), RowDecodingScheduler.defaults());

      // when
      final Throwable thrown =
          catchThrowable(
              () ->
                  Mono.from(
                          connection.insertStreaming(
                              "INSERT INTO t FORMAT TabSeparated",
                              Flux.just(
                                  ByteBuffer.wrap("1\tAda\n".getBytes(StandardCharsets.UTF_8)))))
                      .block(Duration.ofSeconds(5)));

      // then
      assertThat(thrown).isInstanceOf(R2dbcException.class);
      assertThat(((R2dbcException) thrown).getErrorCode()).isEqualTo(241);
    }
  }

  @Test
  void shouldNotifyTheObservationListenerOfASuccessfulInsert() {
    // given
    final byte[] rowData = "1\tAda\n2\tGrace\n".getBytes(StandardCharsets.UTF_8);
    final RecordingDriverObservationListener listener = new RecordingDriverObservationListener();

    try (final var server =
        ControlledClickHouseServer.startAcceptingInsertsAndRespondingWithSummary(
            "{\"written_rows\":\"2\"}")) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()),
              RowDecodingScheduler.defaults(),
              listener);

      // when
      Mono.from(
              connection.insertStreaming(
                  "INSERT INTO t FORMAT TabSeparated", Flux.just(ByteBuffer.wrap(rowData))))
          .block(Duration.ofSeconds(5));

      // then
      assertThat(listener.startedEvents()).hasSize(1);
      assertThat(listener.startedEvents().getFirst().operationKind())
          .isEqualTo(OperationKind.INSERT);
      // and
      assertThat(listener.completedEvents()).hasSize(1);
      assertThat(listener.completedEvents().getFirst().rowCount()).isEqualTo(2L);
      assertThat(listener.completedEvents().getFirst().byteCount()).isEqualTo(rowData.length);
      assertThat(listener.completedEvents().getFirst().timeToFirstRow()).isEqualTo(Duration.ZERO);
      assertThat(listener.failedEvents()).isEmpty();
    }
  }

  @Test
  void shouldNotifyTheObservationListenerOfAFailedInsert() {
    // given
    final RecordingDriverObservationListener listener = new RecordingDriverObservationListener();

    try (final var server =
        ControlledClickHouseServer.startRespondingWithClickHouseError(
            241, "Memory limit exceeded", 500)) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()),
              RowDecodingScheduler.defaults(),
              listener);

      // when
      catchThrowable(
          () ->
              Mono.from(
                      connection.insertStreaming(
                          "INSERT INTO t FORMAT TabSeparated",
                          Flux.just(ByteBuffer.wrap("1\tAda\n".getBytes(StandardCharsets.UTF_8)))))
                  .block(Duration.ofSeconds(5)));

      // then
      assertThat(listener.startedEvents()).hasSize(1);
      assertThat(listener.failedEvents()).hasSize(1);
      assertThat(listener.completedEvents()).isEmpty();
    }
  }
}

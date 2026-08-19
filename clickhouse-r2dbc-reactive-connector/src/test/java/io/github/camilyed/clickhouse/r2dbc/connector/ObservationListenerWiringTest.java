package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.connector.fakes.RecordingDriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.OperationKind;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Proves {@link ClickHouseConnectionFactoryProvider#OBSERVATION_LISTENER} is actually wired end to
 * end — built from {@link ConnectionFactoryOptions} through to a real query running against a
 * {@link ControlledClickHouseServer} — not just accepted and ignored.
 */
class ObservationListenerWiringTest {

  private final RecordingDriverObservationListener listener =
      new RecordingDriverObservationListener();

  @Test
  void shouldNotifyTheConfiguredListenerOfAQueryThatConsumesItsRows() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER, listener)
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

      // when
      final long rowCount =
          Mono.from(factory.create())
              .flatMapMany(connection -> connection.createStatement("SELECT 1").execute())
              .flatMap(result -> Flux.from(result.map((row, metadata) -> row)))
              .count()
              .block(Duration.ofSeconds(5));

      // then
      assertThat(rowCount).isEqualTo(1L);
      assertThat(listener.startedEvents()).hasSize(1);
      assertThat(listener.startedEvents().getFirst().operationKind())
          .isEqualTo(OperationKind.QUERY);
      // and
      assertThat(listener.completedEvents()).hasSize(1);
      assertThat(listener.completedEvents().getFirst().rowCount()).isEqualTo(1L);
      assertThat(listener.completedEvents().getFirst().byteCount()).isPositive();
      assertThat(listener.failedEvents()).isEmpty();
      assertThat(listener.cancelledEvents()).isEmpty();
    }
  }

  @Test
  void shouldNotNotifyAnyListenerWhenACallerOnlyReadsRowsUpdated() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER, listener)
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

      // when
      Mono.from(factory.create())
          .flatMapMany(connection -> connection.createStatement("SELECT 1").execute())
          .flatMap(Result::getRowsUpdated)
          .blockLast(Duration.ofSeconds(5));

      // then - documented v1 limitation: no rows were ever subscribed to, so no completed event
      assertThat(listener.startedEvents()).hasSize(1);
      assertThat(listener.completedEvents()).isEmpty();
    }
  }

  @Test
  void shouldStillDecodeRowsCorrectlyWithNoListenerConfigured() {
    // given - OBSERVATION_LISTENER left unset, so every query goes through the
    // isEnabled()==false fast path (DriverObservationListener.NOOP by default) instead of the
    // instrumented one the other tests in this class exercise.
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

      // when
      final long rowCount =
          Mono.from(factory.create())
              .flatMapMany(connection -> connection.createStatement("SELECT 1").execute())
              .flatMap(result -> Flux.from(result.map((row, metadata) -> row)))
              .count()
              .block(Duration.ofSeconds(5));

      // then
      assertThat(rowCount).isEqualTo(1L);
    }
  }
}

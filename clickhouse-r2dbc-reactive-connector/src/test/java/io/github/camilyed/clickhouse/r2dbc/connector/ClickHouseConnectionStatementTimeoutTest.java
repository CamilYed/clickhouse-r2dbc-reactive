package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Proves {@link ClickHouseConnection#setStatementTimeout} is actually wired end to end into a
 * statement's request, black-box through {@link ClickHouseConnection#createStatement} rather than
 * unit-testing {@link ClickHouseStatement}'s settings-building logic in isolation.
 */
class ClickHouseConnectionStatementTimeoutTest {

  private final RowDecodingScheduler decodingScheduler = RowDecodingScheduler.defaults();

  @Test
  void shouldSendMaxExecutionTimeForAStatementCreatedAfterSettingATimeout() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()), decodingScheduler);
      Mono.from(connection.setStatementTimeout(Duration.ofSeconds(5))).block(Duration.ofSeconds(5));

      Flux.from(connection.createStatement("SELECT 1").execute()).blockLast(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).contains("max_execution_time=5.000");
    }
  }

  @Test
  void shouldNotSendMaxExecutionTimeWhenNoTimeoutWasEverConfigured() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()), decodingScheduler);

      Flux.from(connection.createStatement("SELECT 1").execute()).blockLast(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).doesNotContain("max_execution_time");
    }
  }

  @Test
  void shouldNotApplyATimeoutToAStatementCreatedBeforeItWasSet() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()), decodingScheduler);
      final var statement = connection.createStatement("SELECT 1");
      Mono.from(connection.setStatementTimeout(Duration.ofSeconds(5))).block(Duration.ofSeconds(5));

      Flux.from(statement.execute()).blockLast(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).doesNotContain("max_execution_time");
    }
  }

  @Test
  void shouldSendAnExplicitZeroWhenATimeoutOfZeroWasConfigured() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(server.baseUrl()), decodingScheduler);
      Mono.from(connection.setStatementTimeout(Duration.ZERO)).block(Duration.ofSeconds(5));

      Flux.from(connection.createStatement("SELECT 1").execute()).blockLast(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).contains("max_execution_time=0.000");
    }
  }
}

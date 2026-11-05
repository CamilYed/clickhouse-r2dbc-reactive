package io.github.camilyed.clickhouse.r2dbc.connector.publicapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnection;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactoryProvider;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseStatement;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class ClickHouseStatementSettingsTest {

  @Test
  void shouldSendAClickHouseSettingThroughThePublicStatementExtension() {
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(
            ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes())) {
      // given
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.RESPONSE_COMPRESSION, false)
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
      final ClickHouseConnection connection = factory.create().block(Duration.ofSeconds(5));
      final ClickHouseStatement statement = connection.createStatement("SELECT 1");
      statement.setClickHouseSetting("max_threads", "2");

      try {
        // when
        Flux.from(statement.execute()).blockLast(Duration.ofSeconds(5));

        // then
        assertThat(server.receivedUri()).contains("max_threads=2");
      } finally {
        factory.dispose();
      }
    }
  }

  @Test
  void shouldLetAStatementSettingOverrideTheConnectionStatementTimeout() {
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(
            ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes())) {
      // given
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.RESPONSE_COMPRESSION, false)
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
      final ClickHouseConnection connection = factory.create().block(Duration.ofSeconds(5));
      Mono.from(connection.setStatementTimeout(Duration.ofSeconds(5))).block(Duration.ofSeconds(5));
      final ClickHouseStatement statement = connection.createStatement("SELECT 1");
      statement.setClickHouseSetting("max_execution_time", "1");

      try {
        // when
        Flux.from(statement.execute()).blockLast(Duration.ofSeconds(5));

        // then
        assertThat(server.receivedUri())
            .contains("max_execution_time=1")
            .doesNotContain("max_execution_time=5.000");
      } finally {
        factory.dispose();
      }
    }
  }

  @Test
  void shouldReplaceThePreviousValueOfTheSameSetting() {
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(
            ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes())) {
      // given
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.RESPONSE_COMPRESSION, false)
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
      final ClickHouseConnection connection = factory.create().block(Duration.ofSeconds(5));
      final ClickHouseStatement statement = connection.createStatement("SELECT 1");
      statement.setClickHouseSetting("max_threads", "2");
      statement.setClickHouseSetting("max_threads", "4");

      try {
        // when
        Flux.from(statement.execute()).blockLast(Duration.ofSeconds(5));

        // then
        assertThat(server.receivedUri()).contains("max_threads=4").doesNotContain("max_threads=2");
      } finally {
        factory.dispose();
      }
    }
  }

  @Test
  void shouldRejectASettingNameThatCouldInjectAnotherQueryParameter() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.PORT, 1)
            .build();
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
    final ClickHouseConnection connection = factory.create().block(Duration.ofSeconds(5));
    final ClickHouseStatement statement = connection.createStatement("SELECT 1");

    try {
      // when
      final Throwable thrown =
          catchThrowable(() -> statement.setClickHouseSetting("max_threads&readonly", "2"));

      // then
      assertThat(thrown)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("setting name");
    } finally {
      factory.dispose();
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "query",
        "query_id",
        "compress",
        "format",
        "default_format",
        "database",
        "output_format_binary_write_json_as_string",
        "param_id"
      })
  void shouldRejectARequestParameterOwnedByTheDriver(final String settingName) {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.PORT, 1)
            .build();
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
    final ClickHouseConnection connection = factory.create().block(Duration.ofSeconds(5));
    final ClickHouseStatement statement = connection.createStatement("SELECT 1");

    try {
      // when
      final Throwable thrown =
          catchThrowable(() -> statement.setClickHouseSetting(settingName, "value"));

      // then
      assertThat(thrown)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("reserved");
    } finally {
      factory.dispose();
    }
  }
}

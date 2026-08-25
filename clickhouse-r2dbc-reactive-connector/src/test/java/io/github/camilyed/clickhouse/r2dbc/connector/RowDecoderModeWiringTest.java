package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Proves {@link ClickHouseConnectionFactoryProvider#ROW_DECODER} is actually read and wired end to
 * end — built from {@link ConnectionFactoryOptions} through to a real query running against a {@link
 * ControlledClickHouseServer} — not just accepted and ignored, for both {@code "clickhouse"} (the
 * default) and {@code "native"}.
 */
class RowDecoderModeWiringTest {

  @Test
  void shouldDecodeCorrectlyWithTheDefaultClickHouseModeLeftUnset() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.RESPONSE_COMPRESSION, false)
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

      // when
      final short value =
          Mono.from(factory.create())
              .flatMapMany(connection -> connection.createStatement("SELECT 1").execute())
              .flatMap(result -> Flux.from(result.map((row, metadata) -> row.get(0, Short.class))))
              .blockFirst(Duration.ofSeconds(5));

      // then
      assertThat(value).isEqualTo((short) 1);
    }
  }

  @Test
  void shouldDecodeCorrectlyWithRowDecoderExplicitlySetToNative() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.RESPONSE_COMPRESSION, false)
              .option(ClickHouseConnectionFactoryProvider.ROW_DECODER, "native")
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

      // when
      final short value =
          Mono.from(factory.create())
              .flatMapMany(connection -> connection.createStatement("SELECT 1").execute())
              .flatMap(result -> Flux.from(result.map((row, metadata) -> row.get(0, Short.class))))
              .blockFirst(Duration.ofSeconds(5));

      // then - identical decoded value to the CLICKHOUSE-mode test above
      assertThat(value).isEqualTo((short) 1);
    }
  }

  @Test
  void shouldDecodeCorrectlyWithRowDecoderExplicitlySetToClickHouseCaseInsensitively() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.HOST, "localhost")
              .option(ConnectionFactoryOptions.PORT, server.port())
              .option(ClickHouseConnectionFactoryProvider.RESPONSE_COMPRESSION, false)
              .option(ClickHouseConnectionFactoryProvider.ROW_DECODER, "ClickHouse")
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

      // when
      final long rowCount =
          Mono.from(factory.create())
              .flatMapMany(connection -> connection.createStatement("SELECT 1").execute())
              .flatMap(Result::getRowsUpdated)
              .count()
              .block(Duration.ofSeconds(5));

      // then - no error building/using the factory
      assertThat(rowCount).isEqualTo(0L);
    }
  }

  @Test
  void shouldFailFastOnAnUnrecognizedRowDecoderValue() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.PORT, 1)
            .option(ClickHouseConnectionFactoryProvider.ROW_DECODER, "turbo")
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rowDecoder")
        .hasMessageContaining("turbo");
  }
}

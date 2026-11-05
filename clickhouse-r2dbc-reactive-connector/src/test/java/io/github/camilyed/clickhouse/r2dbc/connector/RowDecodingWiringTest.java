package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Proves row decoding is wired through the public R2DBC factory path. */
class RowDecodingWiringTest {

  @Test
  void shouldDecodeRowsThroughThePublicR2dbcFactoryPath() {
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
  void shouldRejectTheRemovedRowDecoderOption() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.PORT, 1)
            .option(Option.valueOf("rowDecoder"), "native")
            .build();

    // when
    final Throwable thrown = catchThrowable(() -> ClickHouseConnectionFactory.from(options));

    // then
    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rowDecoder")
        .hasMessageContaining("removed");
  }
}

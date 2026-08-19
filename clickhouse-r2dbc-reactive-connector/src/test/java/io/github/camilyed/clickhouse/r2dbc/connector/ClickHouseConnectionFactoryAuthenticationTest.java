package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Proves exactly what HTTP Basic {@code Authorization} header value {@link
 * ClickHouseConnectionFactory#from(ConnectionFactoryOptions)} produces for the {@code user}/{@code
 * password} option combinations — hermetically, against {@link ControlledClickHouseServer}'s
 * captured request, rather than against a real ClickHouse server's own accept/reject decision
 * (which depends on server-side grants {@link
 * ClickHouseConnectionFactoryAgainstRealClickHouseTest}'s container user doesn't have, e.g. {@code
 * CREATE USER}). This is the more precise proof for a wire-format bug: it asserts on the literal
 * bytes sent, not on a downstream server's interpretation of them.
 */
class ClickHouseConnectionFactoryAuthenticationTest {

  @Test
  void shouldSendAnEmptyPasswordWhenUserIsPresentAndPasswordIsAbsent() {
    // given
    final byte[] responseBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(responseBody)) {
      final URI baseUrl = URI.create(server.baseUrl());
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
              .option(ConnectionFactoryOptions.HOST, baseUrl.getHost())
              .option(ConnectionFactoryOptions.PORT, baseUrl.getPort())
              .option(ConnectionFactoryOptions.USER, "alice")
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
      final Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(5));

      Flux.from(connection.createStatement("SELECT 1").execute())
          .flatMap(result -> Flux.from(result.map((row, metadata) -> row)))
          .blockLast(Duration.ofSeconds(5));

      // then
      final String expectedAuthorizationHeader =
          "Basic " + Base64.getEncoder().encodeToString("alice:".getBytes(StandardCharsets.UTF_8));
      assertThat(server.receivedHeader("Authorization")).isEqualTo(expectedAuthorizationHeader);
    }
  }

  @Test
  void shouldNotSendTheLiteralStringNullAsThePasswordWhenPasswordIsAbsent() {
    // given
    final byte[] responseBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(responseBody)) {
      final URI baseUrl = URI.create(server.baseUrl());
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
              .option(ConnectionFactoryOptions.HOST, baseUrl.getHost())
              .option(ConnectionFactoryOptions.PORT, baseUrl.getPort())
              .option(ConnectionFactoryOptions.USER, "alice")
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
      final Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(5));

      Flux.from(connection.createStatement("SELECT 1").execute())
          .flatMap(result -> Flux.from(result.map((row, metadata) -> row)))
          .blockLast(Duration.ofSeconds(5));

      // then
      final String regressionAuthorizationHeader =
          "Basic "
              + Base64.getEncoder().encodeToString("alice:null".getBytes(StandardCharsets.UTF_8));
      assertThat(server.receivedHeader("Authorization"))
          .isNotEqualTo(regressionAuthorizationHeader);
    }
  }

  @Test
  void shouldSendNoAuthorizationHeaderWhenUserIsAbsent() {
    // given
    final byte[] responseBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(responseBody)) {
      final URI baseUrl = URI.create(server.baseUrl());
      final ConnectionFactoryOptions options =
          ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
              .option(ConnectionFactoryOptions.HOST, baseUrl.getHost())
              .option(ConnectionFactoryOptions.PORT, baseUrl.getPort())
              .build();
      final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
      final Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(5));

      Flux.from(connection.createStatement("SELECT 1").execute())
          .flatMap(result -> Flux.from(result.map((row, metadata) -> row)))
          .blockLast(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedHeader("Authorization")).isNull();
    }
  }
}

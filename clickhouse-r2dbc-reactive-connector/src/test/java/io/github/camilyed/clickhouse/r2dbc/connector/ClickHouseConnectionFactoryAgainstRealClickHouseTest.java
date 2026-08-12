package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ValidationDepth;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * Proves {@link ClickHouseConnectionFactory#from(ConnectionFactoryOptions)} produces a {@link
 * Connection} that actually talks to a real ClickHouse server — not just that it constructs without
 * error, which {@link ClickHouseConnectionFactoryTest} already covers hermetically.
 */
class ClickHouseConnectionFactoryAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  @Test
  void shouldOpenAConnectionAgainstARealClickHouseServerAndValidateIt() {
    // given
    final URI httpUrl = URI.create(clickHouseHttpUrl());
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .option(ConnectionFactoryOptions.HOST, httpUrl.getHost())
            .option(ConnectionFactoryOptions.PORT, httpUrl.getPort())
            .option(ConnectionFactoryOptions.USER, clickHouseUsername())
            .option(ConnectionFactoryOptions.PASSWORD, clickHousePassword())
            .build();
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // when
    final Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(10));
    final Boolean valid =
        Mono.from(connection.validate(ValidationDepth.REMOTE)).block(Duration.ofSeconds(10));

    // then
    assertThat(valid).isTrue();
  }
}

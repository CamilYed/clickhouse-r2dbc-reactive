package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.NoSuchOptionException;
import org.junit.jupiter.api.Test;

class ClickHouseConnectionFactoryTest {

  @Test
  void shouldBuildAFactoryFromOptionsWithoutTouchingTheNetwork() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectOptionsWithNoHost() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(NoSuchOptionException.class);
  }
}

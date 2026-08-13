package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

class ClickHouseConnectionFactoryProviderTest {

  private final ClickHouseConnectionFactoryProvider provider =
      new ClickHouseConnectionFactoryProvider();

  @Test
  void shouldReportItsDriverIdentifier() {
    // when
    final String driver = provider.getDriver();

    // then
    assertThat(driver).isEqualTo("clickhouse");
  }

  @Test
  void shouldSupportOptionsConfiguredForTheClickHouseDriver() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .build();

    // when
    final boolean supported = provider.supports(options);

    // then
    assertThat(supported).isTrue();
  }

  @Test
  void shouldNotSupportOptionsConfiguredForAnotherDriver() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "postgresql")
            .build();

    // when
    final boolean supported = provider.supports(options);

    // then
    assertThat(supported).isFalse();
  }

  @Test
  void shouldNotSupportOptionsWithNoDriverAtAll() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .build();

    // when
    final boolean supported = provider.supports(options);

    // then
    assertThat(supported).isFalse();
  }

  @Test
  void shouldBeDiscoverableThroughTheStandardR2dbcServiceLoaderBootstrapPath() {
    // given - proves the META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider file actually
    // works, not just that this class's own methods behave correctly in isolation. Doesn't touch
    // the network: ConnectionFactories.get(...) only builds the ConnectionFactory, exactly like
    // ClickHouseConnectionFactory.from does on its own.
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .build();

    // when
    final ConnectionFactory connectionFactory = ConnectionFactories.get(options);

    // then
    assertThat(connectionFactory).isInstanceOf(ClickHouseConnectionFactory.class);
  }
}

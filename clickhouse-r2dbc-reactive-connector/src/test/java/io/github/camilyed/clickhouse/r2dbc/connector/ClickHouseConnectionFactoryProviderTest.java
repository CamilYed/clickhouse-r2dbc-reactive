package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

class ClickHouseConnectionFactoryProviderTest {

    private final ClickHouseConnectionFactoryProvider provider = new ClickHouseConnectionFactoryProvider();

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
                ConnectionFactoryOptions.builder().option(ConnectionFactoryOptions.DRIVER, "clickhouse").build();

        // when
        final boolean supported = provider.supports(options);

        // then
        assertThat(supported).isTrue();
    }

    @Test
    void shouldNotSupportOptionsConfiguredForAnotherDriver() {
        // given
        final ConnectionFactoryOptions options =
                ConnectionFactoryOptions.builder().option(ConnectionFactoryOptions.DRIVER, "postgresql").build();

        // when
        final boolean supported = provider.supports(options);

        // then
        assertThat(supported).isFalse();
    }

    @Test
    void shouldNotSupportOptionsWithNoDriverAtAll() {
        // given
        final ConnectionFactoryOptions options =
                ConnectionFactoryOptions.builder().option(ConnectionFactoryOptions.HOST, "localhost").build();

        // when
        final boolean supported = provider.supports(options);

        // then
        assertThat(supported).isFalse();
    }
}

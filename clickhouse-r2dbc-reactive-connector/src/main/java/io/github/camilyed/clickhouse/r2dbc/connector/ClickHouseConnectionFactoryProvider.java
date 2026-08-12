package io.github.camilyed.clickhouse.r2dbc.connector;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import java.util.ServiceLoader;

/**
 * R2DBC service-provider entry point for the ClickHouse driver.
 *
 * <p>Discovered via {@link ServiceLoader} once registered in {@code
 * META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider} — not registered yet. {@link
 * #create(ConnectionFactoryOptions)} now returns a working {@link ClickHouseConnectionFactory},
 * but {@code Statement.execute()} still isn't implemented (see {@code ClickHouseStatement}), so a
 * consumer discovering this driver via {@code ServiceLoader} today could open a connection but not
 * run a single query. The SPI file is added once that's no longer true.
 */
public final class ClickHouseConnectionFactoryProvider implements ConnectionFactoryProvider {

    /** The driver identifier this provider answers to, e.g. an {@code r2dbc:clickhouse://...} URL. */
    public static final String DRIVER = "clickhouse";

    @Override
    public ConnectionFactory create(final ConnectionFactoryOptions connectionFactoryOptions) {
        return ClickHouseConnectionFactory.from(connectionFactoryOptions);
    }

    @Override
    public boolean supports(final ConnectionFactoryOptions connectionFactoryOptions) {
        return DRIVER.equals(connectionFactoryOptions.getValue(ConnectionFactoryOptions.DRIVER));
    }

    @Override
    public String getDriver() {
        return DRIVER;
    }
}

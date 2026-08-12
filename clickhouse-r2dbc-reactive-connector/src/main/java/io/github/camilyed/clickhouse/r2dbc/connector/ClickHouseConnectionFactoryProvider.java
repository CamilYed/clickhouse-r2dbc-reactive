package io.github.camilyed.clickhouse.r2dbc.connector;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import java.util.ServiceLoader;

/**
 * R2DBC service-provider entry point for the ClickHouse driver.
 *
 * <p>Discovered via {@link ServiceLoader} once registered in {@code
 * META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider} — not registered yet, since {@link
 * #create(ConnectionFactoryOptions)} has no working {@code ConnectionFactory}/{@code Connection}
 * behind it yet. Registering the SPI file before {@code create()} actually works would let
 * discovery find a provider that can't create anything, which is worse than not being discoverable
 * at all; that happens once {@code create()} is real.
 */
public final class ClickHouseConnectionFactoryProvider implements ConnectionFactoryProvider {

    /** The driver identifier this provider answers to, e.g. an {@code r2dbc:clickhouse://...} URL. */
    public static final String DRIVER = "clickhouse";

    @Override
    public ConnectionFactory create(final ConnectionFactoryOptions connectionFactoryOptions) {
        throw new UnsupportedOperationException("ClickHouseConnectionFactory does not exist yet");
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

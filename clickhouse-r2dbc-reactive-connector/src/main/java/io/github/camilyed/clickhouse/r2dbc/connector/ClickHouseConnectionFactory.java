package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Mono;

/**
 * Creates {@link ClickHouseConnection}s against a ClickHouse server over its HTTP interface.
 *
 * <p>Every {@link Connection} produced by {@link #create()} shares this factory's single {@link
 * ClickHouseHttpTransport} — and therefore its connection pool — rather than each opening its own.
 */
public final class ClickHouseConnectionFactory implements ConnectionFactory {

    private static final int DEFAULT_HTTP_PORT = 8123;

    private final ClickHouseHttpTransport transport;

    ClickHouseConnectionFactory(final ClickHouseHttpTransport transport) {
        this.transport = transport;
    }

    /**
     * Builds a factory from R2DBC {@link ConnectionFactoryOptions} — {@code host} (required), {@code
     * port} (default {@value #DEFAULT_HTTP_PORT}), {@code ssl} (default {@code false}), and {@code
     * user}/{@code password} (default: no authentication, relying on the server allowing anonymous
     * access).
     */
    public static ClickHouseConnectionFactory from(final ConnectionFactoryOptions options) {
        final String host = (String) options.getRequiredValue(ConnectionFactoryOptions.HOST);
        final Integer port = (Integer) options.getValue(ConnectionFactoryOptions.PORT);
        final boolean ssl = Boolean.TRUE.equals(options.getValue(ConnectionFactoryOptions.SSL));
        final int resolvedPort = port == null ? DEFAULT_HTTP_PORT : port;
        final String baseUrl = (ssl ? "https" : "http") + "://" + host + ":" + resolvedPort;

        final String user = (String) options.getValue(ConnectionFactoryOptions.USER);
        final CharSequence password = (CharSequence) options.getValue(ConnectionFactoryOptions.PASSWORD);
        final Authentication authentication =
                user == null ? Authentication.none() : Authentication.basic(user, String.valueOf(password));

        return new ClickHouseConnectionFactory(new ClickHouseHttpTransport(baseUrl, authentication));
    }

    @Override
    public Mono<ClickHouseConnection> create() {
        return Mono.fromSupplier(() -> new ClickHouseConnection(transport));
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return ClickHouseConnectionFactoryMetadata.INSTANCE;
    }
}

package io.github.camilyed.clickhouse.r2dbc.transport.http;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/** Non-blocking HTTP transport for ClickHouse queries, built on Reactor Netty's {@link HttpClient}. */
public final class ClickHouseHttpTransport {

    private final HttpClient httpClient;

    public ClickHouseHttpTransport(final String baseUrl) {
        this(baseUrl, ConnectionProvider.create("clickhouse-http-transport"));
    }

    public ClickHouseHttpTransport(final String baseUrl, final int maxConnections) {
        this(baseUrl, ConnectionProvider.create("clickhouse-http-transport", maxConnections));
    }

    private ClickHouseHttpTransport(final String baseUrl, final ConnectionProvider connectionProvider) {
        this.httpClient = HttpClient.create(connectionProvider).baseUrl(baseUrl).responseTimeout(Duration.ofSeconds(2));
    }

    /**
     * Sends {@code sql} to ClickHouse and returns the response body as a stream of chunks.
     *
     * <p>Nothing is sent over the network until the returned {@link ByteBufFlux} is subscribed to.
     * Chunks are emitted as they arrive, never aggregated into a single buffer; cancelling the
     * subscription closes the underlying connection.
     */
    public ByteBufFlux query(final String sql) {
        return httpClient.post().uri("/?query=" + encode(sql)).responseContent();
    }

    private static String encode(final String sql) {
        return URLEncoder.encode(sql, StandardCharsets.UTF_8);
    }
}
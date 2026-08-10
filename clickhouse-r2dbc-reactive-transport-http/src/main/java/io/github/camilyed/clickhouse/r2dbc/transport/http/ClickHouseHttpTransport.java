package io.github.camilyed.clickhouse.r2dbc.transport.http;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/** Non-blocking HTTP transport for ClickHouse queries, built on Reactor Netty's {@link HttpClient}. */
public final class ClickHouseHttpTransport {

    private final HttpClient httpClient;
    private final Authentication authentication;

    public ClickHouseHttpTransport(final String baseUrl) {
        this(baseUrl, new Authentication.None(), ConnectionProvider.create("clickhouse-http-transport"));
    }

    public ClickHouseHttpTransport(final String baseUrl, final int maxConnections) {
        this(baseUrl, new Authentication.None(), ConnectionProvider.create("clickhouse-http-transport", maxConnections));
    }

    /** Authenticates every request with HTTP Basic auth, as required by a password-protected ClickHouse server. */
    public ClickHouseHttpTransport(final String baseUrl, final String user, final String password) {
        this(baseUrl, new Authentication.Basic(user, password), ConnectionProvider.create("clickhouse-http-transport"));
    }

    private ClickHouseHttpTransport(
            final String baseUrl, final Authentication authentication, final ConnectionProvider connectionProvider) {
        this.httpClient = HttpClient.create(connectionProvider).baseUrl(baseUrl).responseTimeout(Duration.ofSeconds(2));
        this.authentication = authentication;
    }

    /**
     * Sends {@code query} to ClickHouse and returns the response body as a stream of chunks.
     *
     * <p>Nothing is sent over the network until the returned {@link ByteBufFlux} is subscribed to.
     * Chunks are emitted as they arrive, never aggregated into a single buffer; cancelling the
     * subscription closes the underlying connection. The result format is fixed to {@code
     * RowBinaryWithNamesAndTypes} — the one shape {@code core}'s decoder currently understands —
     * via the {@code X-ClickHouse-Format} header, so a bare query with no {@code FORMAT} clause
     * doesn't fall back to ClickHouse's default {@code TabSeparated}. {@link
     * ClickHouseQuery#queryId()} is sent as {@code X-ClickHouse-Query-Id}, so the server echoes it
     * back in responses/errors and it can later be used for {@code KILL QUERY} cancellation.
     */
    public ByteBufFlux query(final ClickHouseQuery query) {
        return httpClient
                .headers(headers -> {
                    headers.set("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes");
                    headers.set("X-ClickHouse-Query-Id", query.queryId());
                    authentication.addTo(headers);
                })
                .post()
                .uri("/?query=" + encode(query.sql()))
                .responseContent();
    }

    private static String encode(final String sql) {
        return URLEncoder.encode(sql, StandardCharsets.UTF_8);
    }
}
package io.github.camilyed.clickhouse.r2dbc.transport.http;

import com.clickhouse.client.api.ServerException;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.netty.buffer.ByteBuf;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;

/** Non-blocking HTTP transport for ClickHouse queries, built on Reactor Netty's {@link HttpClient}. */
public final class ClickHouseHttpTransport {

    private static final String FORMAT_HEADER = "X-ClickHouse-Format";
    private static final String QUERY_ID_HEADER = "X-ClickHouse-Query-Id";
    private static final String EXCEPTION_CODE_HEADER = "X-ClickHouse-Exception-Code";

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
     * ClickHouseQuery#queryId()} is sent as {@code X-ClickHouse-Query-Id}, confirmed against a real
     * server to be sufficient on its own (see {@code docs/CLIENT_V2_HTTP_REFERENCE.md}).
     *
     * <p>A response carrying {@code X-ClickHouse-Exception-Code}, or any HTTP status {@code >= 400},
     * is never handed to the caller as if it were a valid body — client-v2's own {@code
     * HttpAPIClientHelper} treats {@code X-ClickHouse-Exception-Code} as authoritative regardless of
     * HTTP status (a {@code 200} does not guarantee success), and this transport does the same. The
     * returned {@link ByteBufFlux} instead terminates with client-v2's public {@link ServerException}
     * — reused rather than reimplemented, since it already carries ClickHouse's own numeric error
     * code. Our pinned client-v2 version (see the version catalog) predates that class's {@code
     * queryId}/{@code isRetryable()} fields, so {@link ClickHouseQuery#queryId()} is folded into the
     * message text instead — re-check this Javadoc if client-v2 is ever upgraded.
     */
    public ByteBufFlux query(final ClickHouseQuery query) {
        return ByteBufFlux.fromInbound(httpClient
                .headers(headers -> {
                    headers.set(FORMAT_HEADER, "RowBinaryWithNamesAndTypes");
                    headers.set(QUERY_ID_HEADER, query.queryId());
                    authentication.addTo(headers);
                })
                .post()
                .uri("/?query=" + encode(query.sql()))
                .response((response, content) -> receiveOrFail(response, content, query.queryId())));
    }

    private static Publisher<ByteBuf> receiveOrFail(
            final HttpClientResponse response, final ByteBufFlux content, final String queryId) {
        if (!isError(response)) {
            return content;
        }
        final int serverCode = exceptionCode(response);
        final int httpStatus = response.status().code();
        return content.aggregate()
                .asString(StandardCharsets.UTF_8)
                .defaultIfEmpty("")
                .flatMap(body -> Mono.error(
                        new ServerException(serverCode, body.strip() + " (queryId=" + queryId + ")", httpStatus)));
    }

    private static boolean isError(final HttpClientResponse response) {
        return response.responseHeaders().contains(EXCEPTION_CODE_HEADER) || response.status().code() >= 400;
    }

    private static int exceptionCode(final HttpClientResponse response) {
        final String header = response.responseHeaders().get(EXCEPTION_CODE_HEADER);
        if (header == null) {
            return ServerException.CODE_UNKNOWN;
        }
        try {
            return Integer.parseInt(header);
        } catch (final NumberFormatException e) {
            return ServerException.CODE_UNKNOWN;
        }
    }

    private static String encode(final String sql) {
        return URLEncoder.encode(sql, StandardCharsets.UTF_8);
    }
}
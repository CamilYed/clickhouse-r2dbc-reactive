package io.github.camilyed.clickhouse.r2dbc.transport.http;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;

public final class ClickHouseHttpTransport {

    private final HttpClient httpClient;

    public ClickHouseHttpTransport(final String baseUrl) {
        this.httpClient = HttpClient.create().baseUrl(baseUrl);
    }

    public ByteBufFlux query(final String sql) {
        return httpClient.post().uri("/?query=" + encode(sql)).responseContent();
    }

    private static String encode(final String sql) {
        return URLEncoder.encode(sql, StandardCharsets.UTF_8);
    }
}
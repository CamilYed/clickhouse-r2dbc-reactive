package io.github.camilyed.clickhouse.r2dbc.testkit;

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * A minimal, controlled stand-in for a ClickHouse HTTP endpoint, for transport contract tests.
 *
 * <p>Starts a real, non-blocking Reactor Netty {@link HttpServer} on a loopback port and answers
 * every {@code POST} with a fixed, pre-encoded response body — no ClickHouse instance required.
 *
 * <p>This first version only proves the shape end to end (headers, body bytes, deterministic
 * shutdown). Delayed headers/body, fragmented rows, slow-subscriber, pool-saturation, and
 * cancellation scenarios are added incrementally as the transport spike needs them (see
 * ROADMAP.md, Phase 1, step 7).
 */
public final class ControlledClickHouseServer implements AutoCloseable {

    private final DisposableServer server;

    private ControlledClickHouseServer(final DisposableServer server) {
        this.server = server;
    }

    public static ControlledClickHouseServer startRespondingToSelectOneWith(final byte[] responseBody) {
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> response.header(
                                "X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                        .header("Content-Type", "application/octet-stream")
                        .sendByteArray(Mono.just(responseBody))))
                .bindNow();
        return new ControlledClickHouseServer(started);
    }

    public int port() {
        return server.port();
    }

    public String baseUrl() {
        return "http://localhost:" + server.port();
    }

    @Override
    public void close() {
        server.disposeNow();
    }
}

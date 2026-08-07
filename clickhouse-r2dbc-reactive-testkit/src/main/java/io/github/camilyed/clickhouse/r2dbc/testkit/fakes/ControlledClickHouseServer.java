package io.github.camilyed.clickhouse.r2dbc.testkit.fakes;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

public final class ControlledClickHouseServer implements AutoCloseable {

    private final DisposableServer server;
    private final AtomicBoolean requestReceived;
    private final AtomicBoolean connectionClosed;

    private ControlledClickHouseServer(
            final DisposableServer server, final AtomicBoolean requestReceived, final AtomicBoolean connectionClosed) {
        this.server = server;
        this.requestReceived = requestReceived;
        this.connectionClosed = connectionClosed;
    }

    public static ControlledClickHouseServer startRespondingToSelectOneWith(final byte[] responseBody) {
        return startRespondingWith(Flux.just(responseBody));
    }

    public static ControlledClickHouseServer startRespondingToSelectOneWithChunks(final byte[]... chunks) {
        return startRespondingWith(Flux.fromArray(chunks).delayElements(Duration.ofMillis(50)));
    }

    public static ControlledClickHouseServer startRespondingWithFirstChunkThenHanging(final byte[] firstChunk) {
        return startRespondingWith(Flux.concat(Flux.just(firstChunk), Flux.never()));
    }

    public static ControlledClickHouseServer startRespondingToSelectOneWithDelay(final byte[] body, final Duration delay) {
        return startRespondingWith(Flux.just(body).delaySubscription(delay));
    }

    public static ControlledClickHouseServer startRespondingToSelectOneWithBodyDelay(final byte[] body, final Duration delay) {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        final AtomicBoolean connectionClosed = new AtomicBoolean(false);
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    response.withConnection(conn -> conn.onDispose(() -> connectionClosed.set(true)));
                    return response.header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .header("Content-Type", "application/octet-stream")
                            .sendHeaders()
                            .sendByteArray(Mono.just(body).delayElement(delay));
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed);
    }

    private static ControlledClickHouseServer startRespondingWith(final Flux<byte[]> body) {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        final AtomicBoolean connectionClosed = new AtomicBoolean(false);
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    response.withConnection(conn -> conn.onDispose(() -> connectionClosed.set(true)));
                    return response.header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .header("Content-Type", "application/octet-stream")
                            .sendByteArray(body);
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed);
    }

    public static ControlledClickHouseServer startAcceptingButNeverResponding() {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        final AtomicBoolean connectionClosed = new AtomicBoolean(false);
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    response.withConnection(conn -> conn.onDispose(() -> connectionClosed.set(true)));
                    return Mono.never();
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed);
    }

    public static ControlledClickHouseServer startRespondingThenResettingConnection(
            final byte[] firstChunk, final Duration beforeReset) {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        final AtomicBoolean connectionClosed = new AtomicBoolean(false);
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    response.withConnection(conn -> {
                        conn.onDispose(() -> connectionClosed.set(true));
                        Mono.delay(beforeReset).subscribe(ignored -> conn.channel().close());
                    });
                    return response.header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .header("Content-Type", "application/octet-stream")
                            .sendByteArray(Flux.concat(Flux.just(firstChunk), Flux.never()));
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed);
    }

    public boolean hasReceivedRequest() {
        return requestReceived.get();
    }

    public boolean hasClosedConnection() {
        return connectionClosed.get();
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
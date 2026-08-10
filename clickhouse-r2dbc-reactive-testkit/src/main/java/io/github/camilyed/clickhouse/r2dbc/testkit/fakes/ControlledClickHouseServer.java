package io.github.camilyed.clickhouse.r2dbc.testkit.fakes;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

public final class ControlledClickHouseServer implements AutoCloseable {

    private final DisposableServer server;
    private final AtomicBoolean requestReceived;
    private final AtomicBoolean connectionClosed;
    private final AtomicInteger activeConnections;
    private final AtomicReference<String> receivedQueryId;

    private ControlledClickHouseServer(
            final DisposableServer server,
            final AtomicBoolean requestReceived,
            final AtomicBoolean connectionClosed,
            final AtomicInteger activeConnections,
            final AtomicReference<String> receivedQueryId) {
        this.server = server;
        this.requestReceived = requestReceived;
        this.connectionClosed = connectionClosed;
        this.activeConnections = activeConnections;
        this.receivedQueryId = receivedQueryId;
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
        final AtomicInteger activeConnections = new AtomicInteger(0);
        final AtomicReference<String> receivedQueryId = new AtomicReference<>();
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    activeConnections.incrementAndGet();
                    receivedQueryId.set(request.requestHeaders().get("X-ClickHouse-Query-Id"));
                    response.withConnection(conn -> conn.onDispose(() -> {
                        connectionClosed.set(true);
                        activeConnections.decrementAndGet();
                    }));
                    return response.header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .header("Content-Type", "application/octet-stream")
                            .sendHeaders()
                            .sendByteArray(Mono.just(body).delayElement(delay));
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed, activeConnections, receivedQueryId);
    }

    private static ControlledClickHouseServer startRespondingWith(final Flux<byte[]> body) {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        final AtomicBoolean connectionClosed = new AtomicBoolean(false);
        final AtomicInteger activeConnections = new AtomicInteger(0);
        final AtomicReference<String> receivedQueryId = new AtomicReference<>();
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    activeConnections.incrementAndGet();
                    receivedQueryId.set(request.requestHeaders().get("X-ClickHouse-Query-Id"));
                    response.withConnection(conn -> conn.onDispose(() -> {
                        connectionClosed.set(true);
                        activeConnections.decrementAndGet();
                    }));
                    return response.header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .header("Content-Type", "application/octet-stream")
                            .sendByteArray(body);
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed, activeConnections, receivedQueryId);
    }

    public static ControlledClickHouseServer startAcceptingButNeverResponding() {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        final AtomicBoolean connectionClosed = new AtomicBoolean(false);
        final AtomicInteger activeConnections = new AtomicInteger(0);
        final AtomicReference<String> receivedQueryId = new AtomicReference<>();
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    activeConnections.incrementAndGet();
                    receivedQueryId.set(request.requestHeaders().get("X-ClickHouse-Query-Id"));
                    response.withConnection(conn -> conn.onDispose(() -> {
                        connectionClosed.set(true);
                        activeConnections.decrementAndGet();
                    }));
                    return Mono.never();
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed, activeConnections, receivedQueryId);
    }

    public static ControlledClickHouseServer startRespondingThenResettingConnection(
            final byte[] firstChunk, final Duration beforeReset) {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        final AtomicBoolean connectionClosed = new AtomicBoolean(false);
        final AtomicInteger activeConnections = new AtomicInteger(0);
        final AtomicReference<String> receivedQueryId = new AtomicReference<>();
        final DisposableServer started = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/", (request, response) -> {
                    requestReceived.set(true);
                    activeConnections.incrementAndGet();
                    receivedQueryId.set(request.requestHeaders().get("X-ClickHouse-Query-Id"));
                    response.withConnection(conn -> {
                        conn.onDispose(() -> {
                            connectionClosed.set(true);
                            activeConnections.decrementAndGet();
                        });
                        Mono.delay(beforeReset).subscribe(ignored -> conn.channel().close());
                    });
                    return response.header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .header("Content-Type", "application/octet-stream")
                            .sendByteArray(Flux.concat(Flux.just(firstChunk), Flux.never()));
                }))
                .bindNow();
        return new ControlledClickHouseServer(started, requestReceived, connectionClosed, activeConnections, receivedQueryId);
    }

    public boolean hasReceivedRequest() {
        return requestReceived.get();
    }

    public boolean hasClosedConnection() {
        return connectionClosed.get();
    }

    public int activeConnectionCount() {
        return activeConnections.get();
    }

    /** The {@code X-ClickHouse-Query-Id} header value from the most recent request, if any was received. */
    public String receivedQueryId() {
        return receivedQueryId.get();
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
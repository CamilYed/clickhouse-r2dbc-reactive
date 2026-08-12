package io.github.camilyed.clickhouse.r2dbc.testkit.fakes;

import io.netty.handler.codec.http.HttpHeaders;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * A minimal, in-process fake of ClickHouse's HTTP interface, for forcing wire-level conditions a
 * real server won't reliably give you on demand (delayed/fragmented responses, a connection reset
 * mid-body, a hanging response, a ClickHouse-style error) — see ROADMAP.md's "Why both a fake
 * server and real ClickHouse" note for the full rationale. Each {@code startRespondingXxx} factory
 * starts a new server bound to a random free port; call {@link #close()} to stop it.
 */
public final class ControlledClickHouseServer implements AutoCloseable {

  private final DisposableServer server;
  private final AtomicBoolean requestReceived;
  private final AtomicBoolean connectionClosed;
  private final AtomicInteger activeConnections;
  private final AtomicReference<HttpHeaders> receivedHeaders;

  private ControlledClickHouseServer(
      final DisposableServer server,
      final AtomicBoolean requestReceived,
      final AtomicBoolean connectionClosed,
      final AtomicInteger activeConnections,
      final AtomicReference<HttpHeaders> receivedHeaders) {
    this.server = server;
    this.requestReceived = requestReceived;
    this.connectionClosed = connectionClosed;
    this.activeConnections = activeConnections;
    this.receivedHeaders = receivedHeaders;
  }

  /** Responds to every request with {@code responseBody} in a single chunk, immediately. */
  public static ControlledClickHouseServer startRespondingToSelectOneWith(
      final byte[] responseBody) {
    return startRespondingWith(Flux.just(responseBody));
  }

  /** Responds with {@code chunks} spread out over time, so the body arrives fragmented. */
  public static ControlledClickHouseServer startRespondingToSelectOneWithChunks(
      final byte[]... chunks) {
    return startRespondingWith(Flux.fromArray(chunks).delayElements(Duration.ofMillis(50)));
  }

  /** Sends {@code firstChunk}, then never completes the response — simulates a stalled body. */
  public static ControlledClickHouseServer startRespondingWithFirstChunkThenHanging(
      final byte[] firstChunk) {
    return startRespondingWith(Flux.concat(Flux.just(firstChunk), Flux.never()));
  }

  /** Waits {@code delay} before sending {@code body} at all — simulates delayed headers. */
  public static ControlledClickHouseServer startRespondingToSelectOneWithDelay(
      final byte[] body, final Duration delay) {
    return startRespondingWith(Flux.just(body).delaySubscription(delay));
  }

  /** Sends headers immediately, then waits {@code delay} before sending {@code body}. */
  public static ControlledClickHouseServer startRespondingToSelectOneWithBodyDelay(
      final byte[] body, final Duration delay) {
    final AtomicBoolean requestReceived = new AtomicBoolean(false);
    final AtomicBoolean connectionClosed = new AtomicBoolean(false);
    final AtomicInteger activeConnections = new AtomicInteger(0);
    final AtomicReference<HttpHeaders> receivedHeaders = new AtomicReference<>();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          requestReceived.set(true);
                          activeConnections.incrementAndGet();
                          receivedHeaders.set(request.requestHeaders());
                          response.withConnection(
                              conn ->
                                  conn.onDispose(
                                      () -> {
                                        connectionClosed.set(true);
                                        activeConnections.decrementAndGet();
                                      }));
                          return response
                              .header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                              .header("Content-Type", "application/octet-stream")
                              .sendHeaders()
                              .sendByteArray(Mono.just(body).delayElement(delay));
                        }))
            .bindNow();
    return new ControlledClickHouseServer(
        started, requestReceived, connectionClosed, activeConnections, receivedHeaders);
  }

  private static ControlledClickHouseServer startRespondingWith(final Flux<byte[]> body) {
    final AtomicBoolean requestReceived = new AtomicBoolean(false);
    final AtomicBoolean connectionClosed = new AtomicBoolean(false);
    final AtomicInteger activeConnections = new AtomicInteger(0);
    final AtomicReference<HttpHeaders> receivedHeaders = new AtomicReference<>();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          requestReceived.set(true);
                          activeConnections.incrementAndGet();
                          receivedHeaders.set(request.requestHeaders());
                          response.withConnection(
                              conn ->
                                  conn.onDispose(
                                      () -> {
                                        connectionClosed.set(true);
                                        activeConnections.decrementAndGet();
                                      }));
                          return response
                              .header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                              .header("Content-Type", "application/octet-stream")
                              .sendByteArray(body);
                        }))
            .bindNow();
    return new ControlledClickHouseServer(
        started, requestReceived, connectionClosed, activeConnections, receivedHeaders);
  }

  /** Accepts the connection and reads the request, but never sends any response at all. */
  public static ControlledClickHouseServer startAcceptingButNeverResponding() {
    final AtomicBoolean requestReceived = new AtomicBoolean(false);
    final AtomicBoolean connectionClosed = new AtomicBoolean(false);
    final AtomicInteger activeConnections = new AtomicInteger(0);
    final AtomicReference<HttpHeaders> receivedHeaders = new AtomicReference<>();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          requestReceived.set(true);
                          activeConnections.incrementAndGet();
                          receivedHeaders.set(request.requestHeaders());
                          response.withConnection(
                              conn ->
                                  conn.onDispose(
                                      () -> {
                                        connectionClosed.set(true);
                                        activeConnections.decrementAndGet();
                                      }));
                          return Mono.never();
                        }))
            .bindNow();
    return new ControlledClickHouseServer(
        started, requestReceived, connectionClosed, activeConnections, receivedHeaders);
  }

  /**
   * Responds with a ClickHouse-style error: the exception-code header, plus the given HTTP status
   * and body.
   */
  public static ControlledClickHouseServer startRespondingWithClickHouseError(
      final int errorCode, final String message, final int httpStatus) {
    final AtomicBoolean requestReceived = new AtomicBoolean(false);
    final AtomicBoolean connectionClosed = new AtomicBoolean(false);
    final AtomicInteger activeConnections = new AtomicInteger(0);
    final AtomicReference<HttpHeaders> receivedHeaders = new AtomicReference<>();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          requestReceived.set(true);
                          activeConnections.incrementAndGet();
                          receivedHeaders.set(request.requestHeaders());
                          response.withConnection(
                              conn ->
                                  conn.onDispose(
                                      () -> {
                                        connectionClosed.set(true);
                                        activeConnections.decrementAndGet();
                                      }));
                          return response
                              .status(httpStatus)
                              .header("X-ClickHouse-Exception-Code", String.valueOf(errorCode))
                              .sendString(Mono.just(message));
                        }))
            .bindNow();
    return new ControlledClickHouseServer(
        started, requestReceived, connectionClosed, activeConnections, receivedHeaders);
  }

  /** Sends {@code firstChunk}, then resets the TCP connection after {@code beforeReset}. */
  public static ControlledClickHouseServer startRespondingThenResettingConnection(
      final byte[] firstChunk, final Duration beforeReset) {
    final AtomicBoolean requestReceived = new AtomicBoolean(false);
    final AtomicBoolean connectionClosed = new AtomicBoolean(false);
    final AtomicInteger activeConnections = new AtomicInteger(0);
    final AtomicReference<HttpHeaders> receivedHeaders = new AtomicReference<>();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          requestReceived.set(true);
                          activeConnections.incrementAndGet();
                          receivedHeaders.set(request.requestHeaders());
                          response.withConnection(
                              conn -> {
                                conn.onDispose(
                                    () -> {
                                      connectionClosed.set(true);
                                      activeConnections.decrementAndGet();
                                    });
                                Mono.delay(beforeReset)
                                    .subscribe(ignored -> conn.channel().close());
                              });
                          return response
                              .header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                              .header("Content-Type", "application/octet-stream")
                              .sendByteArray(Flux.concat(Flux.just(firstChunk), Flux.never()));
                        }))
            .bindNow();
    return new ControlledClickHouseServer(
        started, requestReceived, connectionClosed, activeConnections, receivedHeaders);
  }

  /** Whether this server has received at least one request since it started. */
  public boolean hasReceivedRequest() {
    return requestReceived.get();
  }

  /** Whether the most recent connection to this server has been closed. */
  public boolean hasClosedConnection() {
    return connectionClosed.get();
  }

  /** How many connections to this server are currently open. */
  public int activeConnectionCount() {
    return activeConnections.get();
  }

  /**
   * The {@code X-ClickHouse-Query-Id} header value from the most recent request, if any was
   * received.
   */
  public @Nullable String receivedQueryId() {
    return receivedHeader("X-ClickHouse-Query-Id");
  }

  /** Any header value from the most recent request, if any was received; {@code null} if absent. */
  public @Nullable String receivedHeader(final String name) {
    final HttpHeaders headers = receivedHeaders.get();
    return headers == null ? null : headers.get(name);
  }

  /** The random free port this server bound to. */
  public int port() {
    return server.port();
  }

  /** This server's base URL, e.g. {@code http://localhost:54321}. */
  public String baseUrl() {
    return "http://localhost:" + server.port();
  }

  @Override
  public void close() {
    server.disposeNow();
  }
}

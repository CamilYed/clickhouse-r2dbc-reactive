package io.github.camilyed.clickhouse.r2dbc.testkit.fakes;

import io.netty.handler.codec.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

/**
 * A minimal, in-process fake of ClickHouse's HTTP interface, for forcing wire-level conditions a
 * real server won't reliably give you on demand (delayed/fragmented responses, a connection reset
 * mid-body, a hanging response, a ClickHouse-style error) — see ROADMAP.md's "Why both a fake
 * server and real ClickHouse" note for the full rationale. Each {@code startRespondingXxx} factory
 * starts a new server bound to a random free port; call {@link #close()} to stop it.
 */
public final class ControlledClickHouseServer implements AutoCloseable {

  private static final String ROW_BINARY_WITH_NAMES_AND_TYPES = "RowBinaryWithNamesAndTypes";
  private static final String CLICKHOUSE_FORMAT_HEADER = "X-ClickHouse-Format";
  private static final String CONTENT_TYPE_HEADER = "Content-Type";
  private static final String OCTET_STREAM = "application/octet-stream";

  private final DisposableServer server;
  private final RequestTracking tracking;

  private ControlledClickHouseServer(
      final DisposableServer server, final RequestTracking tracking) {
    this.server = server;
    this.tracking = tracking;
  }

  /**
   * Everything this fake server tracks about the requests/connections it has seen, bundled into one
   * value so the {@link ControlledClickHouseServer} constructor and every {@code
   * startRespondingXxx} factory don't each carry seven separate {@code Atomic*} parameters.
   */
  private record RequestTracking(
      AtomicBoolean requestReceived,
      AtomicBoolean connectionClosed,
      AtomicInteger activeConnections,
      AtomicInteger totalRequestsReceived,
      AtomicReference<HttpHeaders> receivedHeaders,
      AtomicReference<String> receivedUri,
      AtomicReference<byte[]> receivedBody) {

    static RequestTracking newTracking() {
      return new RequestTracking(
          new AtomicBoolean(false),
          new AtomicBoolean(false),
          new AtomicInteger(0),
          new AtomicInteger(0),
          new AtomicReference<>(),
          new AtomicReference<>(),
          new AtomicReference<>());
    }

    void recordRequestStart(final HttpServerRequest request) {
      requestReceived.set(true);
      activeConnections.incrementAndGet();
      totalRequestsReceived.incrementAndGet();
      receivedHeaders.set(request.requestHeaders());
      receivedUri.set(request.uri());
    }

    void trackConnectionLifecycle(final HttpServerResponse response) {
      response.withConnection(
          conn ->
              conn.onDispose(
                  () -> {
                    connectionClosed.set(true);
                    activeConnections.decrementAndGet();
                  }));
    }
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
    final RequestTracking tracking = RequestTracking.newTracking();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          tracking.recordRequestStart(request);
                          tracking.trackConnectionLifecycle(response);
                          return response
                              .header(CLICKHOUSE_FORMAT_HEADER, ROW_BINARY_WITH_NAMES_AND_TYPES)
                              .header(CONTENT_TYPE_HEADER, OCTET_STREAM)
                              .sendHeaders()
                              .sendByteArray(Mono.just(body).delayElement(delay));
                        }))
            .bindNow();
    return new ControlledClickHouseServer(started, tracking);
  }

  /**
   * Responds to every request with {@code responseBody}, plus {@code summaryJson} as the {@code
   * X-ClickHouse-Summary} header — the header ClickHouse itself sends carrying {@code
   * written_rows}/{@code read_rows}/etc. as JSON (see clickhouse.com/docs/interfaces/http).
   */
  public static ControlledClickHouseServer startRespondingToSelectOneWithSummary(
      final byte[] responseBody, final String summaryJson) {
    final RequestTracking tracking = RequestTracking.newTracking();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          tracking.recordRequestStart(request);
                          tracking.trackConnectionLifecycle(response);
                          return response
                              .header(CLICKHOUSE_FORMAT_HEADER, ROW_BINARY_WITH_NAMES_AND_TYPES)
                              .header(CONTENT_TYPE_HEADER, OCTET_STREAM)
                              .header("X-ClickHouse-Summary", summaryJson)
                              .sendByteArray(Mono.just(responseBody));
                        }))
            .bindNow();
    return new ControlledClickHouseServer(started, tracking);
  }

  /**
   * Accepts a request WITH a streamed body (an INSERT), buffers the received body bytes so a test
   * can assert on exactly what was streamed (see {@link #receivedRequestBody()}), then responds
   * with {@code summaryJson} as the {@code X-ClickHouse-Summary} header — the same summary
   * mechanism {@link #startRespondingToSelectOneWithSummary} uses for reads, since ClickHouse sends
   * {@code written_rows} back for INSERTs the same way it sends {@code read_rows} back for SELECTs
   * (see clickhouse.com/docs/interfaces/http).
   */
  public static ControlledClickHouseServer startAcceptingInsertsAndRespondingWithSummary(
      final String summaryJson) {
    final RequestTracking tracking = RequestTracking.newTracking();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          tracking.recordRequestStart(request);
                          tracking.trackConnectionLifecycle(response);
                          return request
                              .receive()
                              .aggregate()
                              .asByteArray()
                              .defaultIfEmpty(new byte[0])
                              .flatMap(
                                  bytes -> {
                                    tracking.receivedBody().set(bytes);
                                    return response
                                        .header("X-ClickHouse-Summary", summaryJson)
                                        .sendByteArray(Mono.just(new byte[0]))
                                        .then();
                                  });
                        }))
            .bindNow();
    return new ControlledClickHouseServer(started, tracking);
  }

  private static ControlledClickHouseServer startRespondingWith(final Flux<byte[]> body) {
    final RequestTracking tracking = RequestTracking.newTracking();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          tracking.recordRequestStart(request);
                          tracking.trackConnectionLifecycle(response);
                          return response
                              .header(CLICKHOUSE_FORMAT_HEADER, ROW_BINARY_WITH_NAMES_AND_TYPES)
                              .header(CONTENT_TYPE_HEADER, OCTET_STREAM)
                              .sendByteArray(body);
                        }))
            .bindNow();
    return new ControlledClickHouseServer(started, tracking);
  }

  /** Accepts the connection and reads the request, but never sends any response at all. */
  public static ControlledClickHouseServer startAcceptingButNeverResponding() {
    final RequestTracking tracking = RequestTracking.newTracking();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          tracking.recordRequestStart(request);
                          tracking.trackConnectionLifecycle(response);
                          return Mono.never();
                        }))
            .bindNow();
    return new ControlledClickHouseServer(started, tracking);
  }

  /**
   * Responds with a ClickHouse-style error: the exception-code header, plus the given HTTP status
   * and body.
   */
  public static ControlledClickHouseServer startRespondingWithClickHouseError(
      final int errorCode, final String message, final int httpStatus) {
    final RequestTracking tracking = RequestTracking.newTracking();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          tracking.recordRequestStart(request);
                          tracking.trackConnectionLifecycle(response);
                          return response
                              .status(httpStatus)
                              .header("X-ClickHouse-Exception-Code", String.valueOf(errorCode))
                              .sendString(Mono.just(message));
                        }))
            .bindNow();
    return new ControlledClickHouseServer(started, tracking);
  }

  /** Sends {@code firstChunk}, then resets the TCP connection after {@code beforeReset}. */
  public static ControlledClickHouseServer startRespondingThenResettingConnection(
      final byte[] firstChunk, final Duration beforeReset) {
    final RequestTracking tracking = RequestTracking.newTracking();
    final DisposableServer started =
        HttpServer.create()
            .port(0)
            .route(
                routes ->
                    routes.post(
                        "/",
                        (request, response) -> {
                          tracking.recordRequestStart(request);
                          response.withConnection(
                              conn -> {
                                conn.onDispose(
                                    () -> {
                                      tracking.connectionClosed().set(true);
                                      tracking.activeConnections().decrementAndGet();
                                    });
                                Mono.delay(beforeReset)
                                    .subscribe(ignored -> conn.channel().close());
                              });
                          return response
                              .header(CLICKHOUSE_FORMAT_HEADER, ROW_BINARY_WITH_NAMES_AND_TYPES)
                              .header(CONTENT_TYPE_HEADER, OCTET_STREAM)
                              .sendByteArray(Flux.concat(Flux.just(firstChunk), Flux.never()));
                        }))
            .bindNow();
    return new ControlledClickHouseServer(started, tracking);
  }

  /** Whether this server has received at least one request since it started. */
  public boolean hasReceivedRequest() {
    return tracking.requestReceived().get();
  }

  /** Whether the most recent connection to this server has been closed. */
  public boolean hasClosedConnection() {
    return tracking.connectionClosed().get();
  }

  /** How many connections to this server are currently open. */
  public int activeConnectionCount() {
    return tracking.activeConnections().get();
  }

  /** How many requests this server has received in total since it started. */
  public int totalRequestsReceived() {
    return tracking.totalRequestsReceived().get();
  }

  /**
   * The {@code X-ClickHouse-Query-Id} header value from the most recent request, if any was
   * received.
   */
  public @Nullable String receivedQueryId() {
    return receivedHeader("X-ClickHouse-Query-Id");
  }

  /** The request URI (path + query string) from the most recent request, if any was received. */
  public @Nullable String receivedUri() {
    return tracking.receivedUri().get();
  }

  /** Any header value from the most recent request, if any was received; {@code null} if absent. */
  public @Nullable String receivedHeader(final String name) {
    final HttpHeaders headers = tracking.receivedHeaders().get();
    return headers == null ? null : headers.get(name);
  }

  /**
   * The full request body bytes received from the most recent request, if any was received and this
   * server was started via a factory that captures the body (currently only {@link
   * #startAcceptingInsertsAndRespondingWithSummary}); {@code null} otherwise.
   */
  public byte @Nullable [] receivedRequestBody() {
    return tracking.receivedBody().get();
  }

  /**
   * {@link #receivedRequestBody()} decoded as UTF-8 text, for asserting on human-readable insert
   * payloads (e.g. {@code TabSeparated}/{@code CSV}) without the caller handling byte arrays.
   */
  public @Nullable String receivedRequestBodyAsString() {
    final byte[] bytes = tracking.receivedBody().get();
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
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

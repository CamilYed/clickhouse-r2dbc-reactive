package io.github.camilyed.clickhouse.r2dbc.transport.http;

import com.clickhouse.client.api.ServerException;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import java.io.ByteArrayInputStream;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.Http11SslContextSpec;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.retry.Retry;

/**
 * Non-blocking HTTP transport for ClickHouse queries, built on Reactor Netty's {@link HttpClient}.
 *
 * <p>Owns a dedicated {@link ConnectionProvider} (never Reactor Netty's process-wide default) —
 * call {@link #dispose()} or {@link #disposeLater()} once this instance is no longer needed to
 * release its pooled connections; see those methods' Javadoc.
 */
public final class ClickHouseHttpTransport {

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseHttpTransport.class);

  private static final String FORMAT_HEADER = "X-ClickHouse-Format";
  private static final String QUERY_ID_HEADER = "X-ClickHouse-Query-Id";
  private static final String DATABASE_HEADER = "X-ClickHouse-Database";
  private static final String EXCEPTION_CODE_HEADER = "X-ClickHouse-Exception-Code";
  private static final String SUMMARY_HEADER = "X-ClickHouse-Summary";
  private static final Pattern WRITTEN_ROWS_PATTERN =
      Pattern.compile("\"written_rows\"\\s*:\\s*\"(\\d+)\"");

  /**
   * Appended unconditionally to every {@link #queryWithSummary} request so a {@code JSON} column,
   * if the result set has one, comes back as a plain string rather than ClickHouse's default binary
   * encoding for the type — matched on the decode side by {@code core}'s {@code
   * RowBinaryDecoder#newReader}, which sets the corresponding local {@code QuerySettings} flag so
   * client-v2's reader expects the same thing. Harmless (a no-op) for queries with no JSON column,
   * so this is sent unconditionally rather than exposed as an opt-in {@code
   * ConnectionFactoryOptions} setting — no extra configuration for a caller (e.g. Spring's {@code
   * DatabaseClient}) that just wants JSON columns to work.
   */
  private static final String JSON_AS_STRING_QUERY_PARAM =
      "&output_format_binary_write_json_as_string=1";

  private static final String CONNECTION_PROVIDER_NAME = "clickhouse-http-transport";

  private final HttpClient httpClient;
  private final ConnectionProvider connectionProvider;
  private final Authentication authentication;
  private final RetryPolicy retryPolicy;
  private final @Nullable String database;
  private final ResponseCompression responseCompression;

  public ClickHouseHttpTransport(final String baseUrl) {
    this(baseUrl, TransportOptions.defaults());
  }

  public ClickHouseHttpTransport(final String baseUrl, final int maxConnections) {
    this(baseUrl, TransportOptions.defaults().withMaxConnections(maxConnections));
  }

  /**
   * Authenticates every request with the given {@link Authentication} mode and bounds the
   * underlying connection pool to {@code maxConnections} — the combination every other constructor
   * is missing (each configures one or the other, never both together). Added specifically so a
   * caller (e.g. a benchmark modeling "many logical concurrent queries over a small, deliberately
   * bounded connection pool") can size the pool explicitly on an authenticated server, instead of
   * being stuck with {@link ConnectionProvider#create(String)}'s default sizing. Every other
   * parameter this transport supports ({@code responseTimeout}, {@code connectTimeout}, a custom
   * trusted certificate, {@link RetryPolicy}) keeps its default via this constructor — use the
   * general-entry-point constructors below directly if one of those also needs to be non-default at
   * the same time.
   */
  public ClickHouseHttpTransport(
      final String baseUrl, final Authentication authentication, final int maxConnections) {
    this(
        baseUrl,
        TransportOptions.defaults()
            .withAuthentication(authentication)
            .withMaxConnections(maxConnections));
  }

  /**
   * Authenticates every request with HTTP Basic auth, as required by a password-protected
   * ClickHouse server.
   */
  public ClickHouseHttpTransport(final String baseUrl, final String user, final String password) {
    this(
        baseUrl,
        TransportOptions.defaults().withAuthentication(Authentication.basic(user, password)));
  }

  /**
   * Authenticates every request using the given {@link Authentication} mode — the general entry
   * point for auth modes beyond plain HTTP Basic (e.g. {@link Authentication#userKey}).
   */
  public ClickHouseHttpTransport(final String baseUrl, final Authentication authentication) {
    this(baseUrl, TransportOptions.defaults().withAuthentication(authentication));
  }

  /**
   * The general entry point for configuring a per-request response timeout alongside {@code
   * authentication}.
   *
   * <p>{@code responseTimeout} is {@code null} (no timeout at all) via every other constructor —
   * deliberately: ClickHouse is an analytical database where a legitimate query can easily run far
   * longer than a typical OLTP request, so this transport does not impose an arbitrary global time
   * limit on query execution unless a caller explicitly asks for one. An earlier version of this
   * class hardcoded a 2-second response timeout with no way to change it, which would have failed
   * every real query that took longer than that — corrected here. A caller that wants a hard
   * per-query time limit should configure one explicitly via this constructor; the R2DBC connector
   * separately supports a server-side limit through {@code Connection.setStatementTimeout} and
   * ClickHouse's {@code max_execution_time} setting.
   */
  public ClickHouseHttpTransport(
      final String baseUrl,
      final Authentication authentication,
      final @Nullable Duration responseTimeout) {
    this(
        baseUrl,
        TransportOptions.defaults()
            .withAuthentication(authentication)
            .withResponseTimeout(responseTimeout));
  }

  /**
   * The general entry point for configuring every timeout this transport supports: {@code
   * responseTimeout} (see the other constructor's Javadoc — how long to wait for a response once a
   * request has been sent) and {@code connectTimeout} (how long to wait for the underlying TCP
   * connection itself to establish, before any request is even sent). Both default to {@code null}
   * (no timeout) via every other constructor. Wired from R2DBC's standard {@code
   * ConnectionFactoryOptions.CONNECT_TIMEOUT} by {@code ClickHouseConnectionFactory.from} — without
   * this, a caller going through the standard R2DBC URL/options bootstrap path (as opposed to
   * constructing this class directly) had no way to bound how long connecting to an unreachable or
   * firewalled host could hang.
   */
  public ClickHouseHttpTransport(
      final String baseUrl,
      final Authentication authentication,
      final @Nullable Duration responseTimeout,
      final @Nullable Duration connectTimeout) {
    this(
        baseUrl,
        TransportOptions.defaults()
            .withAuthentication(authentication)
            .withResponseTimeout(responseTimeout)
            .withConnectTimeout(connectTimeout));
  }

  /**
   * The general entry point for configuring a custom trusted certificate for TLS handshakes against
   * an {@code https://} {@code baseUrl} — e.g. a self-signed or internal-CA certificate a
   * Kubernetes/service-mesh deployment (Tanzu and similar) commonly uses, which the JVM's default
   * trust store has no way to know about. {@code trustedCertificatePem} is raw PEM-encoded
   * certificate bytes (a single certificate or a chain); {@code null} via every other constructor,
   * which means "use the JVM's default trust store" — see {@code ClickHouseHttpTransportTlsTest}
   * for what a handshake against an untrusted self-signed certificate looks like in that case.
   *
   * <p>Passing a non-{@code null} certificate together with a {@code baseUrl} that isn't {@code
   * https://} is rejected eagerly with {@link IllegalArgumentException}: there is no TLS handshake
   * to trust a certificate for over plain HTTP, so silently ignoring the certificate would hide a
   * configuration mistake rather than surface it. Wired from the R2DBC-facing {@code sslRootCert}
   * connection option by {@code ClickHouseConnectionFactory.from}, modeled on r2dbc-postgresql's
   * {@code sslRootCert} option (classpath-resource-or-filesystem-path resolution happens there, not
   * here — this constructor only ever deals in already-resolved bytes, keeping this module free of
   * classpath/filesystem concerns).
   */
  public ClickHouseHttpTransport(
      final String baseUrl,
      final Authentication authentication,
      final @Nullable Duration responseTimeout,
      final @Nullable Duration connectTimeout,
      final byte @Nullable [] trustedCertificatePem) {
    this(
        baseUrl,
        TransportOptions.defaults()
            .withAuthentication(authentication)
            .withResponseTimeout(responseTimeout)
            .withConnectTimeout(connectTimeout)
            .withTrustedCertificatePem(trustedCertificatePem));
  }

  /**
   * The general entry point for configuring this transport's {@link RetryPolicy} — see that type's
   * Javadoc for exactly what gets retried and why it's safe to do so unconditionally, regardless of
   * whether a query is a {@code SELECT} or an {@code INSERT}. {@link RetryPolicy#defaultPolicy()}
   * via every other constructor; pass {@link RetryPolicy#disabled()} to turn retrying off entirely.
   * Wired from the R2DBC-facing {@code retryMaxAttempts}/{@code retryDelay} connection options by
   * {@code ClickHouseConnectionFactory.from}.
   */
  public ClickHouseHttpTransport(
      final String baseUrl,
      final Authentication authentication,
      final @Nullable Duration responseTimeout,
      final @Nullable Duration connectTimeout,
      final byte @Nullable [] trustedCertificatePem,
      final RetryPolicy retryPolicy) {
    this(
        baseUrl,
        TransportOptions.defaults()
            .withAuthentication(authentication)
            .withResponseTimeout(responseTimeout)
            .withConnectTimeout(connectTimeout)
            .withTrustedCertificatePem(trustedCertificatePem)
            .withRetryPolicy(retryPolicy));
  }

  /**
   * The general entry point every other constructor above delegates to: every construction path
   * routes through this single {@link TransportOptions} object rather than growing yet another
   * constructor overload each time a new option is added — see {@link TransportOptions}'s own
   * Javadoc, in particular for {@code maxConnections}/{@code pendingAcquireMaxCount}/{@code
   * pendingAcquireTimeout}/{@code maxIdleTime}/{@code maxLifeTime}, the transport-pool options only
   * reachable through this constructor (no shorthand overload exists for them, deliberately, to
   * avoid the overload growth this constructor exists to stop).
   */
  public ClickHouseHttpTransport(final String baseUrl, final TransportOptions options) {
    if (options.trustedCertificatePem() != null && !baseUrl.startsWith("https://")) {
      throw new IllegalArgumentException(
          "trustedCertificatePem can only be used with an https:// baseUrl, got: " + baseUrl);
    }
    final ConnectionProvider connectionProvider = buildConnectionProvider(options);
    HttpClient client = HttpClient.create(connectionProvider).baseUrl(baseUrl);
    if (options.responseTimeout() != null) {
      client = client.responseTimeout(options.responseTimeout());
    }
    if (options.connectTimeout() != null) {
      client =
          client.option(
              ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) options.connectTimeout().toMillis());
    }
    if (options.trustedCertificatePem() != null) {
      final byte[] trustedCertificatePem = options.trustedCertificatePem();
      client =
          client.secure(
              spec ->
                  spec.sslContext(
                      Http11SslContextSpec.forClient()
                          .configure(
                              builder ->
                                  builder.trustManager(
                                      new ByteArrayInputStream(trustedCertificatePem)))));
    }
    this.httpClient = client;
    this.connectionProvider = connectionProvider;
    this.authentication = options.authentication();
    this.retryPolicy = options.retryPolicy();
    this.database = options.database();
    this.responseCompression = options.responseCompression();
  }

  /**
   * Whether this transport asks ClickHouse to compress response bodies with its own custom LZ4
   * block framing (sent as {@code compress=1}, see {@link #queryWithSummary}) — {@link
   * TransportOptions#defaults()}'s default of {@link ResponseCompression#LZ4} unless overridden.
   * Read by {@code core.rowbinary.RowBinaryDecoder}'s caller so the decode side knows whether to
   * unwrap the same framing before decoding — see {@link ResponseCompression}'s Javadoc for why one
   * value threads both directions.
   */
  public ResponseCompression responseCompression() {
    return responseCompression;
  }

  /**
   * Releases every pooled connection this transport's underlying Reactor Netty {@link
   * ConnectionProvider} holds, and disposes the provider itself. Fire-and-forget — returns
   * immediately; the underlying connections are released asynchronously. Idempotent — safe to call
   * more than once. Use {@link #disposeLater()} instead if the caller needs to know once disposal
   * has actually finished (e.g. before exiting the JVM).
   *
   * <p>Not called automatically by anything in this class — see {@code
   * io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory#dispose()}, which
   * owns exactly one transport per factory and is responsible for disposing it once the factory
   * itself is no longer needed.
   */
  public void dispose() {
    connectionProvider.dispose();
  }

  /**
   * Same as {@link #dispose()}, but returns a {@link Mono} that completes once every pooled
   * connection has actually been released.
   */
  public Mono<Void> disposeLater() {
    return connectionProvider.disposeLater();
  }

  /**
   * Whether {@link #dispose()} (or a subscribed {@link #disposeLater()}) has already run.
   *
   * <p><b>Vacuously {@code true} on a transport that has never actually sent a request</b> —
   * delegates directly to {@link ConnectionProvider#isDisposed()}, whose actual implementation
   * (Reactor Netty's {@code PooledConnectionProvider}) is {@code channelPools.isEmpty() ||
   * ...allMatch(Disposable::isDisposed)}: no per-remote-host pool exists at all until the first
   * request is actually sent, and an empty collection vacuously satisfies {@code allMatch(...)}.
   * Not useful as an "is this transport ready to use" check before the first query — only
   * meaningful after at least one request has been sent, or after {@link #dispose()}/{@link
   * #disposeLater()} has actually been called.
   */
  public boolean isDisposed() {
    return connectionProvider.isDisposed();
  }

  // Every setter below is called only when the corresponding TransportOptions field is non-null,
  // leaving it unset otherwise - Reactor Netty's own ConnectionProvider.Builder then applies
  // exactly the same defaults its own static factory methods (ConnectionProvider.create(name)/
  // create(name, maxConnections)) already document, since those factories are themselves built on
  // top of this same builder.
  private static ConnectionProvider buildConnectionProvider(final TransportOptions options) {
    final ConnectionProvider.Builder builder = ConnectionProvider.builder(CONNECTION_PROVIDER_NAME);
    if (options.maxConnections() != null) {
      builder.maxConnections(options.maxConnections());
    }
    if (options.pendingAcquireMaxCount() != null) {
      builder.pendingAcquireMaxCount(options.pendingAcquireMaxCount());
    }
    if (options.pendingAcquireTimeout() != null) {
      builder.pendingAcquireTimeout(options.pendingAcquireTimeout());
    }
    if (options.maxIdleTime() != null) {
      builder.maxIdleTime(options.maxIdleTime());
    }
    if (options.maxLifeTime() != null) {
      builder.maxLifeTime(options.maxLifeTime());
    }
    return builder.build();
  }

  /**
   * Sends {@code query} to ClickHouse and returns the response body as a stream of chunks.
   *
   * <p>Nothing is sent over the network until the returned {@link ByteBufFlux} is subscribed to.
   * Chunks are emitted as they arrive, never aggregated into a single buffer; cancelling the
   * subscription closes the underlying connection <b>and</b>, if the request had already been sent,
   * best-effort sends an explicit {@code KILL QUERY WHERE query_id = '...' ASYNC} on a separate
   * request — see {@link #queryWithSummary}'s Javadoc for why this exists and its limits. The
   * result format is fixed to {@code RowBinaryWithNamesAndTypes} — the one shape {@code core}'s
   * decoder currently understands — via the {@code X-ClickHouse-Format} header, so a bare query
   * with no {@code FORMAT} clause doesn't fall back to ClickHouse's default {@code TabSeparated}.
   * {@link ClickHouseQuery#queryId()} is sent as {@code X-ClickHouse-Query-Id}, confirmed against a
   * real server to be sufficient on its own (see {@code docs/CLIENT_V2_HTTP_REFERENCE.md}).
   *
   * <p>A response carrying {@code X-ClickHouse-Exception-Code}, or any HTTP status {@code >= 400},
   * is never handed to the caller as if it were a valid body — client-v2's own {@code
   * HttpAPIClientHelper} treats {@code X-ClickHouse-Exception-Code} as authoritative regardless of
   * HTTP status (a {@code 200} does not guarantee success), and this transport does the same. The
   * returned {@link ByteBufFlux} instead terminates with client-v2's public {@link ServerException}
   * — reused rather than reimplemented, since it already carries ClickHouse's own numeric error
   * code. {@link ClickHouseQuery#queryId()} is passed as {@link ServerException}'s own {@code
   * queryId} constructor argument (available since client-v2 0.9.8 — see the version catalog), so
   * it is available via {@link ServerException#getQueryId()} rather than folded into the message
   * text. This transport also honors {@link ServerException#isRetryable()} when the caller opts in
   * through {@link ClickHouseQuery#withServerErrorRetryEnabled()} and no response bytes have been
   * emitted yet — see the retry contract below and {@link RetryPolicy}'s Javadoc.
   *
   * <p>{@link ClickHouseQuery#parameters()} — already encoded into ClickHouse's own wire format by
   * {@link ClickHouseQuery#withParameters(java.util.Map)} — are sent one {@code
   * param_<name>=<value>} query parameter per entry, alongside {@code query}, exactly as
   * ClickHouse's own parameterized- query mechanism expects (see {@code
   * docs/CLIENT_V2_HTTP_REFERENCE.md}). {@link ClickHouseQuery#settings()} are sent the same way
   * but with no {@code param_} prefix — ClickHouse's own server settings (e.g. {@code
   * max_execution_time}) are plain {@code <name>=<value>} request parameters, unrelated to the
   * {@code {name:Type}} placeholder mechanism {@code parameters()} feeds.
   *
   * <p>Delegates to {@link #queryWithSummary} — see that method if the caller also needs
   * ClickHouse's reported written-row count for this query.
   */
  public ByteBufFlux query(final ClickHouseQuery query) {
    return ByteBufFlux.fromInbound(
        queryWithSummary(query).flatMapMany(ClickHouseQueryResponse::body));
  }

  /**
   * Same as {@link #query(ClickHouseQuery)}, but also exposes ClickHouse's own reported written-row
   * count for {@code query}, parsed from the {@code X-ClickHouse-Summary} response header (see
   * clickhouse.com/docs/interfaces/http, "Response Buffering"/progress section). ClickHouse sends
   * this header on every response — {@code SELECT} included, where it reports {@code 0} written
   * rows — so {@link ClickHouseQueryResponse#writtenRows()} is always a real, server-reported count
   * rather than a guess.
   *
   * <p>Deliberately does <em>not</em> use an operator like {@code Flux.next()}/{@code single()} to
   * turn the underlying {@code .response(...)} call into this method's {@code Mono} return type —
   * either would cancel or otherwise terminate that response's subscription as soon as it emits its
   * one item, before the caller ever gets to subscribe to {@link ClickHouseQueryResponse#body()}
   * separately; Reactor Netty then has no reason to keep streaming a body nobody in its own
   * response-scope subscription is reading, so the body arrives empty. Instead, this constructs the
   * still-fully-lazy {@code body} {@link ByteBufFlux} synchronously (nothing sent over the network
   * yet, same as {@link #query(ClickHouseQuery)}) and wraps it in {@link Mono#just}, so the
   * <em>caller's own later subscription to {@code body}</em> is the one and only subscription to
   * the underlying HTTP response, exactly as in {@link #query(ClickHouseQuery)}.
   *
   * <p><b>Cancellation and {@code KILL QUERY}.</b> ClickHouse's own HTTP interface does not stop a
   * running query when the client's connection closes — verified against a real server, not assumed
   * (see {@code QueryCancellationAgainstRealClickHouseTest} and ROADMAP.md's Production readiness
   * review). To compensate, cancelling {@code body}'s subscription after the request has actually
   * been sent (via Reactor Netty's {@code doAfterRequest} hook — deliberately <em>not</em> gated on
   * a response ever arriving, since a slow query may not send any response bytes back for a long
   * time even though it is already running server-side; cancelling before the request is even sent
   * means there is nothing running server-side yet to kill) fires a best-effort, fire-and-forget
   * {@code KILL QUERY WHERE query_id = '...' ASYNC} over a separate request, reusing this
   * transport's own {@code authentication} — deliberately the same user as the original query,
   * since ClickHouse lets any user stop their own queries without a separate {@code KILL QUERY}
   * grant (see clickhouse.com/docs/reference/statements/kill, "Read-only users can only stop their
   * own queries"). If that kill attempt itself fails — most commonly because the connecting user
   * lacks the privilege, but also possibly a transport error — it is logged at {@code WARN} and
   * otherwise swallowed; it never surfaces as an error on the (already cancelled) original
   * subscription. This is best-effort, not a guarantee: if the kill request itself cannot reach the
   * server (e.g. the server is unreachable at all), the original query keeps running with no
   * further retry.
   *
   * <p><b>Retry.</b> A failure that happens strictly <em>before</em> the request has been fully
   * sent — the same {@code doAfterRequest}-driven signal the cancellation/kill logic above already
   * relies on — is retried according to this transport's {@link RetryPolicy}, since the server
   * never received any bytes of that attempt and retrying it cannot make the query run twice
   * server-side; see {@link RetryPolicy}'s Javadoc for the full reasoning and how this compares to
   * client-v2's own default retry behavior. A failure after the request was sent is, by default,
   * never retried here, for the same non-idempotency reason — <b>unless</b> {@code query} opted in
   * via {@link ClickHouseQuery#withServerErrorRetryEnabled()}, {@link
   * ServerException#isRetryable()} classifies the specific failure as retryable, and, critically,
   * no response bytes have been emitted downstream yet for this query (tracked independently of
   * {@code requestSent} — the request can be fully sent and ClickHouse can still fail before
   * writing any of the response body, which is exactly the window this extra opt-in widens retrying
   * into). Once any response bytes have been emitted, this transport never retries, opt-in or not —
   * see {@link ClickHouseQuery#withServerErrorRetryEnabled()}'s Javadoc for the full safety
   * reasoning and why this is a per-query decision rather than a blanket connection-level one.
   */
  public Mono<ClickHouseQueryResponse> queryWithSummary(final ClickHouseQuery query) {
    final AtomicLong writtenRows = new AtomicLong();
    final AtomicBoolean requestSent = new AtomicBoolean(false);
    final AtomicBoolean anyResponseBytesEmitted = new AtomicBoolean(false);
    final Flux<ByteBuf> response =
        httpClient
            .headers(
                headers -> {
                  headers.set(FORMAT_HEADER, "RowBinaryWithNamesAndTypes");
                  headers.set(QUERY_ID_HEADER, query.queryId());
                  if (database != null) {
                    headers.set(DATABASE_HEADER, database);
                  }
                  authentication.addTo(headers);
                })
            .doAfterRequest((request, connection) -> requestSent.set(true))
            .post()
            .uri(
                "/?query="
                    + encode(query.sql())
                    + parameterQueryString(query)
                    + settingsQueryString(query)
                    + compressionQueryString()
                    + JSON_AS_STRING_QUERY_PARAM)
            .response(
                (httpResponse, content) ->
                    receiveOrFail(httpResponse, content, query.queryId(), writtenRows))
            .doOnNext(ignored -> anyResponseBytesEmitted.set(true));
    final ByteBufFlux rawBody =
        ByteBufFlux.fromInbound(
            retryPolicy.isEnabled()
                ? response.retryWhen(
                    Retry.fixedDelay(retryPolicy.maxAttempts(), retryPolicy.delay())
                        .filter(
                            error ->
                                canRetry(
                                    error,
                                    query,
                                    requestSent.get(),
                                    anyResponseBytesEmitted.get())))
                : response);
    final ByteBufFlux body =
        ByteBufFlux.fromInbound(
            rawBody.doOnCancel(
                () -> {
                  if (requestSent.get()) {
                    killQueryBestEffort(query.queryId());
                  }
                }));
    return Mono.just(new ClickHouseQueryResponse(writtenRows::get, body));
  }

  /**
   * Whether a failure of {@code query} is safe to retry — see {@link #queryWithSummary}'s Javadoc
   * "Retry" section for the full reasoning behind each branch below.
   *
   * <p>{@code requestSent}/{@code anyResponseBytesEmitted} are read once per filter invocation
   * rather than passed as a single combined signal, since they answer two independently necessary
   * questions: whether <em>any</em> retry is even on the table for this failure shape
   * (pre-send-connection-level, or the opt-in server-error case), and, for the opt-in case, whether
   * it is still safe given what has already reached the caller.
   *
   * <p>Package-private rather than {@code private} deliberately: this is a small, pure decision
   * function with several independent branches (pre-send vs. post-send, bytes already emitted or
   * not, opted in or not, retryable or not), and the "bytes already emitted, then a retryable
   * server error" branch specifically is impractical to force through a real or fake HTTP server
   * (once response headers/body have started flowing successfully, a genuine mid-stream failure no
   * longer arrives as a clean {@code ServerException} the way a failure detected before any bytes
   * were sent does — see {@code MidStreamQueryFailureAgainstRealClickHouseTest}, item 5 in
   * ROADMAP.md's Phase 8). Testing this function directly with real {@link ServerException}
   * instances is more precise and complete than trying to reproduce every branch end to end, while
   * {@link ClickHouseHttpTransportTest}'s server-backed tests still separately prove the actual
   * {@code retryWhen} wiring engages this function correctly against a real response.
   */
  static boolean canRetry(
      final Throwable error,
      final ClickHouseQuery query,
      final boolean requestSent,
      final boolean anyResponseBytesEmitted) {
    if (!requestSent) {
      return true;
    }
    if (anyResponseBytesEmitted) {
      return false;
    }
    return query.serverErrorRetryEnabled()
        && error instanceof ServerException serverException
        && serverException.isRetryable();
  }

  /**
   * Sends {@code query} (an {@code INSERT}) with {@code data} streamed as the HTTP request body,
   * rather than embedded in the URL the way {@link #queryWithSummary} sends {@code query.sql()} —
   * the pattern ClickHouse's own HTTP interface documents as preferred for inserts, since it avoids
   * both loading the entire payload into memory and URL length limits (see
   * clickhouse.com/docs/interfaces/http, the {@code curl --data-binary @-} example). {@code data}
   * is bridged onto Reactor Netty's {@code Publisher<ByteBuf>} via {@link Unpooled#wrappedBuffer(
   * ByteBuffer)} with no copying or aggregation — chunks are written to the socket as they're
   * emitted, so this supports streaming an arbitrarily large insert without buffering it all
   * client-side first. As with {@link #queryWithSummary}, the input format defaults to {@code
   * RowBinaryWithNamesAndTypes} via the {@code X-ClickHouse-Format} header; a caller that wants a
   * different input format (e.g. {@code TabSeparated}, {@code CSV}) supplies an explicit {@code
   * FORMAT} clause in {@code query.sql()} itself, which ClickHouse treats as taking precedence over
   * the header (see {@code docs/CLIENT_V2_HTTP_REFERENCE.md}).
   *
   * <p><b>Never retried, unconditionally — this is the important difference from {@link
   * #queryWithSummary}.</b> That method's retry safety rests entirely on {@code requestSent}
   * (driven by Reactor Netty's {@code doAfterRequest}/{@code REQUEST_SENT} signal) meaning "zero
   * bytes of this attempt reached the server yet" for a request with no body — true because for a
   * bodyless request, flushing the headers <em>is</em> sending the whole request. That equivalence
   * breaks the moment a request has a body: {@code REQUEST_SENT} most likely only fires once the
   * entire request — headers <em>and</em> body — has been fully written to the socket, so there is
   * a window during body transmission where some of {@code data} may have already reached the
   * server while {@code requestSent} still reads {@code false}. Retrying in that window on a
   * connection-level failure could resend a partially-delivered insert, silently duplicating rows.
   * Rather than trying to track exactly how many bytes made it out (fragile, and not something
   * Reactor Netty exposes), this method disables retrying entirely for the streaming-body path — a
   * caller that wants insert-level retry safety should make its own {@code INSERT} idempotent (e.g.
   * via a distinct {@code query_id}-based dedup strategy, or by using a table engine that make
   * duplicates harmless) and retry at that level instead. See {@link RetryPolicy}'s Javadoc for why
   * the pre-send-only retry {@link #queryWithSummary} performs is safe for that method
   * specifically.
   *
   * <p>Cancellation triggers the same best-effort {@code KILL QUERY} as {@link #queryWithSummary}
   * once the request has been sent — see that method's Javadoc for the full reasoning and limits.
   */
  public Mono<ClickHouseQueryResponse> insertWithSummary(
      final ClickHouseQuery query, final Publisher<ByteBuffer> data) {
    final AtomicLong writtenRows = new AtomicLong();
    final AtomicBoolean requestSent = new AtomicBoolean(false);
    final Publisher<ByteBuf> requestBody = Flux.from(data).map(Unpooled::wrappedBuffer);
    final Flux<ByteBuf> response =
        httpClient
            .headers(
                headers -> {
                  headers.set(FORMAT_HEADER, "RowBinaryWithNamesAndTypes");
                  headers.set(QUERY_ID_HEADER, query.queryId());
                  if (database != null) {
                    headers.set(DATABASE_HEADER, database);
                  }
                  authentication.addTo(headers);
                })
            .doAfterRequest((request, connection) -> requestSent.set(true))
            .post()
            .uri(
                "/?query="
                    + encode(query.sql())
                    + parameterQueryString(query)
                    + settingsQueryString(query)
                    + compressionQueryString())
            .send(requestBody)
            .response(
                (httpResponse, content) ->
                    receiveOrFail(httpResponse, content, query.queryId(), writtenRows));
    final ByteBufFlux body =
        ByteBufFlux.fromInbound(
            response.doOnCancel(
                () -> {
                  if (requestSent.get()) {
                    killQueryBestEffort(query.queryId());
                  }
                }));
    return Mono.just(new ClickHouseQueryResponse(writtenRows::get, body));
  }

  private void killQueryBestEffort(final String queryId) {
    final String killSql =
        "KILL QUERY WHERE query_id = '" + escapeForSingleQuotedSql(queryId) + "' ASYNC";
    query(ClickHouseQuery.of(killSql))
        .aggregate()
        .asByteArray()
        .subscribe(
            ignored -> {},
            error ->
                LOG.warn(
                    "Best-effort KILL QUERY failed for cancelled query_id={} - ClickHouse may keep "
                        + "running it server-side. This commonly means the connecting user lacks "
                        + "the KILL QUERY privilege for its own queries.",
                    queryId,
                    error));
  }

  private static String escapeForSingleQuotedSql(final String value) {
    return value.replace("'", "''");
  }

  private static String parameterQueryString(final ClickHouseQuery query) {
    final StringBuilder queryString = new StringBuilder();
    query
        .parameters()
        .forEach(
            (name, value) ->
                queryString.append("&param_").append(name).append('=').append(encode(value)));
    return queryString.toString();
  }

  // Deliberately no "param_" prefix, unlike parameterQueryString above: these are raw ClickHouse
  // server settings (e.g. max_execution_time), not values bound to a {name:Type} placeholder
  // declared in the SQL text - see ClickHouseQuery.withSettings's Javadoc.
  private static String settingsQueryString(final ClickHouseQuery query) {
    final StringBuilder queryString = new StringBuilder();
    query
        .settings()
        .forEach(
            (name, value) ->
                queryString.append('&').append(name).append('=').append(encode(value)));
    return queryString.toString();
  }

  /**
   * {@code &compress=1} when {@link #responseCompression} is {@link ResponseCompression#LZ4},
   * otherwise empty — matches client-v2's own {@code HttpAPIClientHelper.addRequestParams} behavior
   * in its non-standard-HTTP-compression mode (verified against client-v2 0.9.8's real source, not
   * assumed): a plain ClickHouse query parameter, not a standard HTTP {@code Accept-Encoding}
   * header, is what actually turns on ClickHouse's own custom LZ4 response framing (see {@code
   * core.rowbinary.ClickHouseLz4InputStream}'s Javadoc for the wire format this then requires the
   * decode side to unwrap).
   */
  private String compressionQueryString() {
    return responseCompression == ResponseCompression.LZ4 ? "&compress=1" : "";
  }

  private static Publisher<ByteBuf> receiveOrFail(
      final HttpClientResponse response,
      final ByteBufFlux content,
      final String queryId,
      final AtomicLong writtenRows) {
    if (!isError(response)) {
      writtenRows.set(writtenRows(response));
      return content;
    }
    final int serverCode = exceptionCode(response);
    final int httpStatus = response.status().code();
    return content
        .aggregate()
        .asString(StandardCharsets.UTF_8)
        .defaultIfEmpty("")
        .flatMap(
            body -> Mono.error(new ServerException(serverCode, body.strip(), httpStatus, queryId)));
  }

  private static boolean isError(final HttpClientResponse response) {
    return response.responseHeaders().contains(EXCEPTION_CODE_HEADER)
        || response.status().code() >= 400;
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

  private static long writtenRows(final HttpClientResponse response) {
    final String summary = response.responseHeaders().get(SUMMARY_HEADER);
    if (summary == null) {
      return 0L;
    }
    final Matcher matcher = WRITTEN_ROWS_PATTERN.matcher(summary);
    return matcher.find() ? Long.parseLong(matcher.group(1)) : 0L;
  }

  private static String encode(final String sql) {
    return URLEncoder.encode(sql, StandardCharsets.UTF_8);
  }
}

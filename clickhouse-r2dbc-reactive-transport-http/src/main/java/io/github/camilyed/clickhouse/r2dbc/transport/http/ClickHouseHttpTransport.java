package io.github.camilyed.clickhouse.r2dbc.transport.http;

import com.clickhouse.client.api.ServerException;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import java.net.URLEncoder;
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
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;

/**
 * Non-blocking HTTP transport for ClickHouse queries, built on Reactor Netty's {@link HttpClient}.
 */
public final class ClickHouseHttpTransport {

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseHttpTransport.class);

  private static final String FORMAT_HEADER = "X-ClickHouse-Format";
  private static final String QUERY_ID_HEADER = "X-ClickHouse-Query-Id";
  private static final String EXCEPTION_CODE_HEADER = "X-ClickHouse-Exception-Code";
  private static final String SUMMARY_HEADER = "X-ClickHouse-Summary";
  private static final Pattern WRITTEN_ROWS_PATTERN =
      Pattern.compile("\"written_rows\"\\s*:\\s*\"(\\d+)\"");

  private final HttpClient httpClient;
  private final Authentication authentication;

  public ClickHouseHttpTransport(final String baseUrl) {
    this(
        baseUrl,
        Authentication.none(),
        ConnectionProvider.create("clickhouse-http-transport"),
        null,
        null);
  }

  public ClickHouseHttpTransport(final String baseUrl, final int maxConnections) {
    this(
        baseUrl,
        Authentication.none(),
        ConnectionProvider.create("clickhouse-http-transport", maxConnections),
        null,
        null);
  }

  /**
   * Authenticates every request with HTTP Basic auth, as required by a password-protected
   * ClickHouse server.
   */
  public ClickHouseHttpTransport(final String baseUrl, final String user, final String password) {
    this(
        baseUrl,
        Authentication.basic(user, password),
        ConnectionProvider.create("clickhouse-http-transport"),
        null,
        null);
  }

  /**
   * Authenticates every request using the given {@link Authentication} mode — the general entry
   * point for auth modes beyond plain HTTP Basic (e.g. {@link Authentication#userKey}).
   */
  public ClickHouseHttpTransport(final String baseUrl, final Authentication authentication) {
    this(
        baseUrl,
        authentication,
        ConnectionProvider.create("clickhouse-http-transport"),
        null,
        null);
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
   * per-query time limit should configure one explicitly via this constructor (or, once
   * implemented, per statement via {@code Connection.setStatementTimeout}).
   */
  public ClickHouseHttpTransport(
      final String baseUrl,
      final Authentication authentication,
      final @Nullable Duration responseTimeout) {
    this(
        baseUrl,
        authentication,
        ConnectionProvider.create("clickhouse-http-transport"),
        responseTimeout,
        null);
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
        authentication,
        ConnectionProvider.create("clickhouse-http-transport"),
        responseTimeout,
        connectTimeout);
  }

  private ClickHouseHttpTransport(
      final String baseUrl,
      final Authentication authentication,
      final ConnectionProvider connectionProvider,
      final @Nullable Duration responseTimeout,
      final @Nullable Duration connectTimeout) {
    HttpClient client = HttpClient.create(connectionProvider).baseUrl(baseUrl);
    if (responseTimeout != null) {
      client = client.responseTimeout(responseTimeout);
    }
    if (connectTimeout != null) {
      client = client.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis());
    }
    this.httpClient = client;
    this.authentication = authentication;
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
   * code. Our pinned client-v2 version (see the version catalog) predates that class's {@code
   * queryId}/{@code isRetryable()} fields, so {@link ClickHouseQuery#queryId()} is folded into the
   * message text instead — re-check this Javadoc if client-v2 is ever upgraded.
   *
   * <p>{@link ClickHouseQuery#parameters()} — already encoded into ClickHouse's own wire format by
   * {@link ClickHouseQuery#withParameters(java.util.Map)} — are sent one {@code
   * param_<name>=<value>} query parameter per entry, alongside {@code query}, exactly as
   * ClickHouse's own parameterized- query mechanism expects (see {@code
   * docs/CLIENT_V2_HTTP_REFERENCE.md}).
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
   */
  public Mono<ClickHouseQueryResponse> queryWithSummary(final ClickHouseQuery query) {
    final AtomicLong writtenRows = new AtomicLong();
    final AtomicBoolean requestSent = new AtomicBoolean(false);
    final ByteBufFlux rawBody =
        ByteBufFlux.fromInbound(
            httpClient
                .headers(
                    headers -> {
                      headers.set(FORMAT_HEADER, "RowBinaryWithNamesAndTypes");
                      headers.set(QUERY_ID_HEADER, query.queryId());
                      authentication.addTo(headers);
                    })
                .doAfterRequest((request, connection) -> requestSent.set(true))
                .post()
                .uri("/?query=" + encode(query.sql()) + parameterQueryString(query))
                .response(
                    (response, content) ->
                        receiveOrFail(response, content, query.queryId(), writtenRows)));
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
            body ->
                Mono.error(
                    new ServerException(
                        serverCode, body.strip() + " (queryId=" + queryId + ")", httpStatus)));
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

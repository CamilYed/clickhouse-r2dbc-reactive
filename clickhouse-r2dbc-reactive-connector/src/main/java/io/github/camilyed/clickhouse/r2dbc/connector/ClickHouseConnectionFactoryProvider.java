package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import java.time.Duration;
import java.util.ServiceLoader;

/**
 * R2DBC service-provider entry point for the ClickHouse driver.
 *
 * <p>Discovered via {@link ServiceLoader}, registered in {@code
 * META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider} — a consumer using the standard
 * bootstrap path ({@code io.r2dbc.spi.ConnectionFactories#get(ConnectionFactoryOptions)}) finds
 * this provider with no direct dependency on this class. Verified, not just declared: {@code
 * ClickHouseConnectionFactoryProviderTest
 * .shouldBeDiscoverableThroughTheStandardR2dbcServiceLoaderBootstrapPath}.
 */
public final class ClickHouseConnectionFactoryProvider implements ConnectionFactoryProvider {

  /** The driver identifier this provider answers to, e.g. an {@code r2dbc:clickhouse://...} URL. */
  public static final String DRIVER = "clickhouse";

  /**
   * How long to wait for ClickHouse's HTTP response once a request has been sent, e.g. {@code
   * responseTimeout=PT30S} — applied to every query run over a {@link ClickHouseConnection} this
   * factory produces. Defaults to {@code null} (no timeout at all): ClickHouse is an analytical
   * database where a legitimate query can easily run far longer than a typical OLTP request, so
   * this driver does not impose an arbitrary global time limit unless a caller explicitly asks for
   * one — see {@code ClickHouseHttpTransport}'s own Javadoc for the full reasoning (an earlier
   * version of this transport hardcoded a 2-second response timeout with no way to change it).
   *
   * <p>Four different timeouts exist across this driver, deliberately kept separate because each
   * bounds a different phase of a request and conflating them silently changes what actually gets
   * protected:
   *
   * <ul>
   *   <li>{@code responseTimeout} (this option) — how long to wait for response bytes once the
   *       request has been sent, measured client-side. Applies to every query from this factory.
   *   <li>{@link ConnectionFactoryOptions#CONNECT_TIMEOUT} — how long to wait for the underlying
   *       TCP connection itself to establish, before any request is even sent. Wired through to
   *       Reactor Netty's {@code CONNECT_TIMEOUT_MILLIS} channel option by {@link
   *       ClickHouseConnectionFactory#from}.
   *   <li>{@code statementTimeout} ({@code ClickHouseConnection#setStatementTimeout}) —
   *       ClickHouse's own server-side {@code max_execution_time} setting: how long the server
   *       itself is willing to keep running the query, regardless of how promptly the client reads
   *       the response. Set per connection (inherited by statements created afterward), not per
   *       factory — see that method's Javadoc.
   *   <li>{@link #TRANSPORT_PENDING_ACQUIRE_TIMEOUT} — how long a queued acquisition waits for a
   *       pooled connection to free up before failing, entirely before any request is sent at all.
   * </ul>
   *
   * A slow query that has genuinely started executing on the server is bounded by {@code
   * statementTimeout} (server-side) and this option (client-side wait for bytes), not by {@code
   * connectTimeout} or {@link #TRANSPORT_PENDING_ACQUIRE_TIMEOUT}, which have both already
   * succeeded by the time a query is running.
   */
  public static final Option<Duration> RESPONSE_TIMEOUT = Option.valueOf("responseTimeout");

  /**
   * Whether ClickHouse should compress response bodies with its own custom LZ4 block framing (sent
   * as the {@code compress=1} query parameter — not standard HTTP {@code Content-Encoding}), e.g.
   * {@code responseCompression=false} to turn it off. <b>Defaults to {@code true}</b>, matching
   * client-v2's own default of {@code COMPRESS_SERVER_RESPONSE=true} — a caller migrating from
   * client-v2, or comparing throughput against it, gets the same on-the-wire behavior with no extra
   * configuration. Applied to every query run over a {@link ClickHouseConnection} this factory
   * produces. See {@code io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression}'s Javadoc
   * for the wire format this turns on.
   */
  public static final Option<Boolean> RESPONSE_COMPRESSION = Option.valueOf("responseCompression");

  /**
   * A trusted TLS certificate for {@code ssl=true} connections, as a classpath resource path or a
   * filesystem path to a PEM-encoded certificate (or chain) — see {@link
   * ClickHouseConnectionFactory#from} for exactly how the value is resolved. Named and shaped after
   * r2dbc-postgresql's own {@code sslRootCert} option, so it's convenient to configure the same way
   * (e.g. mounted as a file in a Kubernetes {@code Secret}/{@code ConfigMap}, or bundled as a
   * classpath resource) for a self-signed or internal-CA certificate the JVM's default trust store
   * doesn't know about. Only meaningful alongside {@code ssl=true}; see {@link
   * ClickHouseConnectionFactory#from} for what happens if it's set without {@code ssl=true}.
   */
  public static final Option<String> SSL_ROOT_CERT = Option.valueOf("sslRootCert");

  /**
   * How many times a query is retried after a failure that happened before any request bytes
   * reached the server — see {@code io.github.camilyed.clickhouse.r2dbc.transport.http.RetryPolicy}
   * for exactly what does and doesn't get retried, and why that scope is safe regardless of whether
   * the query is a {@code SELECT} or an {@code INSERT}. Defaults to 3 (matching client-v2's own
   * default retry count for connection-level failures); set to {@code 0} to disable retrying
   * entirely.
   */
  public static final Option<Integer> RETRY_MAX_ATTEMPTS = Option.valueOf("retryMaxAttempts");

  /**
   * The fixed delay between retry attempts governed by {@link #RETRY_MAX_ATTEMPTS}. Defaults to
   * 50ms. Ignored when {@link #RETRY_MAX_ATTEMPTS} is {@code 0}.
   */
  public static final Option<Duration> RETRY_DELAY = Option.valueOf("retryDelay");

  /**
   * The maximum number of connections in the underlying Reactor Netty HTTP connection pool — the
   * transport-level pool, distinct from any R2DBC-level pool (e.g. {@code
   * io.r2dbc.pool.ConnectionPool}) a caller layers on top; see the README's "Connection pooling"
   * section. Named {@code transport...}, not {@code pool...}, specifically so it's never confused
   * with {@code spring.r2dbc.pool.*}, which configures that other, R2DBC-level pool. Defaults to
   * Reactor Netty's own default (see {@code TransportOptions}'s Javadoc) when not set.
   */
  public static final Option<Integer> TRANSPORT_MAX_CONNECTIONS =
      Option.valueOf("transportMaxConnections");

  /**
   * An explicit override for {@link ClickHouseConnectionFactory}'s {@code RowDecodingScheduler}
   * worker count, independent of {@link #TRANSPORT_MAX_CONNECTIONS} — see {@link
   * ClickHouseConnectionFactory#from} for how the two interact. Defaults to {@code null}, meaning
   * the decoder stays coupled to the resolved connection pool size (this driver's long-standing
   * default — see {@code docs/operations/connection-pooling.md}'s "The decode worker pool tracks
   * this pool's size, not the CPU core count").
   *
   * <p>Phase 11 PR5 (see ROADMAP.md) added this option after a trusted-profile benchmark run showed
   * this driver's p90-p99 per-query latency running 15-25% behind client-v2's at every tested
   * concurrency (8/32/128), while p50 stayed tied and GC time stayed equal despite this driver
   * allocating ~3.3x less per query — ruling out GC pauses and pointing at decode-worker queueing
   * (fixed at exactly {@code transportMaxConnections} workers) as the more likely tail- latency
   * driver: a query whose decode has to wait for a worker, because every worker happened to be busy
   * decoding another concurrent query's response at that instant, pays that wait as pure added
   * latency with no corresponding allocation cost. Setting this higher than {@link
   * #TRANSPORT_MAX_CONNECTIONS} gives decode work more workers to spread across than there are
   * physical connections, without also widening the connection pool itself (which would change the
   * very physical-pool-size comparison the matched-pool benchmarks exist to hold fixed) — an
   * explicit, opt-in escape hatch from the coupled default, not a replacement for it.
   */
  public static final Option<Integer> DECODER_WORKER_COUNT = Option.valueOf("decoderWorkerCount");

  /**
   * Runs the decode-worker pool on JDK 21 virtual threads (one per decode task, capped at the same
   * worker count {@link #DECODER_WORKER_COUNT}/{@link #TRANSPORT_MAX_CONNECTIONS} would otherwise
   * resolve to) instead of a bounded platform-thread pool — see {@code
   * RowDecodingScheduler#virtualThreads(int)}'s Javadoc for the full motivation and pinning-risk
   * analysis. Defaults to {@code false} (the bounded platform-thread pool), matching this driver's
   * long-standing default.
   *
   * <p>An experimental, opt-in escape hatch, not a replacement for the default: JFR evidence
   * (ROADMAP.md, Phase 11 PR5 follow-up, 2026-08-24) showed decode is I/O-wait-dominated, which
   * motivates trying virtual threads for the resource cost of parked platform threads — but this
   * does not raise the throughput ceiling, still capped by the physical connection pool, and has
   * not yet been validated by a trusted benchmark run the way {@link #DECODER_WORKER_COUNT} has.
   */
  public static final Option<Boolean> DECODER_USE_VIRTUAL_THREADS =
      Option.valueOf("decoderUseVirtualThreads");

  /**
   * The maximum number of acquisitions allowed to queue once {@link #TRANSPORT_MAX_CONNECTIONS} is
   * reached, before further acquisitions are rejected outright rather than queued. Defaults to
   * Reactor Netty's own default when not set.
   */
  public static final Option<Integer> TRANSPORT_PENDING_ACQUIRE_MAX_COUNT =
      Option.valueOf("transportPendingAcquireMaxCount");

  /**
   * How long a queued acquisition waits for a connection to free up before failing — entirely
   * before any request is sent, distinct from {@link #RESPONSE_TIMEOUT} (see that option's Javadoc
   * for how all four of this driver's timeouts relate). Defaults to Reactor Netty's own default
   * when not set.
   */
  public static final Option<Duration> TRANSPORT_PENDING_ACQUIRE_TIMEOUT =
      Option.valueOf("transportPendingAcquireTimeout");

  /**
   * How long a pooled connection may sit idle before it's closed and evicted from the pool.
   * Defaults to Reactor Netty's own default (no idle eviction) when not set.
   */
  public static final Option<Duration> TRANSPORT_MAX_IDLE_TIME =
      Option.valueOf("transportMaxIdleTime");

  /**
   * The maximum total lifetime of a pooled connection, regardless of how recently it was used,
   * before it's closed and evicted from the pool. Defaults to Reactor Netty's own default (no
   * lifetime limit) when not set.
   */
  public static final Option<Duration> TRANSPORT_MAX_LIFE_TIME =
      Option.valueOf("transportMaxLifeTime");

  /**
   * A {@link DriverObservationListener} to notify of query lifecycle events — see that interface's
   * Javadoc for the full contract, including exactly which events fire and when. Only meaningful
   * when {@link ClickHouseConnectionFactory} is built programmatically via {@link
   * ConnectionFactoryOptions#builder()}{@code .option(OBSERVATION_LISTENER, myListener)}: unlike
   * every other option on this provider, a {@link DriverObservationListener} instance has no
   * URL-string form, so it cannot be set through an {@code r2dbc:clickhouse://...} connection URL.
   * Defaults to {@link DriverObservationListener#NOOP} when not set.
   */
  public static final Option<DriverObservationListener> OBSERVATION_LISTENER =
      Option.valueOf("observationListener");

  @Override
  public ConnectionFactory create(final ConnectionFactoryOptions connectionFactoryOptions) {
    return ClickHouseConnectionFactory.from(connectionFactoryOptions);
  }

  @Override
  public boolean supports(final ConnectionFactoryOptions connectionFactoryOptions) {
    return DRIVER.equals(connectionFactoryOptions.getValue(ConnectionFactoryOptions.DRIVER));
  }

  @Override
  public String getDriver() {
    return DRIVER;
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

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
   * The maximum number of acquisitions allowed to queue once {@link #TRANSPORT_MAX_CONNECTIONS} is
   * reached, before further acquisitions are rejected outright rather than queued. Defaults to
   * Reactor Netty's own default when not set.
   */
  public static final Option<Integer> TRANSPORT_PENDING_ACQUIRE_MAX_COUNT =
      Option.valueOf("transportPendingAcquireMaxCount");

  /**
   * How long a queued acquisition waits for a connection to free up before failing. Defaults to
   * Reactor Netty's own default when not set.
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

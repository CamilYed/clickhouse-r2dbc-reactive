package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.RetryPolicy;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Creates {@link ClickHouseConnection}s against a ClickHouse server over its HTTP interface.
 *
 * <p>Every {@link Connection} produced by {@link #create()} shares this factory's single {@link
 * ClickHouseHttpTransport} — and therefore its connection pool — rather than each opening its own.
 * Every produced {@link Connection} likewise shares this factory's single {@link
 * RowDecodingScheduler}, disposed together with this factory rather than recreated per connection —
 * see that class's Javadoc for the full ownership contract. Every produced {@link Connection} also
 * shares this factory's single {@link DriverObservationListener} — see {@link
 * ClickHouseConnectionFactoryProvider#OBSERVATION_LISTENER}'s Javadoc.
 */
public final class ClickHouseConnectionFactory implements ConnectionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionFactory.class);

  private static final int DEFAULT_HTTP_PORT = 8123;

  /**
   * Reactor Netty's own {@code ConnectionProvider} default pool size when {@code maxConnections} is
   * left unset — see {@code docs/operations/connection-pooling.md}'s "Reactor Netty's own defaults"
   * table. Duplicated here (rather than read back from the transport, which doesn't expose it)
   * specifically so {@link #resolveDecoderWorkerCount} can size the decoder to match the pool even
   * when the pool itself is left at this default.
   */
  private static final int REACTOR_NETTY_DEFAULT_POOL_SIZE_MULTIPLIER = 2;

  private static final int REACTOR_NETTY_DEFAULT_POOL_SIZE_FLOOR = 8;

  /** How {@link #logEffectiveSettings} renders an option that was left unset. */
  private static final String UNSET_MARKER = "unset";

  private final ClickHouseHttpTransport transport;
  private final RowDecodingScheduler decodingScheduler;
  private final DriverObservationListener observationListener;

  ClickHouseConnectionFactory(
      final ClickHouseHttpTransport transport,
      final RowDecodingScheduler decodingScheduler,
      final DriverObservationListener observationListener) {
    this.transport = transport;
    this.decodingScheduler = decodingScheduler;
    this.observationListener = observationListener;
  }

  /**
   * Builds a factory from R2DBC {@link ConnectionFactoryOptions} — {@code host} (required), {@code
   * port} (default {@value #DEFAULT_HTTP_PORT}), {@code ssl} (default {@code false}), {@code
   * user}/{@code password}: absent {@code user} means no authentication at all, relying on the
   * server allowing anonymous access; a present {@code user} with an absent {@code password} means
   * HTTP Basic auth with an <em>empty</em> password, never the literal four-character string {@code
   * "null"} a naive {@code String.valueOf(password)} on a {@code null} password would send, {@code
   * database} (default: none, meaning the connecting user's own default database — sent as {@code
   * X-ClickHouse-Database} on every request once set, see {@link
   * io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions#database()}), {@code
   * connectTimeout} (default: none — see {@link ClickHouseHttpTransport}'s Javadoc for why this
   * driver never imposes an implicit timeout), {@link
   * ClickHouseConnectionFactoryProvider#RESPONSE_TIMEOUT} (default: none, same reasoning — see that
   * option's Javadoc for how it relates to {@code connectTimeout}, {@code statementTimeout}, and
   * {@link ClickHouseConnectionFactoryProvider#TRANSPORT_PENDING_ACQUIRE_TIMEOUT}), and {@link
   * ClickHouseConnectionFactoryProvider#SSL_ROOT_CERT} (default: none, meaning the JVM's default
   * trust store).
   *
   * <p>{@code sslRootCert}, when present, is resolved first as a classpath resource (via this
   * class's own {@link ClassLoader}), then — if no such resource exists — as a filesystem path,
   * mirroring r2dbc-postgresql's own {@code sslRootCert} option so the same value works whether the
   * certificate ships bundled in a jar or is mounted onto disk (e.g. a Kubernetes {@code Secret}).
   * If neither resolves to readable bytes, or if it's set without {@code ssl=true}, building the
   * factory fails fast with {@link IllegalArgumentException} rather than silently connecting
   * without the intended trust configuration.
   *
   * <p>{@link ClickHouseConnectionFactoryProvider#RETRY_MAX_ATTEMPTS}/{@link
   * ClickHouseConnectionFactoryProvider#RETRY_DELAY} configure this factory's {@link RetryPolicy} —
   * each defaults independently to {@link RetryPolicy#defaultPolicy()}'s corresponding value when
   * not set, so setting only one of the two still leaves the other at its sensible default; set
   * {@code retryMaxAttempts=0} to disable retrying entirely. See {@link RetryPolicy}'s Javadoc for
   * exactly what gets retried.
   *
   * <p>{@link ClickHouseConnectionFactoryProvider#TRANSPORT_MAX_CONNECTIONS}/{@link
   * ClickHouseConnectionFactoryProvider#TRANSPORT_PENDING_ACQUIRE_MAX_COUNT}/{@link
   * ClickHouseConnectionFactoryProvider#TRANSPORT_PENDING_ACQUIRE_TIMEOUT}/{@link
   * ClickHouseConnectionFactoryProvider#TRANSPORT_MAX_IDLE_TIME}/{@link
   * ClickHouseConnectionFactoryProvider#TRANSPORT_MAX_LIFE_TIME} configure the underlying Reactor
   * Netty HTTP connection pool via {@link TransportOptions} — each independently defaults to
   * Reactor Netty's own default when not set (see {@link TransportOptions}'s Javadoc); an invalid
   * value (e.g. a negative duration, a non-positive connection count) fails fast right here, at
   * factory creation, never silently falling back to a default. {@code transportMaxConnections} in
   * particular also sizes this factory's {@link RowDecodingScheduler} (see {@link
   * #resolveDecoderWorkerCount}) — the decoder is never a smaller, hidden concurrency ceiling
   * underneath the pool this option configures, unless {@link
   * ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT} explicitly overrides it — see that
   * option's own Javadoc for why a caller would ever set it independently of the pool size. {@link
   * ClickHouseConnectionFactoryProvider#DECODER_USE_VIRTUAL_THREADS} (default {@code false})
   * independently selects which kind of thread the decoder pool is built from — see that option's
   * Javadoc.
   *
   * <p>{@link ClickHouseConnectionFactoryProvider#RESPONSE_COMPRESSION} configures {@link
   * TransportOptions#responseCompression()} — defaults to {@code true} ({@link
   * ResponseCompression#LZ4}) when not set, matching client-v2's own default; see that option's
   * Javadoc.
   *
   * <p>There is deliberately no {@code statementTimeout} option here: that needs to apply per
   * statement, not per factory — see {@code ClickHouseConnection.setStatementTimeout}.
   */
  public static ClickHouseConnectionFactory from(final ConnectionFactoryOptions options) {
    final String host = (String) options.getRequiredValue(ConnectionFactoryOptions.HOST);
    final Integer port = (Integer) options.getValue(ConnectionFactoryOptions.PORT);
    final boolean ssl = Boolean.TRUE.equals(options.getValue(ConnectionFactoryOptions.SSL));
    final int resolvedPort = port == null ? DEFAULT_HTTP_PORT : port;
    final String baseUrl = (ssl ? "https" : "http") + "://" + host + ":" + resolvedPort;

    final String user = (String) options.getValue(ConnectionFactoryOptions.USER);
    final CharSequence password =
        (CharSequence) options.getValue(ConnectionFactoryOptions.PASSWORD);
    // user absent -> no auth; user present, password absent -> empty password (never the literal
    // string "null" that String.valueOf(password) would produce for a null CharSequence); user +
    // password -> exact password.
    final Authentication authentication =
        user == null
            ? Authentication.none()
            : Authentication.basic(user, password == null ? "" : password.toString());

    final String database = (String) options.getValue(ConnectionFactoryOptions.DATABASE);

    final Duration connectTimeout =
        (Duration) options.getValue(ConnectionFactoryOptions.CONNECT_TIMEOUT);
    final Duration responseTimeout =
        durationOption(options, ClickHouseConnectionFactoryProvider.RESPONSE_TIMEOUT);

    final String sslRootCert =
        (String) options.getValue(ClickHouseConnectionFactoryProvider.SSL_ROOT_CERT);
    if (sslRootCert != null && !ssl) {
      throw new IllegalArgumentException(
          "sslRootCert was set but ssl=true was not - a trusted certificate has no TLS handshake"
              + " to apply to");
    }
    final byte @Nullable [] trustedCertificatePem =
        sslRootCert == null ? null : resolveTrustedCertificatePem(sslRootCert);

    final Integer retryMaxAttempts =
        intOption(options, ClickHouseConnectionFactoryProvider.RETRY_MAX_ATTEMPTS);
    final Duration retryDelay =
        durationOption(options, ClickHouseConnectionFactoryProvider.RETRY_DELAY);
    final RetryPolicy retryPolicy =
        new RetryPolicy(
            retryMaxAttempts == null ? RetryPolicy.defaultPolicy().maxAttempts() : retryMaxAttempts,
            retryDelay == null ? RetryPolicy.defaultPolicy().delay() : retryDelay);

    final Integer transportMaxConnections =
        intOption(options, ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS);
    final Integer transportPendingAcquireMaxCount =
        intOption(options, ClickHouseConnectionFactoryProvider.TRANSPORT_PENDING_ACQUIRE_MAX_COUNT);
    final Duration transportPendingAcquireTimeout =
        durationOption(
            options, ClickHouseConnectionFactoryProvider.TRANSPORT_PENDING_ACQUIRE_TIMEOUT);
    final Duration transportMaxIdleTime =
        durationOption(options, ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_IDLE_TIME);
    final Duration transportMaxLifeTime =
        durationOption(options, ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_LIFE_TIME);

    final Boolean responseCompressionEnabled =
        booleanOption(options, ClickHouseConnectionFactoryProvider.RESPONSE_COMPRESSION);
    final ResponseCompression responseCompression =
        Boolean.FALSE.equals(responseCompressionEnabled)
            ? ResponseCompression.NONE
            : ResponseCompression.LZ4;

    final TransportOptions transportOptions =
        TransportOptions.defaults()
            .withAuthentication(authentication)
            .withConnectTimeout(connectTimeout)
            .withResponseTimeout(responseTimeout)
            .withTrustedCertificatePem(trustedCertificatePem)
            .withRetryPolicy(retryPolicy)
            .withMaxConnections(transportMaxConnections)
            .withPendingAcquireMaxCount(transportPendingAcquireMaxCount)
            .withPendingAcquireTimeout(transportPendingAcquireTimeout)
            .withMaxIdleTime(transportMaxIdleTime)
            .withMaxLifeTime(transportMaxLifeTime)
            .withDatabase(database)
            .withResponseCompression(responseCompression);

    final DriverObservationListener observationListener =
        observationListenerOption(
            options, ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER);

    final Integer decoderWorkerCountOverride =
        intOption(options, ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT);

    final int resolvedDecoderWorkerCount =
        resolveDecoderWorkerCount(decoderWorkerCountOverride, transportMaxConnections);
    final Boolean decoderUseVirtualThreads =
        booleanOption(options, ClickHouseConnectionFactoryProvider.DECODER_USE_VIRTUAL_THREADS);
    final RowDecodingScheduler decodingScheduler =
        Boolean.TRUE.equals(decoderUseVirtualThreads)
            ? RowDecodingScheduler.virtualThreads(resolvedDecoderWorkerCount)
            : RowDecodingScheduler.withWorkerCount(resolvedDecoderWorkerCount);

    logEffectiveSettings(
        resolvedDecoderWorkerCount,
        decodingScheduler.isVirtualThreadBacked(),
        transportMaxConnections,
        transportPendingAcquireMaxCount,
        transportPendingAcquireTimeout);

    return new ClickHouseConnectionFactory(
        new ClickHouseHttpTransport(baseUrl, transportOptions),
        decodingScheduler,
        observationListener);
  }

  /**
   * The number of workers {@link #decodingScheduler} gets sized to. Three tiers, checked in order:
   * an explicit {@link ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT} always wins first
   * (see that option's own Javadoc for why a caller would ever set it above the pool size); absent
   * that, the resolved connection pool size, so the decoder can never become a smaller, hidden
   * concurrency ceiling underneath a pool a caller explicitly asked for; absent both, {@code
   * transportMaxConnections} not being set means the pool itself resolves to Reactor Netty's own
   * default ({@link #REACTOR_NETTY_DEFAULT_POOL_SIZE_FLOOR}/{@link
   * #REACTOR_NETTY_DEFAULT_POOL_SIZE_MULTIPLIER} — see their Javadoc), so this mirrors that exact
   * formula rather than falling back to an unrelated, typically much smaller number like the CPU
   * core count.
   */
  private static int resolveDecoderWorkerCount(
      final @Nullable Integer decoderWorkerCountOverride,
      final @Nullable Integer transportMaxConnections) {
    if (decoderWorkerCountOverride != null) {
      return decoderWorkerCountOverride;
    }
    if (transportMaxConnections != null) {
      return transportMaxConnections;
    }
    return Math.max(
            Runtime.getRuntime().availableProcessors(), REACTOR_NETTY_DEFAULT_POOL_SIZE_FLOOR)
        * REACTOR_NETTY_DEFAULT_POOL_SIZE_MULTIPLIER;
  }

  /**
   * Logs the values that actually govern this factory's concurrency behavior — every one of them
   * independently tunable, none of them visible to a caller who only set {@code
   * transportMaxConnections} (or nothing at all). Exists because Phase 11 PR5's trusted-run finding
   * (see the root {@code ROADMAP.md}) showed these four numbers interacting in a way that isn't
   * obvious from any single option's own Javadoc: {@code decoderWorkerCount} isn't only a decode-
   * throughput knob, it's this driver's actual admission-control gate (see {@link
   * ClickHouseConnectionFactoryProvider#DECODER_WORKER_COUNT}'s Javadoc), and widening it without
   * also widening {@code transportPendingAcquireMaxCount} trades a slow response for an outright
   * rejected one. A caller debugging exactly that failure shouldn't need to already know this
   * investigation to find the numbers in play — they're one log line away at factory construction.
   */
  private static void logEffectiveSettings(
      final int resolvedDecoderWorkerCount,
      final boolean decoderUsesVirtualThreads,
      final @Nullable Integer transportMaxConnections,
      final @Nullable Integer transportPendingAcquireMaxCount,
      final @Nullable Duration transportPendingAcquireTimeout) {
    LOG.info(
        "ClickHouseConnectionFactory effective settings: decoderWorkerCount={},"
            + " decoderUsesVirtualThreads={}, transportMaxConnections={},"
            + " transportPendingAcquireMaxCount={}, transportPendingAcquireTimeout={} (\"{}\" means"
            + " the option was left unset and falls back to Reactor Netty's own connection-pool"
            + " default - not shown here, since Reactor Netty doesn't expose what it actually"
            + " resolved an unset value to; see docs/operations/connection-pooling.md)",
        resolvedDecoderWorkerCount,
        decoderUsesVirtualThreads,
        describeOrUnset(transportMaxConnections),
        describeOrUnset(transportPendingAcquireMaxCount),
        describeOrUnset(transportPendingAcquireTimeout),
        UNSET_MARKER);
  }

  private static String describeOrUnset(final @Nullable Object value) {
    return value == null ? UNSET_MARKER : String.valueOf(value);
  }

  // See intOption's comment above - same reasoning, for DriverObservationListener-typed options.
  // Unlike every other option this factory reads, DriverObservationListener has no URL-string
  // form at all (see ClickHouseConnectionFactoryProvider#OBSERVATION_LISTENER's Javadoc) - a
  // String value here is always a configuration mistake, not an alternate encoding to parse.
  private static DriverObservationListener observationListenerOption(
      final ConnectionFactoryOptions options, final Option<?> option) {
    final Object raw = options.getValue(option);
    if (raw == null) {
      return DriverObservationListener.NOOP;
    }
    if (raw instanceof final DriverObservationListener value) {
      return value;
    }
    throw new IllegalArgumentException(
        option.name() + " must be a DriverObservationListener instance, got: " + raw);
  }

  // ConnectionFactoryOptions.parse(url) has no way to know a custom (non-R2DBC-well-known)
  // Option's intended type due to Java's type erasure - every query-string value ends up stored as
  // a plain String, regardless of the Option<T> it's read back through. Built via
  // ConnectionFactoryOptions.builder().option(...) instead, values keep their real type. Both
  // helpers below deliberately accept Option<?>, not Option<Integer>/Option<Duration>: calling
  // options.getValue(...) through a wildcard-typed Option avoids the compiler inserting an
  // implicit checkcast to the "real" type at the call site (the classic generics-erasure gotcha) -
  // that checkcast would otherwise throw ClassCastException on a String value before this method's
  // own instanceof checks below ever run, defeating the whole point of these helpers.
  private static @Nullable Integer intOption(
      final ConnectionFactoryOptions options, final Option<?> option) {
    final Object raw = options.getValue(option);
    if (raw == null) {
      return null;
    }
    if (raw instanceof final Integer value) {
      return value;
    }
    if (raw instanceof final String text) {
      return Integer.valueOf(text);
    }
    throw new IllegalArgumentException(option.name() + " must be an integer, got: " + raw);
  }

  // See intOption's comment above - same reasoning, for Duration-typed options. A String value is
  // parsed as ISO-8601 (java.time.Duration's own text format, e.g. "PT5S"), the same format
  // Duration#toString() produces, so any Duration value round-trips through a URL unchanged.
  private static @Nullable Duration durationOption(
      final ConnectionFactoryOptions options, final Option<?> option) {
    final Object raw = options.getValue(option);
    if (raw == null) {
      return null;
    }
    if (raw instanceof final Duration value) {
      return value;
    }
    if (raw instanceof final String text) {
      return Duration.parse(text);
    }
    throw new IllegalArgumentException(option.name() + " must be a duration, got: " + raw);
  }

  // See intOption's comment above - same reasoning, for Boolean-typed options.
  private static @Nullable Boolean booleanOption(
      final ConnectionFactoryOptions options, final Option<?> option) {
    final Object raw = options.getValue(option);
    if (raw == null) {
      return null;
    }
    if (raw instanceof final Boolean value) {
      return value;
    }
    if (raw instanceof final String text) {
      return Boolean.valueOf(text);
    }
    throw new IllegalArgumentException(option.name() + " must be a boolean, got: " + raw);
  }

  private static byte[] resolveTrustedCertificatePem(final String sslRootCert) {
    final byte @Nullable [] fromClasspath = readClasspathResource(sslRootCert);
    if (fromClasspath != null) {
      return fromClasspath;
    }
    try {
      return Files.readAllBytes(Path.of(sslRootCert));
    } catch (final IOException e) {
      throw new IllegalArgumentException(
          "sslRootCert '" + sslRootCert + "' is neither a classpath resource nor a readable file",
          e);
    }
  }

  private static byte @Nullable [] readClasspathResource(final String resourcePath) {
    try (InputStream resource =
        ClickHouseConnectionFactory.class.getClassLoader().getResourceAsStream(resourcePath)) {
      return resource == null ? null : resource.readAllBytes();
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed reading classpath resource '" + resourcePath + "'", e);
    }
  }

  @Override
  public Mono<ClickHouseConnection> create() {
    return Mono.fromSupplier(
        () -> new ClickHouseConnection(transport, decodingScheduler, observationListener));
  }

  /**
   * The worker count {@link #decodingScheduler} was actually sized to — package-private, test-only
   * window onto the real capacity guarantee {@link #resolveDecoderWorkerCount} computes, not an
   * implementation detail (see {@link RowDecodingScheduler#workerCount()}'s own Javadoc).
   */
  int decoderWorkerCount() {
    return decodingScheduler.workerCount();
  }

  /**
   * Whether {@link #decodingScheduler} was built by {@link
   * ClickHouseConnectionFactoryProvider#DECODER_USE_VIRTUAL_THREADS} — package-private, test-only
   * window, same reasoning as {@link #decoderWorkerCount()}.
   */
  boolean decoderUsesVirtualThreads() {
    return decodingScheduler.isVirtualThreadBacked();
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return ClickHouseConnectionFactoryMetadata.INSTANCE;
  }

  /**
   * Releases every resource this factory owns and shares across every {@link Connection} it has
   * produced: the underlying {@link ClickHouseHttpTransport}'s Reactor Netty connection pool, and
   * the dedicated {@link RowDecodingScheduler} worker pool. Both dispose fire-and-forget, matching
   * the "returns immediately, releases asynchronously" contract each already documents on its own
   * {@code dispose()}; idempotent, safe to call more than once.
   *
   * <p>Meant for when the factory itself is no longer needed (e.g. application shutdown) — a {@link
   * Connection} already in flight when this is called may fail as the transport/scheduler it shares
   * with every other {@link Connection} from this factory is torn down from under it.
   */
  public void dispose() {
    decodingScheduler.dispose();
    transport.dispose();
  }

  /**
   * Whether {@link #dispose()} has already released every resource this factory owns — {@code true}
   * only once both the {@link RowDecodingScheduler} and the {@link ClickHouseHttpTransport}'s
   * connection pool report disposed. Unlike calling {@link ClickHouseHttpTransport#isDisposed()}
   * directly, this is safe to read before the factory has ever produced a query result: {@link
   * RowDecodingScheduler#isDisposed()} is never vacuously {@code true} the way an unused
   * transport's connection pool is (see that method's Javadoc), so requiring both together means
   * this only reports {@code true} once {@link #dispose()} has genuinely run.
   */
  public boolean isDisposed() {
    return decodingScheduler.isDisposed() && transport.isDisposed();
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.transport.http.Authentication;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.RetryPolicy;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 * Creates {@link ClickHouseConnection}s against a ClickHouse server over its HTTP interface.
 *
 * <p>Every {@link Connection} produced by {@link #create()} shares this factory's single {@link
 * ClickHouseHttpTransport} — and therefore its connection pool — rather than each opening its own.
 */
public final class ClickHouseConnectionFactory implements ConnectionFactory {

  private static final int DEFAULT_HTTP_PORT = 8123;

  private final ClickHouseHttpTransport transport;

  ClickHouseConnectionFactory(final ClickHouseHttpTransport transport) {
    this.transport = transport;
  }

  /**
   * Builds a factory from R2DBC {@link ConnectionFactoryOptions} — {@code host} (required), {@code
   * port} (default {@value #DEFAULT_HTTP_PORT}), {@code ssl} (default {@code false}), {@code
   * user}/{@code password} (default: no authentication, relying on the server allowing anonymous
   * access), {@code connectTimeout} (default: none — see {@link ClickHouseHttpTransport}'s Javadoc
   * for why this driver never imposes an implicit timeout), and {@link
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
   * <p>There is deliberately no {@code statementTimeout}/response-timeout option here yet: that
   * needs to apply per statement, not per factory, and {@code
   * ClickHouseConnection.setStatementTimeout} still throws {@link UnsupportedOperationException}
   * rather than being wired to anything.
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
    final Authentication authentication =
        user == null ? Authentication.none() : Authentication.basic(user, String.valueOf(password));

    final Duration connectTimeout =
        (Duration) options.getValue(ConnectionFactoryOptions.CONNECT_TIMEOUT);

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
        (Integer) options.getValue(ClickHouseConnectionFactoryProvider.RETRY_MAX_ATTEMPTS);
    final Duration retryDelay =
        (Duration) options.getValue(ClickHouseConnectionFactoryProvider.RETRY_DELAY);
    final RetryPolicy retryPolicy =
        new RetryPolicy(
            retryMaxAttempts == null ? RetryPolicy.defaultPolicy().maxAttempts() : retryMaxAttempts,
            retryDelay == null ? RetryPolicy.defaultPolicy().delay() : retryDelay);

    return new ClickHouseConnectionFactory(
        new ClickHouseHttpTransport(
            baseUrl, authentication, null, connectTimeout, trustedCertificatePem, retryPolicy));
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
    return Mono.fromSupplier(() -> new ClickHouseConnection(transport));
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return ClickHouseConnectionFactoryMetadata.INSTANCE;
  }
}

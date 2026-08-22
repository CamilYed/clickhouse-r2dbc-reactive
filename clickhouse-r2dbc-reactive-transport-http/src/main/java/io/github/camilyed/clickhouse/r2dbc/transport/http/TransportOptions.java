package io.github.camilyed.clickhouse.r2dbc.transport.http;

import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import reactor.netty.resources.ConnectionProvider;

/**
 * Every option {@link ClickHouseHttpTransport}'s construction can be configured with, besides the
 * target {@code baseUrl} itself — the one config object every {@link ClickHouseHttpTransport}
 * constructor routes through internally, instead of piling up yet another constructor overload each
 * time a new option is added.
 *
 * <p>{@code maxConnections}, {@code pendingAcquireMaxCount}, {@code pendingAcquireTimeout}, {@code
 * maxIdleTime}, and {@code maxLifeTime} configure Reactor Netty's own transport-level HTTP
 * connection pool ({@link ConnectionProvider}) — distinct from, and underneath, any R2DBC-level
 * pool (e.g. {@code io.r2dbc.pool.ConnectionPool}) a caller layers on top; see {@link
 * ClickHouseHttpTransport}'s own Javadoc and the README's "Connection pooling" section for how the
 * two relate. All five default to {@code null}, meaning "use Reactor Netty's own default for that
 * setting" — this class never invents its own default value for any of them, so behavior when
 * nothing is configured is identical to Reactor Netty's own out-of-the-box pool sizing.
 *
 * <p>Every field is validated eagerly in the canonical constructor — an invalid value fails fast at
 * {@link ClickHouseHttpTransport} construction time (factory creation), never silently falls back
 * to a default, and never surfaces later as a confusing failure at first connection acquire.
 */
public record TransportOptions(
    Authentication authentication,
    @Nullable Duration responseTimeout,
    @Nullable Duration connectTimeout,
    byte @Nullable [] trustedCertificatePem,
    RetryPolicy retryPolicy,
    @Nullable Integer maxConnections,
    @Nullable Integer pendingAcquireMaxCount,
    @Nullable Duration pendingAcquireTimeout,
    @Nullable Duration maxIdleTime,
    @Nullable Duration maxLifeTime,
    @Nullable String database,
    ResponseCompression responseCompression) {

  public TransportOptions {
    requirePositive(maxConnections, "maxConnections");
    requirePositiveOrUnbounded(pendingAcquireMaxCount, "pendingAcquireMaxCount");
    requireNonNegative(pendingAcquireTimeout, "pendingAcquireTimeout");
    requireNonNegative(maxIdleTime, "maxIdleTime");
    requireNonNegative(maxLifeTime, "maxLifeTime");
  }

  /**
   * Every option at its "use Reactor Netty's own default" value — except {@code
   * responseCompression}, which defaults to {@link ResponseCompression#LZ4}, not {@link
   * ResponseCompression#NONE}: this driver's default matches client-v2's own default of {@code
   * COMPRESS_SERVER_RESPONSE=true}, so a caller migrating from client-v2 gets the same on-the-wire
   * behavior without opting in explicitly. See {@link ResponseCompression}'s Javadoc.
   */
  public static TransportOptions defaults() {
    return new TransportOptions(
        Authentication.none(),
        null,
        null,
        null,
        RetryPolicy.defaultPolicy(),
        null,
        null,
        null,
        null,
        null,
        null,
        ResponseCompression.LZ4);
  }

  public TransportOptions withAuthentication(final Authentication authentication) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withResponseTimeout(final @Nullable Duration responseTimeout) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withConnectTimeout(final @Nullable Duration connectTimeout) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withTrustedCertificatePem(final byte @Nullable [] trustedCertificatePem) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withRetryPolicy(final RetryPolicy retryPolicy) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withMaxConnections(final @Nullable Integer maxConnections) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withPendingAcquireMaxCount(
      final @Nullable Integer pendingAcquireMaxCount) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withPendingAcquireTimeout(
      final @Nullable Duration pendingAcquireTimeout) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withMaxIdleTime(final @Nullable Duration maxIdleTime) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  public TransportOptions withMaxLifeTime(final @Nullable Duration maxLifeTime) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  /**
   * The database to select via {@code X-ClickHouse-Database} on every request this transport sends
   * — {@code null} (the default) means "use the connecting user's own default database", matching
   * ClickHouse's own server-side behavior when no database is specified. Wired from R2DBC's
   * standard {@code ConnectionFactoryOptions.DATABASE} by {@code ClickHouseConnectionFactory.from}.
   */
  public TransportOptions withDatabase(final @Nullable String database) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  /**
   * Whether ClickHouse should compress the response body with its own custom LZ4 block framing
   * ({@link ResponseCompression#LZ4}, sent as the {@code compress=1} query parameter — see {@link
   * ClickHouseHttpTransport}) or send it uncompressed ({@link ResponseCompression#NONE}). {@link
   * ResponseCompression#LZ4} is this transport's default (see {@link #defaults()}) — pass {@link
   * ResponseCompression#NONE} to turn it off, e.g. to compare against an uncompressed baseline.
   * Wired from the R2DBC-facing {@code responseCompression} connection option by {@code
   * ClickHouseConnectionFactory.from}.
   */
  public TransportOptions withResponseCompression(final ResponseCompression responseCompression) {
    return new TransportOptions(
        authentication,
        responseTimeout,
        connectTimeout,
        trustedCertificatePem,
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  // The generated record equals/hashCode/toString compare trustedCertificatePem by array
  // reference identity (arrays don't override Object#equals/hashCode/toString), which is
  // misleading for a byte[] holding certificate content - overridden here to compare/hash/print
  // its actual bytes via java.util.Arrays instead.
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj
        instanceof
        TransportOptions(
            Authentication otherAuthentication,
            Duration otherResponseTimeout,
            Duration otherConnectTimeout,
            byte[] otherTrustedCertificatePem,
            RetryPolicy otherRetryPolicy,
            Integer otherMaxConnections,
            Integer otherPendingAcquireMaxCount,
            Duration otherPendingAcquireTimeout,
            Duration otherMaxIdleTime,
            Duration otherMaxLifeTime,
            String otherDatabase,
            ResponseCompression otherResponseCompression))) {
      return false;
    }
    return Objects.equals(authentication, otherAuthentication)
        && Objects.equals(responseTimeout, otherResponseTimeout)
        && Objects.equals(connectTimeout, otherConnectTimeout)
        && Arrays.equals(trustedCertificatePem, otherTrustedCertificatePem)
        && Objects.equals(retryPolicy, otherRetryPolicy)
        && Objects.equals(maxConnections, otherMaxConnections)
        && Objects.equals(pendingAcquireMaxCount, otherPendingAcquireMaxCount)
        && Objects.equals(pendingAcquireTimeout, otherPendingAcquireTimeout)
        && Objects.equals(maxIdleTime, otherMaxIdleTime)
        && Objects.equals(maxLifeTime, otherMaxLifeTime)
        && Objects.equals(database, otherDatabase)
        && responseCompression == otherResponseCompression;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        authentication,
        responseTimeout,
        connectTimeout,
        Arrays.hashCode(trustedCertificatePem),
        retryPolicy,
        maxConnections,
        pendingAcquireMaxCount,
        pendingAcquireTimeout,
        maxIdleTime,
        maxLifeTime,
        database,
        responseCompression);
  }

  @Override
  public String toString() {
    return "TransportOptions["
        + "authentication="
        + authentication
        + ", responseTimeout="
        + responseTimeout
        + ", connectTimeout="
        + connectTimeout
        + ", trustedCertificatePem="
        + Arrays.toString(trustedCertificatePem)
        + ", retryPolicy="
        + retryPolicy
        + ", maxConnections="
        + maxConnections
        + ", pendingAcquireMaxCount="
        + pendingAcquireMaxCount
        + ", pendingAcquireTimeout="
        + pendingAcquireTimeout
        + ", maxIdleTime="
        + maxIdleTime
        + ", maxLifeTime="
        + maxLifeTime
        + ", database="
        + database
        + ", responseCompression="
        + responseCompression
        + ']';
  }

  private static void requirePositive(final @Nullable Integer value, final String name) {
    if (value != null && value <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got: " + value);
    }
  }

  // -1 is Reactor Netty's own documented value for "no upper limit" on the pending-acquire queue.
  private static void requirePositiveOrUnbounded(final @Nullable Integer value, final String name) {
    if (value != null && value <= 0 && value != -1) {
      throw new IllegalArgumentException(
          name + " must be positive, or -1 for unbounded, got: " + value);
    }
  }

  private static void requireNonNegative(final @Nullable Duration value, final String name) {
    if (value != null && value.isNegative()) {
      throw new IllegalArgumentException(name + " must not be negative, got: " + value);
    }
  }
}

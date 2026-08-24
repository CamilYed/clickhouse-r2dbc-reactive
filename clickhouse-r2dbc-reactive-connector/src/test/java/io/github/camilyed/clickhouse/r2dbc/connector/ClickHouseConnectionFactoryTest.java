package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.connector.fakes.RecordingDriverObservationListener;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.NoSuchOptionException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import org.junit.jupiter.api.Test;

class ClickHouseConnectionFactoryTest {

  // A syntactically valid, self-signed X.509 certificate (generated once via `openssl req -x509`,
  // not tied to any real host) - Netty's SslContextBuilder.trustManager(...) parses its input
  // eagerly at construction time (see ClickHouseHttpTransport's constructor), so placeholder text
  // is rejected immediately with IllegalArgumentException rather than accepted and only failing
  // later at handshake time. A real certificate is needed here purely to prove the resolution
  // (classpath resource / filesystem path) reaches ClickHouseHttpTransport correctly - whether that
  // certificate is actually trusted by a real server is covered separately by
  // ClickHouseHttpTransportTlsTest.shouldSucceedTheHandshakeWhenTheServerCertificateIsExplicitlyTrusted.
  private static final String A_VALID_CERTIFICATE_PEM =
      """
      -----BEGIN CERTIFICATE-----
      MIIC/zCCAeegAwIBAgIUS4P/jb+igpCdsGqRz8WVGlB6N1IwDQYJKoZIhvcNAQEL
      BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA4MTMwODA0NDhaFw0zNjA4MTAwODA0
      NDhaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
      AoIBAQDv9INFKnn2FxMBovVBcFOefd+iLIE1CCJjdPlB2IG+Gq6xJoGiLOzKlurl
      9DHnp8j+uOR0+P+HZAlHHLofpcwuvNywVGsL/oMezwfR7xtSbfpltJXjwu6SmgIl
      /W+Ut6H20CHkDboQMGq1qd7VjjMpZhtXSif5m6KEdpHT4LCIWOc1H0HSSzVOn8VQ
      S21InEKwExQyfOkTbPj3MFsSH+K7KfO2DmcecGwRNy+yiKYXj/k6fgBSZ+IGP6Ec
      OSpFeOznbTcdtzsAwXZnLxmpPg7MPF2AHY5Q7Pr48dypCuUHfyyjG2yOCIpSwqf4
      hceskIdX+A2IKahG9ps35D+0ZqYtAgMBAAGjUzBRMB0GA1UdDgQWBBSSjA7RLoiW
      0okfnKCKQud3IKzNcjAfBgNVHSMEGDAWgBSSjA7RLoiW0okfnKCKQud3IKzNcjAP
      BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQA7tdOMq3LhMi0kawzn
      +L4QFpdbNLhdECdElCihsUh29HSFE2R/TIXJoWlN86fl/vRYnITk0pwyxg8zUuHg
      c0QiEQeEl3HhvblA5H3S/8ZspUVlL+ot+oAMdmd8pAXOLfZwZCS+vZfysUEeYcr/
      95VKlBtLluHGeWul+l09qgOzmXHEvcc+jMwcmr63rYNig/4CqKDXn3n/GhmzfSMe
      cunT+sAupKyIECxrXKc8xrJ9vUbecc1BYeC7y7v9z7D6Swdm93QIIJw1fLJi1z3m
      HpPObHAFTYvukdVlBMSz2y5s8Nx5Ao9bLSljLfl0BSmnDkjAtW0LmAFMng0FU7Qa
      VF9l
      -----END CERTIFICATE-----
      """;

  @Test
  void shouldBuildAFactoryFromOptionsWithoutTouchingTheNetwork() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldAcceptAConfiguredConnectTimeout() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.ofSeconds(5))
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldAcceptAConfiguredResponseTimeout() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.RESPONSE_TIMEOUT, Duration.ofSeconds(30))
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldAcceptAResponseTimeoutParsedFromAUrlQueryString() {
    // given - ConnectionFactoryOptions.parse(url) stores query-string values as plain Strings, not
    // as the Duration this option is declared as; this is the actual R2DBC-URL bootstrap path (as
    // opposed to shouldAcceptAConfiguredResponseTimeout above, which builds a typed option
    // directly).
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse("r2dbc:clickhouse://localhost:8123?responseTimeout=PT30S");

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectAnInvalidResponseTimeoutParsedFromAUrlQueryString() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:clickhouse://localhost:8123?responseTimeout=not-a-duration");

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(DateTimeParseException.class);
  }

  @Test
  void shouldRejectOptionsWithNoHost() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(NoSuchOptionException.class);
  }

  @Test
  void shouldRejectASslRootCertOptionWhenSslIsNotEnabled() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.SSL_ROOT_CERT, "test-ssl-root-cert.pem")
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ssl=true");
  }

  @Test
  void shouldBuildAFactoryWhenSslRootCertIsAClasspathResource() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.SSL, true)
            .option(ClickHouseConnectionFactoryProvider.SSL_ROOT_CERT, "test-ssl-root-cert.pem")
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldBuildAFactoryWhenSslRootCertIsAFilesystemPath() throws Exception {
    // given
    final Path certificateFile = Files.createTempFile("ssl-root-cert", ".pem");
    certificateFile.toFile().deleteOnExit();
    Files.writeString(certificateFile, A_VALID_CERTIFICATE_PEM);
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.SSL, true)
            .option(
                ClickHouseConnectionFactoryProvider.SSL_ROOT_CERT,
                certificateFile.toAbsolutePath().toString())
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectASslRootCertThatIsNeitherAClasspathResourceNorAFile() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.SSL, true)
            .option(
                ClickHouseConnectionFactoryProvider.SSL_ROOT_CERT, "does-not-exist-anywhere.pem")
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does-not-exist-anywhere.pem");
  }

  @Test
  void shouldAcceptCustomRetryMaxAttemptsAndDelay() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.RETRY_MAX_ATTEMPTS, 5)
            .option(ClickHouseConnectionFactoryProvider.RETRY_DELAY, Duration.ofMillis(200))
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldAcceptRetryMaxAttemptsZeroToDisableRetrying() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.RETRY_MAX_ATTEMPTS, 0)
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectANegativeRetryMaxAttempts() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.RETRY_MAX_ATTEMPTS, -1)
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldAcceptCustomTransportPoolOptions() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, 5)
            .option(ClickHouseConnectionFactoryProvider.TRANSPORT_PENDING_ACQUIRE_MAX_COUNT, 10)
            .option(
                ClickHouseConnectionFactoryProvider.TRANSPORT_PENDING_ACQUIRE_TIMEOUT,
                Duration.ofSeconds(2))
            .option(
                ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_IDLE_TIME, Duration.ofMinutes(1))
            .option(
                ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_LIFE_TIME, Duration.ofMinutes(30))
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectANonPositiveTransportMaxConnections() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, 0)
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectANegativeTransportPendingAcquireTimeout() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(
                ClickHouseConnectionFactoryProvider.TRANSPORT_PENDING_ACQUIRE_TIMEOUT,
                Duration.ofSeconds(-1))
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldAcceptTransportPoolOptionsParsedFromAUrlQueryString() {
    // given - ConnectionFactoryOptions.parse(url) stores query-string values as plain Strings, not
    // as the Integer/Duration these options are declared as; this is the actual R2DBC-URL bootstrap
    // path (as opposed to every other test in this class, which builds typed options directly).
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:clickhouse://localhost:8123"
                + "?transportMaxConnections=5"
                + "&transportPendingAcquireMaxCount=10"
                + "&transportPendingAcquireTimeout=PT2S"
                + "&transportMaxIdleTime=PT1M"
                + "&transportMaxLifeTime=PT30M");

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectAnInvalidTransportPendingAcquireTimeoutParsedFromAUrlQueryString() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:clickhouse://localhost:8123?transportPendingAcquireTimeout=not-a-duration");

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(DateTimeParseException.class);
  }

  @Test
  void shouldAcceptAConfiguredObservationListener() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(
                ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER,
                new RecordingDriverObservationListener())
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldAcceptAUserOptionWithNoAccompanyingPasswordOption() {
    // given - the real proof that this authenticates with an empty password, not the literal
    // string "null", is ClickHouseConnectionFactoryAuthenticationTest's
    // shouldSendAnEmptyPasswordWhenUserIsPresentAndPasswordIsAbsent; this test only covers that
    // building the factory itself doesn't blow up.
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.USER, "someone")
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldAcceptAConfiguredDatabase() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.DATABASE, "analytics")
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.getMetadata().getName()).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectAnObservationListenerThatIsNotADriverObservationListenerInstance() {
    // given - unlike every other option on this provider, DriverObservationListener has no
    // URL-string form; a String value here can only be a configuration mistake.
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:clickhouse://localhost:8123?observationListener=not-a-listener");

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldNotBeDisposedBeforeDisposeIsCalled() {
    // given
    final ClickHouseConnectionFactory factory =
        ClickHouseConnectionFactory.from(
            ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.HOST, "localhost")
                .build());

    // then
    assertThat(factory.isDisposed()).isFalse();
  }

  @Test
  void shouldReportDisposedAfterDisposeReleasesEveryOwnedResource() {
    // given - the transport's connection pool and the row-decoding scheduler are the two resources
    // this factory owns and shares across every Connection it produces; isDisposed() is true only
    // once both report disposed, not just one of them.
    final ClickHouseConnectionFactory factory =
        ClickHouseConnectionFactory.from(
            ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.HOST, "localhost")
                .build());

    // when
    factory.dispose();

    // then
    assertThat(factory.isDisposed()).isTrue();
  }

  @Test
  void shouldBeIdempotentWhenDisposedMoreThanOnce() {
    // given
    final ClickHouseConnectionFactory factory =
        ClickHouseConnectionFactory.from(
            ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.HOST, "localhost")
                .build());
    factory.dispose();

    // when / then
    assertThatCode(factory::dispose).doesNotThrowAnyException();
  }

  @Test
  void shouldSizeTheDecoderSchedulerToTheExplicitTransportMaxConnections() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, 5)
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then - the decoder must never be a smaller, hidden concurrency ceiling than the pool a
    // caller explicitly asked for
    assertThat(factory.decoderWorkerCount()).isEqualTo(5);
  }

  @Test
  void shouldSizeTheDecoderSchedulerToReactorNettysOwnDefaultPoolFormulaWhenPoolSizeIsUnset() {
    // given - same formula documented in docs/operations/connection-pooling.md's "Reactor Netty's
    // own defaults" table: max(availableProcessors, 8) * 2, at least 16
    final int expectedWorkerCount = Math.max(Runtime.getRuntime().availableProcessors(), 8) * 2;
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.decoderWorkerCount()).isEqualTo(expectedWorkerCount);
  }

  @Test
  void shouldSizeTheDecoderSchedulerToAnExplicitDecoderWorkerCountEvenWhenLargerThanThePool() {
    // given - Phase 11 PR5 (see ROADMAP.md): decoderWorkerCount lets a caller widen the decode
    // pool beyond the physical connection pool, to absorb transient decode-queueing tail latency
    // without also widening the connection pool itself
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, 8)
            .option(ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT, 32)
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then - the explicit override wins over the pool-size-coupled default
    assertThat(factory.decoderWorkerCount()).isEqualTo(32);
  }

  @Test
  void shouldRejectANonPositiveDecoderWorkerCount() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT, 0)
            .build();

    // when / then
    assertThatThrownBy(() -> ClickHouseConnectionFactory.from(options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldUsePlatformThreadDecoderByDefault() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then
    assertThat(factory.decoderUsesVirtualThreads()).isFalse();
  }

  @Test
  void shouldUseVirtualThreadDecoderWhenExplicitlyEnabled() {
    // given - experimental opt-in escape hatch (Phase 11, JDK 21 virtual-thread decoder scheduler
    // experiment, see ROADMAP.md); not yet a trusted-benchmark-validated default
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.TRANSPORT_MAX_CONNECTIONS, 8)
            .option(ClickHouseConnectionFactoryProvider.DECODER_USE_VIRTUAL_THREADS, true)
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then - the threading model changes, the worker-count contract does not
    assertThat(factory.decoderUsesVirtualThreads()).isTrue();
    assertThat(factory.decoderWorkerCount()).isEqualTo(8);
  }

  @Test
  void shouldStillCoupleTheVirtualThreadDecoderMaxConcurrencyToThePoolSize() {
    // given
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ClickHouseConnectionFactoryProvider.DECODER_USE_VIRTUAL_THREADS, true)
            .option(ClickHouseConnectionFactoryProvider.DECODER_WORKER_COUNT, 12)
            .build();

    // when
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // then - an explicit decoderWorkerCount still wins over the pool-size-coupled default, exactly
    // as it does for the platform-thread scheduler
    assertThat(factory.decoderUsesVirtualThreads()).isTrue();
    assertThat(factory.decoderWorkerCount()).isEqualTo(12);
  }
}

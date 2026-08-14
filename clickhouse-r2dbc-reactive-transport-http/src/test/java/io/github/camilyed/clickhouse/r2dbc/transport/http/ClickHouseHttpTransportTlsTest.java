package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.Http11SslContextSpec;
import reactor.netty.http.server.HttpServer;

/**
 * Verifies, against a real (if self-signed) TLS server, that an {@code https://} base URL actually
 * triggers a TLS handshake through {@link ClickHouseHttpTransport} — the claim {@code
 * ClickHouseConnectionFactory}'s Javadoc and README make about {@code ssl=true}, previously
 * untested (see ROADMAP.md's Production readiness review) — and that a caller-supplied trusted
 * certificate lets that handshake actually succeed against a self-signed or internal-CA
 * certificate, which real ClickHouse deployments (Kubernetes/Tanzu-style, in particular) commonly
 * use.
 *
 * <p>Reactor Netty's own documentation states that an {@code https://} URI automatically applies a
 * default {@code SslProvider} with no explicit {@code .secure(...)} call needed on {@link
 * ClickHouseHttpTransport}'s side — {@link
 * #shouldAttemptATlsHandshakeWhenConnectingToAnHttpsBaseUrl} proves that empirically rather than
 * trusting the docs, by deliberately using a self-signed certificate the client does <em>not</em>
 * trust: if the handshake is attempted at all (proving auto-negotiation actually happened), it
 * fails with an {@link SSLException} somewhere in its cause chain — a fundamentally different
 * failure mode than a plain connection error, which is what would happen if the {@code https://}
 * scheme were silently ignored and plaintext HTTP were sent to a TLS-only port instead. {@link
 * #shouldSucceedTheHandshakeWhenTheServerCertificateIsExplicitlyTrusted} then proves the positive
 * case: the same self-signed certificate, but supplied to the transport as a trusted certificate,
 * lets the handshake succeed and a real response come back.
 *
 * <p>Needs Bouncy Castle ({@code bcpkix-jdk18on}, {@code testRuntimeOnly} only — never shipped in
 * the production artifact) on the test runtime classpath: {@link SelfSignedCertificate}'s
 * JDK-internal fallback certificate generator doesn't work on every JDK version, and Bouncy Castle
 * is the provider it prefers when available.
 */
class ClickHouseHttpTransportTlsTest {

  @Test
  void shouldAttemptATlsHandshakeWhenConnectingToAnHttpsBaseUrl() throws Exception {
    // given
    final SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
    final Http11SslContextSpec serverSslContext =
        Http11SslContextSpec.forServer(
            selfSignedCertificate.certificate(), selfSignedCertificate.privateKey());
    final DisposableServer server =
        HttpServer.create()
            .port(0)
            .secure(spec -> spec.sslContext(serverSslContext))
            .route(
                routes ->
                    routes.post("/", (request, response) -> response.sendString(Mono.just("ok"))))
            .bindNow();

    try {
      final var transport = new ClickHouseHttpTransport("https://localhost:" + server.port());

      // when
      final Throwable thrown =
          catchThrowable(
              () ->
                  transport
                      .query(ClickHouseQuery.of("SELECT 1"))
                      .aggregate()
                      .asByteArray()
                      .block(Duration.ofSeconds(5)));

      // then
      assertThat(thrown).isNotNull();
      assertThat(hasSslExceptionInCauseChain(thrown)).isTrue();
    } finally {
      server.disposeNow();
    }
  }

  @Test
  void shouldSucceedTheHandshakeWhenTheServerCertificateIsExplicitlyTrusted() throws Exception {
    // given
    final SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
    final Http11SslContextSpec serverSslContext =
        Http11SslContextSpec.forServer(
            selfSignedCertificate.certificate(), selfSignedCertificate.privateKey());
    final DisposableServer server =
        HttpServer.create()
            .port(0)
            .secure(spec -> spec.sslContext(serverSslContext))
            .route(
                routes ->
                    routes.post("/", (request, response) -> response.sendString(Mono.just("ok"))))
            .bindNow();
    final byte[] trustedCertificatePem =
        Files.readAllBytes(selfSignedCertificate.certificate().toPath());

    try {
      final var transport =
          new ClickHouseHttpTransport(
              "https://localhost:" + server.port(),
              Authentication.none(),
              null,
              null,
              trustedCertificatePem);

      // when
      final byte[] response =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .aggregate()
              .asByteArray()
              .block(Duration.ofSeconds(5));

      // then
      assertThat(response).isEqualTo("ok".getBytes());
    } finally {
      server.disposeNow();
    }
  }

  @Test
  void shouldRejectATrustedCertificateSuppliedForAPlainHttpBaseUrl() {
    // given
    final byte[] trustedCertificatePem = "not a real certificate".getBytes();

    // when / then
    assertThatThrownBy(
            () ->
                new ClickHouseHttpTransport(
                    "http://localhost:8123",
                    Authentication.none(),
                    null,
                    null,
                    trustedCertificatePem))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("https://");
  }

  private static boolean hasSslExceptionInCauseChain(final Throwable throwable) {
    return Stream.iterate(throwable, Objects::nonNull, Throwable::getCause)
        .anyMatch(SSLException.class::isInstance);
  }
}

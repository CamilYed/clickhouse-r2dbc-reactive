package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.netty.handler.ssl.util.SelfSignedCertificate;
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
 * untested (see ROADMAP.md's Production readiness review).
 *
 * <p>Reactor Netty's own documentation states that an {@code https://} URI automatically applies a
 * default {@code SslProvider} with no explicit {@code .secure(...)} call needed on {@link
 * ClickHouseHttpTransport}'s side — this test proves that empirically rather than trusting the
 * docs. {@link ClickHouseHttpTransport} has no constructor for supplying a custom trust store, so
 * this deliberately uses a self-signed certificate the client does <em>not</em> trust: if the
 * handshake is attempted at all (proving auto-negotiation actually happened), it fails with an
 * {@link SSLException} somewhere in its cause chain — a fundamentally different failure mode than a
 * plain connection error, which is what would happen if the {@code https://} scheme were silently
 * ignored and plaintext HTTP were sent to a TLS-only port instead.
 *
 * <p>What this test does <em>not</em> cover — deliberately out of scope, tracked as a separate open
 * question in ROADMAP.md rather than silently assumed solved: a successful, fully-trusted TLS round
 * trip against a self-signed or internal-CA certificate, which real ClickHouse deployments commonly
 * use. Doing that would require {@link ClickHouseHttpTransport} to expose some way to configure a
 * custom trust store/{@code SslContext}, which does not exist today.
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

  private static boolean hasSslExceptionInCauseChain(final Throwable throwable) {
    return Stream.iterate(throwable, Objects::nonNull, Throwable::getCause)
        .anyMatch(SSLException.class::isInstance);
  }
}

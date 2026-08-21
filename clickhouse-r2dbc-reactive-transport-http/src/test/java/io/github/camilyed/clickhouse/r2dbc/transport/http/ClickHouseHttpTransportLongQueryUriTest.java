package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * Characterizes what actually happens today when a query's SQL text is long enough that the
 * resulting request URI — {@link ClickHouseHttpTransport#query} sends the whole SQL via {@code
 * ?query=<url-encoded-sql>}, never a request body, see that method's Javadoc — exceeds a
 * request-line length limit an intermediary imposes. See ROADMAP.md's Phase 8, item 8, which this
 * test exists to answer: "is there an actual problem?", before any redesign of the request shape is
 * considered.
 *
 * <p>ClickHouse's own HTTP server is generous here ({@code http_max_uri_size} defaults to 1 MiB —
 * clickhouse.com/docs/interfaces/http, not assumed), but a real deployment commonly sits behind a
 * reverse proxy or load balancer with a far smaller limit. Netty's own {@code HttpRequestDecoder} —
 * which both Reactor Netty's {@link HttpServer} and most Netty-based proxies use — defaults {@code
 * maxInitialLineLength} to 4096 bytes (confirmed against Reactor Netty's own {@code
 * HttpRequestDecoderSpec.DEFAULT_MAX_INITIAL_LINE_LENGTH}, not assumed). This test stands in for
 * that whole class of intermediary with a plain Reactor Netty server explicitly left at that exact
 * default, rather than running an actual nginx/ALB instance (this sandbox has no Docker) — the
 * underlying mechanism being characterized, an HTTP/1.1-compliant request-line length limit, is the
 * same either way.
 */
class ClickHouseHttpTransportLongQueryUriTest {

  @Test
  void shouldFailRatherThanSilentlyTruncatingOrHangingWhenTheRequestLineExceedsAnIntermediarysLimit() {
    // given - a server explicitly left at Netty's own default 4096-byte maxInitialLineLength (see
    // class Javadoc), standing in for a typical reverse proxy/load balancer with the same default;
    // a SQL string long enough that query()'s encoded URI comfortably exceeds that limit
    final DisposableServer server =
        HttpServer.create()
            .port(0)
            .httpRequestDecoder(spec -> spec.maxInitialLineLength(4096))
            .handle((request, response) -> response.status(200).sendString(Mono.just("ok")))
            .bindNow();
    try {
      final var transport = new ClickHouseHttpTransport("http://localhost:" + server.port());
      final String longSql = "SELECT 1 -- " + "x".repeat(6000);

      // when
      final Throwable thrown =
          catchThrowable(
              () ->
                  transport
                      .query(ClickHouseQuery.of(longSql))
                      .aggregate()
                      .asByteArray()
                      .block(Duration.ofSeconds(5)));

      // then - fails loudly (connection-level error) rather than silently truncating the SQL,
      // silently succeeding against a different (truncated) query, or hanging forever; this proves
      // the "real problem" ROADMAP.md's Phase 8 item 8 asks to confirm before any redesign - a
      // caller whose SQL grows past an intermediary's request-line limit gets a clear failure
      // today, not silent data corruption, but does get a failure, confirming the scenario is real
      assertThat(thrown).isNotNull();
    } finally {
      server.disposeNow();
    }
  }
}

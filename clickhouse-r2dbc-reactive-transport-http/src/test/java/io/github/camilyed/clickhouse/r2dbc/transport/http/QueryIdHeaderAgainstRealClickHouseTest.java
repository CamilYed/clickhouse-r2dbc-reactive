package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import reactor.netty.http.client.HttpClient;

/**
 * A verification spike, not a permanent contract test — deliberately bypasses {@link
 * ClickHouseHttpTransport} and talks to Reactor Netty directly.
 *
 * <p>client-v2's own {@code ClickHouseHttpProto} source carries a suspicious contradiction: {@code
 * HEADER_QUERY_ID}'s Javadoc says "Response only header ... Cannot be used in request", yet
 * client-v2's own {@code addHeaders} code sets it as a <b>request</b> header anyway (and separately
 * also sends a {@code query_id} URL query parameter). We already send {@code X-ClickHouse-Query-Id}
 * as a request header in {@link ClickHouseHttpTransport#query}, proven only against our own
 * controlled fake server, which trivially echoes back whatever header it receives — that proves
 * nothing about whether a <em>real</em> ClickHouse server actually honors it. This test settles
 * that empirically: send an explicit {@code query_id} as a request header only (no query
 * parameter), and check whether the server's own response header echoes the same value back, which
 * is the one way to observe the server actually used it rather than generating its own.
 */
class QueryIdHeaderAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  @Test
  void shouldEchoBackTheQueryIdWeSentAsARequestHeader() {
    // given
    final String credentials = clickHouseUsername() + ":" + clickHousePassword();
    final String basicAuth =
        "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    final HttpClient httpClient =
        HttpClient.create()
            .baseUrl(clickHouseHttpUrl())
            .headers(headers -> headers.set("Authorization", basicAuth));

    // when
    final String echoedQueryId =
        httpClient
            .headers(headers -> headers.set("X-ClickHouse-Query-Id", "my-explicit-query-id"))
            .post()
            .uri("/?query=" + URLEncoder.encode("SELECT 1", StandardCharsets.UTF_8))
            .response(
                (response, content) ->
                    content
                        .aggregate()
                        .asString()
                        .thenReturn(response.responseHeaders().get("X-ClickHouse-Query-Id")))
            .blockLast(Duration.ofSeconds(10));

    // then
    assertThat(echoedQueryId).isEqualTo("my-explicit-query-id");
  }
}

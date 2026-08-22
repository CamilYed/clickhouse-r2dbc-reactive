package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.github.camilyed.clickhouse.r2dbc.transport.http.TransportOptions;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves {@link ClickHouseStatement#execute()}'s row decoding never runs on the Reactor Netty
 * event-loop thread that delivered the response — the exact property {@link RowDecodingScheduler}
 * exists to guarantee (see its Javadoc and {@code RowBinaryDecoder#decode}'s). Without {@link
 * RowBinaryDecoder#decode}'s per-row {@code subscribeOn}, this would fail: a {@code Flux.generate}
 * source with no scheduler of its own runs its generator function on whatever thread calls {@code
 * request()}, which here is the event-loop thread that just delivered the response headers, not the
 * thread that originally subscribed.
 *
 * <p>Builds its transport with {@link ResponseCompression#NONE}, overriding this driver's own
 * {@code responseCompression=true} default (see {@code TransportOptions#defaults()}): {@link
 * ControlledClickHouseServer} always sends its configured response bytes verbatim, regardless of
 * whether a request asked for {@code compress=1} — it has no ClickHouse-style LZ4 encoder of its
 * own — so decoding its response as compressed would fail. This class's own concern (which thread
 * decodes a row) is orthogonal to compression either way.
 */
class RowDecodingSchedulerOwnershipTest {

  @Test
  void shouldNeverDecodeARowOnTheReactorNettyEventLoopThread() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(
                  server.baseUrl(),
                  TransportOptions.defaults().withResponseCompression(ResponseCompression.NONE)),
              RowDecodingScheduler.defaults());

      // when
      final List<String> rowThreadNames =
          Flux.from(connection.createStatement("SELECT 1").execute())
              .flatMap(result -> result.map((row, metadata) -> Thread.currentThread().getName()))
              .collectList()
              .block(Duration.ofSeconds(5));

      // then
      assertThat(rowThreadNames).isNotEmpty();
      assertThat(rowThreadNames).noneMatch(name -> name.startsWith("reactor-http-nio"));
    }
  }
}

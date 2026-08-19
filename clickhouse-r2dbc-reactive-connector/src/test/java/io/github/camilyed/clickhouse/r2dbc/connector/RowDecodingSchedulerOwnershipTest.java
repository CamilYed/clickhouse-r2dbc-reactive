package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
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
              new ClickHouseHttpTransport(server.baseUrl()), RowDecodingScheduler.defaults());

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

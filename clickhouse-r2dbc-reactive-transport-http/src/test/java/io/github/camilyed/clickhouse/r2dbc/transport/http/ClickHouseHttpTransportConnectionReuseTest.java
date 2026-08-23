package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * Confirms this driver's underlying Reactor Netty connection pool actually reuses one TCP
 * connection across two sequential point-shaped queries — the property {@code
 * BoundedPoolConcurrencyBenchmark}'s "small bounded pool" story (docs/PERFORMANCE.md) depends on.
 *
 * <p>Verified via {@code request.remoteAddress()} observed server-side, not via Reactor Netty's
 * {@code doOnConnection} hook — an earlier version of this test used {@code doOnConnection} to
 * count TCP connections and got {@code 2} for what turned out to be one reused connection: {@code
 * doOnConnection} fires per HTTP exchange over a channel, not per raw TCP accept, so it double
 * -counted a single kept-alive connection serving two sequential requests. {@code
 * request.remoteAddress()} is a reliable signal instead — the OS assigns a new client-side
 * ephemeral port per new outbound TCP connection, confirmed against this exact scenario's Reactor
 * Netty debug log ({@code reactor.netty.resources.PooledConnectionProvider}: "Releasing channel" /
 * "Channel cleaned, ... 1 inactive connections" after request 1, then "Channel acquired, ... 0
 * inactive connections" for request 2 — same channel ID both times, textbook pool reuse).
 */
class ClickHouseHttpTransportConnectionReuseTest {

  @Test
  void shouldReuseTheUnderlyingConnectionAcrossTwoSequentialSingleRowQueriesConsumedViaNext() {
    // given — the exact access pattern PointQueryBenchmark/BoundedPoolConcurrencyBenchmark use:
    // consume only the first (and, for this fixture, only) row via Flux#next(), which cancels the
    // upstream the moment it has that one element rather than waiting for a natural onComplete.
    final byte[] responseBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    final List<SocketAddress> remoteAddressesSeen = new CopyOnWriteArrayList<>();
    final DisposableServer server = startServer(responseBody, remoteAddressesSeen);
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport("http://localhost:" + server.port());

    try {
      // when / then — releasing the first request's connection back to the pool after its early
      // cancellation (via next()) is Reactor Netty's own asynchronous bookkeeping, happening on
      // the event loop independently of when FluxInputStreamBridge#close() (see its Javadoc)
      // observes end-of-stream on our side. Nothing on ClickHouseHttpTransport's public surface
      // exposes an idle-connection-count or pool-release hook to poll for "the connection is back
      // in the pool yet" before issuing the second query, so there is no condition to synchronize
      // on directly. Awaitility retries the whole two-request sequence instead of widening a fixed
      // sleep: it says out loud what's actually being waited for (reuse is eventually observed,
      // once that race resolves), and a genuine regression still fails the test after the budget,
      // since asking again wouldn't make a real defect start reusing the connection.
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(20))
          .untilAsserted(
              () -> {
                remoteAddressesSeen.clear();

                final DecodedRow first = firstRowOf(transport);
                final DecodedRow second = firstRowOf(transport);

                assertThat(first).isNotNull();
                assertThat(second).isNotNull();
                assertThat(remoteAddressesSeen)
                    .as(
                        "both requests should have come from the same client-side (ephemeral "
                            + "port) address, i.e. the same reused TCP connection")
                    .hasSize(2)
                    .containsOnly(remoteAddressesSeen.get(0));
              });
    } finally {
      server.disposeNow();
    }
  }

  @Test
  void shouldReuseTheUnderlyingConnectionAcrossTwoSequentialQueriesFullyDrained() {
    // given — the same query fully drained (collectList lets the underlying Flux.generate reach
    // its own sink.complete(), no early cancel) instead of stopped early via next().
    final byte[] responseBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    final List<SocketAddress> remoteAddressesSeen = new CopyOnWriteArrayList<>();
    final DisposableServer server = startServer(responseBody, remoteAddressesSeen);
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport("http://localhost:" + server.port());

    try {
      // when
      final List<DecodedRow> firstRows = allRowsOf(transport);
      final List<DecodedRow> secondRows = allRowsOf(transport);

      // then
      assertThat(firstRows).hasSize(1);
      assertThat(secondRows).hasSize(1);
      assertThat(remoteAddressesSeen)
          .as(
              "both requests should have come from the same client-side (ephemeral port) "
                  + "address, i.e. the same reused TCP connection")
          .hasSize(2)
          .containsOnly(remoteAddressesSeen.get(0));
    } finally {
      server.disposeNow();
    }
  }

  private static DisposableServer startServer(
      final byte[] responseBody, final List<SocketAddress> remoteAddressesSeen) {
    return HttpServer.create()
        .port(0)
        .route(
            routes ->
                routes.post(
                    "/",
                    (request, response) -> {
                      remoteAddressesSeen.add(request.remoteAddress());
                      return response
                          .header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                          .header("Content-Type", "application/octet-stream")
                          .sendByteArray(Mono.just(responseBody));
                    }))
        .bindNow();
  }

  private static DecodedRow firstRowOf(final ClickHouseHttpTransport transport) {
    final Flux<ByteBuffer> body =
        transport.query(ClickHouseQuery.of("SELECT 1")).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decodeRows(body, ResponseCompression.NONE)
        .next()
        .block(Duration.ofSeconds(5));
  }

  private static List<DecodedRow> allRowsOf(final ClickHouseHttpTransport transport) {
    final Flux<ByteBuffer> body =
        transport.query(ClickHouseQuery.of("SELECT 1")).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decodeRows(body, ResponseCompression.NONE)
        .collectList()
        .block(Duration.ofSeconds(5));
  }
}

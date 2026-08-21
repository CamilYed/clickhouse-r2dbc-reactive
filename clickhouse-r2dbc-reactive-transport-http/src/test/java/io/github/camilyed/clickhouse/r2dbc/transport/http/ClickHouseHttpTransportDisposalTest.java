package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.testkit.abilities.ToByteArrayAbility;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * {@link ClickHouseHttpTransport#dispose()}/{@link ClickHouseHttpTransport#disposeLater()} release
 * this transport's underlying Reactor Netty connection pool — see {@code
 * io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory}'s own {@code
 * dispose()}, which owns exactly one {@link ClickHouseHttpTransport} per factory and is responsible
 * for disposing it.
 *
 * <p>{@code isDisposed()} on a transport that has never actually sent a request is vacuously {@code
 * true}, not {@code false}, as this class's own Javadoc documents — Reactor Netty's {@code
 * PooledConnectionProvider#isDisposed()} is {@code channelPools.isEmpty() || ...allMatch(isDisposed)},
 * and no per-remote-host pool exists at all until the first {@code acquire()} (i.e. the first
 * request actually sent). {@link #shouldNotBeDisposedBeforeDisposeIsCalled()} below sends one real
 * request first specifically to avoid asserting against that vacuous-true state.
 */
class ClickHouseHttpTransportDisposalTest implements ToByteArrayAbility {

  @Test
  void shouldNotBeDisposedBeforeDisposeIsCalled() {
    // given - a real request first, so the underlying connection pool actually exists; see this
    // class's Javadoc for why isDisposed() would otherwise be vacuously true on an unused transport
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());
      transport.query(ClickHouseQuery.of("SELECT 1")).aggregate().asByteArray().block(
          Duration.ofSeconds(5));

      // then
      assertThat(transport.isDisposed()).isFalse();
    }
  }

  @Test
  void shouldReportDisposedAfterDispose() {
    // given
    final var transport = new ClickHouseHttpTransport("http://localhost:1");

    // when
    transport.dispose();

    // then
    assertThat(transport.isDisposed()).isTrue();
  }

  @Test
  void shouldBeIdempotentWhenDisposedMoreThanOnce() {
    // given
    final var transport = new ClickHouseHttpTransport("http://localhost:1");
    transport.dispose();

    // when / then
    assertThatCode(transport::dispose).doesNotThrowAnyException();
  }

  @Test
  void shouldReportDisposedAfterDisposeLaterCompletes() {
    // given
    final var transport = new ClickHouseHttpTransport("http://localhost:1");

    // when
    transport.disposeLater().block(Duration.ofSeconds(5));

    // then
    assertThat(transport.isDisposed()).isTrue();
  }
}

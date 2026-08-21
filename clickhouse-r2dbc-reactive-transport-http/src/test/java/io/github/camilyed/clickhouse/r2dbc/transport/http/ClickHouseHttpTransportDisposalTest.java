package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * {@link ClickHouseHttpTransport#dispose()}/{@link ClickHouseHttpTransport#disposeLater()} release
 * this transport's underlying Reactor Netty connection pool — see {@code
 * io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory}'s own {@code
 * dispose()}, which owns exactly one {@link ClickHouseHttpTransport} per factory and is responsible
 * for disposing it. None of these tests ever send a request — building a transport and configuring
 * its {@code HttpClient} touches no network by itself (see {@link ClickHouseHttpTransport}'s
 * constructor), so disposal can be exercised entirely against an un-contacted pool.
 */
class ClickHouseHttpTransportDisposalTest {

  @Test
  void shouldNotBeDisposedBeforeDisposeIsCalled() {
    // given
    final var transport = new ClickHouseHttpTransport("http://localhost:1");

    // then
    assertThat(transport.isDisposed()).isFalse();
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

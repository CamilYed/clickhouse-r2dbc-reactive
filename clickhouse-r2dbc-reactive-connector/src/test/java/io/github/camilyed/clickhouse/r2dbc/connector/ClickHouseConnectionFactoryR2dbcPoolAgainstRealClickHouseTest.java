package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ValidationDepth;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

/**
 * Proves this driver's {@link ClickHouseConnectionFactory} works correctly when wrapped by {@code
 * io.r2dbc.pool}'s {@link ConnectionPool} — the standard way Spring Boot's {@code
 * spring.r2dbc.pool.*} properties (and any other R2DBC-SPI-compliant pooling layer) apply pooling
 * on top of an arbitrary driver; see {@code examples/spring-boot-webflux-demo}'s {@code
 * R2dbcConfiguration} for the real usage this mirrors. No test anywhere in this repo exercised this
 * combination before this class.
 *
 * <p>Note on what "pooling" means at two different layers, so this class's scope is clear: {@link
 * ConnectionPool} here pools {@link ClickHouseConnection} wrapper objects at the R2DBC SPI level —
 * a separate, lower-level pool (this driver's own {@link
 * io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport}, backed by Reactor
 * Netty's {@code ConnectionProvider}) is what actually pools the underlying TCP connections, shared
 * across every {@link ClickHouseConnection} this factory produces regardless of what {@link
 * ConnectionPool} does above it (see {@code ClickHouseConnectionFactory}'s own Javadoc). This class
 * tests the R2DBC-SPI-level pool's own contract (acquire/validate/bound concurrency) working
 * correctly against this driver — not a claim about TCP-connection-level behavior, which {@link
 * ClickHouseHttpTransportConnectionReuseTest} (a different module) already covers directly.
 */
class ClickHouseConnectionFactoryR2dbcPoolAgainstRealClickHouseTest
    extends BaseClickHouseIntegrationTest {

  @Test
  void shouldExecuteAQueryThroughAPooledConnection() {
    // given
    final ConnectionPool pool = newPool(poolConfig().build());

    try {
      // when
      final Object value =
          Mono.usingWhen(
                  pool.create(),
                  connection ->
                      Mono.from(connection.createStatement("SELECT 1").execute())
                          .flatMap(result -> Mono.from(result.map((row, meta) -> row.get(0)))),
                  Connection::close)
              .block(Duration.ofSeconds(10));

      // then
      assertThat(value).isNotNull();
    } finally {
      pool.dispose();
    }
  }

  @Test
  void shouldValidateAPooledConnectionRemotely() {
    // given
    final ConnectionPool pool =
        newPool(poolConfig().validationDepth(ValidationDepth.REMOTE).build());

    try {
      // when
      final Boolean valid =
          Mono.usingWhen(
                  pool.create(),
                  connection -> Mono.from(connection.validate(ValidationDepth.REMOTE)),
                  Connection::close)
              .block(Duration.ofSeconds(10));

      // then
      assertThat(valid).isTrue();
    } finally {
      pool.dispose();
    }
  }

  @Test
  void shouldBoundConcurrentPooledConnectionsToMaxSize() {
    // given — maxSize=1: while the only connection is checked out, a second acquire attempt
    // must not complete until the first one is released back to the pool.
    //
    // Note on an earlier, discarded version of this test: it tracked concurrency with a shared
    // AtomicInteger incremented/decremented around each connection's use and asserted the
    // observed maximum was 1 — it flaked to 2. The counter's own decrement (in a doFinally on the
    // *inner* query Mono) fires before Mono.usingWhen's cleanup action (Connection::close, which
    // is what actually returns the connection to the pool) has run, so the counter could read
    // "released" slightly before the pool agreed. That is an instrumentation timing gap, not
    // evidence about the pool itself — the same class of mistake this suite already hit once with
    // doOnConnection in ClickHouseHttpTransportConnectionReuseTest. This version instead asserts
    // the pool's own observable behavior directly: a second acquire genuinely blocks while the
    // first connection is held, with no counter in between.
    final ConnectionPool pool = newPool(poolConfig().maxSize(1).initialSize(0).build());

    try {
      // when — the only connection is acquired and held open.
      final Connection first = Mono.from(pool.create()).block(Duration.ofSeconds(10));

      // and — a second acquire is bounded to a short timeout while the first is still held.
      final Throwable secondAcquireOutcome =
          catchThrowable(() -> Mono.from(pool.create()).timeout(Duration.ofMillis(500)).block());

      // then — the second acquire could not complete in time; the pool genuinely blocked it.
      // Exceptions.unwrap strips Reactor's RuntimeException wrapper around the checked
      // TimeoutException if block() applied one; it is a harmless no-op if it did not.
      assertThat(Exceptions.unwrap(secondAcquireOutcome)).isInstanceOf(TimeoutException.class);

      // and — once the first connection is released, a new acquire succeeds normally.
      Mono.from(first.close()).block(Duration.ofSeconds(10));
      final Connection afterRelease = Mono.from(pool.create()).block(Duration.ofSeconds(10));
      assertThat(afterRelease).isNotNull();
      Mono.from(afterRelease.close()).block(Duration.ofSeconds(10));
    } finally {
      pool.dispose();
    }
  }

  private static ConnectionPool newPool(final ConnectionPoolConfiguration config) {
    return new ConnectionPool(config);
  }

  private ConnectionPoolConfiguration.Builder poolConfig() {
    final URI httpUrl = URI.create(clickHouseHttpUrl());
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .option(ConnectionFactoryOptions.HOST, httpUrl.getHost())
            .option(ConnectionFactoryOptions.PORT, httpUrl.getPort())
            .option(ConnectionFactoryOptions.USER, clickHouseUsername())
            .option(ConnectionFactoryOptions.PASSWORD, clickHousePassword())
            .build();
    return ConnectionPoolConfiguration.builder(ClickHouseConnectionFactory.from(options));
  }
}

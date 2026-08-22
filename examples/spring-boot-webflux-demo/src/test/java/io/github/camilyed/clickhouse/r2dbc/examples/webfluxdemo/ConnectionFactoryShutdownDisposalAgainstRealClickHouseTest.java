package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import reactor.core.publisher.Mono;

/**
 * Proves, rather than just asserts in prose, that closing the Spring context actually disposes the
 * driver's own {@code ClickHouseConnectionFactory} — not just the {@code io.r2dbc.pool}
 * {@code ConnectionPool} wrapping it — see {@code R2dbcConfiguration#connectionFactory}'s Javadoc
 * for the full reasoning ({@code ConnectionPool#disposeLater()}, confirmed directly against its own
 * source, never calls {@code dispose()} on the factory it wraps).
 *
 * <p>Deliberately never imports a single class from the driver itself, matching every other test in
 * this module (see {@code R2dbcConfiguration}'s and {@code build.gradle.kts}'s Javadoc/comments on
 * why): autowires {@code R2dbcConfiguration#baseConnectionFactory} — the driver's raw, unpooled
 * {@link ConnectionFactory} bean — purely through the standard R2DBC SPI, then proves disposal by
 * its <em>effect</em> rather than by inspecting driver-internal state. A driver whose {@code
 * dispose()} was never actually called at shutdown would still be able to serve a query afterward,
 * since the real ClickHouse container this test runs against keeps running independently of the
 * Spring context; this only fails once the driver's own transport connection pool was genuinely
 * released.
 */
@SpringBootTest
class ConnectionFactoryShutdownDisposalAgainstRealClickHouseTest {

  private static final ClickHouseContainer CLICK_HOUSE =
      new ClickHouseContainer("clickhouse/clickhouse-server:latest");

  static {
    CLICK_HOUSE.start();
  }

  @DynamicPropertySource
  static void clickHouseProperties(final DynamicPropertyRegistry registry) {
    registry.add(
        "spring.r2dbc.url",
        () -> CLICK_HOUSE.getHttpUrl().replaceFirst("^http://", "r2dbc:clickhouse://"));
    registry.add("spring.r2dbc.username", CLICK_HOUSE::getUsername);
    registry.add("spring.r2dbc.password", CLICK_HOUSE::getPassword);
  }

  @Autowired private ConfigurableApplicationContext applicationContext;

  @Autowired
  @Qualifier("baseConnectionFactory")
  private ConnectionFactory baseConnectionFactory;

  @Test
  @DirtiesContext
  void shouldDisposeTheDriversOwnConnectionFactoryWhenTheSpringContextCloses() {
    // given - the raw, unpooled ConnectionFactory works normally before shutdown
    assertThatCode(() -> runTrivialQuery(baseConnectionFactory)).doesNotThrowAnyException();

    // when
    applicationContext.close();

    // then - the same query against the same factory reference now fails, even though the real
    // ClickHouse container it talks to is still running - the only thing that changed is that the
    // driver's own transport connection pool was released when the Spring context closed
    final Throwable thrown = catchThrowable(() -> runTrivialQuery(baseConnectionFactory));
    assertThat(thrown).isNotNull();
  }

  private static void runTrivialQuery(final ConnectionFactory connectionFactory) {
    Mono.usingWhen(
            connectionFactory.create(),
            connection ->
                Mono.from(connection.createStatement("SELECT 1").execute())
                    .flatMap(result -> Mono.from(result.map((row, meta) -> row.get(0)))),
            Connection::close)
        .block(Duration.ofSeconds(10));
  }
}

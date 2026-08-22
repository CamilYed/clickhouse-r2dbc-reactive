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
 * Proves, rather than just asserts in prose, that closing the Spring context disposes this driver's
 * own {@code ClickHouseConnectionFactory} — not only the outer {@code r2dbc-pool} {@code
 * ConnectionPool} wrapping it. See {@code R2dbcConfiguration#baseConnectionFactory}'s own Javadoc
 * for why the two-bean split with an explicit {@code destroyMethod = "dispose"} is what makes this
 * work: {@code ConnectionPool#disposeLater()} only tears down the pool it owns and never calls
 * anything on the delegate factory it was built from.
 *
 * <p>Runs against driver {@code 0.2.1} specifically — the {@code dispose()}/{@code isDisposed()}
 * methods this test relies on do not exist on the previously-pinned {@code 0.2.0} release (see this
 * module's {@code build.gradle.kts}), which is exactly why an earlier attempt at this same proof
 * failed with a real {@code BeanDefinitionValidationException} at context-startup time.
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
    // given
    assertThatCode(() -> runTrivialQuery(baseConnectionFactory)).doesNotThrowAnyException();

    // when
    applicationContext.close();

    // then
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

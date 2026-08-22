package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
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
 * <p>Deliberately builds its own standalone {@link ConfigurableApplicationContext} via {@link
 * SpringApplicationBuilder} instead of using {@code @SpringBootTest} — confirmed the hard way that
 * {@code @SpringBootTest}'s shared, cached context does not tolerate a test closing it manually:
 * Spring's {@code EventPublishingTestExecutionListener} still tries to publish an {@code
 * AfterTestExecutionEvent} against it afterward, which goes through {@code
 * DefaultContextCache.restartContextIfNecessary} and throws {@code IllegalStateException:
 * LifecycleProcessor not initialized}, since {@code close()} already tore down the context's
 * lifecycle processor and {@code restart()} isn't the same operation as a fresh {@code refresh()}.
 * A context this test builds and owns itself, entirely outside the TestContext framework's cache,
 * has no such interaction — closing it mid-test is exactly what it's for.
 *
 * <p>Runs against driver {@code 0.2.1} specifically — the {@code dispose()}/{@code isDisposed()}
 * methods this test relies on do not exist on the previously-pinned {@code 0.2.0} release (see this
 * module's {@code build.gradle.kts}), which is exactly why an earlier attempt at this same proof
 * failed with a real {@code BeanDefinitionValidationException} at context-startup time.
 */
class ConnectionFactoryShutdownDisposalAgainstRealClickHouseTest {

  private static final ClickHouseContainer CLICK_HOUSE =
      new ClickHouseContainer("clickhouse/clickhouse-server:latest");

  static {
    CLICK_HOUSE.start();
  }

  @Test
  void shouldDisposeTheDriversOwnConnectionFactoryWhenTheSpringContextCloses() {
    // given
    final ConfigurableApplicationContext context = startApplicationContext();
    final ConnectionFactory baseConnectionFactory =
        context.getBean("baseConnectionFactory", ConnectionFactory.class);
    assertThatCode(() -> runTrivialQuery(baseConnectionFactory)).doesNotThrowAnyException();

    // when
    context.close();

    // then
    final Throwable thrown = catchThrowable(() -> runTrivialQuery(baseConnectionFactory));
    assertThat(thrown).isNotNull();
  }

  private static ConfigurableApplicationContext startApplicationContext() {
    return new SpringApplicationBuilder(DemoApplication.class)
        .web(WebApplicationType.NONE)
        .properties(
            "spring.r2dbc.url="
                + CLICK_HOUSE.getHttpUrl().replaceFirst("^http://", "r2dbc:clickhouse://"),
            "spring.r2dbc.username=" + CLICK_HOUSE.getUsername(),
            "spring.r2dbc.password=" + CLICK_HOUSE.getPassword())
        .run();
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

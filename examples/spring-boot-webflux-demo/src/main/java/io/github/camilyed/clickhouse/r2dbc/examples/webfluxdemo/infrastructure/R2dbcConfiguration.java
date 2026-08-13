package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.infrastructure;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.time.Duration;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.transaction.ReactiveTransactionManager;

/**
 * Builds this demo's {@link ConnectionFactory}, {@link DatabaseClient}, and {@link
 * ReactiveTransactionManager} beans by hand instead of relying on Spring Boot's own R2DBC
 * auto-configuration.
 *
 * <p>Two reasons this exists rather than letting {@code spring-boot-starter-data-r2dbc}'s
 * auto-configuration do it, both discovered while building this demo, not anticipated up front:
 *
 * <ol>
 *   <li>Building {@link DatabaseClient} explicitly, with an explicit {@link BindMarkersFactory},
 *       means Spring Boot's own {@code ConnectionFactoryDependentConfiguration.r2dbcDatabaseClient}
 *       bean method (annotated {@code @ConditionalOnMissingBean(DatabaseClient.class)}) never runs,
 *       so it never calls {@code BindMarkersFactoryResolver.resolve(...)}. That resolver only
 *       recognizes a small hardcoded list of drivers (Postgres, MySQL, MariaDB, SQL Server, H2 —
 *       see Spring Framework's own {@code r2dbc.adoc}, "Currently supported databases"); ClickHouse
 *       isn't on it, so left to Boot's own auto-configuration, {@code DatabaseClient} bean creation
 *       fails outright with {@code IllegalStateException: Cannot determine a BindMarkersFactory for
 *       ClickHouse} before a single query ever runs. Declaring the bean ourselves sidesteps that
 *       resolution path entirely — simpler than this project's first fix attempt, a {@code
 *       BindMarkersFactoryResolver.BindMarkerFactoryProvider} SPI extension registered via {@code
 *       META-INF/spring.factories}, which is no longer needed once {@code DatabaseClient} is built
 *       here instead of by Boot.
 *   <li>Full pool configuration ({@code initial-size}, {@code min-idle}, {@code max-size}, the
 *       various timeouts, {@code validation-depth}) the way {@link ConnectionPoolConfiguration}
 *       exposes it, plus fail-fast validation of the pool settings and a startup log line stating
 *       exactly what was configured — useful for a demo meant to look like a real application's
 *       wiring, not a toy.
 * </ol>
 *
 * <p>What this does <b>not</b> fix: {@link BindMarkersFactory} only ever produces placeholder
 * <em>text</em> (e.g. {@code "?"}), with no way to carry the parameter <em>type</em> that
 * ClickHouse's own {@code {name:Type}} parameterized-query syntax requires inline in the SQL text.
 * So even with a {@link DatabaseClient} bean that constructs successfully, calling {@code
 * DatabaseClient.sql(...).bind(...)} or {@code R2dbcEntityTemplate}'s object-mapped insert/query
 * still fails at query-<em>execution</em> time (confirmed: throws {@link IndexOutOfBoundsException}
 * from this driver's {@code ClickHouseStatement.bind}, since the generated SQL declares zero {@code
 * {name:Type}} parameters to bind against). {@code DatabaseClientOrderEventRepository} therefore
 * only ever issues fully pre-formed SQL with escaped literal values, never {@code .bind(...)}. See
 * ROADMAP.md's Phase 6 for the full write-up.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(R2dbcProperties.class)
class R2dbcConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(R2dbcConfiguration.class);
  private static final String DISABLED = "<disabled>";

  private final R2dbcProperties properties;

  R2dbcConfiguration(final R2dbcProperties properties) {
    this.properties = properties;
  }

  /**
   * The application's single {@link ConnectionFactory}, pooled via {@link ConnectionPool} unless
   * {@code spring.r2dbc.pool.enabled=false}.
   */
  @Bean
  @Primary
  ConnectionFactory connectionFactory() {
    final R2dbcProperties.Pool pool = properties.getPool();
    validatePoolConfiguration(pool);

    final ConnectionFactoryOptions options = buildConnectionFactoryOptions();
    rejectNestedPoolUrl(options);

    final ConnectionFactory baseConnectionFactory = ConnectionFactories.get(options);
    logConfiguration(options, pool, baseConnectionFactory);

    if (!pool.isEnabled()) {
      LOG.info("R2DBC pool is disabled; using an unpooled ConnectionFactory");
      return baseConnectionFactory;
    }

    final ConnectionPool connectionPool =
        new ConnectionPool(buildPoolConfiguration(baseConnectionFactory, pool));
    LOG.info(
        "R2DBC pool initialized: factory={}, maxSize={}",
        connectionPool.getMetadata().getName(),
        pool.getMaxSize());
    return connectionPool;
  }

  /**
   * A {@link DatabaseClient} built explicitly against an anonymous {@code "?"} {@link
   * BindMarkersFactory} — see this class's own Javadoc for why building it here, rather than
   * letting Spring Boot auto-configure it, is what makes the bean constructible against this driver
   * at all.
   */
  @Bean
  DatabaseClient databaseClient(final ConnectionFactory connectionFactory) {
    return DatabaseClient.builder()
        .connectionFactory(connectionFactory)
        .bindMarkers(BindMarkersFactory.anonymous("?"))
        .build();
  }

  /**
   * A real, application-wired {@link ReactiveTransactionManager} — deliberately still registered
   * despite {@code ClickHouseConnection.beginTransaction()} always failing (see that class's own
   * Javadoc and {@code TransactionManagerDivergenceTest}), so that test proves the failure through
   * the application's actual bean wiring rather than a manually-constructed instance.
   */
  @Bean
  ReactiveTransactionManager transactionManager(final ConnectionFactory connectionFactory) {
    return new R2dbcTransactionManager(connectionFactory);
  }

  private ConnectionFactoryOptions buildConnectionFactoryOptions() {
    final String url = requireText(properties.getUrl(), "spring.r2dbc.url must be configured");
    final ConnectionFactoryOptions.Builder builder = ConnectionFactoryOptions.parse(url).mutate();

    if (hasText(properties.getUsername())) {
      builder.option(ConnectionFactoryOptions.USER, properties.getUsername());
    }
    if (properties.getPassword() != null) {
      builder.option(ConnectionFactoryOptions.PASSWORD, properties.getPassword());
    }
    properties
        .getProperties()
        .forEach((name, value) -> builder.option(Option.valueOf(name), value));

    return builder.build();
  }

  private void rejectNestedPoolUrl(final ConnectionFactoryOptions options) {
    final Object driver = options.getValue(ConnectionFactoryOptions.DRIVER);
    if ("pool".equals(driver)) {
      throw new IllegalStateException(
          "spring.r2dbc.url uses driver=pool - use r2dbc:clickhouse://... directly; this "
              + "configuration builds its own ConnectionPool around the base ConnectionFactory.");
    }
  }

  private ConnectionPoolConfiguration buildPoolConfiguration(
      final ConnectionFactory baseConnectionFactory, final R2dbcProperties.Pool pool) {
    final ConnectionPoolConfiguration.Builder builder =
        ConnectionPoolConfiguration.builder(baseConnectionFactory)
            .name("clickhouse-r2dbc-demo")
            .initialSize(pool.getInitialSize())
            .minIdle(pool.getMinIdle())
            .maxSize(pool.getMaxSize())
            .maxIdleTime(pool.getMaxIdleTime())
            .validationDepth(pool.getValidationDepth())
            .acquireRetry(pool.getAcquireRetry());

    applyIfPresent(pool.getMaxLifeTime(), builder::maxLifeTime);
    applyIfPresent(pool.getMaxAcquireTime(), builder::maxAcquireTime);
    applyIfPresent(pool.getMaxCreateConnectionTime(), builder::maxCreateConnectionTime);
    applyIfPresent(pool.getMaxValidationTime(), builder::maxValidationTime);

    return builder.build();
  }

  private void validatePoolConfiguration(final R2dbcProperties.Pool pool) {
    if (pool.getInitialSize() < 0) {
      throw new IllegalStateException("spring.r2dbc.pool.initial-size must not be negative");
    }
    if (pool.getMaxSize() < 1) {
      throw new IllegalStateException("spring.r2dbc.pool.max-size must be greater than zero");
    }
    if (pool.getInitialSize() > pool.getMaxSize()) {
      throw new IllegalStateException(
          "spring.r2dbc.pool.initial-size must not be greater than max-size");
    }
    if (pool.getMinIdle() < 0 || pool.getMinIdle() > pool.getMaxSize()) {
      throw new IllegalStateException(
          "spring.r2dbc.pool.min-idle must be between zero and max-size");
    }
  }

  private void logConfiguration(
      final ConnectionFactoryOptions options,
      final R2dbcProperties.Pool pool,
      final ConnectionFactory baseConnectionFactory) {
    LOG.info(
        """

        R2DBC configuration
          driver            : {}
          connectionFactory : {}

        R2DBC pool
          enabled           : {}
          initialSize       : {}
          minIdle           : {}
          maxSize           : {}
          maxIdleTime       : {}
          maxLifeTime       : {}
          maxAcquireTime    : {}
          validationDepth   : {}
          acquireRetry      : {}
        """,
        options.getValue(ConnectionFactoryOptions.DRIVER),
        baseConnectionFactory.getClass().getName(),
        pool.isEnabled(),
        pool.getInitialSize(),
        pool.getMinIdle(),
        pool.getMaxSize(),
        formatDuration(pool.getMaxIdleTime()),
        formatDuration(pool.getMaxLifeTime()),
        formatDuration(pool.getMaxAcquireTime()),
        pool.getValidationDepth(),
        pool.getAcquireRetry());
  }

  private static String formatDuration(final Duration duration) {
    return duration == null ? DISABLED : duration.toString();
  }

  private static <T> void applyIfPresent(final T value, final Consumer<T> consumer) {
    if (value != null) {
      consumer.accept(value);
    }
  }

  private static String requireText(final String value, final String message) {
    if (!hasText(value)) {
      throw new IllegalStateException(message);
    }
    return value;
  }

  private static boolean hasText(final String value) {
    return value != null && !value.isBlank();
  }
}

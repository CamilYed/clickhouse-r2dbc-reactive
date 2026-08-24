package io.github.camilyed.clickhouse.r2dbc.macrobench.r2dbc;

import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.BenchmarkQueryBackend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.config.BenchmarkProperties;
import io.github.camilyed.clickhouse.r2dbc.macrobench.config.ConditionalOnBackendEnabled;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;

/**
 * This driver's {@link ConnectionFactory}/{@link DatabaseClient} beans for the {@code r2dbc}
 * backend - only created when {@code benchmark.backend} is {@code r2dbc} or {@code dual} (see
 * {@link ConditionalOnBackendEnabled}). Deliberately no {@code io.r2dbc.pool.ConnectionPool}
 * wrapper, unlike {@code examples/spring-boot-webflux-demo}: ROADMAP.md's Phase 12 fairness config
 * is explicit that the real pool here is already this driver's own {@code ClickHouseHttpTransport}
 * Reactor Netty {@code ConnectionProvider} - an outer logical pool would add a queue on top of it
 * that this project's own docs already say most users don't need, contaminating the primary
 * r2dbc-vs-client-v2 comparison this module exists for. Pool pinned to {@link
 * BenchmarkProperties#poolSize()} (default 8) via {@code transportMaxConnections} unless the caller
 * already set it explicitly through {@code spring.r2dbc.properties.*} - see that record's Javadoc
 * for why an unpinned comparison is not trustworthy.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(R2dbcProperties.class)
class R2dbcBackendConfiguration {

  private static final String TRANSPORT_MAX_CONNECTIONS_OPTION = "transportMaxConnections";

  private final R2dbcProperties properties;
  private final BenchmarkProperties benchmark;

  R2dbcBackendConfiguration(final R2dbcProperties properties, final BenchmarkProperties benchmark) {
    this.properties = properties;
    this.benchmark = benchmark;
  }

  @Bean(destroyMethod = "dispose")
  @ConditionalOnBackendEnabled(Backend.R2DBC)
  ConnectionFactory r2dbcConnectionFactory() {
    final ConnectionFactoryOptions.Builder builder =
        ConnectionFactoryOptions.parse(requireUrl()).mutate();
    if (hasText(properties.getUsername())) {
      builder.option(ConnectionFactoryOptions.USER, properties.getUsername());
    }
    if (properties.getPassword() != null) {
      builder.option(ConnectionFactoryOptions.PASSWORD, properties.getPassword());
    }
    properties
        .getProperties()
        .forEach((name, value) -> builder.option(Option.valueOf(name), value));
    if (!benchmark.unpinR2dbcPool()
        && !properties.getProperties().containsKey(TRANSPORT_MAX_CONNECTIONS_OPTION)) {
      builder.option(
          Option.valueOf(TRANSPORT_MAX_CONNECTIONS_OPTION), String.valueOf(benchmark.poolSize()));
    }
    return ConnectionFactories.get(builder.build());
  }

  @Bean
  @ConditionalOnBackendEnabled(Backend.R2DBC)
  DatabaseClient r2dbcDatabaseClient(final ConnectionFactory r2dbcConnectionFactory) {
    return DatabaseClient.builder()
        .connectionFactory(r2dbcConnectionFactory)
        .bindMarkers(BindMarkersFactory.anonymous("?"))
        .build();
  }

  @Bean
  @ConditionalOnBackendEnabled(Backend.R2DBC)
  BenchmarkQueryBackend r2dbcBenchmarkQueryBackend(final DatabaseClient r2dbcDatabaseClient) {
    return new R2dbcBenchmarkQueryBackend(r2dbcDatabaseClient);
  }

  private String requireUrl() {
    final String url = properties.getUrl();
    if (url == null || url.isBlank()) {
      throw new IllegalStateException("spring.r2dbc.url must be configured");
    }
    return url;
  }

  private static boolean hasText(final String value) {
    return value != null && !value.isBlank();
  }
}

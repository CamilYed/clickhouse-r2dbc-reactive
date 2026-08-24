package io.github.camilyed.clickhouse.r2dbc.macrobench.clientv2;

import com.clickhouse.client.api.Client;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.BenchmarkQueryBackend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.config.BenchmarkProperties;
import io.github.camilyed.clickhouse.r2dbc.macrobench.config.ClickHouseEndpointProperties;
import io.github.camilyed.clickhouse.r2dbc.macrobench.config.ConditionalOnBackendEnabled;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * A client-v2 {@link Client} for the {@code client-v2} comparison backend - only created when
 * {@code benchmark.backend} is {@code client-v2} or {@code dual} (see {@link
 * ConditionalOnBackendEnabled}). {@code useAsyncRequests(true)} is not optional here - see {@link
 * ClientV2BenchmarkQueryBackend}'s Javadoc, same fairness reasoning already proven in {@code
 * clickhouse-r2dbc-reactive-benchmarks}' {@code ClientV2PointQueryClient}. Pool pinned to {@link
 * BenchmarkProperties#poolSize()} (default 8) - see that record's Javadoc for why an unpinned
 * comparison is not trustworthy.
 */
@Configuration(proxyBeanMethods = false)
class ClientV2BackendConfiguration {

  @Bean(destroyMethod = "close")
  @ConditionalOnBackendEnabled(Backend.CLIENT_V2)
  Client clientV2Client(
      final ClickHouseEndpointProperties endpoint, final BenchmarkProperties benchmark) {
    requireHttpUrl(endpoint);
    return new Client.Builder()
        .addEndpoint(endpoint.httpUrl())
        .setUsername(endpoint.username())
        .setPassword(endpoint.password())
        .setDefaultDatabase("default")
        .useAsyncRequests(true)
        .enableConnectionPool(true)
        .setMaxConnections(benchmark.poolSize())
        .build();
  }

  @Bean
  @ConditionalOnBackendEnabled(Backend.CLIENT_V2)
  BenchmarkQueryBackend clientV2BenchmarkQueryBackend(final Client clientV2Client) {
    return new ClientV2BenchmarkQueryBackend(clientV2Client);
  }

  private static void requireHttpUrl(final ClickHouseEndpointProperties endpoint) {
    if (endpoint.httpUrl() == null || endpoint.httpUrl().isBlank()) {
      throw new IllegalStateException("benchmark.clickhouse.http-url must be configured");
    }
  }
}

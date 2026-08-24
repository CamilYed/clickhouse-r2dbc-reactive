package io.github.camilyed.clickhouse.r2dbc.macrobench.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code benchmark.clickhouse.*} - the plain ClickHouse HTTP endpoint (bare {@code
 * http://host:port}, not an {@code r2dbc:clickhouse://...} URL) that both client-v2's {@code
 * Client.Builder} (the {@code client-v2} backend) and this module's own dataset-seeding admin
 * client connect to. The {@code r2dbc} backend instead uses {@code spring.r2dbc.url} (Spring's own
 * {@code R2dbcProperties}) - a deliberately separate config surface from this one, since
 * client-v2's builder and this driver's R2DBC {@code ConnectionFactoryOptions} take different URL
 * shapes for the same underlying server. Both should point at the same ClickHouse instance for the
 * comparison to mean anything - see ROADMAP.md's Phase 12.
 */
@ConfigurationProperties("benchmark.clickhouse")
public record ClickHouseEndpointProperties(String httpUrl, String username, String password) {}

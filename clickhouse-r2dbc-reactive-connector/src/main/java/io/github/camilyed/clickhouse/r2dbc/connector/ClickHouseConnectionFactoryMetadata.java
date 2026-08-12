package io.github.camilyed.clickhouse.r2dbc.connector;

import io.r2dbc.spi.ConnectionFactoryMetadata;

/**
 * Identifies ClickHouse as the product a {@link io.r2dbc.spi.ConnectionFactory} built by this
 * driver connects to.
 */
final class ClickHouseConnectionFactoryMetadata implements ConnectionFactoryMetadata {

  static final ClickHouseConnectionFactoryMetadata INSTANCE =
      new ClickHouseConnectionFactoryMetadata();

  private ClickHouseConnectionFactoryMetadata() {}

  @Override
  public String getName() {
    return "ClickHouse";
  }
}

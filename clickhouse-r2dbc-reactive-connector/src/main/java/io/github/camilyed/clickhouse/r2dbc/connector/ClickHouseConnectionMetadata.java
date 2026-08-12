package io.github.camilyed.clickhouse.r2dbc.connector;

import io.r2dbc.spi.ConnectionMetadata;

/**
 * Identifies the ClickHouse server a {@link ClickHouseConnection} is connected to.
 *
 * <p>{@link #getDatabaseVersion()} is a fixed {@code "unknown"} for now — reporting the real
 * server version needs a query round trip ({@code SELECT version()}) that nothing wires up yet.
 * A deliberate, documented gap rather than a guess, not a design decision to revisit later.
 */
final class ClickHouseConnectionMetadata implements ConnectionMetadata {

    static final ClickHouseConnectionMetadata INSTANCE = new ClickHouseConnectionMetadata();

    private ClickHouseConnectionMetadata() {}

    @Override
    public String getDatabaseProductName() {
        return "ClickHouse";
    }

    @Override
    public String getDatabaseVersion() {
        return "unknown";
    }
}

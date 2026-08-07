/**
 * Thin R2DBC SPI connector for ClickHouse.
 *
 * <p>This package is responsible for R2DBC discovery ({@code ConnectionFactoryProvider}), URL and
 * option parsing, logical connection lifecycle, statement creation and parameter binding, deferred
 * execution, result and metadata adaptation, R2DBC exception mapping, cancellation propagation,
 * and explicit handling of unsupported transaction semantics.
 *
 * <p>It has no dependency on Spring. Business logic (protocol, decoding) lives in {@code
 * io.github.camilyed.clickhouse.r2dbc.core}; transport concerns live in {@code
 * io.github.camilyed.clickhouse.r2dbc.transport.http}. This package should stay thin and not
 * become a second general-purpose ClickHouse client.
 */
package io.github.camilyed.clickhouse.r2dbc.connector;

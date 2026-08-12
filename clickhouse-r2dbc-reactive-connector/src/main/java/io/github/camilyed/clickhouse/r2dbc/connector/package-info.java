/**
 * Thin R2DBC SPI connector for ClickHouse.
 *
 * <p>This package is responsible for R2DBC discovery ({@code ConnectionFactoryProvider}), URL and
 * option parsing, logical connection lifecycle, statement creation and parameter binding, deferred
 * execution, result and metadata adaptation, R2DBC exception mapping, cancellation propagation, and
 * explicit handling of unsupported transaction semantics.
 *
 * <p>It has no dependency on Spring. Business logic (protocol, decoding) lives in {@code
 * io.github.camilyed.clickhouse.r2dbc.core}; transport concerns live in {@code
 * io.github.camilyed.clickhouse.r2dbc.transport.http}. This package should stay thin and not become
 * a second general-purpose ClickHouse client.
 *
 * <p>{@code @NullMarked}: every parameter, return value, and field is non-null unless explicitly
 * annotated {@code @Nullable}. Note this is <em>our own</em> null-safety convention, not r2dbc-spi's
 * — that SPI's own {@code Nullable}/{@code NonNullApi} annotations are package-private to {@code
 * io.r2dbc.spi} and cannot be reused by implementors like this driver.
 */
@NullMarked
package io.github.camilyed.clickhouse.r2dbc.connector;

import org.jspecify.annotations.NullMarked;

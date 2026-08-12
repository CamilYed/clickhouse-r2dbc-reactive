/**
 * Non-blocking HTTP transport adapter for the reactive ClickHouse R2DBC driver, built on Reactor
 * Netty.
 *
 * <p>This package owns non-blocking connection acquisition, active/pending-request limits,
 * connect/acquire/response/idle timeouts, streaming response chunks, aborting active requests,
 * connection reuse decisions, and transport metrics.
 *
 * <p>Reactor Netty is a candidate implementation, not a public contract: the transport SPI exposed
 * to {@code io.github.camilyed.clickhouse.r2dbc.core} must not leak Netty-specific types.
 */
@NullMarked
package io.github.camilyed.clickhouse.r2dbc.transport.http;

import org.jspecify.annotations.NullMarked;

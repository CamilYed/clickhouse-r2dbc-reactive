/**
 * Transport-independent query and protocol core for the reactive ClickHouse R2DBC driver.
 *
 * <p>This package reuses stable {@code com.clickhouse:client-v2} components (settings, protocol
 * encoding, response metadata and row decoding) instead of duplicating them, and adds only the
 * transport-independent lifecycle rules required by a non-blocking, cancellable, bounded
 * execution path: query request representation, {@code query_id} handling, and cancellation
 * state.
 *
 * <p>No transport (HTTP, native TCP, ...) is referenced from this package. See {@code
 * io.github.camilyed.clickhouse.r2dbc.transport.http} for the first transport adapter.
 */
package io.github.camilyed.clickhouse.r2dbc.core;

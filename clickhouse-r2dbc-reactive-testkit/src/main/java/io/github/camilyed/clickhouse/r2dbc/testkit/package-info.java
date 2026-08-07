/**
 * Controlled local server and contract-test support for the reactive ClickHouse R2DBC driver.
 *
 * <p>Provides deterministic scenarios for transport contract tests: immediate response, delayed
 * headers, delayed body, fragmented metadata/rows, partial final record, slow subscriber, no
 * response, connection reset, error after partial data, cancellation before/while
 * queued/during body receive, pool saturation, and pending-acquire timeout.
 *
 * <p>This module is test-support code, not part of the public driver API.
 */
package io.github.camilyed.clickhouse.r2dbc.testkit;

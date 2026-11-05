/**
 * RowBinaryWithNamesAndTypes decoding through the client-v2-backed reader.
 *
 * <p>This package is transport-independent. It consumes response bytes supplied by the core bridge
 * and exposes decoded domain rows without depending on HTTP, Netty, or R2DBC types.
 */
@NullMarked
package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import org.jspecify.annotations.NullMarked;

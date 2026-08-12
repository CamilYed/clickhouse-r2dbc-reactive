package io.github.camilyed.clickhouse.r2dbc.transport.http;

import java.util.function.LongSupplier;
import reactor.netty.ByteBufFlux;

/**
 * The result of {@link ClickHouseHttpTransport#queryWithSummary}: the response body, alongside
 * ClickHouse's own reported written-row count for the query that produced it — see that method's
 * Javadoc for where {@code writtenRows} comes from.
 *
 * <p>{@code writtenRows} is a {@link LongSupplier}, not a plain {@code long}, deliberately: {@code
 * body} is lazy (nothing is sent until it's subscribed), so the actual count isn't known yet at the
 * point this record is constructed — only once the response headers have actually arrived, which
 * happens partway through consuming {@code body}. Call {@code writtenRows().getAsLong()} only after
 * {@code body} has started being consumed (e.g. {@code core.RowBinaryDecoder#decode} already
 * guarantees this — it reads the schema, which only happens after headers arrive, before resolving).
 */
public record ClickHouseQueryResponse(LongSupplier writtenRows, ByteBufFlux body) {}

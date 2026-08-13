package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * A single order lifecycle event — this demo's domain model, independent of both the REST layer
 * ({@code api}) and the persistence adapter ({@code infrastructure}), per the same Ports & Adapters
 * split the driver itself uses between {@code core} and its transport/connector adapters (see the
 * root project's {@code CLAUDE.md}, Architecture section).
 *
 * <p>{@code id} is always supplied by the caller (a fresh {@link UUID}, see {@code
 * OrderEventController#create}) — ClickHouse's {@code INSERT} has no {@code
 * RETURNING}/generated-key mechanism the way Postgres/MySQL do.
 *
 * <p>Field-to-column mapping (see {@code OrderEventsSchemaInitializer} for the exact DDL), chosen
 * specifically to exercise a broad slice of this driver's decode surface through Spring's {@code
 * DatabaseClient}/{@code Row}, not just the trivial types:
 *
 * <ul>
 *   <li>{@code category} — {@code LowCardinality(String)}, decodes as a plain {@link String} (the
 *       wrapper is transparently unwrapped, same as {@code Nullable(...)}).
 *   <li>{@code tags} — {@code Array(String)}, decodes as {@link List}.
 *   <li>{@code amount} — {@code Decimal(18,4)}, decodes as {@link BigDecimal}.
 *   <li>{@code discount} — {@code Nullable(Decimal(18,4))}, an {@link Optional} at this boundary
 *       since core/domain code should never see a silent {@code null} (see {@code CLAUDE.md}'s "No
 *       {@code null} as a silent default" rule) even though the REST/persistence layers do have to
 *       deal with R2DBC's own {@code null}-returning {@code Row.get(...)} directly.
 *   <li>{@code status} — {@code Enum8}, see {@link OrderStatus}'s own Javadoc for a real decode
 *       rough edge this surfaced.
 *   <li>{@code clientIp} — {@code IPv4}, decodes as {@link java.net.InetAddress}.
 *   <li>{@code occurredAt} — {@code DateTime64(3)}, decodes as {@link java.time.ZonedDateTime},
 *       narrowed to {@link Instant} here since the domain doesn't care about the server's reporting
 *       zone.
 * </ul>
 *
 * <p>The full type matrix (all integer widths, {@code Map}, {@code Tuple}, {@code Nested}, {@code
 * Nullable} in general, ...) is already covered at the wire level by {@code transport-http}'s
 * {@code RealWorldTableAgainstRealClickHouseTest} — this demo isn't re-proving that, just proving a
 * representative, realistic slice survives the extra {@code DatabaseClient}/{@code Row} hop.
 */
public record OrderEvent(
    UUID id,
    UUID customerId,
    String category,
    List<String> tags,
    BigDecimal amount,
    Optional<BigDecimal> discount,
    OrderStatus status,
    String clientIp,
    Instant occurredAt) {}

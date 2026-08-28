package io.github.camilyed.clickhouse.r2dbc.transport.http.abilities;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoderMode;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Flux;

/**
 * Test DSL: runs SQL against a real ClickHouse server through the full production pipeline
 * (transport → bridge → {@link RowBinaryDecoder}) instead of each test wiring that plumbing itself.
 */
public interface RealClickHouseQueryAbility {

  // A single scheduler shared across every call this ability makes for the lifetime of the JVM -
  // fine for test code (bounded, daemon-backed, never needs to be disposed for tests to pass),
  // unlike the shipped connector's own one-per-ClickHouseConnectionFactory ownership.
  RowDecodingScheduler DECODING_SCHEDULER = RowDecodingScheduler.defaults();

  ClickHouseHttpTransport transport();

  /** Runs {@code sql} and waits for it to finish (DDL/DML — no rows expected back). */
  default void execute(final String sql) {
    transport()
        .query(ClickHouseQuery.of(sql))
        .aggregate()
        .asByteArray()
        .block(Duration.ofSeconds(10));
  }

  /**
   * Same as {@link #queryRows(String, RowBinaryDecoderMode)}, decoding via {@link
   * RowBinaryDecoderMode#CLICKHOUSE} — this driver's default.
   */
  default List<Map<String, Object>> queryRows(final String sql) {
    return queryRows(sql, RowBinaryDecoderMode.CLICKHOUSE);
  }

  /**
   * Runs {@code sql} and decodes every row it returns, keyed by column name — the production decode
   * path itself returns positional {@link DecodedRow}s (see that class's Javadoc for why), but
   * tests read far more naturally by name; this rebuilds a name-keyed {@link Map} once per row
   * here, at the test-DSL boundary only, not on the driver's own hot path.
   *
   * <p>{@code mode} selects {@link RowBinaryDecoderMode#CLICKHOUSE} (client-v2's own reader) or
   * {@link RowBinaryDecoderMode#NATIVE} (this driver's own reader, with automatic per-result
   * fallback to the exact same client-v2 path for anything it doesn't natively cover) — see {@link
   * RowBinaryDecoder}'s Javadoc. Exposed explicitly so a single type-coverage test suite can run
   * unchanged against both, proving real ClickHouse decodes identically either way rather than
   * asserting that only against one.
   */
  default List<Map<String, Object>> queryRows(final String sql, final RowBinaryDecoderMode mode) {
    final Flux<ByteBuffer> body =
        transport().query(ClickHouseQuery.of(sql)).asByteArray().map(ByteBuffer::wrap);
    final DecodedResult decoded =
        RowBinaryDecoder.decode(body, DECODING_SCHEDULER, transport().responseCompression(), mode)
            .block(Duration.ofSeconds(10));
    final List<String> columnNames =
        decoded.columns().stream().map(ColumnDescriptor::name).toList();
    return decoded
        .rows()
        .map(row -> toMap(columnNames, row))
        .collectList()
        .block(Duration.ofSeconds(10));
  }

  private static Map<String, Object> toMap(final List<String> columnNames, final DecodedRow row) {
    final Map<String, Object> values = new LinkedHashMap<>();
    for (int index = 0; index < columnNames.size(); index++) {
      values.put(columnNames.get(index), row.valueAt(index));
    }
    return values;
  }
}

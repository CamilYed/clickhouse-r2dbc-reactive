package io.github.camilyed.clickhouse.r2dbc.transport.http.abilities;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
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
   * Runs {@code sql} and decodes every row it returns, keyed by column name — the production decode
   * path itself returns positional {@link DecodedRow}s (see that class's Javadoc for why), but
   * tests read far more naturally by name; this rebuilds a name-keyed {@link Map} once per row
   * here, at the test-DSL boundary only, not on the driver's own hot path.
   */
  default List<Map<String, Object>> queryRows(final String sql) {
    final Flux<ByteBuffer> body =
        transport().query(ClickHouseQuery.of(sql)).asByteArray().map(ByteBuffer::wrap);
    final DecodedResult decoded = RowBinaryDecoder.decode(body).block(Duration.ofSeconds(10));
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

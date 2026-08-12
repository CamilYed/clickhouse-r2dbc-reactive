package io.github.camilyed.clickhouse.r2dbc.transport.http.abilities;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
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

  /** Runs {@code sql} and decodes every row it returns. */
  default List<Map<String, Object>> queryRows(final String sql) {
    final Flux<ByteBuffer> body =
        transport().query(ClickHouseQuery.of(sql)).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decodeRows(body).collectList().block(Duration.ofSeconds(10));
  }
}

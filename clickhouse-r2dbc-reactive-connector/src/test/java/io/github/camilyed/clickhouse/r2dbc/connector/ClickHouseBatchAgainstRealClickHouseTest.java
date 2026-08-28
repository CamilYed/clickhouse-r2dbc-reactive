package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves {@link ClickHouseBatch#execute()} runs several standalone statements against a real
 * ClickHouse server, in order, and returns one {@link io.r2dbc.spi.Result} per statement — not just
 * that the pieces compile against hermetic fixtures (already covered by {@link
 * ClickHouseBatchTest}).
 */
class ClickHouseBatchAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  private ClickHouseConnection connection;

  private ClickHouseConnection connection() {
    if (connection == null) {
      connection =
          new ClickHouseConnection(
              new ClickHouseHttpTransport(
                  clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword()),
              RowDecodingScheduler.defaults());
    }
    return connection;
  }

  @Test
  void shouldRunEveryStatementInOrderAndReturnOneResultEach() {
    // given
    final var batch =
        connection()
            .createBatch()
            .add("CREATE TABLE batch_execute_test (id UInt32) ENGINE = Memory")
            .add("INSERT INTO batch_execute_test VALUES (1), (2), (3)")
            .add("SELECT toInt64(count()) AS total FROM batch_execute_test");

    // when
    final List<Long> totals =
        Flux.from(batch.execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("total", Long.class)))
            .collectList()
            .block(Duration.ofSeconds(10));

    // then
    assertThat(totals).containsExactly(3L);
  }
}

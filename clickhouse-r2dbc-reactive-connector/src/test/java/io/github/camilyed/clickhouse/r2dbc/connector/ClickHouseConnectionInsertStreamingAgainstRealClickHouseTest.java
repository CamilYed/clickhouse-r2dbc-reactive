package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Result;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves {@link ClickHouseConnection#insertStreaming} actually lands rows in a real ClickHouse
 * table via a streamed request body, end to end — not just that the fake-server-based hermetic test
 * in {@link ClickHouseConnectionInsertStreamingTest} passes, which only proves the bytes reach
 * whatever's on the other end of the socket, not that a real ClickHouse server accepts a
 * streamed-body {@code TabSeparated} insert the way its own HTTP docs describe.
 */
class ClickHouseConnectionInsertStreamingAgainstRealClickHouseTest
    extends BaseClickHouseIntegrationTest {

  private ClickHouseHttpTransport transport;

  private ClickHouseHttpTransport transport() {
    if (transport == null) {
      transport =
          new ClickHouseHttpTransport(
              clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    }
    return transport;
  }

  private final RowDecodingScheduler decodingScheduler = RowDecodingScheduler.defaults();

  private ClickHouseConnection connection() {
    return new ClickHouseConnection(transport(), decodingScheduler);
  }

  @Test
  void shouldInsertStreamedTabSeparatedDataAndReportTheWrittenRowCount() {
    // given
    execute("CREATE TABLE insert_streaming_test (id UInt32, name String) ENGINE = Memory");
    final byte[] tsvData = "1\tAda\n2\tGrace\n".getBytes(StandardCharsets.UTF_8);

    // when
    final Long rowsUpdated =
        Flux.from(
                connection()
                    .insertStreaming(
                        "INSERT INTO insert_streaming_test FORMAT TabSeparated",
                        Flux.just(ByteBuffer.wrap(tsvData))))
            .flatMap(Result::getRowsUpdated)
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(rowsUpdated).isEqualTo(2L);
    // and - the rows genuinely landed in the table, not just an accepted-but-discarded request
    final List<String> names =
        Flux.from(
                connection()
                    .createStatement("SELECT name FROM insert_streaming_test ORDER BY id")
                    .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("name", String.class)))
            .collectList()
            .block(Duration.ofSeconds(10));
    assertThat(names).containsExactly("Ada", "Grace");
  }

  private void execute(final String sql) {
    transport()
        .query(ClickHouseQuery.of(sql))
        .aggregate()
        .asByteArray()
        .block(Duration.ofSeconds(10));
  }
}

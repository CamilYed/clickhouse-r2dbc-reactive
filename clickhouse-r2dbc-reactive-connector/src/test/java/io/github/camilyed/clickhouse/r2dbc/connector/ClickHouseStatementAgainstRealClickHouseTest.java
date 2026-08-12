package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves {@link ClickHouseStatement#execute()} runs a real query end to end — transport, {@code
 * RowBinaryDecoder}, and {@link ClickHouseResult}/{@link ClickHouseRow}/{@link
 * ClickHouseRowMetadata} together — against a real ClickHouse server, not just that the pieces
 * compile against hermetic fixtures (already covered by each class's own unit test).
 */
class ClickHouseStatementAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  private ClickHouseHttpTransport transport;

  private ClickHouseHttpTransport transport() {
    if (transport == null) {
      transport =
          new ClickHouseHttpTransport(
              clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    }
    return transport;
  }

  private ClickHouseConnection connection() {
    return new ClickHouseConnection(transport());
  }

  @Test
  void shouldRunASelectAndMapEveryDecodedRow() {
    // given
    execute("CREATE TABLE statement_execute_test (id UInt32, name String) ENGINE = Memory");
    execute("INSERT INTO statement_execute_test VALUES (1, 'Ada'), (2, 'Grace')");

    // when
    final List<String> names =
        Flux.from(
                connection()
                    .createStatement("SELECT id, name FROM statement_execute_test ORDER BY id")
                    .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("name", String.class)))
            .collectList()
            .block(Duration.ofSeconds(10));

    // then
    assertThat(names).containsExactly("Ada", "Grace");
  }

  @Test
  void shouldExposeClickHousesRealTypeNameAsColumnMetadata() {
    // given
    execute("CREATE TABLE statement_execute_metadata_test (id UInt32) ENGINE = Memory");
    execute("INSERT INTO statement_execute_metadata_test VALUES (1)");

    // when
    final String typeName =
        Flux.from(
                connection()
                    .createStatement("SELECT id FROM statement_execute_metadata_test")
                    .execute())
            .flatMap(
                result ->
                    result.map(
                        (row, rowMetadata) ->
                            rowMetadata.getColumnMetadata("id").getType().getName()))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(typeName).isEqualTo("UInt32");
  }

  private void execute(final String sql) {
    transport().query(ClickHouseQuery.of(sql)).aggregate().asByteArray().block(Duration.ofSeconds(10));
  }
}

package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.R2dbcException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Proves {@link ClickHouseConnection#setStatementTimeout} actually bounds query execution
 * server-side against a real ClickHouse server — not just that the setting is sent on the wire
 * (already covered, hermetically, by {@link ClickHouseConnectionStatementTimeoutTest}), but that
 * ClickHouse itself cuts the query off and that the resulting error maps onto {@link
 * R2dbcException} the same way every other server-side failure does.
 */
class ClickHouseConnectionStatementTimeoutAgainstRealClickHouseTest
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
  void shouldCutOffAQueryThatExceedsTheConfiguredStatementTimeout() {
    // given
    final ClickHouseConnection connection = connection();
    Mono.from(connection.setStatementTimeout(Duration.ofMillis(200))).block(Duration.ofSeconds(5));

    // when
    final Throwable thrown =
        catchThrowable(
            () ->
                Flux.from(connection.createStatement("SELECT sleep(3)").execute())
                    .blockLast(Duration.ofSeconds(15)));

    // then
    assertThat(thrown).isInstanceOf(R2dbcException.class);
  }

  @Test
  void shouldLetAQueryFinishWithinAGenerousStatementTimeout() {
    // given
    final ClickHouseConnection connection = connection();
    Mono.from(connection.setStatementTimeout(Duration.ofSeconds(30))).block(Duration.ofSeconds(5));

    // when
    final Long value =
        Flux.from(connection.createStatement("SELECT 1 AS value").execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("value", Long.class)))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(value).isEqualTo(1L);
  }
}

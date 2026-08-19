package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.R2dbcException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves {@link ClickHouseStatement#execute()} maps a real ClickHouse server error onto {@link
 * R2dbcException} — not just that {@link ClickHouseR2dbcException#wrap} does the right thing in
 * isolation (already covered by {@link ClickHouseR2dbcExceptionTest}), but that it is actually
 * wired into the {@code Publisher} a caller subscribes to.
 */
class ClickHouseStatementErrorMappingTest {

  @Test
  void shouldFailWithAnR2dbcExceptionCarryingTheServersErrorCode() {
    // given
    try (final var server =
        ControlledClickHouseServer.startRespondingWithClickHouseError(60, "Table not found", 404)) {
      final var statement =
          new ClickHouseStatement(
              new ClickHouseHttpTransport(server.baseUrl()),
              "SELECT 1",
              RowDecodingScheduler.defaults());

      // when
      final Throwable thrown =
          catchThrowable(() -> Flux.from(statement.execute()).blockLast(Duration.ofSeconds(5)));

      // then
      assertThat(thrown).isInstanceOf(R2dbcException.class);
      assertThat(((R2dbcException) thrown).getErrorCode()).isEqualTo(60);
    }
  }
}

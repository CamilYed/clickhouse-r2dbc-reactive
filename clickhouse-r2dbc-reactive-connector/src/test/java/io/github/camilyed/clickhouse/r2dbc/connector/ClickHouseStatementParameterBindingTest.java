package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Parameters;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves {@link ClickHouseStatement#bind(String, Object)} accepts a value wrapped in {@link
 * io.r2dbc.spi.Parameter} (as produced by {@link Parameters#in(Object)}/{@link
 * Parameters#in(Class)} — the R2DBC SPI's own explicit-type binding mechanism, exercised by the
 * TCK's {@code bindValueAsParameter()}/{@code bindNullAsParameter()}) the same way it accepts a raw
 * value — black-box, through the actual request {@link ClickHouseHttpTransport} sends, not by
 * inspecting {@link ClickHouseStatement}'s internal binding map.
 */
class ClickHouseStatementParameterBindingTest {

  private final RowDecodingScheduler decodingScheduler = RowDecodingScheduler.defaults();

  @Test
  void shouldSendTheWrappedValueWhenBindingAnExplicitlyTypedParameter() {
    // given
    final byte[] responseBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(responseBody)) {
      final var statement =
          new ClickHouseStatement(
              new ClickHouseHttpTransport(server.baseUrl()),
              "SELECT {id:Nullable(Int32)}",
              decodingScheduler);
      statement.bind("id", Parameters.in(100));

      Flux.from(statement.execute()).blockLast(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).contains("param_id=100");
    }
  }

  @Test
  void shouldSendTheClickHouseNullMarkerWhenBindingAnExplicitlyTypedNullParameter() {
    // given
    final byte[] responseBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(responseBody)) {
      final var statement =
          new ClickHouseStatement(
              new ClickHouseHttpTransport(server.baseUrl()),
              "SELECT {id:Nullable(Int32)}",
              decodingScheduler);
      statement.bind("id", Parameters.in(Integer.class));

      Flux.from(statement.execute()).blockLast(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).contains("param_id=%5CN");
    }
  }
}

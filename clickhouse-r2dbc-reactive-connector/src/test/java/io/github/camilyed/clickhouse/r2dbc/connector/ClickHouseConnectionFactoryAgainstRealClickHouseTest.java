package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.ValidationDepth;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Proves {@link ClickHouseConnectionFactory#from(ConnectionFactoryOptions)} produces a {@link
 * Connection} that actually talks to a real ClickHouse server — not just that it constructs without
 * error, which {@link ClickHouseConnectionFactoryTest} already covers hermetically.
 */
class ClickHouseConnectionFactoryAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  @Test
  void shouldOpenAConnectionAgainstARealClickHouseServerAndValidateIt() {
    // given
    final URI httpUrl = URI.create(clickHouseHttpUrl());
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .option(ConnectionFactoryOptions.HOST, httpUrl.getHost())
            .option(ConnectionFactoryOptions.PORT, httpUrl.getPort())
            .option(ConnectionFactoryOptions.USER, clickHouseUsername())
            .option(ConnectionFactoryOptions.PASSWORD, clickHousePassword())
            .build();
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);

    // when
    final Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(10));
    final Boolean valid =
        Mono.from(connection.validate(ValidationDepth.REMOTE)).block(Duration.ofSeconds(10));

    // then
    assertThat(valid).isTrue();
  }

  /**
   * Proves the {@code DATABASE} connection option is actually applied server-side, not merely
   * accepted without error (already covered hermetically by {@link
   * ClickHouseConnectionFactoryTest#shouldAcceptAConfiguredDatabase}) — a query with no explicit
   * database qualifier runs against the configured database, confirmed via ClickHouse's own {@code
   * currentDatabase()} function.
   */
  @Test
  void shouldUseDatabaseFromConnectionFactoryOptions() {
    // given
    createDatabase("factory_option_db");
    final URI httpUrl = URI.create(clickHouseHttpUrl());
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .option(ConnectionFactoryOptions.HOST, httpUrl.getHost())
            .option(ConnectionFactoryOptions.PORT, httpUrl.getPort())
            .option(ConnectionFactoryOptions.USER, clickHouseUsername())
            .option(ConnectionFactoryOptions.PASSWORD, clickHousePassword())
            .option(ConnectionFactoryOptions.DATABASE, "factory_option_db")
            .build();
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
    final Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(10));

    // when
    final String currentDatabase =
        Flux.from(connection.createStatement("SELECT currentDatabase() AS db").execute())
            .flatMap(
                result -> Flux.from(result.map((row, metadata) -> row.get("db", String.class))))
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(currentDatabase).isEqualTo("factory_option_db");
  }

  /**
   * Proves a query against a database that does not exist fails clearly as an {@link
   * R2dbcException}, rather than silently falling back to the connecting user's own default
   * database — the same mapping every other server-side ClickHouse failure goes through (see {@link
   * ClickHouseR2dbcException}).
   */
  @Test
  void shouldFailClearlyForUnknownDatabase() {
    // given
    final URI httpUrl = URI.create(clickHouseHttpUrl());
    final ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "clickhouse")
            .option(ConnectionFactoryOptions.HOST, httpUrl.getHost())
            .option(ConnectionFactoryOptions.PORT, httpUrl.getPort())
            .option(ConnectionFactoryOptions.USER, clickHouseUsername())
            .option(ConnectionFactoryOptions.PASSWORD, clickHousePassword())
            .option(ConnectionFactoryOptions.DATABASE, "does_not_exist_db")
            .build();
    final ClickHouseConnectionFactory factory = ClickHouseConnectionFactory.from(options);
    final Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(10));

    // when
    final Throwable thrown =
        catchThrowable(
            () ->
                Flux.from(connection.createStatement("SELECT 1").execute())
                    .blockLast(Duration.ofSeconds(10)));

    // then
    assertThat(thrown).isInstanceOf(R2dbcException.class);
  }
}

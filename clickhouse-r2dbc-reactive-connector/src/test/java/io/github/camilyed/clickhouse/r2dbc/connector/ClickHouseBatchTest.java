package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Batch;
import org.junit.jupiter.api.Test;

class ClickHouseBatchTest {

  // No network call happens in any assertion below - add() only stores sql, so an unreachable
  // address keeps this test hermetic.
  private final ClickHouseHttpTransport transport = new ClickHouseHttpTransport("http://localhost:1");

  private final ClickHouseBatch batch = new ClickHouseBatch(transport);

  @Test
  void shouldReturnItselfWhenAddingAStatement() {
    // when
    final Batch returned = batch.add("SELECT 1");

    // then
    assertThat(returned).isSameAs(batch);
  }

  @Test
  void shouldRejectAddingANullStatement() {
    // when / then
    assertThatThrownBy(() -> batch.add(null)).isInstanceOf(IllegalArgumentException.class);
  }
}

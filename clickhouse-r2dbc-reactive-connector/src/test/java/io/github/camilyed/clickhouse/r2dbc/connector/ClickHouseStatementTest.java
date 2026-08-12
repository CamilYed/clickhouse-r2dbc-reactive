package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class ClickHouseStatementTest {

  // No network call happens in any assertion below - binding only validates/stores values and
  // never touches transport, so an unreachable address keeps this test hermetic.
  private final ClickHouseHttpTransport transport =
      new ClickHouseHttpTransport("http://localhost:1");

  @Test
  void shouldAcceptBindingAKnownNamedParameter() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when
    final Object returned = statement.bind("id", 5);

    // then
    assertThat(returned).isSameAs(statement);
  }

  @Test
  void shouldRejectBindingAnUnknownNamedParameter() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(() -> statement.bind("missing", 5))
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void shouldRejectBindingANullValueByName() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(() -> statement.bind("id", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldBindByIndexInFirstOccurrenceOrder() {
    // given
    final ClickHouseStatement statement =
        new ClickHouseStatement(transport, "SELECT {b:String}, {a:UInt32}");

    // when / then
    assertThatThrownBy(() -> statement.bind(2, "out of range"))
        .isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  void shouldAcceptBindingNullToAKnownNamedParameter() {
    // given
    final ClickHouseStatement statement =
        new ClickHouseStatement(transport, "SELECT {id:Nullable(UInt32)}");

    // when
    final Object returned = statement.bindNull("id", Integer.class);

    // then
    assertThat(returned).isSameAs(statement);
  }

  @Test
  void shouldRejectBindingNullToAnUnknownNamedParameter() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(() -> statement.bindNull("missing", Integer.class))
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void shouldRejectExecutingWithUnboundDeclaredParameters() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(statement::execute).isInstanceOf(IllegalStateException.class);
  }
}

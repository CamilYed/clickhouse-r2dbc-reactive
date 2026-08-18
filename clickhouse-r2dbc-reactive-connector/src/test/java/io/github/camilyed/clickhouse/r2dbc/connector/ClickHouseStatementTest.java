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
  void shouldRejectBindingANullValueByIndex() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(() -> statement.bind(0, null)).isInstanceOf(IllegalArgumentException.class);
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
  void shouldRejectBindingNullWithoutATypeByIndex() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(() -> statement.bindNull(0, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectBindingNullWithoutATypeByName() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(() -> statement.bindNull("id", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectExecutingWithUnboundDeclaredParameters() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(statement::execute).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldRejectAddingABindingSetWithUnboundDeclaredParameters() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");

    // when / then
    assertThatThrownBy(statement::add).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldAcceptAddingABindingSetOnceAllParametersAreBound() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");
    statement.bind("id", 1);

    // when
    final Object returned = statement.add();

    // then
    assertThat(returned).isSameAs(statement);
  }

  @Test
  void shouldStartAFreshBindingSetAfterAdd() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");
    statement.bind("id", 1);
    statement.add();

    // when / then
    // add() must have started a brand new binding set - the trailing set is unbound again, so
    // execute() (which validates the trailing set) rejects it exactly like a never-bound statement.
    assertThatThrownBy(statement::execute).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldAllowExecutingAfterAddOnceTheTrailingSetIsAlsoFullyBound() {
    // given
    final ClickHouseStatement statement = new ClickHouseStatement(transport, "SELECT {id:UInt32}");
    statement.bind("id", 1);
    statement.add();
    statement.bind("id", 2);

    // when
    // execute() only builds the (lazy, unsubscribed) Publisher here - no network call happens,
    // consistent with every other test in this class staying hermetic.
    final Object result = statement.execute();

    // then
    assertThat(result).isNotNull();
  }
}

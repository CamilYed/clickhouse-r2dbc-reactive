package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.clickhouse.ClickHouseContainer;
import reactor.core.publisher.Mono;

/**
 * Proves, rather than just asserts in prose, the ClickHouse transaction-semantics divergence
 * ROADMAP.md's Phase 6 commits to documenting: wiring Spring's standard declarative-transaction
 * machinery ({@link R2dbcTransactionManager}/{@link TransactionalOperator}) over this driver fails
 * clearly, because {@code ClickHouseConnection.beginTransaction()} always returns an error — see
 * that class's own Javadoc for the full reasoning (ClickHouse's transaction feature is
 * experimental, native-protocol-only, and this driver only speaks the stateless HTTP interface).
 * This is <b>not</b> a bug this demo works around; it is the documented, correct behavior for a
 * driver that does not silently pretend to support something the server doesn't reliably offer over
 * the transport this driver actually uses.
 *
 * <p>Uses the application's own {@link ReactiveTransactionManager} bean ({@code
 * infrastructure.R2dbcConfiguration#transactionManager}) rather than constructing one by hand, so
 * this proves the actually-configured app fails this way, not just a scratch instance built inside
 * the test.
 */
@SpringBootTest
class TransactionManagerDivergenceTest {

  private static final ClickHouseContainer CLICK_HOUSE =
      new ClickHouseContainer("clickhouse/clickhouse-server:latest");

  static {
    CLICK_HOUSE.start();
  }

  @DynamicPropertySource
  static void clickHouseProperties(final DynamicPropertyRegistry registry) {
    registry.add(
        "spring.r2dbc.url",
        () -> CLICK_HOUSE.getHttpUrl().replaceFirst("^http://", "r2dbc:clickhouse://"));
    registry.add("spring.r2dbc.username", CLICK_HOUSE::getUsername);
    registry.add("spring.r2dbc.password", CLICK_HOUSE::getPassword);
  }

  @Autowired private ReactiveTransactionManager transactionManager;

  @Test
  void shouldFailToWireStandardSpringDeclarativeTransactionsOverThisDriver() {
    // given
    final TransactionalOperator transactionalOperator =
        TransactionalOperator.create(transactionManager);

    // when
    final Throwable thrown =
        catchThrowable(() -> transactionalOperator.transactional(Mono.just("ignored")).block());

    // then
    assertThat(thrown).hasRootCauseInstanceOf(UnsupportedOperationException.class);
  }
}

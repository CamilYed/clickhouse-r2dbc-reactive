package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ClickHouseConnectionTest {

  // No network call happens during construction or in any assertion below - ClickHouseHttpTransport
  // configures a lazy HttpClient and none of these tests execute a query, so an unreachable
  // address is safe and keeps this test hermetic (see CLAUDE.md: unit tests, no containers).
  private final ClickHouseConnection connection =
      new ClickHouseConnection(new ClickHouseHttpTransport("http://localhost:1"));

  @Test
  void shouldAlwaysBeInAutoCommitMode() {
    // when
    final boolean autoCommit = connection.isAutoCommit();

    // then
    assertThat(autoCommit).isTrue();
  }

  @Test
  void shouldReportClickHouseAsTheDatabaseProduct() {
    // when
    final String name = connection.getMetadata().getDatabaseProductName();

    // then
    assertThat(name).isEqualTo("ClickHouse");
  }

  @Test
  void shouldRejectCreatingAStatementWithoutSql() {
    // when / then
    assertThatThrownBy(() -> connection.createStatement(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectStartingATransaction() {
    // when / then
    StepVerifier.create(connection.beginTransaction())
        .expectError(UnsupportedOperationException.class)
        .verify();
  }

  @Test
  void shouldTreatReleasingASavepointAsANoOp() {
    // when / then
    StepVerifier.create(connection.releaseSavepoint("sp1")).verifyComplete();
  }

  @Test
  void shouldRejectDisablingAutoCommit() {
    // when / then
    StepVerifier.create(connection.setAutoCommit(false))
        .expectError(UnsupportedOperationException.class)
        .verify();
  }

  @Test
  void shouldAcceptEnablingAutoCommitAsANoOp() {
    // when / then
    StepVerifier.create(connection.setAutoCommit(true)).verifyComplete();
  }

  @Test
  void shouldReportReadUncommittedAsTheIsolationLevel() {
    // when
    final IsolationLevel level = connection.getTransactionIsolationLevel();

    // then
    assertThat(level).isEqualTo(IsolationLevel.READ_UNCOMMITTED);
  }

  @Test
  void shouldPassLocalValidationWhenOpen() {
    // when / then
    StepVerifier.create(connection.validate(ValidationDepth.LOCAL))
        .expectNext(true)
        .verifyComplete();
  }

  @Test
  void shouldFailLocalValidationAfterClose() {
    // given
    Mono.from(connection.close()).block(Duration.ofSeconds(1));

    // when / then
    StepVerifier.create(connection.validate(ValidationDepth.LOCAL))
        .expectNext(false)
        .verifyComplete();
  }
}

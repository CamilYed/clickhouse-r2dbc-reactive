package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ClickHouseConnectionTest {

  // No network call happens during construction or in any assertion below - ClickHouseHttpTransport
  // configures a lazy HttpClient and none of these tests execute a query, so an unreachable
  // address is safe and keeps this test hermetic (see CLAUDE.md: unit tests, no containers).
  private final ClickHouseConnection connection =
      new ClickHouseConnection(
          new ClickHouseHttpTransport("http://localhost:1"), RowDecodingScheduler.defaults());

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
  void shouldCreateABatch() {
    // when
    final var batch = connection.createBatch();

    // then
    assertThat(batch).isNotNull();
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

  @Test
  void shouldFailLocalValidationAfterCloseEvenWhenTheValidatePublisherWasObtainedBeforeClose() {
    // given
    // The R2DBC TCK's own validate() test builds every validate()/close() Publisher up front - via
    // Flux.concat(connection.validate(LOCAL), ..., connection.close(), connection.validate(LOCAL),
    // ...) - before subscribing to any of them, then relies on Flux.concat's sequential
    // subscription order to close the connection before the second validate(LOCAL) actually runs.
    // validate(...) must therefore check closed state lazily, at subscription time, not eagerly
    // when the method is called to build the Publisher - otherwise this second Publisher, obtained
    // before close() was ever subscribed to, would wrongly capture "still open".
    final var validateAfterClose = connection.validate(ValidationDepth.LOCAL);
    Mono.from(connection.close()).block(Duration.ofSeconds(1));

    // when / then
    StepVerifier.create(validateAfterClose).expectNext(false).verifyComplete();
  }

  @Test
  void shouldRejectCreatingAStatementAfterClose() {
    // given
    Mono.from(connection.close()).block(Duration.ofSeconds(1));

    // when / then
    assertThatThrownBy(() -> connection.createStatement("SELECT 1"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldRejectCreatingABatchAfterClose() {
    // given
    Mono.from(connection.close()).block(Duration.ofSeconds(1));

    // when / then
    assertThatThrownBy(connection::createBatch).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldRejectInsertStreamingWithoutSql() {
    // given
    final Flux<ByteBuffer> data = Flux.empty();

    // when / then
    assertThatThrownBy(() -> connection.insertStreaming(null, data))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectInsertStreamingWithoutData() {
    // when / then
    assertThatThrownBy(() -> connection.insertStreaming("INSERT INTO t FORMAT TabSeparated", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldAcceptANonNegativeStatementTimeout() {
    // when / then
    StepVerifier.create(connection.setStatementTimeout(Duration.ofSeconds(5))).verifyComplete();
  }

  @Test
  void shouldTreatZeroAsAnExplicitNoTimeout() {
    // when / then
    StepVerifier.create(connection.setStatementTimeout(Duration.ZERO)).verifyComplete();
  }

  @Test
  void shouldRejectANegativeStatementTimeout() {
    // when / then
    StepVerifier.create(connection.setStatementTimeout(Duration.ofSeconds(-1)))
        .expectError(IllegalArgumentException.class)
        .verify();
  }

  @Test
  void shouldRejectInsertStreamingAfterClose() {
    // given
    Mono.from(connection.close()).block(Duration.ofSeconds(1));
    // and
    final Flux<ByteBuffer> data = Flux.just(ByteBuffer.wrap(new byte[0]));

    // when / then
    assertThatThrownBy(() -> connection.insertStreaming("INSERT INTO t FORMAT TabSeparated", data))
        .isInstanceOf(IllegalStateException.class);
  }
}

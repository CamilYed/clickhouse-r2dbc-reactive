package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.clickhouse.client.api.ServerException;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.testkit.abilities.ToByteArrayAbility;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class ClickHouseHttpTransportTest implements ToByteArrayAbility {

  @Test
  void shouldReturnTheConfiguredResponseBody() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    final byte[] receivedBody;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      receivedBody =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .aggregate()
              .asByteArray()
              .block(Duration.ofSeconds(5));
    }

    // then
    assertThat(receivedBody).isEqualTo(configuredBody);
  }

  @Test
  void shouldParseWrittenRowsFromTheClickHouseSummaryHeader() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    final long writtenRows;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWithSummary(
            configuredBody, "{\"read_rows\":\"1\",\"written_rows\":\"3\"}")) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());
      final ClickHouseQueryResponse received =
          transport.queryWithSummary(ClickHouseQuery.of("SELECT 1")).block(Duration.ofSeconds(5));

      // the written-row count is only known once the response headers have arrived, which
      // consuming the body already waits on - see ClickHouseQueryResponse's Javadoc.
      received.body().aggregate().asByteArray().block(Duration.ofSeconds(5));
      writtenRows = received.writtenRows().getAsLong();
    }

    // then
    assertThat(writtenRows).isEqualTo(3L);
  }

  @Test
  void shouldReportZeroWrittenRowsWhenTheSummaryHeaderIsAbsent() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    final long writtenRows;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());
      final ClickHouseQueryResponse received =
          transport.queryWithSummary(ClickHouseQuery.of("SELECT 1")).block(Duration.ofSeconds(5));

      received.body().aggregate().asByteArray().block(Duration.ofSeconds(5));
      writtenRows = received.writtenRows().getAsLong();
    }

    // then
    assertThat(writtenRows).isZero();
  }

  @Test
  void shouldNotSendTheRequestBeforeSubscription() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      // when
      transport.query(ClickHouseQuery.of("SELECT 1"));

      // then
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofMillis(500))
          .untilAsserted(() -> assertThat(server.hasReceivedRequest()).isFalse());
    }
  }

  @Test
  void shouldEmitEachChunkSeparatelyInsteadOfAggregating() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);
    final byte[] secondChunk = "second-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    final List<byte[]> receivedChunks;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWithChunks(firstChunk, secondChunk)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      receivedChunks =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .map(this::toByteArray)
              .collectList()
              .block(Duration.ofSeconds(5));
    }

    // then
    assertThat(receivedChunks).containsExactly(firstChunk, secondChunk);
  }

  @Test
  void shouldCloseTheConnectionWhenTheSubscriptionIsCancelled() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport.query(ClickHouseQuery.of("SELECT 1")).take(1).blockLast(Duration.ofSeconds(5));

      // then
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.hasClosedConnection()).isTrue());
    }
  }

  @Test
  void shouldSendABestEffortKillQueryWhenCancelledAfterTheRequestWasSent() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport
          .query(ClickHouseQuery.of("SELECT 1", "the-cancelled-query-id"))
          .take(1)
          .blockLast(Duration.ofSeconds(5));

      // then
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(
              () ->
                  assertThat(server.receivedUri())
                      .contains("KILL")
                      .contains("the-cancelled-query-id"));
    }
  }

  @Test
  void shouldSendABestEffortKillQueryEvenWhenCancelledBeforeAnyResponseArrives() {
    // given
    // when
    try (final var server = ControlledClickHouseServer.startAcceptingButNeverResponding()) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      final var subscription =
          transport.query(ClickHouseQuery.of("SELECT 1", "the-unanswered-query-id")).subscribe();
      await().atMost(Duration.ofSeconds(2)).until(server::hasReceivedRequest);
      subscription.dispose();

      // then
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(
              () ->
                  assertThat(server.receivedUri())
                      .contains("KILL")
                      .contains("the-unanswered-query-id"));
    }
  }

  @Test
  void shouldStillReturnTheBodyWhenHeadersAreDelayed() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    final byte[] receivedBody;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWithDelay(
            configuredBody, Duration.ofMillis(300))) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      receivedBody =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .aggregate()
              .asByteArray()
              .block(Duration.ofSeconds(5));
    }

    // then
    assertThat(receivedBody).isEqualTo(configuredBody);
  }

  @Test
  void shouldStillReturnTheBodyWhenBodyIsDelayedAfterHeaders() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    final byte[] receivedBody;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWithBodyDelay(
            configuredBody, Duration.ofMillis(300))) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      receivedBody =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .aggregate()
              .asByteArray()
              .block(Duration.ofSeconds(5));
    }

    // then
    assertThat(receivedBody).isEqualTo(configuredBody);
  }

  @Test
  void shouldNotDeliverDataBeforeTheSubscriberRequestsIt() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);
    final byte[] secondChunk = "second-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWithChunks(firstChunk, secondChunk)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      // then
      StepVerifier.create(transport.query(ClickHouseQuery.of("SELECT 1")).map(this::toByteArray), 0)
          .expectSubscription()
          .thenRequest(1)
          .assertNext(chunk -> assertThat(chunk).isEqualTo(firstChunk))
          .thenRequest(1)
          .assertNext(chunk -> assertThat(chunk).isEqualTo(secondChunk))
          .verifyComplete();
    }
  }

  @Test
  void shouldFailWithinAnExplicitlyConfiguredTimeoutWhenTheServerNeverResponds() {
    // given
    try (final var server = ControlledClickHouseServer.startAcceptingButNeverResponding()) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(), Authentication.none(), Duration.ofSeconds(2));
      final Instant start = Instant.now();

      // when
      final Throwable thrown =
          catchThrowable(
              () ->
                  transport
                      .query(ClickHouseQuery.of("SELECT 1"))
                      .aggregate()
                      .asByteArray()
                      .block(Duration.ofSeconds(10)));

      // then
      assertThat(thrown).isNotNull();
      assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofSeconds(5));
    }
  }

  @Test
  void shouldNotTimeOutByDefaultOnAQueryThatTakesLongerThanTheOldHardcodedLimit() {
    // given - an earlier version of this transport hardcoded a 2-second response timeout with no
    // way to disable it; a query taking longer than that would always fail. The default
    // constructor must impose no such limit, matching standard JDBC/R2DBC driver behavior.
    final byte[] body = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWithDelay(
            body, Duration.ofSeconds(3))) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      // when
      final byte[] received =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .aggregate()
              .asByteArray()
              .block(Duration.ofSeconds(10));

      // then
      assertThat(received).isEqualTo(body);
    }
  }

  @Test
  void shouldStillWorkNormallyWhenAConnectTimeoutIsConfigured() {
    // given - proves the new connectTimeout parameter doesn't interfere with a normal, fast local
    // connection; actually proving the timeout fires on a hung TCP handshake would need a real
    // unreachable host, which this project deliberately avoids in hermetic tests (see
    // ControlledClickHouseServer's Javadoc) - that mechanism is Reactor Netty's own well-tested
    // ChannelOption.CONNECT_TIMEOUT_MILLIS, not something reimplemented here.
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    final byte[] receivedBody;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(), Authentication.none(), null, Duration.ofSeconds(5));

      receivedBody =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .aggregate()
              .asByteArray()
              .block(Duration.ofSeconds(5));
    }

    // then
    assertThat(receivedBody).isEqualTo(configuredBody);
  }

  @Test
  void shouldSignalAnErrorWhenTheConnectionIsResetMidResponse() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingThenResettingConnection(
            firstChunk, Duration.ofMillis(200))) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      final Throwable thrown =
          catchThrowable(
              () ->
                  transport
                      .query(ClickHouseQuery.of("SELECT 1"))
                      .aggregate()
                      .asByteArray()
                      .block(Duration.ofSeconds(5)));

      // then
      assertThat(thrown).isNotNull();
    }
  }

  @Test
  void shouldNotStartASecondRequestUntilAConnectionIsFree() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl(), 1);

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      await().atMost(Duration.ofSeconds(2)).until(() -> server.activeConnectionCount() == 1);

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();

      // then
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.activeConnectionCount()).isEqualTo(1));
    }
  }

  @Test
  void shouldNotReachTheServerWhenAQueuedRequestIsCancelledBeforeAConnectionIsAcquired() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl(), 1);

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      await().atMost(Duration.ofSeconds(2)).until(() -> server.activeConnectionCount() == 1);

      final var queuedSubscription = transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      queuedSubscription.dispose();

      // then
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.activeConnectionCount()).isEqualTo(1));
    }
  }

  @Test
  void shouldNotReachTheServerWhenCancelledImmediatelyAfterSubscribing() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe().dispose();

      // then
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofMillis(500))
          .untilAsserted(() -> assertThat(server.hasReceivedRequest()).isFalse());
    }
  }

  @Test
  void shouldSendTheQueryIdAsARequestHeader() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport
          .query(ClickHouseQuery.of("SELECT 1", "my-query-id"))
          .aggregate()
          .asByteArray()
          .block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedQueryId()).isEqualTo("my-query-id");
    }
  }

  @Test
  void shouldSendBoundParametersAsParamQueryParameters() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    final ClickHouseQuery query =
        ClickHouseQuery.of("SELECT {id:UInt32}, {name:String}")
            .withParameters(Map.of("id", 5, "name", "Ada"));

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport.query(query).aggregate().asByteArray().block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).contains("param_id=5").contains("param_name=Ada");
    }
  }

  @Test
  void shouldSignalAServerErrorWhenTheExceptionCodeHeaderIsPresent() {
    // given
    final String errorBody = "Code: 60. DB::Exception: Table default.missing doesn't exist";

    // when
    final Throwable thrown;
    try (final var server =
        ControlledClickHouseServer.startRespondingWithClickHouseError(60, errorBody, 404)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      thrown =
          catchThrowable(
              () ->
                  transport
                      .query(ClickHouseQuery.of("SELECT * FROM missing"))
                      .aggregate()
                      .asByteArray()
                      .block(Duration.ofSeconds(5)));
    }

    // then
    assertThat(thrown).isInstanceOf(ServerException.class);
    assertThat(((ServerException) thrown).getCode()).isEqualTo(60);
    assertThat(thrown.getMessage()).contains(errorBody);
  }

  @Test
  void shouldAuthenticateWithTheClickHouseUserAndKeyHeaderPairWhenConfigured() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(), Authentication.userKey("alice", "secret-key"));

      transport
          .query(ClickHouseQuery.of("SELECT 1"))
          .aggregate()
          .asByteArray()
          .block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedHeader("X-ClickHouse-User")).isEqualTo("alice");
      assertThat(server.receivedHeader("X-ClickHouse-Key")).isEqualTo("secret-key");
    }
  }
}

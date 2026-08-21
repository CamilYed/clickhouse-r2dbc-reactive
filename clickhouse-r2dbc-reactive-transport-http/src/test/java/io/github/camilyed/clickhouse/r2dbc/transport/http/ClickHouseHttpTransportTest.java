package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.clickhouse.client.api.ServerException;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.abilities.NettyLeakDetectionAbility;
import io.github.camilyed.clickhouse.r2dbc.testkit.abilities.ToByteArrayAbility;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

class ClickHouseHttpTransportTest implements ToByteArrayAbility, NettyLeakDetectionAbility {

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
  void shouldReturnTheConfiguredResponseBodyWhenAuthenticationAndMaxConnectionsAreBothConfigured() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    final byte[] receivedBody;
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(), Authentication.basic("user", "password"), 2);

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
    thereAreNoRecordedByteBufLeaksYet();

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

    // and - cancelling after the first chunk must not strand the ByteBuf it arrived in
    assertNoByteBufLeaksWereDetected();
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
    thereAreNoRecordedByteBufLeaksYet();
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
      // and - a timed-out request never receives any body, but the timeout's own teardown must not
      // strand anything either
      assertNoByteBufLeaksWereDetected();
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
    thereAreNoRecordedByteBufLeaksYet();

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

    // and - the chunk that did arrive before the reset must not be stranded
    assertNoByteBufLeaksWereDetected();
  }

  @Test
  void shouldNotLeakAByteBufWhenTheRowDecoderFailsMidStream() {
    // given - a response that ends abruptly inside a multi-byte value already being read (see
    // ClickHouseWireFixtures#truncatedInt32ValueRowBinaryWithNamesAndTypes()'s Javadoc for why
    // this,
    // and not simply "no rows", is what actually forces the row decoder itself to fail rather than
    // a transport-level error).
    final byte[] truncatedBody =
        ClickHouseWireFixtures.truncatedInt32ValueRowBinaryWithNamesAndTypes();
    thereAreNoRecordedByteBufLeaksYet();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(truncatedBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());
      final Flux<ByteBuffer> body =
          transport.query(ClickHouseQuery.of("SELECT n")).asByteArray().map(ByteBuffer::wrap);

      final Throwable thrown =
          catchThrowable(
              () -> RowBinaryDecoder.decodeRows(body).collectList().block(Duration.ofSeconds(5)));

      // then
      assertThat(thrown).isNotNull();
    }

    // and - the bytes that did arrive before the truncation must not be stranded
    assertNoByteBufLeaksWereDetected();
  }

  @Test
  void shouldNotLeakAByteBufWhenDownstreamCancelsAfterAFewRecords() {
    // given - the same access pattern as connector-level R2DBC consumers that stop reading rows
    // early (e.g. Result.map(...).take(n)), driven through the real production wiring
    // (RowBinaryDecoder#decode + RowDecodingScheduler, exactly like ClickHouseResult uses) rather
    // than the raw decodeRows() entry point, which is documented as unsafe against a live network
    // response.
    final byte[] twoRows = ClickHouseWireFixtures.twoRowsOfUInt8RowBinaryWithNamesAndTypes();
    thereAreNoRecordedByteBufLeaksYet();
    final RowDecodingScheduler scheduler = RowDecodingScheduler.defaults();

    try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(twoRows)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());
      final Flux<ByteBuffer> body =
          transport.query(ClickHouseQuery.of("SELECT 1")).asByteArray().map(ByteBuffer::wrap);

      // when
      final DecodedResult result =
          RowBinaryDecoder.decode(body, scheduler).block(Duration.ofSeconds(5));
      StepVerifier.create(result.rows(), 1)
          .expectNextCount(1)
          .thenCancel()
          .verify(Duration.ofSeconds(5));
    } finally {
      scheduler.dispose();
    }

    // then - stopping after row 1 of 2 must not strand the ByteBuf the second, never-consumed row
    // arrived in
    assertNoByteBufLeaksWereDetected();
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
  void shouldNotExceedTheConfiguredMaxConnectionsViaTransportOptions() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(), TransportOptions.defaults().withMaxConnections(2));

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      await().atMost(Duration.ofSeconds(2)).until(() -> server.activeConnectionCount() == 2);

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();

      // then
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.activeConnectionCount()).isEqualTo(2));
    }
  }

  @Test
  void shouldTimeOutAcquisitionAfterTheConfiguredPendingAcquireTimeout() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(),
              TransportOptions.defaults()
                  .withMaxConnections(1)
                  .withPendingAcquireTimeout(Duration.ofMillis(200)));

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      await().atMost(Duration.ofSeconds(2)).until(() -> server.activeConnectionCount() == 1);

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
  void shouldRejectAcquisitionOnceThePendingQueueIsFull() {
    // given
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(),
              TransportOptions.defaults().withMaxConnections(1).withPendingAcquireMaxCount(1));

      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      await().atMost(Duration.ofSeconds(2)).until(() -> server.activeConnectionCount() == 1);
      transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
      // Give the second acquisition a moment to actually register as pending before the third
      // one - queueing itself is synchronous inside Reactor Netty's pool, but this leaves no room
      // for a scheduling hiccup to make the test flaky.
      await().pollDelay(Duration.ofMillis(150)).until(() -> true);

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
          .during(Duration.ofMillis(400))
          .atMost(Duration.ofMillis(1000))
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
  void shouldSendAttachedSettingsAsPlainQueryParameters() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    final ClickHouseQuery query =
        ClickHouseQuery.of("SELECT 1").withSettings(Map.of("max_execution_time", "5.000"));

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport.query(query).aggregate().asByteArray().block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).contains("max_execution_time=5.000");
    }
  }

  @Test
  void shouldNotSendAParamPrefixForSettings() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    final ClickHouseQuery query =
        ClickHouseQuery.of("SELECT 1").withSettings(Map.of("max_execution_time", "5.000"));

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport.query(query).aggregate().asByteArray().block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).doesNotContain("param_max_execution_time");
    }
  }

  @Test
  void shouldAskForJsonColumnsAsPlainStringsOnEveryQuery() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport
          .query(ClickHouseQuery.of("SELECT 1"))
          .aggregate()
          .asByteArray()
          .block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedUri()).contains("output_format_binary_write_json_as_string=1");
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
  void shouldExposeTheQueryIdOnAServerException() {
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
                      .query(ClickHouseQuery.of("SELECT * FROM missing", "query-id-1"))
                      .aggregate()
                      .asByteArray()
                      .block(Duration.ofSeconds(5)));
    }

    // then
    assertThat(thrown).isInstanceOf(ServerException.class);
    assertThat(((ServerException) thrown).getQueryId()).isEqualTo("query-id-1");
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

  @Test
  void shouldSendTheConfiguredDatabaseAsARequestHeader() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(), TransportOptions.defaults().withDatabase("analytics"));

      transport
          .query(ClickHouseQuery.of("SELECT 1"))
          .aggregate()
          .asByteArray()
          .block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedHeader("X-ClickHouse-Database")).isEqualTo("analytics");
    }
  }

  @Test
  void shouldSendNoDatabaseHeaderWhenNoneIsConfigured() {
    // given
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      transport
          .query(ClickHouseQuery.of("SELECT 1"))
          .aggregate()
          .asByteArray()
          .block(Duration.ofSeconds(5));

      // then
      assertThat(server.receivedHeader("X-ClickHouse-Database")).isNull();
    }
  }

  @Test
  void shouldNotRetryAFailureThatHappensAfterTheRequestWasSent() {
    // given - the connection is reset only after the request already reached the server, so
    // requestSent is true by the time the failure happens; RetryPolicy must not retry this,
    // regardless of how many attempts it's configured to allow.
    final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

    // when
    try (final var server =
        ControlledClickHouseServer.startRespondingThenResettingConnection(
            firstChunk, Duration.ofMillis(200))) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(),
              Authentication.none(),
              null,
              null,
              null,
              new RetryPolicy(5, Duration.ofMillis(20)));

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
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.totalRequestsReceived()).isEqualTo(1));
    }
  }

  @Test
  void shouldRetryAConnectionLevelFailureThatHappensBeforeTheRequestWasSentUntilItSucceeds()
      throws Exception {
    // given - nobody is listening on this port yet, so every connection attempt fails before any
    // request bytes can be sent; a real server starts accepting on that exact port shortly after,
    // simulating a transient "server not reachable yet" condition RetryPolicy should ride out.
    thereAreNoRecordedByteBufLeaksYet();
    final int port;
    try (final ServerSocket portProbe = new ServerSocket(0)) {
      port = portProbe.getLocalPort();
    }
    final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();
    final AtomicReference<DisposableServer> lateServer = new AtomicReference<>();
    Mono.delay(Duration.ofMillis(300))
        .publishOn(Schedulers.boundedElastic())
        .subscribe(ignored -> lateServer.set(startServerRespondingOnPort(port, configuredBody)));
    final var transport =
        new ClickHouseHttpTransport(
            "http://localhost:" + port,
            Authentication.none(),
            null,
            null,
            null,
            new RetryPolicy(20, Duration.ofMillis(50)));

    try {
      // when
      final byte[] receivedBody =
          transport
              .query(ClickHouseQuery.of("SELECT 1"))
              .aggregate()
              .asByteArray()
              .block(Duration.ofSeconds(5));

      // then
      assertThat(receivedBody).isEqualTo(configuredBody);
      // and - neither the failed pre-send attempts nor the eventually-successful one may strand a
      // ByteBuf
      assertNoByteBufLeaksWereDetected();
    } finally {
      await().atMost(Duration.ofSeconds(2)).until(() -> lateServer.get() != null);
      lateServer.get().disposeNow();
    }
  }

  @Test
  void shouldStreamTheRequestBodyToTheServerForAnInsert() {
    // given
    final byte[] rowData = "1\tAda\n2\tGrace\n".getBytes(StandardCharsets.UTF_8);

    // when
    final ClickHouseQueryResponse received;
    final byte[] receivedBody;
    try (final var server =
        ControlledClickHouseServer.startAcceptingInsertsAndRespondingWithSummary(
            "{\"written_rows\":\"2\"}")) {
      final var transport = new ClickHouseHttpTransport(server.baseUrl());

      received =
          transport
              .insertWithSummary(
                  ClickHouseQuery.of("INSERT INTO t FORMAT TabSeparated"),
                  Flux.just(ByteBuffer.wrap(rowData)))
              .block(Duration.ofSeconds(5));
      received.body().aggregate().asByteArray().block(Duration.ofSeconds(5));
      receivedBody = server.receivedRequestBody();
    }

    // then
    assertThat(receivedBody).isEqualTo(rowData);
    assertThat(received.writtenRows().getAsLong()).isEqualTo(2L);
  }

  @Test
  void shouldNeverRetryAnInsertEvenOnAConnectionLevelFailureBeforeTheRequestWasSent()
      throws Exception {
    // given - retry is deliberately disabled for insertWithSummary: unlike queryWithSummary's
    // requestSent flag (safe because flushing the headers of a bodyless request IS sending the
    // whole request), a streaming insert body can be partially transmitted while requestSent is
    // still false, so "not sent yet" can no longer be trusted to mean "zero bytes reached the
    // server" - see insertWithSummary's Javadoc. Nobody is listening on this port at all, so every
    // connection attempt fails immediately; if retrying happened, 20 attempts 200ms apart would
    // take at least 4 seconds.
    final int port;
    try (final ServerSocket portProbe = new ServerSocket(0)) {
      port = portProbe.getLocalPort();
    }
    final var transport =
        new ClickHouseHttpTransport(
            "http://localhost:" + port,
            Authentication.none(),
            null,
            null,
            null,
            new RetryPolicy(20, Duration.ofMillis(200)));
    final Instant start = Instant.now();

    // when - insertWithSummary is lazy the same way queryWithSummary is (see that method's
    // Javadoc): the outer Mono resolves synchronously without touching the network, so the
    // failure only surfaces once the returned response's body is actually subscribed to.
    final Throwable thrown =
        catchThrowable(
            () ->
                transport
                    .insertWithSummary(
                        ClickHouseQuery.of("INSERT INTO t FORMAT TabSeparated"),
                        Flux.just(ByteBuffer.wrap("1\tAda\n".getBytes(StandardCharsets.UTF_8))))
                    .flatMap(response -> response.body().aggregate().asByteArray())
                    .block(Duration.ofSeconds(5)));

    // then
    assertThat(thrown).isNotNull();
    assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofSeconds(2));
  }

  @Test
  void shouldAlwaysAllowRetryingAFailureThatHappensBeforeTheRequestWasSent() {
    // given - requestSent=false is the existing pre-send-connection-level case; canRetry must
    // return true here regardless of the other conditions, since a request that never left the
    // client cannot have caused any server-side effect to duplicate
    final ServerException nonRetryableError = new ServerException(60, "not found", 500, "q1");

    // when
    final boolean canRetry =
        ClickHouseHttpTransport.canRetry(
            nonRetryableError, ClickHouseQuery.of("SELECT 1"), false, false);

    // then
    assertThat(canRetry).isTrue();
  }

  @Test
  void shouldNeverAllowRetryingOnceAnyResponseBytesWereAlreadyEmitted() {
    // given - even an opted-in query with a retryable server error must not retry once the caller
    // has already received some of the response, or a retry would duplicate/corrupt what was
    // already delivered
    final ServerException retryableError =
        new ServerException(202, "too many simultaneous queries", 500, "q1");
    final ClickHouseQuery optedIn = ClickHouseQuery.of("SELECT 1").withServerErrorRetryEnabled();

    // when
    final boolean canRetry = ClickHouseHttpTransport.canRetry(retryableError, optedIn, true, true);

    // then
    assertThat(canRetry).isFalse();
  }

  @Test
  void shouldNotAllowRetryingAServerErrorWhenNotOptedIn() {
    // given - the default ClickHouseQuery.of(...) has serverErrorRetryEnabled()==false
    final ServerException retryableError =
        new ServerException(202, "too many simultaneous queries", 500, "q1");

    // when
    final boolean canRetry =
        ClickHouseHttpTransport.canRetry(
            retryableError, ClickHouseQuery.of("SELECT 1"), true, false);

    // then
    assertThat(canRetry).isFalse();
  }

  @Test
  void shouldAllowRetryingARetryableServerErrorWhenOptedInAndNoBytesEmittedYet() {
    // given - error code 202 (TOO_MANY_SIMULTANEOUS_QUERIES) is one of the codes client-v2's
    // ServerException.discoverIsRetryable classifies as retryable (v0.9.8 source, not assumed)
    final ServerException retryableError =
        new ServerException(202, "too many simultaneous queries", 500, "q1");
    final ClickHouseQuery optedIn = ClickHouseQuery.of("SELECT 1").withServerErrorRetryEnabled();

    // when
    final boolean canRetry = ClickHouseHttpTransport.canRetry(retryableError, optedIn, true, false);

    // then
    assertThat(canRetry).isTrue();
  }

  @Test
  void shouldNotAllowRetryingANonRetryableServerErrorEvenWhenOptedIn() {
    // given - error code 60 (TABLE_NOT_FOUND) is not in client-v2's ServerException.
    // discoverIsRetryable list (v0.9.8 source, not assumed) - retrying would just fail identically
    final ServerException nonRetryableError = new ServerException(60, "not found", 500, "q1");
    final ClickHouseQuery optedIn = ClickHouseQuery.of("SELECT 1").withServerErrorRetryEnabled();

    // when
    final boolean canRetry =
        ClickHouseHttpTransport.canRetry(nonRetryableError, optedIn, true, false);

    // then
    assertThat(canRetry).isFalse();
  }

  @Test
  void shouldNotAllowRetryingANonServerExceptionFailureEvenWhenOptedIn() {
    // given - a connection-level failure after requestSent is not something isRetryable() can
    // classify at all, so it must never be retried through the server-error opt-in path
    final RuntimeException connectionLevelFailure = new RuntimeException("connection reset");
    final ClickHouseQuery optedIn = ClickHouseQuery.of("SELECT 1").withServerErrorRetryEnabled();

    // when
    final boolean canRetry =
        ClickHouseHttpTransport.canRetry(connectionLevelFailure, optedIn, true, false);

    // then
    assertThat(canRetry).isFalse();
  }

  @Test
  void shouldRetryARetryableServerErrorUpToThePolicyBudgetWhenOptedIn() {
    // given - error code 202 (TOO_MANY_SIMULTANEOUS_QUERIES) is confirmed retryable (see the
    // canRetry unit tests above); this fake always responds with it, so every attempt fails and
    // the retry loop is expected to run out its whole budget (maxAttempts retries, 1 + maxAttempts
    // total requests) rather than stop after the first attempt
    try (final var server =
        ControlledClickHouseServer.startRespondingWithClickHouseError(
            202, "too many simultaneous queries", 500)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(),
              Authentication.none(),
              null,
              null,
              null,
              new RetryPolicy(3, Duration.ofMillis(10)));
      final ClickHouseQuery optedIn = ClickHouseQuery.of("SELECT 1").withServerErrorRetryEnabled();

      // when
      final Throwable thrown =
          catchThrowable(
              () ->
                  transport.query(optedIn).aggregate().asByteArray().block(Duration.ofSeconds(5)));

      // then - Reactor wraps the final failure as Exceptions.RetryExhaustedException once the
      // whole retry budget is used up (as opposed to the filter simply declining to retry, which
      // propagates the original error unwrapped - see the shouldNotRetry* tests below), with the
      // actual last ServerException as its cause
      assertThat(thrown).hasCauseInstanceOf(ServerException.class);
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.totalRequestsReceived()).isEqualTo(4));
    }
  }

  @Test
  void shouldNotRetryARetryableServerErrorWhenNotOptedIn() {
    // given - same retryable error code as above, but the query is built via the plain factory
    // (serverErrorRetryEnabled()==false), proving the opt-in - not the mere presence of a
    // retryable error code - is what gates this behavior
    try (final var server =
        ControlledClickHouseServer.startRespondingWithClickHouseError(
            202, "too many simultaneous queries", 500)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(),
              Authentication.none(),
              null,
              null,
              null,
              new RetryPolicy(3, Duration.ofMillis(10)));

      // when
      final Throwable thrown =
          catchThrowable(
              () ->
                  transport
                      .query(ClickHouseQuery.of("SELECT 1"))
                      .aggregate()
                      .asByteArray()
                      .block(Duration.ofSeconds(5)));

      // then
      assertThat(thrown).isInstanceOf(ServerException.class);
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.totalRequestsReceived()).isEqualTo(1));
    }
  }

  @Test
  void shouldNotRetryANonRetryableServerErrorEvenWhenOptedIn() {
    // given - error code 60 (TABLE_NOT_FOUND) is not retryable per client-v2's own classification,
    // so opting a query in must not cause it to be retried anyway
    try (final var server =
        ControlledClickHouseServer.startRespondingWithClickHouseError(60, "not found", 404)) {
      final var transport =
          new ClickHouseHttpTransport(
              server.baseUrl(),
              Authentication.none(),
              null,
              null,
              null,
              new RetryPolicy(3, Duration.ofMillis(10)));
      final ClickHouseQuery optedIn = ClickHouseQuery.of("SELECT 1").withServerErrorRetryEnabled();

      // when
      final Throwable thrown =
          catchThrowable(
              () ->
                  transport.query(optedIn).aggregate().asByteArray().block(Duration.ofSeconds(5)));

      // then
      assertThat(thrown).isInstanceOf(ServerException.class);
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(server.totalRequestsReceived()).isEqualTo(1));
    }
  }

  private static DisposableServer startServerRespondingOnPort(final int port, final byte[] body) {
    return HttpServer.create()
        .port(port)
        .route(
            routes ->
                routes.post(
                    "/",
                    (request, response) ->
                        response
                            .header("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")
                            .header("Content-Type", "application/octet-stream")
                            .sendByteArray(Mono.just(body))))
        .bindNow();
  }
}

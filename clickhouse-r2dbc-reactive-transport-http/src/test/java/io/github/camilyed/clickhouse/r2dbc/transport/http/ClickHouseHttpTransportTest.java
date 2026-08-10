package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.awaitility.Awaitility.await;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.testkit.abilities.ToByteArrayAbility;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.fakes.ControlledClickHouseServer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class ClickHouseHttpTransportTest implements ToByteArrayAbility {

    @Test
    void shouldReturnTheConfiguredResponseBody() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        // when
        final byte[] receivedBody;
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            receivedBody = transport.query(ClickHouseQuery.of("SELECT 1"))
                    .aggregate()
                    .asByteArray()
                    .block(Duration.ofSeconds(5));
        }

        // then
        assertThat(receivedBody).isEqualTo(configuredBody);
    }

    @Test
    void shouldNotSendTheRequestBeforeSubscription() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            // when
            transport.query(ClickHouseQuery.of("SELECT 1"));

            // then
            await().during(Duration.ofMillis(200))
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
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWithChunks(firstChunk, secondChunk)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            receivedChunks = transport.query(ClickHouseQuery.of("SELECT 1"))
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
        try (final var server = ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            transport.query(ClickHouseQuery.of("SELECT 1")).take(1).blockLast(Duration.ofSeconds(5));

            // then
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(server.hasClosedConnection()).isTrue());
        }
    }

    @Test
    void shouldStillReturnTheBodyWhenHeadersAreDelayed() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        // when
        final byte[] receivedBody;
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWithDelay(configuredBody, Duration.ofMillis(300))) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            receivedBody = transport.query(ClickHouseQuery.of("SELECT 1"))
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
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWithBodyDelay(configuredBody, Duration.ofMillis(300))) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            receivedBody = transport.query(ClickHouseQuery.of("SELECT 1"))
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
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWithChunks(firstChunk, secondChunk)) {
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
    void shouldFailWithinItsOwnTimeoutWhenTheServerNeverResponds() {
        // given
        try (final var server = ControlledClickHouseServer.startAcceptingButNeverResponding()) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());
            final Instant start = Instant.now();

            // when
            final Throwable thrown = catchThrowable(() ->
                    transport.query(ClickHouseQuery.of("SELECT 1")).aggregate().asByteArray().block(Duration.ofSeconds(10)));

            // then
            assertThat(thrown).isNotNull();
            assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofSeconds(5));
        }
    }

    @Test
    void shouldSignalAnErrorWhenTheConnectionIsResetMidResponse() {
        // given
        final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

        // when
        try (final var server = ControlledClickHouseServer.startRespondingThenResettingConnection(firstChunk, Duration.ofMillis(200))) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            final Throwable thrown = catchThrowable(() ->
                    transport.query(ClickHouseQuery.of("SELECT 1")).aggregate().asByteArray().block(Duration.ofSeconds(5)));

            // then
            assertThat(thrown).isNotNull();
        }
    }

    @Test
    void shouldNotStartASecondRequestUntilAConnectionIsFree() {
        // given
        final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

        // when
        try (final var server = ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl(), 1);

            transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
            await().atMost(Duration.ofSeconds(2)).until(() -> server.activeConnectionCount() == 1);

            transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();

            // then
            await().during(Duration.ofMillis(300))
                    .atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(server.activeConnectionCount()).isEqualTo(1));
        }
    }

    @Test
    void shouldNotReachTheServerWhenAQueuedRequestIsCancelledBeforeAConnectionIsAcquired() {
        // given
        final byte[] firstChunk = "first-chunk".getBytes(StandardCharsets.UTF_8);

        // when
        try (final var server = ControlledClickHouseServer.startRespondingWithFirstChunkThenHanging(firstChunk)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl(), 1);

            transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
            await().atMost(Duration.ofSeconds(2)).until(() -> server.activeConnectionCount() == 1);

            final var queuedSubscription = transport.query(ClickHouseQuery.of("SELECT 1")).subscribe();
            queuedSubscription.dispose();

            // then
            await().during(Duration.ofMillis(300))
                    .atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(server.activeConnectionCount()).isEqualTo(1));
        }
    }

    @Test
    void shouldNotReachTheServerWhenCancelledImmediatelyAfterSubscribing() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        // when
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            transport.query(ClickHouseQuery.of("SELECT 1")).subscribe().dispose();

            // then
            await().during(Duration.ofMillis(200))
                    .atMost(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(server.hasReceivedRequest()).isFalse());
        }
    }

    @Test
    void shouldSendTheQueryIdAsARequestHeader() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        // when
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            transport.query(ClickHouseQuery.of("SELECT 1", "my-query-id"))
                    .aggregate()
                    .asByteArray()
                    .block(Duration.ofSeconds(5));

            // then
            assertThat(server.receivedQueryId()).isEqualTo("my-query-id");
        }
    }
}
package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.camilyed.clickhouse.r2dbc.testkit.ClickHouseWireFixtures;
import io.github.camilyed.clickhouse.r2dbc.testkit.ControlledClickHouseServer;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ClickHouseHttpTransportTest {

    @Test
    void shouldReturnTheConfiguredResponseBody() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        // when
        final byte[] receivedBody;
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
            final var transport = new ClickHouseHttpTransport(server.baseUrl());

            receivedBody = transport.query("SELECT 1")
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
            transport.query("SELECT 1");

            // then
            await().during(Duration.ofMillis(200))
                    .atMost(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(server.hasReceivedRequest()).isFalse());
        }
    }
}
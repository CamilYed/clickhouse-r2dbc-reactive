package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

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
}
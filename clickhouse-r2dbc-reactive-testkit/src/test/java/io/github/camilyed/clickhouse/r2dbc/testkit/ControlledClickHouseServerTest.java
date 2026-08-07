package io.github.camilyed.clickhouse.r2dbc.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

class ControlledClickHouseServerTest {

    @Test
    void shouldRespondToSelectOneWithTheConfiguredRowBinaryBody() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        // when
        final byte[] receivedBody;
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
            receivedBody = HttpClient.create()
                    .baseUrl(server.baseUrl())
                    .post()
                    .uri("/?query=SELECT+1")
                    .responseSingle((response, body) -> body.asByteArray())
                    .block(Duration.ofSeconds(5));
        }

        // then
        assertThat(receivedBody).isEqualTo(configuredBody);
    }

    @Test
    void shouldExposeTheFormatHeaderClientV2ExpectsWhenDecoding() {
        // given
        final byte[] configuredBody = ClickHouseWireFixtures.selectOneRowBinaryWithNamesAndTypes();

        // when
        final String formatHeader;
        try (final var server = ControlledClickHouseServer.startRespondingToSelectOneWith(configuredBody)) {
            formatHeader = HttpClient.create()
                    .baseUrl(server.baseUrl())
                    .post()
                    .uri("/?query=SELECT+1")
                    .responseSingle((response, body) -> body.then(
                            Mono.fromSupplier(() -> response.responseHeaders().get("X-ClickHouse-Format"))))
                    .block(Duration.ofSeconds(5));
        }

        // then
        assertThat(formatHeader).isEqualTo("RowBinaryWithNamesAndTypes");
    }
}

package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Phase 1 step 6: the same {@code SELECT 1} path proven against {@code testkit}'s controlled
 * server in {@link ClickHouseHttpTransportTest}, now against a real ClickHouse server.
 */
class SelectOneAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

    @Test
    void shouldDecodeSelectOneFromARealClickHouseServer() {
        // given
        final ClickHouseHttpTransport transport =
                new ClickHouseHttpTransport(clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
        final Flux<ByteBuffer> body = transport.query(ClickHouseQuery.of("SELECT 1"))
                .asByteArray()
                .map(ByteBuffer::wrap);

        // when
        final Map<String, Object> row =
                RowBinaryDecoder.decodeRows(body).blockFirst(Duration.ofSeconds(10));

        // then
        assertThat(row).containsEntry("1", (short) 1);
    }
}

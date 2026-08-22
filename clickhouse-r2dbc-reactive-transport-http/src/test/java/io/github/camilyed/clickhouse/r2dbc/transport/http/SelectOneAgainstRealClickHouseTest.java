package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Phase 1 step 6: the same {@code SELECT 1} path proven against {@code testkit}'s controlled server
 * in {@link ClickHouseHttpTransportTest}, now against a real ClickHouse server.
 *
 * <p>Also the simplest real-server proof that response compression round-trips correctly: {@code
 * transport}'s {@link TransportOptions#defaults()} sends {@code compress=1} by default (see {@link
 * ResponseCompression#LZ4}), so this decodes a genuinely LZ4-compressed response — not a hand-built
 * fixture — through {@code core}'s {@code ClickHouseLz4InputStream}.
 */
class SelectOneAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  @Test
  void shouldDecodeSelectOneFromARealClickHouseServer() {
    // given
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport(
            clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    final Flux<ByteBuffer> body =
        transport.query(ClickHouseQuery.of("SELECT 1")).asByteArray().map(ByteBuffer::wrap);

    // when
    final DecodedRow row =
        RowBinaryDecoder.decodeRows(body, transport.responseCompression())
            .blockFirst(Duration.ofSeconds(10));

    // then
    assertThat(row.valueAt(0)).isEqualTo((short) 1);
  }
}

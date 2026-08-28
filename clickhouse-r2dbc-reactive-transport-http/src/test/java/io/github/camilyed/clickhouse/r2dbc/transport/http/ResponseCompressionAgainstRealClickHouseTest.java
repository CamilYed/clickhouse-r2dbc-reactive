package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Proves this driver's LZ4 response decompression against a real ClickHouse server, not just the
 * hand-built fixtures {@code ClickHouseLz4InputStreamTest} uses — the one place a genuine encoding
 * bug (wrong checksum byte order, wrong block-header offset, and similar) would actually surface,
 * since {@code core}'s own fixture builder shares the same primitives the decoder verifies against
 * and so cannot catch a mismatch between this driver's understanding of the wire format and
 * ClickHouse's own.
 *
 * <p>Queries {@code system.numbers} for 100,000 rows specifically because a single row is far too
 * small to span more than one LZ4 block — {@code compressAsSingleBlock}-shaped hermetic tests
 * already cover that trivial case (see {@link SelectOneAgainstRealClickHouseTest}). 100,000 {@code
 * UInt64} rows is several hundred KB of {@code RowBinaryWithNamesAndTypes} wire data, comfortably
 * exceeding client-v2's own {@code ClickHouseLZ4OutputStream.UNCOMPRESSED_BUFF_SIZE} (64KB) —
 * ClickHouse's server-side compressor is the same shape, so this genuinely proves the multi-block
 * path this driver's {@code ClickHouseLz4InputStream} implements, not just a single block.
 */
class ResponseCompressionAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  private static final String QUERY = "SELECT number FROM system.numbers LIMIT 100000";

  @Test
  void shouldDecodeALargeMultiBlockCompressedResponse() {
    // given - TransportOptions.defaults() sends compress=1 (ResponseCompression.LZ4 is this
    // driver's default), so this transport genuinely receives an LZ4-compressed response body.
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport(
            clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());

    // when
    final List<BigInteger> values = queryNumberColumn(transport);

    // then
    assertThat(values)
        .hasSize(100_000)
        .startsWith(BigInteger.valueOf(0), BigInteger.valueOf(1), BigInteger.valueOf(2))
        .endsWith(BigInteger.valueOf(99_998), BigInteger.valueOf(99_999));
  }

  @Test
  void shouldDecodeTheSameResultWithCompressionExplicitlyDisabled() {
    // given
    final TransportOptions options =
        TransportOptions.defaults()
            .withAuthentication(Authentication.basic(clickHouseUsername(), clickHousePassword()))
            .withResponseCompression(ResponseCompression.NONE);
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport(clickHouseHttpUrl(), options);

    // when
    final List<BigInteger> values = queryNumberColumn(transport);

    // then
    assertThat(values)
        .hasSize(100_000)
        .startsWith(BigInteger.valueOf(0), BigInteger.valueOf(1), BigInteger.valueOf(2))
        .endsWith(BigInteger.valueOf(99_998), BigInteger.valueOf(99_999));
  }

  private static List<BigInteger> queryNumberColumn(final ClickHouseHttpTransport transport) {
    final Flux<ByteBuffer> body =
        transport.query(ClickHouseQuery.of(QUERY)).asByteArray().map(ByteBuffer::wrap);
    return RowBinaryDecoder.decodeRows(body, transport.responseCompression())
        .map(row -> (BigInteger) row.valueAt(0))
        .collectList()
        .block(Duration.ofSeconds(30));
  }
}

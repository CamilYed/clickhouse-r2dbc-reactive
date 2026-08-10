package io.github.camilyed.clickhouse.r2dbc.core;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import java.nio.ByteBuffer;
import java.util.Map;
import reactor.core.publisher.Flux;

/**
 * Decodes a {@code RowBinaryWithNamesAndTypes} response body into rows.
 *
 * <p>Wraps client-v2's {@link RowBinaryWithNamesAndTypesFormatReader} — reused here only for
 * decoding, never for transport — around the blocking {@link FluxInputStreamBridge}. Reading a row
 * blocks the calling thread, so callers must subscribe on a dedicated worker, never on the
 * event-loop thread the source {@code Flux<ByteBuffer>} was produced on.
 */
public final class RowBinaryDecoder {

    private RowBinaryDecoder() {}

    /** Decodes {@code source} into rows keyed by column name, in wire order. */
    public static Flux<Map<String, Object>> decodeRows(final Flux<ByteBuffer> source) {
        return Flux.fromIterable(() -> new RowBinaryWithNamesAndTypesFormatReader(
                FluxInputStreamBridge.subscribeTo(source, 4),
                new QuerySettings().setUseTimeZone("UTC"),
                new BinaryStreamReader.DefaultByteBufferAllocator()));
    }
}

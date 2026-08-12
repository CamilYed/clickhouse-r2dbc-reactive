package io.github.camilyed.clickhouse.r2dbc.core;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

/**
 * Decodes a {@code RowBinaryWithNamesAndTypes} response body into rows.
 *
 * <p>Wraps client-v2's {@link RowBinaryWithNamesAndTypesFormatReader} — reused here only for
 * decoding, never for transport — around the blocking {@link FluxInputStreamBridge}. Reading a row
 * blocks the calling thread, so callers must subscribe on a dedicated worker, never on the
 * event-loop thread the source {@code Flux<ByteBuffer>} was produced on.
 *
 * <p>Uses {@link ListDecodingRowBinaryReader} rather than the base reader directly, so {@code
 * Array}/{@code Nested} columns decode as plain {@code List}s instead of client-v2's {@code
 * .internal} {@code ArrayValue} — see that class's Javadoc for why this is safe and narrowly
 * scoped. Every other column type is unaffected.
 *
 * <p>Each row is copied into a plain {@link LinkedHashMap} the moment it's read, rather than handed
 * out as client-v2's own {@code Map} implementation. That implementation ({@code RecordWrapper})
 * stores its values behind a {@link java.lang.ref.WeakReference} to the reader's internal state —
 * reading from it later, after the reader itself is no longer strongly reachable (e.g. once a
 * downstream {@code blockFirst()} has cancelled the subscription), can throw a {@code
 * NullPointerException} if the garbage collector has since run. Copying immediately, while the
 * reader is still on the stack, sidesteps that lifetime trap entirely.
 */
public final class RowBinaryDecoder {

  private RowBinaryDecoder() {}

  /** Decodes {@code source} into rows keyed by column name, in wire order. */
  public static Flux<Map<String, Object>> decodeRows(final Flux<ByteBuffer> source) {
    return Flux.generate(() -> newReader(source), RowBinaryDecoder::emitNextRow);
  }

  private static RowBinaryWithNamesAndTypesFormatReader newReader(final Flux<ByteBuffer> source) {
    return new ListDecodingRowBinaryReader(
        FluxInputStreamBridge.subscribeTo(source, 4),
        new QuerySettings().setUseTimeZone("UTC"),
        new BinaryStreamReader.DefaultByteBufferAllocator());
  }

  private static RowBinaryWithNamesAndTypesFormatReader emitNextRow(
      final RowBinaryWithNamesAndTypesFormatReader reader,
      final SynchronousSink<Map<String, Object>> sink) {
    if (reader.hasNext()) {
      sink.next(new LinkedHashMap<>(reader.next()));
    } else {
      sink.complete();
    }
    return reader;
  }
}

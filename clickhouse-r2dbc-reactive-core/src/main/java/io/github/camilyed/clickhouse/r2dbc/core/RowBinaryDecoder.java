package io.github.camilyed.clickhouse.r2dbc.core;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

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

  /**
   * Decodes {@code source} into its column schema and row stream together, from one reader instance
   * and one subscription to {@code source} — unlike {@link #decodeRows}, which discards the schema
   * client-v2 already reads off the wire before the first row.
   *
   * <p>Constructing the reader blocks (it eagerly reads the {@code RowBinaryWithNamesAndTypes}
   * header — see {@link ListDecodingRowBinaryReader}), so that construction runs on {@link
   * Schedulers#boundedElastic()}, never on the caller's thread. For a real transport that thread is
   * Reactor Netty's event loop; blocking it here would stall every other query sharing it.
   */
  public static Mono<DecodedResult> decode(final Flux<ByteBuffer> source) {
    return Mono.fromCallable(() -> newReader(source))
        .subscribeOn(Schedulers.boundedElastic())
        .map(
            reader ->
                new DecodedResult(
                    columnsOf(reader), Flux.generate(() -> reader, RowBinaryDecoder::emitNextRow)));
  }

  private static List<ColumnDescriptor> columnsOf(
      final RowBinaryWithNamesAndTypesFormatReader reader) {
    // A genuinely empty response body (e.g. a DDL statement, which never sends the
    // RowBinaryWithNamesAndTypes header at all) leaves the reader's schema null rather than an
    // empty TableSchema — reader.getSchema().getColumns() would NPE for that case otherwise.
    final TableSchema schema = reader.getSchema();
    if (schema == null) {
      return List.of();
    }
    return schema.getColumns().stream().map(RowBinaryDecoder::toColumnDescriptor).toList();
  }

  private static ColumnDescriptor toColumnDescriptor(final ClickHouseColumn column) {
    return new ColumnDescriptor(column.getColumnName(), column.getOriginalTypeName());
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

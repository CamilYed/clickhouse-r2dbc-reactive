package io.github.camilyed.clickhouse.r2dbc.core;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import java.nio.ByteBuffer;
import java.util.List;
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
 * <p>Each row is snapshotted into a compact {@link DecodedRow} the moment it's read, via {@link
 * ListDecodingRowBinaryReader#nextRowValues()}, rather than handed out as client-v2's own {@code
 * Map} implementation (its {@code RecordWrapper}) or copied into a {@code LinkedHashMap} built
 * fresh per row (an earlier version of this class did exactly that — see {@link DecodedRow}'s
 * Javadoc for why it was replaced: measured at roughly 576 bytes/row and the dominant per-row cost
 * in this driver's decode path). {@code RecordWrapper} itself stores its values behind a {@link
 * java.lang.ref.WeakReference} to the reader's internal state — reading from it later, after the
 * reader itself is no longer strongly reachable, can throw a {@code NullPointerException} if the
 * garbage collector has since run. Snapshotting into a plain {@code Object[]} immediately, while
 * the reader is still on the stack, sidesteps that lifetime trap the same way the old {@code
 * LinkedHashMap} copy did, without paying for a hash table nobody needs.
 */
public final class RowBinaryDecoder {

  /**
   * How many {@code ByteBuffer} chunks {@link FluxInputStreamBridge} is allowed to hold in flight
   * while the blocking reader decodes a response body (see that class's Javadoc for the exact
   * "credit" backpressure mechanics this bounds). A small, deliberately <em>unbenchmarked</em>
   * default — big enough that network reads and blocking row decoding can overlap a little instead
   * of fully serializing (chunk arrives, decoder blocks reading it, only then is the next chunk
   * requested), without buffering an unbounded number of chunks ahead of a decoder that's fallen
   * behind. Tuning this against real throughput/latency numbers is explicitly out of scope until
   * this project's performance phase starts (see README.md's "Performance and dependency impact"
   * section) — treat this constant as a documented placeholder to revisit with measurements, not a
   * benchmarked value.
   */
  private static final int RESPONSE_CHUNK_DEMAND = 4;

  private RowBinaryDecoder() {}

  /**
   * Decodes {@code source} into rows, in wire column order. See {@link #decode} for name access.
   */
  public static Flux<DecodedRow> decodeRows(final Flux<ByteBuffer> source) {
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

  private static List<ColumnDescriptor> columnsOf(final ListDecodingRowBinaryReader reader) {
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

  private static ListDecodingRowBinaryReader newReader(final Flux<ByteBuffer> source) {
    return new ListDecodingRowBinaryReader(
        FluxInputStreamBridge.subscribeTo(source, RESPONSE_CHUNK_DEMAND),
        new QuerySettings()
            .setUseTimeZone("UTC")
            // Matches ClickHouseHttpTransport#JSON_AS_STRING_QUERY_PARAM, sent unconditionally on
            // every query: the server sends JSON columns back as a plain string when that query
            // parameter is set, so this local reader must be told to expect the same thing, or a
            // JSON column would decode via client-v2's complex .internal JSON object
            // representation instead of a plain String.
            .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1"),
        new BinaryStreamReader.DefaultByteBufferAllocator());
  }

  private static ListDecodingRowBinaryReader emitNextRow(
      final ListDecodingRowBinaryReader reader, final SynchronousSink<DecodedRow> sink) {
    if (reader.hasNext()) {
      sink.next(new DecodedRow(reader.nextRowValues()));
    } else {
      sink.complete();
    }
    return reader;
  }
}

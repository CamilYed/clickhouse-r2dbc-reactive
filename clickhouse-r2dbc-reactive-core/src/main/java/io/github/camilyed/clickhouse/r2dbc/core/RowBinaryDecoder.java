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
import reactor.core.scheduler.Scheduler;

/**
 * Decodes a {@code RowBinaryWithNamesAndTypes} response body into rows.
 *
 * <p>Wraps client-v2's {@link RowBinaryWithNamesAndTypesFormatReader} — reused here only for
 * decoding, never for transport — around the blocking {@link FluxInputStreamBridge}. Reading a row
 * blocks the calling thread; {@link #decode} moves that blocking work onto a caller-owned {@link
 * RowDecodingScheduler} explicitly, rather than relying on whichever thread happens to be driving
 * the subscription — see that method's Javadoc for exactly why "whichever thread happens to
 * request" is not good enough here.
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
   *
   * <p>Deliberately raw and synchronous — every blocking client-v2 call this method makes runs on
   * whatever thread subscribes/requests, with no scheduler of its own, unlike {@link #decode}. Only
   * safe against an already-in-memory or otherwise non-event-loop {@code source} (benchmarks, or a
   * test feeding a hermetic {@code Flux.just(...)} directly); never call this against a source
   * backed by a live Reactor Netty response the way the shipped connector's production path does —
   * that path goes through {@link #decode}, not this method, specifically to get {@link
   * RowDecodingScheduler}'s off-event-loop guarantee.
   */
  public static Flux<DecodedRow> decodeRows(final Flux<ByteBuffer> source) {
    return Flux.generate(
        () -> newReader(source), RowBinaryDecoder::emitNextRow, RowBinaryDecoder::closeReader);
  }

  /**
   * Decodes {@code source} into its column schema and row stream together, from one reader instance
   * and one subscription to {@code source} — unlike {@link #decodeRows}, which discards the schema
   * client-v2 already reads off the wire before the first row.
   *
   * <p>Every blocking client-v2 call this method's result ever makes — constructing the reader
   * (which eagerly reads the {@code RowBinaryWithNamesAndTypes} header, see {@link
   * ListDecodingRowBinaryReader}) <b>and</b> every subsequent {@link
   * ListDecodingRowBinaryReader#nextRowValues()} call the returned {@link DecodedResult#rows()}
   * makes as it's consumed — runs on {@code scheduler}, never on the thread that happens to request
   * the next row. For a real transport that thread is Reactor Netty's event loop; blocking it here
   * would stall every other query sharing it. This is deliberately not just the reader-construction
   * step: a {@code Flux.generate} source with no {@code subscribeOn} of its own runs its generator
   * function on whatever thread calls {@code request()}, which — once the response headers have
   * already arrived asynchronously — is the event-loop thread that delivered them, not the thread
   * that originally subscribed. See {@link RowDecodingScheduler}'s Javadoc for who owns {@code
   * scheduler} and disposes it.
   */
  public static Mono<DecodedResult> decode(
      final Flux<ByteBuffer> source, final RowDecodingScheduler scheduler) {
    final Scheduler reactorScheduler = scheduler.asReactorScheduler();
    return Mono.fromCallable(() -> newReader(source))
        .subscribeOn(reactorScheduler)
        .map(
            reader ->
                new DecodedResult(
                    columnsOf(reader),
                    Flux.generate(
                            () -> reader,
                            RowBinaryDecoder::emitNextRow,
                            RowBinaryDecoder::closeReader)
                        .subscribeOn(reactorScheduler)));
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

  /**
   * {@code Flux.generate}'s disposal hook, called exactly once when the returned sequence
   * terminates for any reason — natural completion, an error, <em>or downstream cancellation</em>
   * (see {@code reactor.core.publisher.FluxGenerate.GenerateSubscription#cleanup}, which calls the
   * supplied {@code Consumer<S>} on every one of those paths, not just normal completion). Without
   * this, {@link #decode}/{@link #decodeRows}' 2-arg {@code Flux.generate} overload used before
   * this method existed silently discarded the reader state on cancellation instead — {@link
   * ListDecodingRowBinaryReader} (inherited from client-v2's {@code AbstractBinaryFormatReader})
   * already implements {@code close()} as {@code input.close()}, i.e. {@link
   * FluxInputStreamBridge#close()}, which cancels the underlying transport subscription; that path
   * was simply never reached when a caller cancelled mid-stream (e.g. an R2DBC consumer that stops
   * reading rows early) rather than letting the sequence complete or error naturally. Left the
   * connection merely idle rather than explicitly torn down — see {@code RowBinaryDecoderTest}'s
   * cancellation tests for the regression coverage.
   */
  private static void closeReader(final ListDecodingRowBinaryReader reader) {
    try {
      reader.close();
    } catch (final Exception e) {
      throw new RowBinaryDecoderCloseException(e);
    }
  }

  /**
   * Wraps a failure closing the row decoder's underlying stream during {@code Flux.generate}'s
   * disposal hook. Deliberately unchecked: {@code Consumer<S>} (the {@code Flux.generate} disposal
   * callback's own type) cannot declare a checked exception, and {@code FluxGenerate}'s own {@code
   * cleanup()} already catches {@code Throwable} here and routes it to Reactor's standard {@code
   * Operators.onErrorDropped} hook — the same place any other cleanup-time failure in this sequence
   * would surface — rather than this class inventing its own logging dependency for a single rare
   * path.
   */
  private static final class RowBinaryDecoderCloseException extends RuntimeException {
    RowBinaryDecoderCloseException(final Exception cause) {
      super("Failed to close the row decoder's underlying stream", cause);
    }
  }
}

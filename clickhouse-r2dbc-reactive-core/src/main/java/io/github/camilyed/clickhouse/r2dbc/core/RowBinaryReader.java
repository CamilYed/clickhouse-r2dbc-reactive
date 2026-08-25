package io.github.camilyed.clickhouse.r2dbc.core;

import java.io.IOException;
import java.util.List;

/**
 * A {@code RowBinaryWithNamesAndTypes} response body reader — column schema plus a row-at-a-time
 * decode protocol, implemented by exactly the reader {@link RowBinaryDecoder} chooses once it has
 * parsed the response header (see that class's Javadoc for the native-vs-fallback decision): {@link
 * NativeRowBinaryReader} when every column resolves to a {@link ColumnDecoder}, {@link
 * ListDecodingRowBinaryReader} (client-v2-backed) otherwise, or {@link EmptyRowBinaryReader} for a
 * response with no {@code RowBinaryWithNamesAndTypes} header at all (e.g. a DDL statement).
 *
 * <p>{@link #hasNext()}/{@link #nextRowValues()} deliberately do not declare a checked exception —
 * matches {@link ListDecodingRowBinaryReader}'s inherited contract (client-v2's {@code
 * AbstractBinaryFormatReader#hasNext()}/{@code #next()} wrap {@link java.io.IOException} in an
 * unchecked failure internally), which {@link RowBinaryDecoder}'s {@code Flux.generate} callback
 * already relies on calling with no {@code try/catch} of its own.
 */
sealed interface RowBinaryReader
    permits NativeRowBinaryReader, ListDecodingRowBinaryReader, EmptyRowBinaryReader {

  /** This result's column schema, in wire order — empty for a response with no header at all. */
  List<ColumnDescriptor> columns();

  /** {@code true} if {@link #nextRowValues()} has another row to decode. */
  boolean hasNext();

  /**
   * Decodes and returns the next row's values, in wire column order. Only valid after {@link
   * #hasNext()} returns {@code true}.
   */
  Object[] nextRowValues();

  /**
   * Releases this reader's underlying stream. Declared as {@link IOException} rather than the
   * generic {@link Exception} so callers get a specific, catchable failure type instead of the
   * broadest possible one. {@link NativeRowBinaryReader} and {@link EmptyRowBinaryReader} only ever
   * throw {@link IOException} directly; {@link ListDecodingRowBinaryReader} inherits client-v2's
   * {@code AbstractBinaryFormatReader.close() throws Exception} (it only ever calls {@code
   * InputStream.close()} internally, so this is not a real widening in practice) and narrows it back
   * down to {@link IOException} itself — see that class's own {@code close()} override.
   */
  void close() throws IOException;
}

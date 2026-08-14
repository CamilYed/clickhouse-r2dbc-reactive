package io.github.camilyed.clickhouse.r2dbc.core;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Decodes {@code Array}/{@code Nested} columns as a plain {@link List} instead of client-v2's
 * {@code .internal} {@code BinaryStreamReader.ArrayValue}.
 *
 * <p>client-v2's own row-reading path ({@code next()}/{@code readRecord()}) always calls {@code
 * BinaryStreamReader.readValue(column, null)} — a {@code null} type hint — so {@code Array}/{@code
 * Nested} values come back as {@code ArrayValue}, a public class nested inside the {@code
 * .internal} package that this project otherwise deliberately avoids depending on (see ROADMAP.md's
 * Phase 0 reuse-boundary decision). {@code BinaryStreamReader} itself already special-cases a
 * {@code List.class} type hint to return a plain {@code List} instead; this class is the one
 * narrow, documented place that supplies it, by overriding the {@code protected} {@code
 * readRecord(Object[])} hook client-v2 exposes specifically for subclassing — the same kind of
 * deliberate, contained compromise as {@link FluxInputStreamBridge}'s bridge to a blocking {@code
 * InputStream} (Phase 0). Every other column type keeps decoding exactly as before (hint {@code
 * null}), so this changes nothing for the type-decoding surface already covered elsewhere.
 */
final class ListDecodingRowBinaryReader extends RowBinaryWithNamesAndTypesFormatReader {

  ListDecodingRowBinaryReader(
      final InputStream inputStream,
      final QuerySettings querySettings,
      final BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
    super(inputStream, querySettings, byteBufferAllocator);
  }

  @Override
  protected boolean readRecord(final Object[] record) throws IOException {
    final List<ClickHouseColumn> columns = getSchema().getColumns();
    if (columns.isEmpty()) {
      return false;
    }
    for (int i = 0; i < columns.size(); i++) {
      final ClickHouseColumn column = columns.get(i);
      try {
        record[i] = binaryStreamReader.readValue(column, listHintFor(column));
      } catch (final EOFException e) {
        if (i == 0) {
          endReached();
          return false;
        }
        throw e;
      }
    }
    return true;
  }

  private static Class<?> listHintFor(final ClickHouseColumn column) {
    final ClickHouseDataType dataType = column.getDataType();
    final boolean isListLike =
        dataType == ClickHouseDataType.Array || dataType == ClickHouseDataType.Nested;
    return isListLike ? List.class : null;
  }

  /**
   * Decodes the next row and returns a snapshot of its values as a plain {@code Object[]}, in wire
   * column order — bypassing client-v2's {@code RecordWrapper}/{@code Map} facade that {@link
   * #next()} builds, once past the one call to {@link #next()} this method still makes (see below
   * for why that part is kept, not reimplemented). See {@link DecodedRow}'s Javadoc for why this
   * exists: the {@code Map} copy this replaces measured at roughly 576 bytes/row, the dominant
   * per-row allocation and latency cost in this driver's decode path (docs/PERFORMANCE.md's Phase 5
   * "Optimization phase" section, hypothesis H1).
   *
   * <p>Deliberately still calls {@link #next()} rather than reimplementing its
   * buffer-swap-and-decode logic directly: that logic lives in client-v2's {@code
   * .internal}-adjacent base class this project has already drawn a boundary around not depending
   * on (see this class's own Javadoc). {@link #next()}'s own allocation — one {@code RecordWrapper}
   * plus two {@code WeakReference}s — is a small, fixed cost that client-v2's own decode path pays
   * identically (it also calls {@code next()}), so it is not a competitive disadvantage, only the
   * avoidable {@code Map}-copy on top of it is.
   *
   * <p>Must only be called when {@link #hasNext()} is {@code true} — same precondition {@link
   * #next()} itself has. {@code currentRecord} is read immediately after {@link #next()} returns,
   * on the same thread, before any later call could swap it out from under this snapshot — safe
   * because exactly one dedicated worker thread ever drives this reader (see {@link
   * FluxInputStreamBridge}'s own Javadoc for why).
   */
  Object[] nextRowValues() {
    next();
    return currentRecord.clone();
  }
}

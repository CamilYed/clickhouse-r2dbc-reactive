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
import org.jspecify.annotations.Nullable;

/**
 * Decodes {@code Array}/{@code Nested} columns as a plain {@link List}, and {@code Enum8}/{@code
 * Enum16} columns as a plain {@link String}, instead of client-v2's {@code .internal} types.
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
 *
 * <p>{@code Enum8}/{@code Enum16} columns get a second, narrower normalization: client-v2 has no
 * type-hint mechanism for them (unlike {@code Array}/{@code Nested}'s {@code List.class} hint
 * above) — {@code readValue(column, null)} always returns its own {@code .internal} {@code
 * EnumValue}. That type happens to override {@link Object#toString()} to return the member name
 * (e.g. {@code "PLACED"}), so this class calls {@code toString()} on the result for any column
 * whose {@link ClickHouseDataType} is {@code Enum8}/{@code Enum16}, once per row, immediately after
 * {@code readValue} returns — turning an internal type a caller would otherwise have to know to
 * unwrap (see {@code RealWorldTableAgainstRealClickHouseTest}'s Javadoc for how this used to be
 * read) into a plain {@link String} at the driver boundary, the same "never leak a client-v2 {@code
 * .internal} type through {@code Row}" goal the {@code Array}/{@code Nested} handling above already
 * serves. A {@code null} value (a {@code Nullable(Enum8)} column with no row set) is left as {@code
 * null} rather than the literal string {@code "null"}.
 */
final class ListDecodingRowBinaryReader extends RowBinaryWithNamesAndTypesFormatReader
    implements RowBinaryReader {

  // Both fixed for the lifetime of this reader once the header is parsed - see columns()/
  // listHints()'s Javadoc for why caching them here, instead of recomputing per row, is safe.
  private @Nullable List<ClickHouseColumn> cachedColumns;
  private Class<?> @Nullable [] cachedListHints;
  private boolean @Nullable [] cachedEnumColumns;

  ListDecodingRowBinaryReader(
      final InputStream inputStream,
      final QuerySettings querySettings,
      final BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
    super(inputStream, querySettings, byteBufferAllocator);
  }

  @Override
  protected boolean readRecord(final Object[] values) throws IOException {
    final List<ClickHouseColumn> columns = clickHouseColumns();
    if (columns.isEmpty()) {
      return false;
    }
    final Class<?>[] listHints = listHints(columns);
    final boolean[] enumColumns = enumColumns(columns);
    for (int i = 0; i < columns.size(); i++) {
      try {
        values[i] = binaryStreamReader.readValue(columns.get(i), listHints[i]);
      } catch (final EOFException e) {
        if (i == 0) {
          endReached();
          return false;
        }
        throw e;
      }
      if (enumColumns[i] && values[i] != null) {
        values[i] = values[i].toString();
      }
    }
    return true;
  }

  /**
   * {@link #getSchema()}'s columns, read once and reused for every subsequent row instead of
   * re-fetched by {@link #readRecord} on every single one — the schema is fixed once the header is
   * parsed (client-v2 does not re-parse or change it mid-result), so calling {@link #getSchema()}
   * {@code R} times (once per row) for a fixed answer was pure repeated work, not something the
   * decode contract requires. See docs/PERFORMANCE.md's "second-opinion review" section (finding 4)
   * for how this was found.
   *
   * <p>{@link #getSchema()} itself is {@code null} rather than an empty schema for a response with no
   * {@code RowBinaryWithNamesAndTypes} header at all (a DDL statement) — see {@link
   * RowBinaryHeader}'s Javadoc. In {@link RowBinaryDecoderMode#NATIVE}, {@link RowBinaryDecoder} only
   * ever constructs this reader once that case has already been routed to {@link
   * EmptyRowBinaryReader} instead, but in {@link RowBinaryDecoderMode#CLICKHOUSE} this reader is
   * constructed directly, with no such pre-check, so the {@code null} case can genuinely reach here —
   * treated the same way {@link RowBinaryDecoder}'s old {@code columnsOf} helper always did, before
   * this method existed: an empty column list, not a {@link NullPointerException}.
   */
  private List<ClickHouseColumn> clickHouseColumns() {
    List<ClickHouseColumn> columns = cachedColumns;
    if (columns == null) {
      final var schema = getSchema();
      columns = schema == null ? List.of() : schema.getColumns();
      cachedColumns = columns;
    }
    return columns;
  }

  /**
   * This result's column schema, in wire order — {@link RowBinaryReader}'s contract. Empty for a
   * response with no header at all — see {@link #clickHouseColumns()}'s Javadoc.
   */
  @Override
  public List<ColumnDescriptor> columns() {
    return clickHouseColumns().stream()
        .map(ListDecodingRowBinaryReader::toColumnDescriptor)
        .toList();
  }

  private static ColumnDescriptor toColumnDescriptor(final ClickHouseColumn column) {
    return new ColumnDescriptor(column.getColumnName(), column.getOriginalTypeName());
  }

  /**
   * One {@link #listHintFor(ClickHouseColumn)} result per column of {@code columns}, computed once
   * for the whole result rather than re-evaluated for every row — same fixed-per-result reasoning
   * as {@link #columns()}, turning the {@code R × C} repeated {@code Array}/{@code Nested} type
   * checks {@link #readRecord} used to perform into a one-time {@code C}-sized precomputation.
   */
  private Class<?>[] listHints(final List<ClickHouseColumn> columns) {
    Class<?>[] listHints = cachedListHints;
    if (listHints == null) {
      listHints = new Class<?>[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        listHints[i] = listHintFor(columns.get(i));
      }
      cachedListHints = listHints;
    }
    return listHints;
  }

  /**
   * Returns {@code null} for every column that isn't {@code Array}/{@code Nested} — the same "no
   * hint" signal client-v2's own decode path always passes (see this class's own Javadoc) — so
   * {@code null} here is a deliberate, meaningful value, not an oversight.
   */
  private static @Nullable Class<?> listHintFor(final ClickHouseColumn column) {
    final ClickHouseDataType dataType = column.getDataType();
    final boolean isListLike =
        dataType == ClickHouseDataType.Array || dataType == ClickHouseDataType.Nested;
    return isListLike ? List.class : null;
  }

  /**
   * One {@link #isEnumColumn(ClickHouseColumn)} result per column of {@code columns}, computed once
   * for the whole result — same fixed-per-result caching reasoning as {@link #listHints}.
   */
  private boolean[] enumColumns(final List<ClickHouseColumn> columns) {
    boolean[] enumColumns = cachedEnumColumns;
    if (enumColumns == null) {
      enumColumns = new boolean[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        enumColumns[i] = isEnumColumn(columns.get(i));
      }
      cachedEnumColumns = enumColumns;
    }
    return enumColumns;
  }

  /** {@code true} for {@code Enum8}/{@code Enum16} — see this class's own Javadoc. */
  private static boolean isEnumColumn(final ClickHouseColumn column) {
    final ClickHouseDataType dataType = column.getDataType();
    return dataType == ClickHouseDataType.Enum8 || dataType == ClickHouseDataType.Enum16;
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
  @Override
  public Object[] nextRowValues() {
    next();
    return currentRecord.clone();
  }
}

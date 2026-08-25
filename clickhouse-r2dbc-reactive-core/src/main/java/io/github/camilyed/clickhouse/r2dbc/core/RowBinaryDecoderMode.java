package io.github.camilyed.clickhouse.r2dbc.core;

/**
 * Which {@link RowBinaryReader} implementation {@link RowBinaryDecoder} uses to decode a {@code
 * RowBinaryWithNamesAndTypes} response body.
 *
 * <p>{@link #CLICKHOUSE} is this driver's default and always has been: every column, of every type,
 * decodes through client-v2's own {@link ListDecodingRowBinaryReader} exactly as it did before {@link
 * NativeRowBinaryReader} existed — the header is never even pre-parsed by {@code core} itself.
 * Choosing this mode costs nothing extra and changes nothing about how any result decodes; it is the
 * safe, reference-implementation baseline every other mode is measured against.
 *
 * <p>{@link #NATIVE} opts into {@link NativeRowBinaryReader} for any result where every column
 * resolves through {@link NativeColumnTypeResolver} — see {@link RowBinaryDecoder}'s own Javadoc for
 * the exact per-result native/fallback decision this makes, and {@code
 * docs/performance/latency-path-isolation.md} for the measured motivation (a decisive, cross-validated
 * ~21.6% per-row/per-column reader-layer cost at 10k-row scale). A result containing even one column
 * outside that native set still decodes exactly as {@link #CLICKHOUSE} would, automatically, with no
 * observable difference in decoded values or types — only the decode path taken to get there changes.
 */
public enum RowBinaryDecoderMode {

  /** Always decode via client-v2's own reader — this driver's long-standing default. */
  CLICKHOUSE,

  /**
   * Decode natively-supported columns via {@link NativeRowBinaryReader}, falling back to {@link
   * #CLICKHOUSE}'s behavior automatically for any result containing an unsupported column.
   */
  NATIVE
}

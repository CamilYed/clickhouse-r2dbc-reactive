package io.github.camilyed.clickhouse.r2dbc.core;

import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder;

/**
 * Whether a ClickHouse HTTP response body uses ClickHouse's native checksummed compression blocks
 * (triggered by {@code compress=1}, distinct from standard HTTP {@code Content-Encoding}) — and,
 * symmetrically, whether the transport should ask the server to compress it in the first place.
 * Each block identifies its codec: older servers normally send LZ4, while ClickHouse 26.9+ normally
 * sends ZSTD. One value threads both directions: {@code transport-http} reads it to decide whether
 * to send {@code compress=1}, and {@code core}'s {@link RowBinaryDecoder} reads it to decide
 * whether to wrap the response body in {@code ClickHouseCompressedInputStream} before decoding.
 *
 * <p>{@link #LZ4} is this driver's default — see {@code TransportOptions#defaults()} — matching
 * client-v2's own default of {@code COMPRESS_SERVER_RESPONSE=true}, so a caller migrating from
 * client-v2 (or comparing throughput against it) gets the same on-the-wire behavior without opting
 * in explicitly.
 */
public enum ResponseCompression {

  /** The response body is sent, and must be read, uncompressed. */
  NONE,

  /**
   * The response body uses ClickHouse's native checksummed block framing. The historical enum name
   * remains for source compatibility; decoding accepts both LZ4 and ZSTD according to each block's
   * method byte. See the internal {@code ClickHouseCompressedInputStream} Javadoc.
   */
  LZ4
}

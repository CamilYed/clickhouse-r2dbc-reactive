package io.github.camilyed.clickhouse.r2dbc.core;

import io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder;

/**
 * Whether a ClickHouse HTTP response body is compressed with ClickHouse's own custom LZ4 block
 * framing (triggered by the {@code compress=1} query parameter, distinct from standard HTTP {@code
 * Content-Encoding}) — and, symmetrically, whether the transport should ask the server to compress
 * it in the first place. One value threads both directions: {@code transport-http} reads it to
 * decide whether to send {@code compress=1}, and {@code core}'s {@link RowBinaryDecoder} reads it
 * to decide whether to wrap the response body in {@code ClickHouseLz4InputStream} before decoding.
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
   * The response body is ClickHouse's own LZ4 block framing — see the internal {@code
   * ClickHouseLz4InputStream} Javadoc for the exact wire format.
   */
  LZ4
}

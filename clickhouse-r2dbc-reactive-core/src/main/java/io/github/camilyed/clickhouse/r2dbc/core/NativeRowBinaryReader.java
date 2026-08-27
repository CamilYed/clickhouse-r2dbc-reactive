package io.github.camilyed.clickhouse.r2dbc.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.UncheckedIOException;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Decodes {@code RowBinaryWithNamesAndTypes} rows directly, bypassing client-v2's {@code
 * RowBinaryWithNamesAndTypesFormatReader}/{@code AbstractBinaryFormatReader}/{@code
 * BinaryStreamReader} machinery entirely — see {@link RowBinaryDecoder}'s Javadoc for when {@link
 * NativeColumnTypeResolver} makes this reader eligible (every column in the result resolves to a
 * {@link ColumnDecoder}) versus when the fallback {@link ListDecodingRowBinaryReader} is used
 * instead.
 *
 * <p>Grew out of {@code MinimalRowBinaryReader}, a benchmark-only prototype (see
 * docs/performance/latency-path-isolation.md's Variant D section) that reported a decisive,
 * reproducible per-row/per-column decode cost from client-v2's generic reader machinery on a
 * multi-column scan (~21.6% faster, ~79x combined error bars, cross-checked by matching
 * mean-latency and throughput ratios) — this class is the production version, covering ClickHouse's
 * scalar types ({@link NativeColumnTypeResolver}'s permitted set) rather than the benchmark's
 * narrow four-type slice, and wired behind an automatic, correctness-preserving fallback rather
 * than opt-in.
 *
 * <p><b>That ~21.6% number is retracted</b> — a 2026-08-26 external review of PR #99 found {@code
 * MinimalRowBinaryReader} decoded {@code UInt8}/{@code UInt64} as {@code Long} (not this class's
 * {@code Short}/{@code BigInteger}) and that the benchmark blackholed client-v2's typed-getter
 * output against this prototype's raw array — different Java representations and different
 * consumption work on each side, not decode cost alone. That mismatch is fixed, and {@link
 * RowBinaryDecoderMode}'s {@code CLICKHOUSE} vs {@code NATIVE} has since been compared directly and
 * symmetrically via {@code DecoderOnlyBenchmark#thisDriver}/{@code #thisDriverNative} (same
 * captured bytes, same production {@code RowBinaryDecoder} call, only the mode differs): a trusted
 * 2026-08-27 run (commit {@code 88020ed}) measured {@code NATIVE} ~13.6-15.2% faster (mean/p50/p90/
 * p95 all agreeing) with ~11.4% lower allocation, consistently at 10k/100k/1M rows, for a {@code
 * UInt64 + String + Decimal(18,4)} row shape. <b>That confirms this class decodes faster in
 * isolation — it does not yet prove the whole driver is faster for real point queries</b>, since
 * network, transport, the decoder scheduler, and connection pooling all sit between this class and
 * an application; {@code PublicApiMatchedPoolThroughputBenchmark#thisDriverNative} exists to answer
 * that end-to-end question next and has not been run yet.
 *
 * <p>End-of-row-stream detection uses the same one-byte peek/{@link PushbackInputStream#unread}
 * approach as that prototype: {@code RowBinaryWithNamesAndTypes} carries no row-count prefix, so
 * the only way to tell "one more row" from "the response is over" is to attempt to read the next
 * byte.
 *
 * <p>{@link #hasNext()}/{@link #nextRowValues()} wrap {@link IOException} in {@link
 * UncheckedIOException} rather than declaring it — see {@link RowBinaryReader}'s Javadoc for why
 * this must match {@link ListDecodingRowBinaryReader}'s inherited unchecked contract.
 */
final class NativeRowBinaryReader implements RowBinaryReader {

  private final PushbackInputStream in;
  private final List<ColumnDescriptor> schema;
  private final ColumnPlan[] plans;

  // Reused across every value of every row - sized to the widest fixed-width read this hierarchy
  // ever performs (Decimal256, 32 bytes) - the same buffer-reuse strategy client-v2's own
  // BinaryStreamReader uses internally (its own per-width instance-field scratch buffers).
  private final byte[] scratch = new byte[32];

  NativeRowBinaryReader(
      final InputStream source, final List<ColumnDescriptor> schema, final ColumnPlan[] plans) {
    this.in = new PushbackInputStream(source, 1);
    this.schema = schema;
    this.plans = plans;
  }

  @Override
  public List<ColumnDescriptor> columns() {
    return schema;
  }

  @Override
  public boolean hasNext() {
    try {
      final int first = in.read();
      if (first == -1) {
        return false;
      }
      in.unread(first);
      return true;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Object[] nextRowValues() {
    try {
      final Object[] values = new Object[plans.length];
      for (int i = 0; i < plans.length; i++) {
        values[i] = decodeOneValue(plans[i]);
      }
      return values;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns {@code null} for a {@code Nullable} column whose wire value is the null marker byte — a
   * legitimate decoded value, not an absent one, so this method is deliberately {@link Nullable}
   * rather than relying on the package's {@code @NullMarked} default.
   */
  private @Nullable Object decodeOneValue(final ColumnPlan plan) throws IOException {
    if (plan.nullable()) {
      final int isNull = RowBinaryWireFormat.readRequiredByte(in);
      if (isNull == 1) {
        return null;
      }
    }
    return plan.decoder().decode(in, scratch);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}

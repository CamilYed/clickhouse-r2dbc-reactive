package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

/**
 * Which {@link RowBinaryReader} implementation {@link RowBinaryDecoder} uses to decode a {@code
 * RowBinaryWithNamesAndTypes} response body.
 *
 * <p>{@link #CLICKHOUSE} is this driver's default and always has been: every column, of every type,
 * decodes through client-v2's own {@link ListDecodingRowBinaryReader} exactly as it did before
 * {@link NativeRowBinaryReader} existed — the header is never even pre-parsed by {@code core}
 * itself. Choosing this mode costs nothing extra and changes nothing about how any result decodes;
 * it is the safe, reference-implementation baseline every other mode is measured against.
 *
 * <p>{@link #NATIVE} opts into {@link NativeRowBinaryReader} for any result where every column
 * resolves through {@link NativeColumnTypeResolver} — see {@link RowBinaryDecoder}'s own Javadoc
 * for the exact per-result native/fallback decision this makes, and {@code
 * docs/performance/latency-path-isolation.md} for the measured motivation. The originally reported
 * ~21.6% figure from the {@code MinimalRowBinaryReader} prototype was <b>retracted</b> by a
 * 2026-08-26 external review of PR #99 (see {@link NativeRowBinaryReader}'s Javadoc for why) and
 * superseded by a trusted, apples-to-apples {@code DecoderOnlyBenchmark#thisDriver}/{@code
 * #thisDriverNative} run against the exact production {@link RowBinaryDecoder} path on 2026-08-27:
 * {@link #NATIVE} measured ~13.6-15.2% lower mean/p50/p90/p95 latency and ~11.4% lower allocation
 * than {@link #CLICKHOUSE}, consistently across 10k/100k/1M rows, for a {@code UInt64 + String +
 * Decimal(18,4)} row shape. That confirms a real decode-layer improvement, but a follow-up trusted
 * {@code PublicApiMatchedPoolThroughputBenchmark#thisDriverNative} run (2026-08-27, commit {@code
 * 3d66d62}) found it does not translate 1:1 to the public, end-to-end path: JMH throughput was
 * statistically indistinguishable from {@link #CLICKHOUSE} at every tested concurrency, merged
 * per-query latency was only ~1-2% lower at low/medium concurrency and flat at high concurrency,
 * and allocation was ~7-8% lower (the one clean, consistent signal) — network, transport, the
 * decoder scheduler, and connection pooling dominate this benchmark's cost far more than the decode
 * step does. A follow-up profiler-free rerun ({@code trusted-clean} profile, 2026-08-27, commit
 * {@code 8c64d73}) confirmed the same picture without JFR/GC overhead as a confound: throughput
 * again statistically indistinguishable, {@link #NATIVE}'s small p50 edge over {@link #CLICKHOUSE}
 * did not reproduce (flat to marginally worse this time, within noise either way), and — the one
 * decision-relevant finding — the tail-latency (p99) gap against {@code clientV2} was essentially
 * identical regardless of decode mode (~22-28% either way), confirming that gap sits upstream of
 * decoding entirely. See ROADMAP.md's "Trusted public-API result" and "Trusted-clean public-API
 * result" entries for the full tables; {@link #NATIVE} stays opt-in on the strength of both. A
 * result containing even one column outside the native set still decodes exactly as {@link
 * #CLICKHOUSE} would, automatically, with no observable difference in decoded values or types —
 * only the decode path taken to get there changes.
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

package io.github.camilyed.clickhouse.r2dbc.macrobench.backend;

import java.util.Locale;

/**
 * Which query backend a request is routed to, and which backend(s) are active at startup ({@code
 * benchmark.backend} - see {@code BenchmarkProperties}). {@link #DUAL} means both {@link #R2DBC}
 * and {@link #CLIENT_V2} are live in the same process (local A/B); {@link #R2DBC}/{@link
 * #CLIENT_V2} alone mean the other backend's beans are never created at all - required for trusted
 * CPU/RSS/thread measurements, since an idle backend's own threads still contaminate process-level
 * resource numbers even with zero traffic reaching it. See ROADMAP.md's Phase 12.
 */
public enum Backend {
  R2DBC,
  CLIENT_V2,
  DUAL;

  /**
   * Parses {@code benchmark.backend}/REST-path-style values ({@code "r2dbc"}, {@code "client-v2"},
   * {@code "dual"}), case-insensitive - the one place the hyphenated {@code "client-v2"} spelling
   * is translated to the {@link #CLIENT_V2} enum constant, shared by {@code
   * BackendEnabledCondition} and {@code BenchmarkController} so both parse identically.
   */
  public static Backend fromProperty(final String value) {
    return valueOf(value.strip().toUpperCase(Locale.ROOT).replace('-', '_'));
  }
}

package io.github.camilyed.clickhouse.r2dbc.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A short, stable, non-reversible identifier for a SQL statement's exact text — attached to a
 * {@link DriverObservationEvent} in place of the SQL itself, which must never be logged or
 * exported to a metrics/tracing backend (it may embed values via string concatenation, even though
 * this driver's own bind mechanism never does). Two instances are equal exactly when the SQL text
 * they were computed from is character-for-character identical; any difference, including
 * whitespace-only changes, produces a different fingerprint.
 */
public record SqlFingerprint(String value) {

  private static final String ALGORITHM = "SHA-256";
  private static final int DISPLAYED_HEX_BYTES = 8;

  public SqlFingerprint {
    if (value.isEmpty()) {
      throw new IllegalArgumentException("value must not be empty");
    }
  }

  /** Computes the fingerprint of {@code sql}'s exact text. */
  public static SqlFingerprint of(final String sql) {
    final byte[] hash = sha256(sql.getBytes(StandardCharsets.UTF_8));
    return new SqlFingerprint(toHex(hash, DISPLAYED_HEX_BYTES));
  }

  private static byte[] sha256(final byte[] input) {
    try {
      return MessageDigest.getInstance(ALGORITHM).digest(input);
    } catch (final NoSuchAlgorithmException e) {
      // Every JVM implementation is required to support SHA-256 (java.security.MessageDigest's
      // own Javadoc lists it as a mandatory standard algorithm) - reaching this means a broken
      // JVM, not a recoverable condition.
      throw new IllegalStateException(ALGORITHM + " is not available", e);
    }
  }

  private static String toHex(final byte[] bytes, final int byteCount) {
    final StringBuilder hex = new StringBuilder(byteCount * 2);
    for (int i = 0; i < byteCount; i++) {
      hex.append(Character.forDigit((bytes[i] >> 4) & 0xF, 16));
      hex.append(Character.forDigit(bytes[i] & 0xF, 16));
    }
    return hex.toString();
  }

  @Override
  public String toString() {
    return value;
  }
}

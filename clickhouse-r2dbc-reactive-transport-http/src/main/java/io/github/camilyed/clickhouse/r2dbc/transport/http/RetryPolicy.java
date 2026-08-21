package io.github.camilyed.clickhouse.r2dbc.transport.http;

import java.time.Duration;
import java.util.Objects;

/**
 * Governs whether and how {@link ClickHouseHttpTransport} retries a failed query.
 *
 * <p>By default, a retry is only ever attempted for a failure that happens <em>before</em> the
 * request has been fully sent to the server — see {@link
 * ClickHouseHttpTransport#queryWithSummary}'s Javadoc for exactly how that's detected. This holds
 * regardless of whether the query is a {@code SELECT} or an {@code INSERT}: since the server never
 * received any bytes of the failed attempt, retrying cannot make the query run twice server-side,
 * so no per-query idempotency flag or SQL-shape guessing is needed to decide whether a retry is
 * safe. A failure that happens after the request was sent — even one that looks transient, like a
 * connection reset mid-response — is never retried under this default, since ClickHouse may already
 * have received and be processing (or have fully applied) the query; retrying that case would risk
 * applying a non-idempotent statement like {@code INSERT} twice.
 *
 * <p>This mirrors client-v2's own default retry behavior (verified against our pinned {@code
 * client-v2} version — see the version catalog): {@code client-v2} retries by default only for
 * connection-level failures that occur before a response is received ({@code NoHttpResponse},
 * {@code ConnectTimeout}, {@code ConnectionRequestTimeout}, {@code SocketTimeout}).
 *
 * <p>A caller may additionally opt a specific query into retrying after a ClickHouse server error
 * that {@code ServerException.isRetryable()} classifies as retryable, via {@code
 * ClickHouseQuery#withServerErrorRetryEnabled()} — see that method's Javadoc for the full safety
 * reasoning and why it is a per-query opt-in rather than a field on this record. This record's own
 * shape ({@code maxAttempts}/{@code delay}) did not need to change to support that: the opt-in
 * widens <em>which failures</em> are eligible for the existing attempt/delay budget below, it does
 * not add a second budget.
 */
public record RetryPolicy(int maxAttempts, Duration delay) {

  public RetryPolicy {
    if (maxAttempts < 0) {
      throw new IllegalArgumentException("maxAttempts must be >= 0, got: " + maxAttempts);
    }
    Objects.requireNonNull(delay, "delay");
    if (delay.isNegative()) {
      throw new IllegalArgumentException("delay must not be negative, got: " + delay);
    }
  }

  /** No retry at all: the first failure, pre-send or not, surfaces directly to the caller. */
  public static RetryPolicy disabled() {
    return new RetryPolicy(0, Duration.ZERO);
  }

  /**
   * Up to 3 retries (4 attempts total) of a pre-send failure, each separated by a fixed 50ms delay
   * — matching client-v2's own default of 3 retries, with a small fixed delay added since, unlike
   * client-v2, this driver has no second node/endpoint to fail over to on retry.
   */
  public static RetryPolicy defaultPolicy() {
    return new RetryPolicy(3, Duration.ofMillis(50));
  }

  /** Whether this policy allows any retry at all. */
  public boolean isEnabled() {
    return maxAttempts > 0;
  }
}

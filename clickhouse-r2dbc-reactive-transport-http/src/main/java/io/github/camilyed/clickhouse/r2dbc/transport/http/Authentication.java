package io.github.camilyed.clickhouse.r2dbc.transport.http;

import io.netty.handler.codec.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * How a request authenticates against ClickHouse's HTTP interface — a closed set of the modes this
 * transport currently supports (see {@code docs/CLIENT_V2_HTTP_REFERENCE.md} for the full set
 * ClickHouse offers; mTLS/SSL certificate auth is documented there but not implemented here yet —
 * it needs a real client-certificate {@code SSLContext} wired into the underlying {@code
 * HttpClient}, not just a header, so it's tracked as separate, larger work rather than half-built).
 *
 * <p>Modeled as a sealed type instead of a nullable/{@code Optional} field on {@link
 * ClickHouseHttpTransport} so "no credentials" is a real, named state rather than the absence of
 * one — {@link #addTo(HttpHeaders)} is total, there's nothing to null-check at the call site.
 */
public sealed interface Authentication {

  /** Adds whatever headers this mode requires to {@code headers}. */
  void addTo(HttpHeaders headers);

  /** No credentials are sent; relies on the ClickHouse server allowing anonymous access. */
  static Authentication none() {
    return new None();
  }

  /** HTTP Basic authentication: {@code Authorization: Basic base64(user:password)}. */
  static Authentication basic(final String user, final String password) {
    return new Basic(user, password);
  }

  /**
   * ClickHouse's own header-pair authentication: {@code X-ClickHouse-User} / {@code
   * X-ClickHouse-Key}. The alternative client-v2 falls back to when HTTP Basic auth is turned off;
   * useful against a server/proxy that strips or mishandles the {@code Authorization} header.
   */
  static Authentication userKey(final String user, final String key) {
    return new UserKey(user, key);
  }

  /** No credentials are sent; relies on the ClickHouse server allowing anonymous access. */
  record None() implements Authentication {
    @Override
    public void addTo(final HttpHeaders headers) {
      // Anonymous access: deliberately nothing to add.
    }
  }

  /** HTTP Basic authentication: {@code Authorization: Basic base64(user:password)}. */
  record Basic(String user, String password) implements Authentication {
    @Override
    public void addTo(final HttpHeaders headers) {
      final String credentials = user + ":" + password;
      final String encoded =
          Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
      headers.set("Authorization", "Basic " + encoded);
    }

    // The generated record toString() would print password in plain text - redacted since any
    // consumer logging a TransportOptions (which embeds this Authentication) would otherwise leak
    // it.
    @Override
    public String toString() {
      return "Basic[user=" + user + ", password=<redacted>]";
    }
  }

  /**
   * ClickHouse's own header-pair authentication: {@code X-ClickHouse-User} / {@code
   * X-ClickHouse-Key}.
   */
  record UserKey(String user, String key) implements Authentication {
    @Override
    public void addTo(final HttpHeaders headers) {
      headers.set("X-ClickHouse-User", user);
      headers.set("X-ClickHouse-Key", key);
    }

    // See Basic#toString() above for why this is redacted rather than generated.
    @Override
    public String toString() {
      return "UserKey[user=" + user + ", key=<redacted>]";
    }
  }
}

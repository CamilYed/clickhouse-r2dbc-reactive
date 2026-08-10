package io.github.camilyed.clickhouse.r2dbc.transport.http;

import io.netty.handler.codec.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * How a request authenticates against ClickHouse's HTTP interface — a closed set of the modes this
 * transport currently supports (see {@code docs/CLIENT_V2_HTTP_REFERENCE.md} for the full set
 * ClickHouse offers; mTLS and the {@code X-ClickHouse-User}/{@code X-ClickHouse-Key} header pair
 * aren't implemented yet).
 *
 * <p>Modeled as a sealed type instead of a nullable/{@code Optional} field on {@link
 * ClickHouseHttpTransport} so "no credentials" is a real, named state rather than the absence of
 * one — {@link #addTo(HttpHeaders)} is total, there's nothing to null-check at the call site.
 */
sealed interface Authentication {

    /** Adds whatever headers this mode requires to {@code headers}. */
    void addTo(HttpHeaders headers);

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
            final String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
            headers.set("Authorization", "Basic " + encoded);
        }
    }
}

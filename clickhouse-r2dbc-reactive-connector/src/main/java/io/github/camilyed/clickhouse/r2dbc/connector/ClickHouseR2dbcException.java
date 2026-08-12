package io.github.camilyed.clickhouse.r2dbc.connector;

import com.clickhouse.client.api.ServerException;
import io.r2dbc.spi.R2dbcException;
import org.jspecify.annotations.Nullable;

/**
 * Wraps every failure this driver can produce while executing a statement or batch into the {@link
 * R2dbcException} standard R2DBC callers already know how to catch.
 *
 * <p>client-v2's {@link ServerException} — reused as-is by {@code transport-http} to carry
 * ClickHouse's own numeric error code (see {@code ClickHouseHttpTransport}'s Javadoc) — does not
 * itself extend {@link R2dbcException}. Left unwrapped, a caller doing {@code catch (R2dbcException
 * e)} around a query would never catch a ClickHouse server error, which defeats the whole point of
 * a standard R2DBC exception hierarchy. This class is where that gap is closed, at the one seam
 * ({@code connector}) that is allowed to know about R2DBC at all.
 */
public final class ClickHouseR2dbcException extends R2dbcException {

  private ClickHouseR2dbcException(
      final @Nullable String reason, final int errorCode, final Throwable cause) {
    super(reason, null, errorCode, cause);
  }

  /**
   * Maps {@code throwable} onto an {@link R2dbcException} suitable for propagating out of this
   * driver's {@code Publisher}s. Package-private: only {@link ClickHouseStatement}, {@link
   * ClickHouseBatch}, and {@link ClickHouseResult} — the seams where this driver's {@code
   * Publisher}s can fail — call this; nothing outside the package needs to.
   *
   * <p>Returns {@code throwable} unchanged if it already is an {@link R2dbcException}. Otherwise,
   * walks {@code throwable}'s cause chain for a client-v2 {@link ServerException} — reading a
   * response body wraps one inside further layers (an {@code IOException} from the transport
   * bridge, then a client-v2 {@code ClientException} from schema reading — see {@code
   * RowBinaryDecoder}'s Javadoc) — and uses its ClickHouse-reported error code and message if
   * found. Any other failure (connection reset, pool exhaustion, a local decode bug) becomes a
   * generic {@link ClickHouseR2dbcException} with error code {@code 0}, {@code throwable} itself as
   * the cause, so standard R2DBC error handling still catches it.
   */
  static R2dbcException wrap(final Throwable throwable) {
    if (throwable instanceof final R2dbcException r2dbcException) {
      return r2dbcException;
    }
    final ServerException serverException = findServerException(throwable);
    if (serverException != null) {
      return new ClickHouseR2dbcException(
          serverException.getMessage(), serverException.getCode(), throwable);
    }
    return new ClickHouseR2dbcException(throwable.getMessage(), 0, throwable);
  }

  private static @Nullable ServerException findServerException(final Throwable throwable) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current instanceof final ServerException serverException) {
        return serverException;
      }
    }
    return null;
  }
}

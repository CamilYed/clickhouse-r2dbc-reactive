package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.awaitility.Awaitility.await;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

/**
 * Proves, against a real ClickHouse server, that cancelling a client-side subscription to {@link
 * ClickHouseHttpTransport#query} now actually stops the query executing server-side — via this
 * driver's own best-effort {@code KILL QUERY}, not via ClickHouse's connection-close detection.
 *
 * <p>ClickHouse's HTTP interface does not do this on its own: closing the connection alone leaves
 * the query running (see clickhouse.com/docs/interfaces/http, <em>"Running requests don't stop
 * automatically if the HTTP connection is lost"</em>, and ClickHouse/ClickHouse#92786, where even
 * the opt-in {@code cancel_http_readonly_queries_on_client_close} setting is shown not to reliably
 * help). {@link ClickHouseHttpTransport#queryWithSummary}'s Javadoc has the full writeup of why
 * this driver now sends an explicit {@code KILL QUERY WHERE query_id = '...' ASYNC} itself on
 * cancellation, reusing the same authenticated user as the original query (ClickHouse lets a user
 * stop their own queries without a separate privilege).
 *
 * <p>Verified from entirely outside this driver, by polling ClickHouse's own {@code
 * system.processes} table (via {@link BaseClickHouseIntegrationTest#isQueryRunning}) — proving the
 * query disappears within a window much shorter than its own ~10 second natural runtime is real
 * evidence of an actual, driver-triggered kill, not a race with normal completion.
 */
class QueryCancellationAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  @Test
  void shouldStopTheQueryServerSideWhenTheClientCancelsTheSubscription() {
    // given
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport(
            clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    final String queryId = UUID.randomUUID().toString();
    final ClickHouseQuery slowQuery =
        ClickHouseQuery.of(
            "SELECT sleepEachRow(0.1) FROM numbers(100) "
                + "SETTINGS function_sleep_max_microseconds_per_block = 60000000",
            queryId);
    final Disposable subscription = transport.query(slowQuery).subscribe();
    await().atMost(Duration.ofSeconds(10)).until(() -> isQueryRunning(queryId));

    // when
    subscription.dispose();

    // then
    await().atMost(Duration.ofSeconds(5)).until(() -> !isQueryRunning(queryId));
  }
}

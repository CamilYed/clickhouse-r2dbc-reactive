package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.awaitility.Awaitility.await;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

/**
 * Documents, against a real ClickHouse server, that cancelling a client-side subscription to {@link
 * ClickHouseHttpTransport#query} does <b>not</b> stop the query executing server-side — despite
 * ClickHouse's own HTTP interface docs describing a "Cancel HTTP Request" mechanism.
 *
 * <p>This was expected to work and is <em>not</em> a bug in this driver to fix; it's a documented,
 * long-standing ClickHouse HTTP interface limitation, verified here rather than assumed:
 *
 * <ul>
 *   <li>ClickHouse's own docs (clickhouse.com/docs/interfaces/http, "Using ClickHouse sessions in
 *       the HTTP protocol" section) state plainly: <em>"Running requests don't stop automatically
 *       if the HTTP connection is lost."</em>
 *   <li>The opt-in setting meant to fix this, {@code cancel_http_readonly_queries_on_client_close},
 *       is itself unreliable — ClickHouse/ClickHouse#92786 reproduces a query that keeps running
 *       after the client disconnects even with that setting enabled, against the then-latest
 *       release (25.6.2), and was closed by ClickHouse's own maintainers as "not planned".
 * </ul>
 *
 * <p>Practical consequence for callers of this driver: disposing a {@code Flux}/{@code Mono}
 * subscription stops <em>this driver</em> from reading further response bytes and closes its HTTP
 * connection (proven client-side by {@link
 * ClickHouseHttpTransportTest#shouldCloseTheConnectionWhenTheSubscriptionIsCancelled}), but a
 * caller that needs the query itself to actually stop running on the server must send an explicit
 * {@code KILL QUERY WHERE query_id = '...'} — this driver already sends {@code query_id} as {@code
 * X-ClickHouse-Query-Id} on every request precisely so a caller has the means to do that (see
 * {@link BaseClickHouseIntegrationTest#killQuery}, used below purely to clean up after this test
 * rather than leave the query running for the rest of the shared container's lifetime).
 *
 * <p>Verified from entirely outside this driver, by polling ClickHouse's own {@code
 * system.processes} table (via {@link BaseClickHouseIntegrationTest#isQueryRunning}) — proving a
 * negative ("still running") this way, over a window comfortably shorter than the query's own ~10
 * second natural runtime, is real evidence the query didn't stop, not just a race that hasn't
 * resolved yet.
 */
class QueryCancellationAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  @Test
  void shouldNotStopTheQueryServerSideWhenTheClientCancelsTheSubscription() {
    // given
    final ClickHouseHttpTransport transport =
        new ClickHouseHttpTransport(
            clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    final String queryId = UUID.randomUUID().toString();
    final ClickHouseQuery slowQuery =
        ClickHouseQuery.of(
            "SELECT sleepEachRow(0.1) FROM numbers(100) "
                + "SETTINGS function_sleep_max_microseconds_per_block = 60000000, "
                + "cancel_http_readonly_queries_on_client_close = 1",
            queryId);
    final Disposable subscription = transport.query(slowQuery).subscribe();
    await().atMost(Duration.ofSeconds(10)).until(() -> isQueryRunning(queryId));

    // when
    subscription.dispose();

    // then
    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(4))
        .until(() -> isQueryRunning(queryId));

    killQuery(queryId);
  }
}

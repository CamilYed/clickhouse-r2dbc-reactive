package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.clickhouse.client.api.ServerException;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.testkit.BaseClickHouseIntegrationTest;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Characterizes what this driver actually does when a query fails <em>after</em> it has already
 * started streaming rows back — some rows already decoded and handed to the caller, then ClickHouse
 * fails server-side, then the response terminates — since nothing in the test suite proved this
 * before now (see ROADMAP.md's Phase 8, item 5).
 *
 * <p>Forces the failure with {@code throwIf(...)} over {@code system.numbers}: the condition is
 * false for the first N rows (each block flows through and out over HTTP as ClickHouse processes
 * it, well before the query as a whole finishes), then becomes true partway through and ClickHouse
 * raises a {@code DB::Exception} mid-query — a reliable, deterministic way to force this exact
 * shape of failure against a real server, rather than depending on a race or an external fault.
 *
 * <p><b>Confirmed against a real ClickHouse server (not just predicted from the HTTP interface
 * docs):</b> once response bytes have already been sent, HTTP's own "headers before body" rule
 * means ClickHouse can no longer retroactively attach {@code X-ClickHouse-Exception-Code} to the
 * initial response headers — the header this transport's {@code isError} check inspects. With this
 * driver's default settings (streaming, no {@code wait_end_of_query}), ClickHouse instead injects
 * the error as plain text appended directly into the response body, after whatever rows had already
 * been written. A real run proved this is worse than "the stream just ends" — the trailing error
 * text gets misdecoded by client-v2's RowBinary reader as further {@code UInt64} values (arbitrary
 * large numbers, indistinguishable in shape from genuine row data) and handed to the subscriber as
 * if they were real rows, before decoding eventually fails on some later, no-longer-parseable byte
 * and the {@link Flux} terminates with an error. So under default settings this driver offers
 * exactly two guarantees, both proven by {@link
 * #shouldFailRatherThanSilentlyCompletingWhenTheFailureHappensAfterStreamingHasStarted}: it never
 * silently completes as if the result were whole, and the genuine prefix of rows already decoded
 * before corruption starts is intact and in order (Reactive Streams' own no-retroactive-un-emitting
 * guarantee, not something this driver has to implement itself). It does <b>not</b> guarantee that
 * every emitted row is genuine — a caller that cannot tolerate spurious garbage rows possibly
 * appearing near the end of a result that then fails must opt into {@code wait_end_of_query=1} (see
 * below), which this driver has no way to enforce or detect on its own. See README's Known
 * limitations section for the caller-facing writeup of this finding.
 *
 * <p>{@link #shouldSurfaceACleanServerExceptionWhenWaitEndOfQueryIsEnabled} answers the roadmap
 * item's other open question directly: {@code wait_end_of_query=1} — ClickHouse's own opt-in
 * "buffer the whole response server-side, then send a single clean header-based error if one
 * occurred" tradeoff — is already reachable today with zero code changes, through {@link
 * ClickHouseQuery#withSettings(Map)}'s existing generic passthrough to raw {@code <name>=<value>}
 * request parameters. Opting in trades away incremental streaming (the whole result is buffered
 * server-side up to ClickHouse's own {@code http_response_buffer_size} before anything is sent) for
 * exactly the clean {@link ServerException} this transport's {@code isError} check was built to
 * catch, with no risk of the garbage-row corruption described above.
 */
class MidStreamQueryFailureAgainstRealClickHouseTest extends BaseClickHouseIntegrationTest {

  private static final String THROW_IF_MESSAGE = "mid_stream_failure_probe";

  private ClickHouseHttpTransport transport;

  private ClickHouseHttpTransport transport() {
    if (transport == null) {
      transport =
          new ClickHouseHttpTransport(
              clickHouseHttpUrl(), clickHouseUsername(), clickHousePassword());
    }
    return transport;
  }

  private final RowDecodingScheduler decodingScheduler = RowDecodingScheduler.defaults();

  private ClickHouseConnection connection() {
    return new ClickHouseConnection(transport(), decodingScheduler);
  }

  @Test
  void shouldFailRatherThanSilentlyCompletingWhenTheFailureHappensAfterStreamingHasStarted() {
    // given - max_block_size=1000 keeps blocks small so earlier blocks are flushed to the client
    // well before block ~50 (row 50000) triggers throwIf; 100000 rows total keeps the query itself
    // fast even though it fails partway through.
    final String sql =
        "SELECT number FROM system.numbers "
            + "WHERE throwIf(number = 50000, '"
            + THROW_IF_MESSAGE
            + "') = 0 "
            + "LIMIT 100000 SETTINGS max_block_size = 1000";
    final List<Long> received = new ArrayList<>();

    // when
    final Throwable thrown =
        catchThrowable(
            () ->
                Flux.from(connection().createStatement(sql).execute())
                    .flatMap(
                        result -> result.map((row, rowMetadata) -> row.get("number", Long.class)))
                    .doOnNext(received::add)
                    .blockLast(Duration.ofSeconds(30)));

    // then - never silently completes as if the result were whole
    assertThat(thrown).isNotNull();
    // and - a sizeable genuine prefix was preserved, not retroactively dropped, and delivered
    // intact
    // and in order — proven against a fixed-size prefix rather than the exact
    // (implementation-detail,
    // block-size-dependent) point where corruption starts; see the class Javadoc for why a real run
    // confirmed later elements in `received` can be corrupted garbage rather than genuine data, so
    // this deliberately does not assert anything about the full list, only this verified-safe
    // prefix
    assertThat(received).startsWith(LongStream.range(0, 1000).boxed().toArray(Long[]::new));
  }

  @Test
  void shouldSurfaceACleanServerExceptionWhenWaitEndOfQueryIsEnabled() {
    // given - a much smaller failure point than the streaming test above: wait_end_of_query=1
    // buffers the whole response server-side up to ClickHouse's own http_response_buffer_size
    // before sending anything, so this keeps the buffered payload trivially small regardless of
    // that limit's exact value.
    final ClickHouseQuery query =
        ClickHouseQuery.of(
                "SELECT number FROM system.numbers "
                    + "WHERE throwIf(number = 500, '"
                    + THROW_IF_MESSAGE
                    + "') = 0 "
                    + "LIMIT 10000 SETTINGS max_block_size = 100")
            .withSettings(Map.of("wait_end_of_query", "1"));

    // when
    final Throwable thrown =
        catchThrowable(
            () -> transport().query(query).aggregate().asByteArray().block(Duration.ofSeconds(30)));

    // then
    assertThat(thrown).isInstanceOf(ServerException.class);
    assertThat(thrown).hasMessageContaining(THROW_IF_MESSAGE);
  }
}

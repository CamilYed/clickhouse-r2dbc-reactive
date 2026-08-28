package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactory;
import io.github.camilyed.clickhouse.r2dbc.connector.ClickHouseConnectionFactoryProvider;
import io.github.camilyed.clickhouse.r2dbc.core.DriverObservationListener;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Statement;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Level 2 ("Public R2DBC SPI") comparison — docs/PERFORMANCE.md's "Comparison levels" section
 * designed this level ({@code ClickHouseConnection}/{@code ClickHouseStatement}/{@code
 * ClickHouseResult}, what an actual driver consumer calls, vs client-v2's {@link Client} API
 * directly) from the start, but no benchmark class ever actually exercised it — every other class
 * in this suite ({@link PointQueryBenchmark} included) calls {@link
 * io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport}/{@link
 * io.github.camilyed.clickhouse.r2dbc.core.rowbinary.RowBinaryDecoder} directly, bypassing the
 * {@code connector} module (and therefore {@code QueryObservation}/{@code ClickHouseResult})
 * entirely. This class is deliberately built through the public R2DBC SPI only ({@link
 * Connection}/{@link Statement}/{@link io.r2dbc.spi.Result}, obtained via {@link
 * ClickHouseConnectionFactory#from}, never a package-private {@code connector} class) — exactly
 * what an application using this driver actually calls.
 *
 * <p>Built specifically to measure the 2026-08-19 NOOP-observability-fast-path fix (see
 * docs/PERFORMANCE.md's "second-opinion review" section): {@link #thisDriverNoopObservation} and
 * {@link #thisDriverEnabledObservation} run the identical query over two connections that differ in
 * exactly one thing — whether {@code ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER} is
 * configured — so the delta between them isolates {@code QueryObservation}'s fingerprinting/
 * timestamping/counting cost from every other variable (network, decode, JIT warmup) instead of
 * comparing against a run from a different day. The enabled-side listener is an anonymous {@link
 * DriverObservationListener} that overrides nothing (every callback stays the inherited no-op
 * default) — deliberately, so the measured cost is {@code ActiveQueryObservation}'s own
 * construction work, not any real listener's logging/metrics overhead layered on top of it.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PublicApiPointQueryBenchmark {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  /** Same pool/seed shape as {@link PointQueryBenchmark} — see that class's Javadoc for why. */
  private static final int ID_POOL_SIZE = 1 << 16;

  private static final long ID_SEED = 42L;

  @Param({"10000"})
  public long rows;

  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  private Connection ourConnectionNoopObservation;
  private Connection ourConnectionEnabledObservation;
  private Client clientV2;

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and opens one long-lived {@link
   * Connection} per observation configuration — connections are reused across every
   * {@code @Benchmark} invocation, the same amortize-setup-once shape {@link PointQueryBenchmark}
   * uses for its own transport/client, so what's measured is query cost, not connection setup.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    ids = PointQueryTable.deterministicIds(rows, ID_POOL_SIZE, ID_SEED);

    ourConnectionNoopObservation = connect(connectionFactoryOptions(null));
    ourConnectionEnabledObservation =
        connect(connectionFactoryOptions(new DriverObservationListener() {}));

    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .build();
  }

  /** Closes both R2DBC connections and client-v2's connection pool at the end of the trial. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    Mono.from(ourConnectionNoopObservation.close()).block(Duration.ofSeconds(10));
    Mono.from(ourConnectionEnabledObservation.close()).block(Duration.ofSeconds(10));
    clientV2.close();
  }

  /**
   * This driver, through the public R2DBC SPI, with no {@code DriverObservationListener} configured
   * — {@code ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER} defaults to {@link
   * DriverObservationListener#NOOP}, so every query attempt takes the {@code
   * NoopQueryObservation}/{@code decodePlain} fast path added 2026-08-19.
   */
  @Benchmark
  public void thisDriverNoopObservation(final Blackhole blackhole) {
    runOurDriverQuery(ourConnectionNoopObservation, blackhole);
  }

  /**
   * Same query, same connection setup, the one difference being an enabled (but otherwise inert)
   * {@link DriverObservationListener} — see this class's own Javadoc for why an anonymous,
   * nothing-overridden listener is the right control here.
   */
  @Benchmark
  public void thisDriverEnabledObservation(final Blackhole blackhole) {
    runOurDriverQuery(ourConnectionEnabledObservation, blackhole);
  }

  /**
   * client-v2: {@link Client#query} + {@link ClickHouseBinaryFormatReader}, parameterized the same
   * way {@link PointQueryBenchmark#clientV2} is — see that method's Javadoc for the fairness
   * reasoning (server-side parameter substitution, not an inlined SQL literal).
   */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    final long id = nextId();
    try (QueryResponse response =
        clientV2.query(SELECT_BY_ID_SQL, Map.of("id", id)).get(10, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        blackhole.consume(reader.getString(1));
        blackhole.consume(reader.getBigDecimal(2));
      }
    }
  }

  private void runOurDriverQuery(final Connection connection, final Blackhole blackhole) {
    final long id = nextId();
    final Statement statement =
        connection.createStatement(SELECT_BY_ID_SQL).bind("id", Parameters.in(id));
    final BigDecimal amount =
        Flux.from(statement.execute())
            .flatMap(
                result ->
                    Flux.from(result.map((row, metadata) -> row.get("amount", BigDecimal.class))))
            .blockLast(Duration.ofSeconds(10));
    blackhole.consume(amount);
  }

  private Connection connect(final ConnectionFactoryOptions options) {
    final ConnectionFactory factory = ClickHouseConnectionFactory.from(options);
    return Mono.from(factory.create()).block(Duration.ofSeconds(10));
  }

  private ConnectionFactoryOptions connectionFactoryOptions(
      final @Nullable DriverObservationListener listener) {
    final ConnectionFactoryOptions.Builder builder =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, BenchmarkEnvironment.host())
            .option(ConnectionFactoryOptions.PORT, BenchmarkEnvironment.port())
            .option(ConnectionFactoryOptions.USER, BenchmarkEnvironment.username())
            .option(ConnectionFactoryOptions.PASSWORD, BenchmarkEnvironment.password());
    if (listener != null) {
      builder.option(ClickHouseConnectionFactoryProvider.OBSERVATION_LISTENER, listener);
    }
    return builder.build();
  }

  /** Same deterministic-cycling shape as {@link PointQueryBenchmark#nextId()}. */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

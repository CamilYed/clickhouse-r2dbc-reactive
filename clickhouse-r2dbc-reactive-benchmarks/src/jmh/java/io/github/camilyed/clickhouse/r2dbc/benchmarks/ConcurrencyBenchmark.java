package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import io.github.camilyed.clickhouse.r2dbc.core.ResponseCompression;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
import io.github.camilyed.clickhouse.r2dbc.core.RowDecodingScheduler;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Level 3, {@code @Threads(N)} shape only: N platform threads, each independently issuing the same
 * parameterized point lookup {@link PointQueryBenchmark} measures single-threaded, against a shared
 * {@link ClickHouseHttpTransport}/{@link Client} instance per JMH's own
 * {@code @State(Scope.Benchmark)} semantics (the same instances {@link PointQueryBenchmark} already
 * shares across its single calling thread — {@code @Threads(N)} here simply drives more of them
 * concurrently). Answers: with the same number of platform threads and each library's own default
 * connection handling left untouched, are the two drivers roughly comparable under concurrent load
 * — a same-resources comparison, per docs/PERFORMANCE.md's Phase 5 "Concurrency/burst" design.
 *
 * <p><b>Deliberately not the whole Level 3 design.</b> The second, arguably more interesting shape
 * described there — this driver's non-blocking pipeline serving many logical concurrent queries
 * over a deliberately <em>small</em> bounded connection pool via a custom {@code Flux.flatMap}
 * harness, the actual "~11 concurrent queries per user action" scenario that motivated this whole
 * project — is not implemented here. {@link ClickHouseHttpTransport} currently exposes no way to
 * bound its underlying Reactor Netty connection pool size (it uses the client's default, not a
 * small, explicitly-sized one); building that harness meaningfully needs either a {@code
 * maxConnections}- style constructor knob added to {@link ClickHouseHttpTransport} first, or
 * accepting Reactor Netty's default pool ceiling as the bound, which wouldn't demonstrate the "few
 * connections, many logical queries" property the scenario is actually about. Left as an explicit,
 * named follow-up rather than built on an arbitrary/unrepresentative pool size.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ConcurrencyBenchmark {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  private static final int ID_POOL_SIZE = 1 << 16;
  private static final long ID_SEED = 42L;

  /**
   * Row-count tier for {@link PointQueryTable} — kept fixed; concurrency is the axis under test.
   */
  @Param({"10000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private RowDecodingScheduler decodingScheduler;
  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and pre-generates the deterministic
   * id pool once per JMH trial — every worker thread shares the same {@link #idCursor}, so no two
   * threads read the same id concurrently, matching {@link PointQueryBenchmark}'s own approach.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    ids = PointQueryTable.deterministicIds(rows, ID_POOL_SIZE, ID_SEED);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(),
            BenchmarkEnvironment.username(),
            BenchmarkEnvironment.password());
    decodingScheduler = RowDecodingScheduler.defaults();
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .build();
  }

  /** Releases both clients' connection pools and this driver's decode scheduler. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
    decodingScheduler.dispose();
  }

  /**
   * This driver under {@code @Threads(8)}: each worker thread blocks on its own {@link
   * RowBinaryDecoder#decode} call — the real production decode path (off the Netty event loop, via
   * the shared {@link #decodingScheduler}), not the scheduler-free {@link
   * RowBinaryDecoder#decodeRows} test/benchmark shortcut. {@link ClickHouseHttpTransport} and its
   * underlying Reactor Netty client are safe to share and call concurrently from multiple threads
   * (no per-call mutable state; every {@link #thisDriver} invocation builds its own {@link Flux}
   * from scratch), and {@link RowDecodingScheduler} is likewise safe for concurrent use by design,
   * so no additional synchronization is introduced here beyond what {@link #nextId()} already
   * provides.
   */
  @Benchmark
  @Threads(8)
  public void thisDriver(final Blackhole blackhole) {
    final long id = nextId();
    final ClickHouseQuery query =
        ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    final DecodedRow row =
        RowBinaryDecoder.decode(body, decodingScheduler, ResponseCompression.NONE)
            .flatMapMany(DecodedResult::rows)
            .blockFirst(Duration.ofSeconds(10));
    blackhole.consume(row);
  }

  /**
   * client-v2 under {@code @Threads(8)}: each worker thread issues its own blocking {@link
   * Client#query} call against the shared {@link #clientV2} instance — client-v2's {@link Client}
   * is documented as thread-safe for concurrent {@code query} calls, backed by its own internal
   * connection pool sized by its defaults (not explicitly configured here, matching {@link
   * #thisDriver} leaving Reactor Netty's pool at its default too — a same-resources comparison, not
   * a pool-size-tuned one).
   */
  @Benchmark
  @Threads(8)
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

  /** Advances through the pre-generated {@link #ids} pool — thread-safe via {@link AtomicLong}. */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

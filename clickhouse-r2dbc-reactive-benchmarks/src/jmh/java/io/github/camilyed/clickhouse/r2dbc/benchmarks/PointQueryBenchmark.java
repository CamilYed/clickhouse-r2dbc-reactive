package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.core.RowBinaryDecoder;
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
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Level 1 ("raw transport + decode") comparison: this driver's {@link ClickHouseHttpTransport} +
 * {@link RowBinaryDecoder} vs client-v2's {@link Client} + {@link ClickHouseBinaryFormatReader},
 * running the same single-row parameterized lookup against {@link PointQueryTable}. Measures the
 * protocol/connection-overhead floor, deliberately without either driver's R2DBC-shape or
 * public-API translation layer in the way — see the "Public R2DBC SPI" level benchmarks (a
 * separate class) for that comparison.
 *
 * <p>First slice of the Phase 5 benchmark suite (see ROADMAP.md) — proves the module/dataset/
 * comparison-level design end to end before the remaining query shapes (full scan, wide decode,
 * aggregation, insert, concurrency burst) are built out the same way.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PointQueryBenchmark {

  private static final String SELECT_BY_ID_SQL =
      "SELECT label, amount FROM " + PointQueryTable.NAME + " WHERE id = {id:UInt64}";

  /**
   * How many distinct ids {@link PointQueryTable#deterministicIds} pre-generates — large enough
   * that a 10s measurement window cycles through it many times over without any single id being
   * disproportionately cache-hot, small enough to build in {@code @Setup} instantly. A power of
   * two so cycling through it is a cheap bitmask, not a division, in the benchmark hot path.
   */
  private static final int ID_POOL_SIZE = 1 << 16;

  private static final long ID_SEED = 42L;

  /** Row-count tier — see ROADMAP.md's Phase 5 "Dataset" table for what each tier is for. */
  @Param({"10000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;
  private long[] ids;
  private final AtomicLong idCursor = new AtomicLong();

  /**
   * Starts the shared container, seeds {@link PointQueryTable}, and pre-generates the deterministic
   * id pool ({@link PointQueryTable#deterministicIds}) once per JMH trial — see that method's
   * Javadoc for why ids are never generated inside a {@code @Benchmark} method.
   */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
    ids = PointQueryTable.deterministicIds(rows, ID_POOL_SIZE, ID_SEED);
    ourTransport =
        new ClickHouseHttpTransport(
            BenchmarkEnvironment.httpUrl(), BenchmarkEnvironment.username(),
            BenchmarkEnvironment.password());
    clientV2 =
        new Client.Builder()
            .addEndpoint(BenchmarkEnvironment.httpUrl())
            .setUsername(BenchmarkEnvironment.username())
            .setPassword(BenchmarkEnvironment.password())
            .setDefaultDatabase("default")
            .build();
  }

  /** Releases both clients' connection pools at the end of the trial. */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    clientV2.close();
  }

  /**
   * This driver: {@link ClickHouseHttpTransport#query} + {@link RowBinaryDecoder#decodeRows},
   * parameterized via ClickHouse's {@code {id:UInt64}} mechanism.
   *
   * <p>The one remaining known asymmetry with {@link #clientV2}, deliberately not forced into
   * false equivalence: this driver materializes each row into a {@code Map<String, Object>} (see
   * {@link RowBinaryDecoder}'s own Javadoc for why — a lifetime-safety tradeoff, not an accident),
   * while client-v2 reads typed values directly off its reader with no intermediate map. A future
   * transport-only benchmark (checksumming raw bytes, no decode at all) would isolate protocol cost
   * from this decode-shape difference — see ROADMAP.md's Phase 5 section.
   */
  @Benchmark
  public void ourDriver(final Blackhole blackhole) {
    final long id = nextId();
    final ClickHouseQuery query =
        ClickHouseQuery.of(SELECT_BY_ID_SQL).withParameters(Map.of("id", id));
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    final Map<String, Object> row =
        RowBinaryDecoder.decodeRows(body).blockFirst(Duration.ofSeconds(10));
    blackhole.consume(row);
  }

  /**
   * client-v2: {@link Client#query} + {@link ClickHouseBinaryFormatReader}, parameterized via the
   * same {@code {id:UInt64}} mechanism and the same {@code query_params} wire shape this driver
   * uses — confirmed by reading {@code Client#query(String, Map, QuerySettings)}'s own Javadoc in
   * the mounted client-v2 source, which documents the identical {@code {name:Type}}/{@code
   * param_<name>} contract. Fixed from an earlier version of this benchmark that inlined the id as
   * a SQL literal for client-v2 only — a real fairness gap, not a stylistic one, since a
   * literal-embedded query and a server-side-substituted one aren't guaranteed to cost ClickHouse
   * the same.
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

  /**
   * Advances through the pre-generated {@link #ids} pool — deterministic and, across separate JMH
   * forks running {@link #ourDriver} and {@link #clientV2} from the same {@link #ID_SEED}, the
   * identical sequence for both. No RNG call in the hot path.
   */
  private long nextId() {
    final int index = (int) (idCursor.getAndIncrement() & (ID_POOL_SIZE - 1));
    return ids[index];
  }
}

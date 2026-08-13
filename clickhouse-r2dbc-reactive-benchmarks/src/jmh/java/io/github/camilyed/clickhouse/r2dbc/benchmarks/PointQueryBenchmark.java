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

  /** Row-count tier — see ROADMAP.md's Phase 5 "Dataset" table for what each tier is for. */
  @Param({"10000"})
  public long rows;

  private ClickHouseHttpTransport ourTransport;
  private Client clientV2;

  /** Starts the shared container and seeds {@link PointQueryTable} once per JMH trial. */
  @Setup(Level.Trial)
  public void setUpTrial() {
    BenchmarkEnvironment.start();
    PointQueryTable.seed(rows);
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

  /** This driver: {@link ClickHouseHttpTransport#query} + {@link RowBinaryDecoder#decodeRows}. */
  @Benchmark
  public void ourDriver(final Blackhole blackhole) {
    final String sql =
        "SELECT label, amount FROM "
            + PointQueryTable.NAME
            + " WHERE id = {id:UInt64}";
    final ClickHouseQuery query =
        ClickHouseQuery.of(sql).withParameters(Map.of("id", PointQueryTable.randomId(rows)));
    final Flux<ByteBuffer> body = ourTransport.query(query).asByteArray().map(ByteBuffer::wrap);
    final Map<String, Object> row =
        RowBinaryDecoder.decodeRows(body).blockFirst(Duration.ofSeconds(10));
    blackhole.consume(row);
  }

  /**
   * client-v2: {@link Client#query} + {@link ClickHouseBinaryFormatReader}.
   *
   * <p>Uses an inlined literal rather than client-v2's own {@code {name:Type}} parameterized-query
   * support deliberately left out of this first slice — flagged here rather than silently glossed
   * over: revisit before treating this benchmark's numbers as final, since a literal-embedded
   * query and a server-side-substituted parameterized query aren't guaranteed to cost the same on
   * ClickHouse's side.
   */
  @Benchmark
  public void clientV2(final Blackhole blackhole) throws Exception {
    final String sql =
        "SELECT label, amount FROM "
            + PointQueryTable.NAME
            + " WHERE id = "
            + PointQueryTable.randomId(rows);
    try (QueryResponse response = clientV2.query(sql).get(10, TimeUnit.SECONDS)) {
      final ClickHouseBinaryFormatReader reader = clientV2.newBinaryFormatReader(response);
      while (reader.next() != null) {
        blackhole.consume(reader.getString(1));
        blackhole.consume(reader.getBigDecimal(2));
      }
    }
  }
}

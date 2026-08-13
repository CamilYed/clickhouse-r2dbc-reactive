package io.github.camilyed.clickhouse.r2dbc.benchmarks;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;

/**
 * A real ClickHouse instance for benchmark classes to share within one JVM fork, plus a plain
 * (driver-independent) admin channel for schema/dataset setup.
 *
 * <p>Deliberately not built on {@code testkit}'s {@code BaseClickHouseIntegrationTest} — that base
 * class wires container startup through JUnit's {@code @Testcontainers}/{@code @Container}
 * lifecycle, which has no equivalent in JMH's own {@code @Setup(Level.Trial)} lifecycle. The
 * container itself ({@link ClickHouseContainer}) is plain Testcontainers with no JUnit dependency,
 * so it's reused directly here; only the JUnit wiring around it isn't.
 *
 * <p><b>Shared per JVM fork, not per whole run — confirmed against a real run, not assumed.</b> JMH
 * forks a fresh JVM per {@code @Benchmark} method by default ({@code fork=1}, no {@code
 * @Fork(warmups=..., value=...)} override applied), so this class's {@code static} container field
 * is only shared across benchmark methods that happen to execute inside the same fork — in
 * practice, one container start per {@code @Benchmark} method, not one for the whole {@code jmh}
 * task invocation. Watched two separate {@code clickhouse/clickhouse-server} containers start on
 * two different ports during a single {@code jmh} run (one per benchmark method), each paying the
 * ~4s Testcontainers startup cost independently. Accepted as-is for now — forcing everything into
 * one fork would trade away JMH's normal fork-level isolation between benchmark methods, which
 * matters more than saving a few seconds of container startup. Worth revisiting only if the {@code
 * large} dataset tier's per-fork reseed cost turns out to dominate a real run's wall time.
 */
public final class BenchmarkEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkEnvironment.class);

  private static final ClickHouseContainer CLICK_HOUSE =
      new ClickHouseContainer("clickhouse/clickhouse-server:latest");

  private static final HttpClient ADMIN_HTTP_CLIENT = HttpClient.newHttpClient();

  private BenchmarkEnvironment() {}

  /** Starts the shared container if it isn't already running. Idempotent. */
  public static synchronized void start() {
    if (!CLICK_HOUSE.isRunning()) {
      LOG.info("Starting shared ClickHouse container for benchmarks...");
      CLICK_HOUSE.start();
      LOG.info("ClickHouse container ready at {}", CLICK_HOUSE.getHttpUrl());
    }
  }

  /** The running container's HTTP endpoint, e.g. {@code http://localhost:32821}. */
  public static String httpUrl() {
    return CLICK_HOUSE.getHttpUrl();
  }

  /** The username Testcontainers configured this container with. */
  public static String username() {
    return CLICK_HOUSE.getUsername();
  }

  /** The password Testcontainers configured this container with. */
  public static String password() {
    return CLICK_HOUSE.getPassword();
  }

  /**
   * Runs {@code sql} over a plain, synchronous {@link HttpClient} — never through this project's
   * own driver or client-v2 — so schema/dataset setup never depends on the very code being
   * benchmarked. Blocks the calling thread; only ever called from JMH {@code @Setup}/{@code
   * @TearDown} methods, never from inside a {@code @Benchmark} method itself.
   */
  public static void executeAdminSql(final String sql) {
    final String credentials = username() + ":" + password();
    final String basicAuth =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(httpUrl() + "/?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)))
            .header("Authorization", "Basic " + basicAuth)
            .timeout(Duration.ofMinutes(5))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    try {
      final HttpResponse<String> response =
          ADMIN_HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() >= 400) {
        throw new IllegalStateException(
            "Admin SQL failed (" + response.statusCode() + "): " + response.body());
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Admin SQL failed: " + sql, e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Admin SQL interrupted: " + sql, e);
    }
  }
}

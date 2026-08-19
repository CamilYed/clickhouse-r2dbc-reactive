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
 * forks a fresh JVM per {@code @Benchmark} method by default ({@code fork=1}, no
 * {@code @Fork(warmups=..., value=...)} override applied), so this class's {@code static} container
 * field is only shared across benchmark methods that happen to execute inside the same fork — in
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

  /**
   * Pinned, not {@code latest} — a performance baseline that silently tracks whatever ClickHouse
   * itself changes underneath it isn't reproducible. Confirmed via Docker Hub (2026-08-13) that
   * {@code latest} currently resolves to exactly this build (same image digest), so pinning here
   * doesn't change today's behavior, only future reproducibility. Bump deliberately, not silently,
   * when there's a reason to benchmark against a newer server.
   */
  private static final String CLICK_HOUSE_IMAGE = "clickhouse/clickhouse-server:26.7.3.19";

  private static final ClickHouseContainer CLICK_HOUSE = new ClickHouseContainer(CLICK_HOUSE_IMAGE);

  private static final HttpClient ADMIN_HTTP_CLIENT = HttpClient.newHttpClient();

  private BenchmarkEnvironment() {}

  /**
   * Starts the shared container if it isn't already running, then logs the environment a benchmark
   * result should be read against — ClickHouse server version (queried from the running container,
   * not assumed from the image tag), JDK, and OS/architecture. A benchmark number without this
   * context isn't reproducible; see the Phase 5 fairness requirements in ROADMAP.md. Idempotent.
   */
  public static synchronized void start() {
    if (!CLICK_HOUSE.isRunning()) {
      LOG.info("Starting shared ClickHouse container ({}) for benchmarks...", CLICK_HOUSE_IMAGE);
      CLICK_HOUSE.start();
      LOG.info("ClickHouse container ready at {}", CLICK_HOUSE.getHttpUrl());
      logEnvironmentMetadata();
    }
  }

  private static void logEnvironmentMetadata() {
    final String clickHouseVersion = queryAdminSql("SELECT version()").strip();
    final String jdkVersion = System.getProperty("java.version");
    final String osName = System.getProperty("os.name");
    final String osArch = System.getProperty("os.arch");
    LOG.info(
        "Benchmark environment: clickHouseImage={}, clickHouseVersion={}, jdk={}, os={}/{}",
        CLICK_HOUSE_IMAGE,
        clickHouseVersion,
        jdkVersion,
        osName,
        osArch);
  }

  /** The running container's HTTP endpoint, e.g. {@code http://localhost:32821}. */
  public static String httpUrl() {
    return CLICK_HOUSE.getHttpUrl();
  }

  /**
   * The running container's host, parsed from {@link #httpUrl()} — for benchmarks that build a
   * {@code ConnectionFactoryOptions} (host/port, not a single base-URL string) rather than
   * constructing a transport directly.
   */
  public static String host() {
    return URI.create(httpUrl()).getHost();
  }

  /** The running container's mapped port, parsed from {@link #httpUrl()} — see {@link #host()}. */
  public static int port() {
    return URI.create(httpUrl()).getPort();
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
   * benchmarked. Discards the response body; use {@link #queryAdminSql} when the result is needed.
   * Blocks the calling thread; only ever called from JMH {@code @Setup}/{@code @TearDown} methods,
   * never from inside a {@code @Benchmark} method itself.
   */
  public static void executeAdminSql(final String sql) {
    queryAdminSql(sql);
  }

  /** Like {@link #executeAdminSql}, but returns the response body. */
  public static String queryAdminSql(final String sql) {
    final String credentials = username() + ":" + password();
    final String basicAuth =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(httpUrl() + "/?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)))
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
      return response.body();
    } catch (final IOException e) {
      throw new IllegalStateException("Admin SQL failed: " + sql, e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Admin SQL interrupted: " + sql, e);
    }
  }
}

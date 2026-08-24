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
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.images.builder.Transferable;

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
 *
 * <h2>External server mode</h2>
 *
 * Set {@code BENCH_CLICKHOUSE_URL} (plus optionally {@code BENCH_CLICKHOUSE_USER}/{@code
 * BENCH_CLICKHOUSE_PASSWORD}) to point every JMH fork at one already-running, pinned ClickHouse
 * server instead of each fork starting its own Testcontainers container - see {@code
 * CLAUDE_REPRESENTATIVE_BENCHMARK_PLAN.md} section 2.5: comparing {@code thisDriver} against one
 * server process and {@code clientV2} against a different one (even same image, same dataset) lets
 * server startup state, page cache, and Docker/CPU scheduling differ between the two methods being
 * compared - noise a "trusted" headline benchmark shouldn't carry. {@code
 * scripts/start-benchmark-clickhouse.sh} starts that one pinned server. Testcontainers remains the
 * default (no environment variables set) for ordinary local development, where per-fork isolation
 * matters more than this.
 */
public final class BenchmarkEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkEnvironment.class);

  private static final @Nullable String EXTERNAL_URL = System.getenv("BENCH_CLICKHOUSE_URL");
  private static final @Nullable String EXTERNAL_USER = System.getenv("BENCH_CLICKHOUSE_USER");
  private static final @Nullable String EXTERNAL_PASSWORD =
      System.getenv("BENCH_CLICKHOUSE_PASSWORD");

  private static boolean externalMetadataLogged;

  /**
   * Pinned, not {@code latest} — a performance baseline that silently tracks whatever ClickHouse
   * itself changes underneath it isn't reproducible. Confirmed via Docker Hub (2026-08-13) that
   * {@code latest} currently resolves to exactly this build (same image digest), so pinning here
   * doesn't change today's behavior, only future reproducibility. Bump deliberately, not silently,
   * when there's a reason to benchmark against a newer server.
   */
  private static final String CLICK_HOUSE_IMAGE = "clickhouse/clickhouse-server:26.7.3.19";

  /**
   * Explicit Docker memory limit for the shared container. Bounded by whatever Docker Desktop
   * itself has been given under Settings → Resources → Memory (~7837 MB observed 2026-08-20) — if
   * that total is below what a benchmark needs, this constant alone can't manufacture more RAM;
   * raise Docker Desktop's own allocation first.
   */
  private static final long CONTAINER_MEMORY_BYTES = 7_000L * 1024 * 1024;

  /**
   * Explicit {@code max_server_memory_usage} override, injected as a config file rather than left
   * to ClickHouse's own auto-detection. A first attempt only raised {@link #CONTAINER_MEMORY_BYTES}
   * (the Docker cgroup limit) and stopped there — the server-reported ceiling barely moved (~5.53
   * GiB, then ~5.18 GiB on the next run) even after the cgroup limit was raised to ~6.84 GiB,
   * meaning this image's auto-detection isn't reliably tracking the cgroup limit under Docker
   * Desktop's Linux VM. Setting the server setting directly removes that guesswork. Left with
   * headroom below {@link #CONTAINER_MEMORY_BYTES} for OS/page-cache/thread memory ClickHouse's own
   * tracker doesn't fully account for.
   */
  private static final long SERVER_MEMORY_USAGE_BYTES = 6_000_000_000L;

  private static final String MEMORY_CONFIG_XML =
      "<clickhouse><max_server_memory_usage>"
          + SERVER_MEMORY_USAGE_BYTES
          + "</max_server_memory_usage></clickhouse>";

  private static final int POOL_SIZE = 8;

  private static final ClickHouseContainer CLICK_HOUSE =
      new ClickHouseContainer(CLICK_HOUSE_IMAGE)
          .withCreateContainerCmdModifier(
              cmd ->
                  cmd.getHostConfig()
                      // memorySwap == memory disables swap entirely - a benchmark run that starts
                      // swapping produces meaningless timing, so fail with a clear OOM instead of a
                      // silently-thrashing container.
                      .withMemory(CONTAINER_MEMORY_BYTES)
                      .withMemorySwap(CONTAINER_MEMORY_BYTES))
          .withCopyToContainer(
              Transferable.of(MEMORY_CONFIG_XML),
              "/etc/clickhouse-server/config.d/benchmark-memory-limit.xml");

  private static final HttpClient ADMIN_HTTP_CLIENT = HttpClient.newHttpClient();

  private BenchmarkEnvironment() {}

  /**
   * In external-server mode ({@link #EXTERNAL_URL} set), verifies the external server is reachable
   * and logs environment metadata once - no container is started. Otherwise starts the shared
   * Testcontainers container if it isn't already running, then logs the same metadata. Either way,
   * a benchmark result should always be read against this logged environment — ClickHouse server
   * version (queried from the running server, not assumed from an image tag), JDK, and
   * OS/architecture. A benchmark number without this context isn't reproducible; see the Phase 5
   * fairness requirements in ROADMAP.md. Idempotent.
   */
  public static synchronized void start() {
    if (EXTERNAL_URL != null) {
      if (!externalMetadataLogged) {
        LOG.info(
            "Using external benchmark ClickHouse server at {} (BENCH_CLICKHOUSE_URL set)",
            EXTERNAL_URL);
        logEnvironmentMetadata();
        externalMetadataLogged = true;
      }
      return;
    }
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

  /**
   * The running server's HTTP endpoint, e.g. {@code http://localhost:32821} — either the external
   * server from {@code BENCH_CLICKHOUSE_URL} if set, or the shared Testcontainers container.
   */
  public static String httpUrl() {
    return EXTERNAL_URL != null ? EXTERNAL_URL : CLICK_HOUSE.getHttpUrl();
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

  /**
   * The username to authenticate with — {@code BENCH_CLICKHOUSE_USER} (defaulting to ClickHouse's
   * own out-of-the-box {@code default} user if that variable is unset but {@code
   * BENCH_CLICKHOUSE_URL} is) in external-server mode, otherwise whatever Testcontainers configured
   * the shared container with.
   */
  public static String username() {
    return EXTERNAL_URL != null
        ? Objects.requireNonNullElse(EXTERNAL_USER, "default")
        : CLICK_HOUSE.getUsername();
  }

  /**
   * The password to authenticate with — see {@link #username()} for the same external-vs-container
   * split. Defaults to empty, matching a ClickHouse server's out-of-the-box {@code default} user.
   */
  public static String password() {
    return EXTERNAL_URL != null
        ? Objects.requireNonNullElse(EXTERNAL_PASSWORD, "")
        : CLICK_HOUSE.getPassword();
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

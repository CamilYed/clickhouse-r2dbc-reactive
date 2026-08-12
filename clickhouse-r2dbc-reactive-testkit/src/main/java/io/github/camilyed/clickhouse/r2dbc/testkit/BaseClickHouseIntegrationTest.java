package io.github.camilyed.clickhouse.r2dbc.testkit;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for tests against a real ClickHouse server.
 *
 * <p>Uses Testcontainers' singleton-container pattern: the {@link ClickHouseContainer} is a single
 * {@code static} field declared here, so every test class that extends this one shares the same
 * running container instead of each starting its own — one container for the whole test JVM.
 * Container start is idempotent ({@code start()} no-ops if already started), so this is safe even
 * though every subclass's {@code @Testcontainers} extension instance references the same field.
 * There is no explicit {@code stop()} call: Testcontainers' Ryuk sidecar reaps the container when
 * the JVM exits — the standard pattern for a container meant to outlive any single test class.
 *
 * <p>{@link #dropAllTables()} runs before every test and drops every table in the default database,
 * so each test starts from a clean, known state — the same isolation rule CLAUDE.md requires of
 * in-memory fakes, applied here to a real database. It talks to ClickHouse over a plain,
 * synchronous {@link HttpClient}, deliberately not through this project's own reactive transport,
 * so cleanup never depends on — or silently masks a bug in — the very driver code these tests exist
 * to verify.
 */
@Testcontainers
public abstract class BaseClickHouseIntegrationTest {

  /** Constructor for subclasses; there is no state to initialize beyond the shared static container. */
  protected BaseClickHouseIntegrationTest() {}

  @Container
  private static final ClickHouseContainer CLICK_HOUSE =
      new ClickHouseContainer("clickhouse/clickhouse-server:latest");

  private static final HttpClient ADMIN_HTTP_CLIENT = HttpClient.newHttpClient();

  /** The running container's HTTP endpoint, e.g. {@code http://localhost:32821}. */
  protected static String clickHouseHttpUrl() {
    return CLICK_HOUSE.getHttpUrl();
  }

  /** The username Testcontainers configured this container with. */
  protected static String clickHouseUsername() {
    return CLICK_HOUSE.getUsername();
  }

  /** The password Testcontainers configured this container with. */
  protected static String clickHousePassword() {
    return CLICK_HOUSE.getPassword();
  }

  /** Drops every table in the default database. Runs automatically before each test. */
  @BeforeEach
  protected void dropAllTables() {
    queryColumn("SHOW TABLES")
        .forEach(table -> executeAdminSql("DROP TABLE IF EXISTS `" + table + "`"));
  }

  private static List<String> queryColumn(final String sql) {
    return sendAdminRequest(sql).lines().filter(line -> !line.isBlank()).toList();
  }

  private static void executeAdminSql(final String sql) {
    sendAdminRequest(sql);
  }

  private static String sendAdminRequest(final String sql) {
    final String credentials = clickHouseUsername() + ":" + clickHousePassword();
    final String basicAuth =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    clickHouseHttpUrl()
                        + "/?query="
                        + URLEncoder.encode(sql, StandardCharsets.UTF_8)))
            .header("Authorization", "Basic " + basicAuth)
            .timeout(Duration.ofSeconds(10))
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

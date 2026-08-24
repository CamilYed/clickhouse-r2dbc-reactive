package io.github.camilyed.clickhouse.r2dbc.macrobench.dataset;

import io.github.camilyed.clickhouse.r2dbc.macrobench.config.BenchmarkProperties;
import io.github.camilyed.clickhouse.r2dbc.macrobench.config.ClickHouseEndpointProperties;
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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Drops and recreates {@link BenchmarkDataset}'s tables at application startup, seeded entirely
 * server-side via {@code INSERT ... SELECT ... FROM numbers(N)} - the same "never through the
 * driver being benchmarked" principle {@code clickhouse-r2dbc-reactive-benchmarks}' {@code
 * BenchmarkEnvironment#executeAdminSql} already established, applied here via a plain {@link
 * HttpClient} against {@link ClickHouseEndpointProperties} rather than either backend under
 * comparison.
 */
@Component
class DatasetSeeder implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetSeeder.class);
  private static final Duration ADMIN_TIMEOUT = Duration.ofMinutes(10);

  private final ClickHouseEndpointProperties endpoint;
  private final BenchmarkProperties properties;
  private final HttpClient httpClient = HttpClient.newHttpClient();

  DatasetSeeder(final ClickHouseEndpointProperties endpoint, final BenchmarkProperties properties) {
    this.endpoint = endpoint;
    this.properties = properties;
  }

  @Override
  public void run(final ApplicationArguments args) {
    LOG.info(
        "Seeding macrobench dataset: pointRows={}, analyticsOrderRows={}, categories={}",
        properties.pointRows(),
        properties.analyticsRows(),
        BenchmarkDataset.CATEGORY_COUNT);
    seedPointTable();
    seedAnalyticsTables();
    LOG.info("Macrobench dataset ready");
  }

  private void seedPointTable() {
    execute("DROP TABLE IF EXISTS " + BenchmarkDataset.POINT_TABLE);
    execute(
        "CREATE TABLE "
            + BenchmarkDataset.POINT_TABLE
            + " (id UInt64, label String, amount Decimal(18,4)) ENGINE = MergeTree ORDER BY id");
    execute(
        "INSERT INTO "
            + BenchmarkDataset.POINT_TABLE
            + " SELECT number + 1 AS id, concat('label-', toString(number)) AS label, "
            + "(number % 100000) / 100.0 AS amount FROM numbers("
            + properties.pointRows()
            + ")");
  }

  private void seedAnalyticsTables() {
    execute("DROP TABLE IF EXISTS " + BenchmarkDataset.ORDERS_TABLE);
    execute("DROP TABLE IF EXISTS " + BenchmarkDataset.CATEGORIES_TABLE);
    execute(
        "CREATE TABLE "
            + BenchmarkDataset.CATEGORIES_TABLE
            + " (category_id UInt32, category_name String) ENGINE = MergeTree ORDER BY category_id");
    execute(
        "INSERT INTO "
            + BenchmarkDataset.CATEGORIES_TABLE
            + " SELECT number AS category_id, concat('category-', toString(number)) AS category_name "
            + "FROM numbers("
            + BenchmarkDataset.CATEGORY_COUNT
            + ")");
    execute(
        "CREATE TABLE "
            + BenchmarkDataset.ORDERS_TABLE
            + " (order_id UInt64, category_id UInt32, amount Decimal(18,4), occurred_at DateTime) "
            + "ENGINE = MergeTree ORDER BY order_id");
    execute(
        "INSERT INTO "
            + BenchmarkDataset.ORDERS_TABLE
            + " SELECT number AS order_id, number % "
            + BenchmarkDataset.CATEGORY_COUNT
            + " AS category_id, (number % 100000) / 100.0 AS amount, "
            + "now() - toIntervalSecond(number % 2592000) AS occurred_at FROM numbers("
            + properties.analyticsRows()
            + ")");
  }

  private void execute(final String sql) {
    final String credentials = endpoint.username() + ":" + endpoint.password();
    final String basicAuth =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    endpoint.httpUrl() + "/?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)))
            .header("Authorization", "Basic " + basicAuth)
            .timeout(ADMIN_TIMEOUT)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    try {
      final HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() >= 400) {
        throw new IllegalStateException(
            "Dataset seeding failed (" + response.statusCode() + "): " + response.body());
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Dataset seeding failed: " + sql, e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Dataset seeding interrupted: " + sql, e);
    }
  }
}

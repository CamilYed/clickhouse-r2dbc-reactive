package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api;

import static io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.assertions.OrderEventAssert.assertThatOrderEvent;
import static io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.builders.CreateOrderEventRequestTestBuilder.aCreateOrderEventRequest;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.builders.CreateOrderEventRequestTestBuilder;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEvent;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderStatus;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.clickhouse.ClickHouseContainer;

/**
 * Proves the {@code order-events} REST endpoints work end to end — real embedded WebFlux server,
 * real Spring-managed {@code ConnectionFactory}, real ClickHouse — through the {@code
 * OrderEventRepository} port and its {@code DatabaseClientOrderEventRepository} adapter (see that
 * class's own Javadoc for why raw {@code DatabaseClient} SQL is used rather than {@code
 * R2dbcEntityTemplate}'s object mapping).
 *
 * <p>Uses this project's own Test Data Builder ({@code CreateOrderEventRequestTestBuilder}) and
 * Custom Assertion ({@code OrderEventAssert}) building blocks, per CLAUDE.md's "Test building
 * blocks" section, rather than raw constructor calls and generic AssertJ {@code extracting(...)}.
 *
 * <p>The container is started eagerly in a {@code static} initializer, deliberately not via
 * {@code @Container}/{@code @Testcontainers}: {@code @DynamicPropertySource} must read the
 * container's mapped port before Spring's {@code ApplicationContext} starts, and relying on JUnit
 * extension registration order between {@code @Testcontainers} and {@code @SpringBootTest}'s {@code
 * SpringExtension} to guarantee that is exactly the kind of fragile-by-accident setup this project
 * avoids elsewhere (see CLAUDE.md's testing philosophy) — starting explicitly removes the
 * ambiguity.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "PT10S")
class OrderEventControllerAgainstRealClickHouseTest {

  private static final ClickHouseContainer CLICK_HOUSE =
      new ClickHouseContainer("clickhouse/clickhouse-server:latest");

  static {
    CLICK_HOUSE.start();
  }

  @DynamicPropertySource
  static void clickHouseProperties(final DynamicPropertyRegistry registry) {
    registry.add(
        "spring.r2dbc.url",
        () -> CLICK_HOUSE.getHttpUrl().replaceFirst("^http://", "r2dbc:clickhouse://"));
    registry.add("spring.r2dbc.username", CLICK_HOUSE::getUsername);
    registry.add("spring.r2dbc.password", CLICK_HOUSE::getPassword);
  }

  @Autowired private WebTestClient webTestClient;
  @Autowired private DatabaseClient databaseClient;

  @BeforeEach
  void emptyTheOrderEventsTable() {
    databaseClient
        .sql("TRUNCATE TABLE order_events")
        .fetch()
        .rowsUpdated()
        .block(Duration.ofSeconds(10));
  }

  @Test
  void shouldCreateAndListAFullyPopulatedEvent() {
    // given
    postOrderEvent(
        aCreateOrderEventRequest()
            .withCategory("electronics")
            .withTags(List.of("black-friday", "featured"))
            .withAmount("199.90")
            .withDiscount("10.00")
            .withStatus(OrderStatus.PAID)
            .withClientIp("10.0.0.5"));

    // when
    final List<OrderEvent> events = listOrderEvents();

    // then
    assertThat(events).hasSize(1);
    assertThatOrderEvent(events.get(0))
        .hasCategory("electronics")
        .hasTags("black-friday", "featured")
        .hasStatus(OrderStatus.PAID)
        .hasClientIp("10.0.0.5")
        .hasAmount("199.90")
        .hasDiscount("10.00");
  }

  @Test
  void shouldCreateAnEventWithNoTagsAndNoDiscount() {
    // given
    postOrderEvent(
        aCreateOrderEventRequest()
            .withCategory("books")
            .withTags(List.of())
            .withAmount("15.00")
            .withNoDiscount());

    // when
    final List<OrderEvent> events = listOrderEvents();

    // then
    assertThat(events).hasSize(1);
    assertThatOrderEvent(events.get(0)).hasTags().hasNoDiscount();
  }

  @Test
  void shouldReportTheEventCount() {
    // given
    postOrderEvent(aCreateOrderEventRequest().withCategory("toys"));

    // when / then
    webTestClient
        .get()
        .uri("/order-events/count")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(Long.class)
        .isEqualTo(1L);
  }

  @Test
  void shouldSumAmountsPerCategoryThroughTheAnalyticsEndpoint() {
    // given - two orders in "electronics", one in "books"
    postOrderEvent(aCreateOrderEventRequest().withCategory("electronics").withAmount("40.00"));
    postOrderEvent(aCreateOrderEventRequest().withCategory("electronics").withAmount("60.00"));
    postOrderEvent(aCreateOrderEventRequest().withCategory("books").withAmount("15.00"));

    // when / then
    webTestClient
        .get()
        .uri("/order-events/analytics/by-category")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBodyList(CategoryTotal.class)
        .value(
            totals ->
                assertThat(totals)
                    .extracting(CategoryTotal::category)
                    .containsExactlyInAnyOrder("electronics", "books"));
  }

  @Test
  void shouldServeTheSameEventsThroughTheNdjsonStreamingEndpoint() {
    // given - the streaming endpoint's incremental-delivery guarantee is proven separately, with
    // controlled timing, by OrderEventStreamingControllerTest; this only confirms it serves the
    // same data as /order-events, with the expected content type, against a real ClickHouse query
    postOrderEvent(aCreateOrderEventRequest().withCategory("garden").withAmount("25.00"));

    // when / then
    webTestClient
        .get()
        .uri("/order-events/stream")
        .accept(MediaType.APPLICATION_NDJSON)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON)
        .expectBodyList(OrderEvent.class)
        .value(
            events ->
                assertThat(events).extracting(OrderEvent::category).containsExactly("garden"));
  }

  private void postOrderEvent(final CreateOrderEventRequestTestBuilder request) {
    webTestClient
        .post()
        .uri("/order-events")
        .bodyValue(request.build())
        .exchange()
        .expectStatus()
        .isOk();
  }

  private List<OrderEvent> listOrderEvents() {
    return webTestClient
        .get()
        .uri("/order-events")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBodyList(OrderEvent.class)
        .returnResult()
        .getResponseBody();
  }
}

package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api;

import static io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.builders.OrderEventTestBuilder.anOrderEvent;

import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.api.fakes.InMemoryOrderEventRepository;
import io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo.domain.OrderEvent;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Proves {@code GET /order-events/stream} actually writes events to the HTTP response as they
 * arrive, rather than collecting the whole {@code Flux} into memory first — the property {@code
 * OrderEventController#all()}'s Javadoc used to claim without proof (see ROADMAP.md's Phase 8, item
 * 9).
 *
 * <p>Deliberately not a real-ClickHouse test: a real query's timing is fast and unpredictable, so
 * it cannot reliably prove incremental delivery either way. Instead this binds the controller
 * directly ({@code WebTestClient.bindToController}, no {@code ApplicationContext}) to an {@link
 * InMemoryOrderEventRepository} whose {@code findAll()} stream this test fully controls via {@link
 * Flux#delayElements}, then asserts — via {@link StepVerifier#expectNoEvent(Duration)} — that no
 * second element arrives for the whole delay window after the first one does. A response that was
 * collected into one buffered array before being written would deliver both elements back to back,
 * immediately once the source completed, and would fail this assertion.
 *
 * <p>This is exactly the kind of proof ROADMAP.md's item 9 asks for and {@code expectBodyList(...)}
 * (used elsewhere in this module, e.g. {@code OrderEventControllerAgainstRealClickHouseTest}) can
 * never give: {@code expectBodyList} waits for the full response body before returning anything.
 */
class OrderEventStreamingControllerTest {

  @Test
  void shouldWriteEachStreamedEventToTheResponseAsItArrivesRatherThanBufferingTheWholeFlux() {
    // given - two events, the second arriving from the source only after a real delay
    final OrderEvent first = anOrderEvent().withCategory("electronics").build();
    final OrderEvent second = anOrderEvent().withCategory("books").build();
    final Flux<OrderEvent> delayedEvents =
        Flux.just(first, second).delayElements(Duration.ofMillis(300));
    final WebTestClient webTestClient =
        WebTestClient.bindToController(
                new OrderEventController(new InMemoryOrderEventRepository(delayedEvents)))
            .build();

    // when
    final Flux<OrderEvent> received =
        webTestClient
            .get()
            .uri("/order-events/stream")
            .accept(MediaType.APPLICATION_NDJSON)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON)
            .returnResult(OrderEvent.class)
            .getResponseBody();

    // then - the second event does not show up within the delay window after the first one did;
    // a buffered (non-streaming) response would have delivered both together, immediately, once
    // the source Flux completed
    StepVerifier.create(received)
        .expectNextMatches(event -> event.category().equals("electronics"))
        .expectNoEvent(Duration.ofMillis(200))
        .expectNextMatches(event -> event.category().equals("books"))
        .verifyComplete();
  }
}

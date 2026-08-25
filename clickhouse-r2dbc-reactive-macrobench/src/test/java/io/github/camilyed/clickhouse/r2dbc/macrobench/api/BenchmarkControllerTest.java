package io.github.camilyed.clickhouse.r2dbc.macrobench.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.camilyed.clickhouse.r2dbc.macrobench.api.fakes.InMemoryBenchmarkQueryBackend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.PointRow;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

/**
 * Plain unit test, no Spring context - {@link BenchmarkController} is a package-private class with
 * a simple {@code List<BenchmarkQueryBackend>} constructor, so it can be instantiated and driven
 * directly against {@link InMemoryBenchmarkQueryBackend} fakes. Backend routing/validation happens
 * synchronously in each handler method (before a {@code Mono}/{@code Flux} is even built), so an
 * inactive or unknown backend surfaces as a synchronous {@link ResponseStatusException} from the
 * method call itself - not as an error signal on the returned publisher - which is what {@link
 * #shouldRejectABackendThatIsNotActiveOnThisRun()} and {@link #shouldRejectAnUnknownBackendName()}
 * assert against.
 */
class BenchmarkControllerTest {

  @Test
  void shouldRouteAPointRequestToTheMatchingBackend() {
    // given
    final BenchmarkController controller =
        new BenchmarkController(List.of(new InMemoryBenchmarkQueryBackend(Backend.R2DBC)));

    // when
    final PointRow row = controller.point("r2dbc", 42L).block();

    // then
    assertThat(row.id()).isEqualTo(42L);
  }

  @Test
  void shouldRejectABackendThatIsNotActiveOnThisRun() {
    // given
    final BenchmarkController controller =
        new BenchmarkController(List.of(new InMemoryBenchmarkQueryBackend(Backend.R2DBC)));

    // when / then
    assertThatThrownBy(() -> controller.point("client-v2", 1L))
        .isInstanceOf(ResponseStatusException.class);
  }

  @Test
  void shouldRejectAnUnknownBackendName() {
    // given
    final BenchmarkController controller =
        new BenchmarkController(List.of(new InMemoryBenchmarkQueryBackend(Backend.R2DBC)));

    // when / then
    assertThatThrownBy(() -> controller.point("postgres", 1L))
        .isInstanceOf(ResponseStatusException.class);
  }
}

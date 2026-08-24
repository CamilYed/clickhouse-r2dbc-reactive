package io.github.camilyed.clickhouse.r2dbc.macrobench.api;

import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.BenchmarkQueryBackend;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.CategoryTotal;
import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.PointRow;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The identical REST endpoint contract ROADMAP.md's Phase 12 goal describes (<code>load generator
 * -&gt; Spring Boot WebFlux -&gt; same endpoint contract -&gt; this driver or client-v2 -&gt; same
 * ClickHouse instance -&gt; same SQL/data/response DTO</code>), routed by a {@code {backend}} path
 * segment ({@code r2dbc} or {@code client-v2}) to whichever {@link BenchmarkQueryBackend}
 * implementations are active for this run - see {@code
 * io.github.camilyed.clickhouse.r2dbc.macrobench.config.ConditionalOnBackendEnabled}. A backend
 * not active for this run (isolated single-backend mode) returns {@code 404}, not a silent
 * fallback to whichever backend happens to be running.
 */
@RestController
@RequestMapping("/benchmark/{backend}")
class BenchmarkController {

  private static final long DEFAULT_STREAM_LIMIT = 10_000;

  private final Map<Backend, BenchmarkQueryBackend> backendsByKind;

  BenchmarkController(final List<BenchmarkQueryBackend> backends) {
    this.backendsByKind =
        backends.stream().collect(Collectors.toMap(BenchmarkQueryBackend::kind, Function.identity()));
  }

  /** Point-lookup scenario: one row by primary key. */
  @GetMapping("/point/{id}")
  Mono<PointRow> point(@PathVariable final String backend, @PathVariable final long id) {
    return backendFor(backend).point(id);
  }

  /** Analytics scenario: a real {@code JOIN}/{@code GROUP BY}/aggregation query. */
  @GetMapping("/analytics")
  Mono<List<CategoryTotal>> analytics(@PathVariable final String backend) {
    return backendFor(backend).analytics();
  }

  /** Stream scenario: up to {@code limit} rows as newline-delimited JSON. */
  @GetMapping(value = "/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
  Flux<PointRow> stream(
      @PathVariable final String backend,
      @RequestParam(defaultValue = "" + DEFAULT_STREAM_LIMIT) final long limit) {
    return backendFor(backend).stream(limit);
  }

  private BenchmarkQueryBackend backendFor(final String pathValue) {
    final Backend requested = parseBackend(pathValue);
    final BenchmarkQueryBackend backend = backendsByKind.get(requested);
    if (backend == null) {
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND,
          "Backend '"
              + pathValue
              + "' is not active on this run (started with benchmark.backend not covering it)");
    }
    return backend;
  }

  private Backend parseBackend(final String pathValue) {
    try {
      return Backend.fromProperty(pathValue);
    } catch (final IllegalArgumentException e) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Unknown backend '" + pathValue + "'", e);
    }
  }
}

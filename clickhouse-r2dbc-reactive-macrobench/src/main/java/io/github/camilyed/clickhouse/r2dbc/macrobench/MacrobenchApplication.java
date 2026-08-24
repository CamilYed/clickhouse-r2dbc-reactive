package io.github.camilyed.clickhouse.r2dbc.macrobench;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Entry point for the Phase 12 macrobenchmark: a real Spring Boot WebFlux request path (load
 * generator -&gt; this application -&gt; this driver or client-v2 -&gt; the same ClickHouse
 * instance) for end-to-end comparison, complementing (not replacing) the JMH suite in {@code
 * clickhouse-r2dbc-reactive-benchmarks}. See ROADMAP.md's Phase 12 section for the full design, the
 * dual-vs-isolated backend modes, and the fairness config both {@link
 * io.github.camilyed.clickhouse.r2dbc.macrobench.backend.BenchmarkQueryBackend} implementations
 * share.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class MacrobenchApplication {

  public static void main(final String[] args) {
    SpringApplication.run(MacrobenchApplication.class, args);
  }
}

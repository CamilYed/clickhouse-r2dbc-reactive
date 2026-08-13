package io.github.camilyed.clickhouse.r2dbc.examples.webfluxdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the {@code clickhouse-r2dbc-reactive} + Spring WebFlux interop demo (ROADMAP.md
 * Phase 6).
 *
 * <p>This application never references a single class from {@code clickhouse-r2dbc-reactive-*}
 * directly — the driver is only on the runtime classpath (see this module's {@code
 * build.gradle.kts}: {@code runtimeOnly(project(":clickhouse-r2dbc-reactive-connector"))}). Spring
 * discovers it purely through {@code spring.r2dbc.url}'s {@code r2dbc:clickhouse://...} scheme,
 * resolved by the standard R2DBC {@code ConnectionFactoryProvider} {@link java.util.ServiceLoader}
 * mechanism — the same path every other R2DBC driver (Postgres, MySQL, ...) is discovered through.
 * That is the entire point of this module: proving the driver behaves as a well-mannered R2DBC
 * citizen under a real Spring Boot application, not exercising any driver-specific API.
 *
 * <p>Everything else lives in {@code domain} (the {@code OrderEvent} model and the {@code
 * OrderEventRepository} port), {@code infrastructure} ({@code DatabaseClientOrderEventRepository},
 * {@code OrderEventsSchemaInitializer}, and {@code R2dbcConfiguration}'s beans), and {@code api}
 * ({@code OrderEventController}) — the same Ports & Adapters split the driver itself uses between
 * {@code core} and its transport/connector adapters.
 */
@SpringBootApplication
public class DemoApplication {

  public static void main(final String[] args) {
    SpringApplication.run(DemoApplication.class, args);
  }
}

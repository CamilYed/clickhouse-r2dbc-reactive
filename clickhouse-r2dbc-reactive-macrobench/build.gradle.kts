plugins {
    `java-library`
    id("org.springframework.boot") version "4.1.0"
}

description =
    "Runnable Spring Boot + WebFlux macrobenchmark target (Phase 12): a real HTTP request path " +
        "(load generator -> WebFlux -> this driver or client-v2 -> same ClickHouse instance) " +
        "for end-to-end comparison, complementing the JMH suite in clickhouse-r2dbc-reactive-benchmarks " +
        "rather than replacing it. Deliberately depends on this repo's own connector source " +
        "(project(\":clickhouse-r2dbc-reactive-connector\")), not the published release, unlike " +
        "examples/spring-boot-webflux-demo - see ROADMAP.md's Phase 12 section for why."

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.0"))

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    // Same reasoning as examples/spring-boot-webflux-demo's build.gradle.kts: spring-boot-starter-r2dbc,
    // not spring-boot-starter-data-r2dbc - this module never uses R2dbcEntityTemplate/Spring Data
    // repositories either, and the latter's DataR2dbcAutoConfiguration eagerly resolves a Dialect
    // this driver has no entry for. See R2dbcConfiguration's own Javadoc in the demo module for the
    // full story.
    implementation("org.springframework.boot:spring-boot-starter-r2dbc")

    // Deliberately project(...), not the published Maven Central coordinate the demo module uses:
    // this module benchmarks the driver as it exists on this branch right now, not the last
    // release - see ROADMAP.md's Phase 12 "Shape of the module".
    implementation(project(":clickhouse-r2dbc-reactive-connector"))

    // The comparison baseline this module's ClientV2BenchmarkQueryBackend is built against -
    // same client-v2 version the rest of this repo is pinned to (gradle/libs.versions.toml).
    implementation(libs.clickhouse.client.v2)

    implementation(libs.jspecify)

    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
    }
    testImplementation("org.springframework.boot:spring-boot-webtestclient")
    testImplementation("io.projectreactor:reactor-test")
}

tasks.named("bootJar") {
    // This module is started manually (or by the manual macro-benchmark.yml workflow) against a
    // real ClickHouse instance for load testing, not packaged/distributed as a release artifact.
    enabled = false
}

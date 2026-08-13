plugins {
    `java-library`
    alias(libs.plugins.jmh)
}

// JMH benchmarks comparing this driver against client-v2, at multiple levels (raw transport,
// public R2DBC SPI, reactive-vs-blocking concurrency). See ROADMAP.md's Phase 5 section for the
// full design and rationale. Not published (see root build.gradle.kts's nonPublishedModules) and
// not part of `check`/`build` — run explicitly via `./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh`.
//
// Benchmark sources live under src/jmh/java (the me.champeau.jmh plugin's convention), which
// extends this module's main source set, so shared setup code (dataset seeding, container
// lifecycle) can live in src/main/java and be reused by every benchmark class without duplication.
dependencies {
    implementation(project(":clickhouse-r2dbc-reactive-core"))
    implementation(project(":clickhouse-r2dbc-reactive-transport-http"))
    implementation(project(":clickhouse-r2dbc-reactive-connector"))
    implementation(project(":clickhouse-r2dbc-reactive-testkit"))

    implementation(libs.reactor.core)
    implementation(libs.r2dbc.spi)
    implementation(libs.r2dbc.pool)

    // The comparison baseline this whole module exists to measure against.
    implementation(libs.clickhouse.client.v2)

    implementation(libs.slf4j.api)
    runtimeOnly(libs.slf4j.simple)

    implementation(platform(libs.testcontainers.bom))
    implementation(libs.testcontainers.clickhouse)

    // Intra-burst latency percentiles for the concurrency/burst benchmark - see Phase 5's "What's
    // measured, and how" section in ROADMAP.md for why JMH's own SampleTime mode isn't enough
    // there (it measures per-@Benchmark-invocation latency, not per-sub-operation latency inside
    // one burst of N concurrent queries).
    implementation(libs.hdrhistogram)

    compileOnly(libs.jspecify)
}

jmh {
    // Kept modest by default so a local "does this still compile and run" pass is fast; the
    // `default`/`large` dataset tiers (see ROADMAP.md) are opted into via -Pjmh.rows, not by
    // raising these.
    warmupIterations.set(1)
    iterations.set(3)
    fork.set(1)
    resultFormat.set("JSON")
    failOnError.set(true)
}

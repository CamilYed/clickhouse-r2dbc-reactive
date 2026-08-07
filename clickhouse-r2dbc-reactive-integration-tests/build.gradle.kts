plugins {
    `java-library`
}

// Black-box, whole-driver integration tests: real ClickHouse via Testcontainers, exercised only
// through the published R2DBC SPI surface (`connector`), never through core/transport-http
// internals. See ROADMAP.md's module map for why this is a separate module from `connector`'s
// own (smaller, whiter-box) integration tests. Not published to Maven Central — see root
// build.gradle.kts's `nonPublishedModules`.
dependencies {
    testImplementation(project(":clickhouse-r2dbc-reactive-connector"))
    testImplementation(project(":clickhouse-r2dbc-reactive-testkit"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)
}

plugins {
    `java-library`
}

dependencies {
    api(libs.r2dbc.spi)
    implementation(project(":clickhouse-r2dbc-reactive-core"))
    implementation(project(":clickhouse-r2dbc-reactive-transport-http"))
    compileOnly(libs.jspecify)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)

    // Brings AssertJ/JUnit plus the real-ClickHouse Testcontainers DSL
    // (BaseClickHouseIntegrationTest and friends) transitively — see ROADMAP.md's module map.
    testImplementation(project(":clickhouse-r2dbc-reactive-testkit"))
}

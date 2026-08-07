plugins {
    `java-library`
}

dependencies {
    api(project(":clickhouse-r2dbc-reactive-core"))
    api(libs.reactor.netty.http)

    api(platform(libs.junit.bom))
    api(libs.junit.jupiter)
    api(libs.assertj.core)

    // Real-ClickHouse Testcontainers support (BaseClickHouseIntegrationTest, data-setup/cleanup
    // Ability DSL) lives here so every consumer of testkit gets it for free — see
    // ROADMAP.md's module map.
    api(platform(libs.testcontainers.bom))
    api(libs.testcontainers.junit.jupiter)
    api(libs.testcontainers.clickhouse)
}

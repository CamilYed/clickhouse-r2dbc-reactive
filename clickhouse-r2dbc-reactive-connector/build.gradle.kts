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

    // Only for ClickHouseConnectionFactoryR2dbcPoolAgainstRealClickHouseTest — proves this
    // driver's ConnectionFactory works correctly when wrapped by io.r2dbc.pool's ConnectionPool,
    // the standard way Spring Boot's spring.r2dbc.pool.* properties apply pooling on top of any
    // R2DBC driver (see examples/spring-boot-webflux-demo's R2dbcConfiguration for the real
    // usage this mirrors). Not a production dependency of this module.
    testImplementation(libs.r2dbc.pool)

    // Brings AssertJ/JUnit plus the real-ClickHouse Testcontainers DSL
    // (BaseClickHouseIntegrationTest and friends) transitively — see ROADMAP.md's module map.
    testImplementation(project(":clickhouse-r2dbc-reactive-testkit"))
}

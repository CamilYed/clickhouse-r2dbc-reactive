plugins {
    `java-library`
}

dependencies {
    api(libs.r2dbc.spi)
    implementation(project(":clickhouse-r2dbc-reactive-core"))
    implementation(project(":clickhouse-r2dbc-reactive-transport-http"))
    compileOnly(libs.jspecify)

    // For Slf4jDriverObservationListener, the reference DriverObservationListener implementation
    // logging query lifecycle events keyed on query_id - the same optional-logging-facade pattern
    // as clickhouse-r2dbc-reactive-transport-http's own use of slf4j.api for its best-effort
    // KILL QUERY warning.
    implementation(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.slf4j.simple)

    // Only for ClickHouseConnectionFactoryR2dbcPoolAgainstRealClickHouseTest — proves this
    // driver's ConnectionFactory works correctly when wrapped by io.r2dbc.pool's ConnectionPool,
    // the standard way Spring Boot's spring.r2dbc.pool.* properties apply pooling on top of any
    // R2DBC driver (see examples/spring-boot-webflux-demo's R2dbcConfiguration for the real
    // usage this mirrors). Not a production dependency of this module.
    testImplementation(libs.r2dbc.pool)

    // Brings AssertJ/JUnit plus the real-ClickHouse Testcontainers DSL
    // (BaseClickHouseIntegrationTest and friends) transitively — see ROADMAP.md's module map.
    testImplementation(project(":clickhouse-r2dbc-reactive-testkit"))

    // For ClickHouseR2dbcSpiCompatibilityTest — the official R2DBC SPI Technology Compatibility
    // Kit, run against a real ClickHouse server. r2dbc-spi-test's TestKit<T> requires a
    // JdbcOperations handle to the same database purely for TCK fixture setup/teardown, hence
    // spring-jdbc and the ClickHouse JDBC driver alongside it; classifier "all" is the shaded jar
    // (bundles clickhouse-jdbc's own transitive deps) so DriverManager/SimpleDriverDataSource can
    // load it with no separate dependency management of its own. None of these three are ever a
    // production dependency of this module.
    testImplementation(libs.r2dbc.spi.test)
    testImplementation(libs.spring.jdbc)
    testImplementation("com.clickhouse:clickhouse-jdbc:${libs.versions.clickhouse.client.v2.get()}:all")

    testRuntimeOnly(libs.junit.platform.launcher)
}

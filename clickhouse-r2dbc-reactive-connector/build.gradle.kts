plugins {
    `java-library`
}

dependencies {
    api(libs.r2dbc.spi)
    implementation(project(":clickhouse-r2dbc-reactive-core"))
    implementation(project(":clickhouse-r2dbc-reactive-transport-http"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)

    testImplementation(project(":clickhouse-r2dbc-reactive-testkit"))

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.clickhouse)
}

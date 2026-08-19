plugins {
    `java-library`
}

dependencies {
    api(libs.reactor.core)
    api(libs.clickhouse.client.v2)
    compileOnly(libs.jspecify)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)
    testImplementation(libs.awaitility)
    testRuntimeOnly(libs.junit.platform.launcher)
}

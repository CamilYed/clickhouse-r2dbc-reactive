plugins {
    `java-library`
}

dependencies {
    api(libs.reactor.core)
    api(libs.clickhouse.client.v2)
    compileOnly(libs.jspecify)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)
}

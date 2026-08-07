plugins {
    `java-library`
}

dependencies {
    implementation(project(":clickhouse-r2dbc-reactive-core"))
    api(libs.reactor.netty.http)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)
}

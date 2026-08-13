plugins {
    `java-library`
}

dependencies {
    implementation(project(":clickhouse-r2dbc-reactive-core"))
    api(libs.reactor.netty.http)
    implementation(libs.slf4j.api)
    compileOnly(libs.jspecify)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.slf4j.simple)
    testRuntimeOnly(libs.bouncycastle.pkix)

    testImplementation(project(":clickhouse-r2dbc-reactive-testkit"))
}

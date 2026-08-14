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
    testImplementation(libs.assertj.core)
    testImplementation(libs.reactor.test)
    testImplementation(project(":clickhouse-r2dbc-reactive-testkit"))
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.slf4j.simple)
    testRuntimeOnly(libs.bouncycastle.pkix)
}

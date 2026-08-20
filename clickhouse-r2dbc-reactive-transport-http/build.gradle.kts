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

// Netty leak-detection test lane (ROADMAP.md Phase 7 item 6) - see testkit's build.gradle.kts for
// why these three specific flags. transport-http is where every ByteBuf this driver ever touches
// actually flows, so it's where a real leak would show up.
tasks.test {
    jvmArgs(
        "-Dio.netty.leakDetection.level=paranoid",
        "-Dio.netty.leakDetection.targetRecords=25",
        "-Dio.netty.customResourceLeakDetector=io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector"
    )
}

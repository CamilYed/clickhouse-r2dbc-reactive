plugins {
    `java-library`
}

dependencies {
    api(project(":clickhouse-r2dbc-reactive-core"))
    api(libs.reactor.netty.http)
    compileOnly(libs.jspecify)

    api(platform(libs.junit.bom))
    api(libs.junit.jupiter)
    api(libs.assertj.core)
    api(libs.awaitility)
    testRuntimeOnly(libs.junit.platform.launcher)

    // Real-ClickHouse Testcontainers support (BaseClickHouseIntegrationTest, data-setup/cleanup
    // Ability DSL) lives here so every consumer of testkit gets it for free — see
    // ROADMAP.md's module map.
    api(platform(libs.testcontainers.bom))
    api(libs.testcontainers.junit.jupiter)
    api(libs.testcontainers.clickhouse)
}

// Netty leak-detection test lane (ROADMAP.md Phase 7 item 6, non-functional requirements section):
// paranoid samples every single ByteBuf allocation instead of Netty's default ~1%, so a forgotten
// .release() in a short-lived test is actually seen instead of silently missed by sampling.
// targetRecords raises the recorded-access history per buffer so a leak report's stack trace shows
// where the buffer was actually last touched, not just where it was allocated. customResourceLeakDetector
// installs LeakRecordingResourceLeakDetector as Netty's detector for every resource type - it must be
// set via this JVM property, not programmatically, because Netty binds AbstractByteBuf's detector into
// a static final field the first time any ByteBuf is ever allocated in the JVM, which always happens
// before any single test method could call an install-at-runtime method (see that class's Javadoc for
// the concrete failure this replaced).
tasks.test {
    jvmArgs(
        "-Dio.netty.leakDetection.level=paranoid",
        "-Dio.netty.leakDetection.targetRecords=25",
        "-Dio.netty.customResourceLeakDetector=io.github.camilyed.clickhouse.r2dbc.testkit.fakes.LeakRecordingResourceLeakDetector"
    )
}

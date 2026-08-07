plugins {
    `java-library`
}

dependencies {
    api(project(":clickhouse-r2dbc-reactive-core"))
    api(libs.reactor.netty.http)

    api(platform(libs.junit.bom))
    api(libs.junit.jupiter)

    testImplementation(libs.assertj.core)
}

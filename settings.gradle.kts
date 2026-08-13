pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

rootProject.name = "clickhouse-r2dbc-reactive"

include(
    "clickhouse-r2dbc-reactive-core",
    "clickhouse-r2dbc-reactive-transport-http",
    "clickhouse-r2dbc-reactive-connector",
    "clickhouse-r2dbc-reactive-testkit",
    "clickhouse-r2dbc-reactive-benchmarks",
    "examples:spring-boot-webflux-demo"
)

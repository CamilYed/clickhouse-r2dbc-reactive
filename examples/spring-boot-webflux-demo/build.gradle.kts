plugins {
    `java-library`
    id("org.springframework.boot") version "4.1.0"
}

description =
    "Runnable Spring Boot + WebFlux demo (Phase 6) proving clickhouse-r2dbc-reactive works " +
        "unmodified through Spring's own R2DBC integration (a hand-built DatabaseClient and " +
        "ConnectionPool wiring, ReactiveTransactionManager) via the standard R2DBC " +
        "ConnectionFactoryProvider SPI - not published, not part of the driver's public surface."

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.0"))

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    // Deliberately spring-boot-starter-r2dbc, NOT spring-boot-starter-data-r2dbc: the latter pulls
    // in org.springframework.boot.data.r2dbc.autoconfigure.DataR2dbcAutoConfiguration, whose
    // constructor eagerly resolves a Spring Data R2DBC Dialect via DialectResolver - a *second*,
    // separate hardcoded driver list from the BindMarkersFactoryResolver one R2dbcConfiguration's
    // Javadoc already documents, and it fails the same way (NoDialectException: Cannot determine a
    // dialect for ClickHouse) regardless of any bean this module defines itself, since it's not
    // gated on @ConditionalOnMissingBean(DatabaseClient.class) the way the base r2dbc
    // auto-configuration is. This demo never uses R2dbcEntityTemplate/Spring Data repositories (see
    // R2dbcConfiguration's and DatabaseClientOrderEventRepository's own Javadoc for why), so the
    // lean starter - ConnectionFactory/DatabaseClient only, no Spring Data R2DBC - is the correct
    // fix, not a workaround: it removes DataR2dbcAutoConfiguration from the classpath entirely
    // instead of trying to satisfy or bypass its Dialect requirement.
    implementation("org.springframework.boot:spring-boot-starter-r2dbc")
    // R2dbcConfiguration builds its own ConnectionPool explicitly (see that class's own Javadoc
    // for why) rather than relying on Spring Boot's pool auto-configuration - needs the
    // ConnectionPool/ConnectionPoolConfiguration classes on the compile classpath. Version managed
    // by the spring-boot-dependencies BOM above (1.0.2.RELEASE for Boot 4.1.0).
    implementation("io.r2dbc:r2dbc-pool")

    // Deliberately runtimeOnly, not implementation/api: this demo's own code never references a
    // single class from our driver directly. Spring discovers it purely through the standard
    // R2DBC ConnectionFactoryProvider SPI (META-INF/services), exactly like every other R2DBC
    // driver - that's the whole point of this module (see ROADMAP.md's Phase 6).
    //
    // Deliberately the *published* Maven Central coordinate, not project(":clickhouse-r2dbc-reactive-connector"):
    // this demo exists to show exactly what an external consumer's build file looks like, so it
    // depends on the driver the same way they would. Trade-off, stated plainly: this module no
    // longer catches a regression in core/transport-http/connector on every root build - it now
    // proves "the last published release still works end to end", not "today's local change
    // works end to end". The whole-driver black-box proof for local changes still lives in
    // connector's own real-ClickHouse tests (see ROADMAP.md's module map). Bump this version
    // alongside every release.
    runtimeOnly("io.github.camilyed:clickhouse-r2dbc-reactive-connector:0.1.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
    }
    // Boot 4 slimmed spring-boot-starter-test down and split WebTestClient's autoconfigure support
    // (org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient) into its
    // own module, no longer pulled in transitively - must be declared explicitly.
    testImplementation("org.springframework.boot:spring-boot-webtestclient")
    // Deliberately plain Testcontainers JUnit5 + @DynamicPropertySource, not Spring Boot's
    // @ServiceConnection: Spring Boot's built-in R2DBC service-connection detection only knows a
    // fixed list of well-known drivers (postgres, mysql, mariadb, mssql, oracle...), which doesn't
    // include this one - @DynamicPropertySource is the explicit, driver-agnostic wiring that works
    // regardless, and matches the pattern this repo's own testkit already uses.
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.clickhouse)
}

tasks.named("bootJar") {
    // Example module: nothing outside this repo is meant to run this as a packaged artifact.
    enabled = false
}

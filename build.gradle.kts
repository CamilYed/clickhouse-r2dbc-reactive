import com.diffplug.gradle.spotless.SpotlessExtension
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.testing.TestDescriptor
import org.gradle.api.tasks.testing.TestListener
import org.gradle.api.tasks.testing.TestResult
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.external.javadoc.CoreJavadocOptions
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.plugins.signing.SigningExtension
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.sonarqube)
}

allprojects {
    group = "io.github.camilyed"
    version = providers.gradleProperty("releaseVersion")
        .orElse("0.1.0-SNAPSHOT")
        .get()
}

// Modules that exist purely to hold tests (no public API of their own) never get published to
// Maven Central. They still get jacoco/spotless/toolchain like every other module — just not
// maven-publish/signing. Keep this list in sync with settings.gradle.kts.
val nonPublishedModules = setOf(
    "clickhouse-r2dbc-reactive-integration-tests"
)

subprojects {
    pluginManager.withPlugin("java-library") {
        pluginManager.apply("jacoco")
        pluginManager.apply("com.diffplug.spotless")

        val isPublished = project.name !in nonPublishedModules
        if (isPublished) {
            pluginManager.apply("maven-publish")
            pluginManager.apply("signing")
        }

        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }

            if (isPublished) {
                withSourcesJar()
                withJavadocJar()
            }
        }

        // Relax only the "missing tag" doclint category (no @param/@return/@throws), not doclint
        // as a whole: CLAUDE.md's own Javadoc rule asks for a one-line contract summary per public
        // member, not exhaustive per-parameter tags that just repeat what the summary already says.
        // Doclint's other checks (malformed HTML, broken {@link} targets, etc.) still run and still
        // fail the build, since those catch genuinely broken documentation.
        tasks.withType<Javadoc>().configureEach {
            (options as? CoreJavadocOptions)?.addStringOption("Xdoclint:all,-missing", "-quiet")
        }

        extensions.configure<JacocoPluginExtension> {
            toolVersion = libs.versions.jacoco.get()
        }

        extensions.configure<SpotlessExtension> {
            java {
                target("src/**/*.java")
                googleJavaFormat()
                removeUnusedImports()
                trimTrailingWhitespace()
                endWithNewline()
            }

            kotlinGradle {
                target("*.gradle.kts")
                trimTrailingWhitespace()
                endWithNewline()
            }

            format("misc") {
                target("*.md", ".gitignore")
                trimTrailingWhitespace()
                endWithNewline()
            }
        }

        if (isPublished) {
            extensions.configure<PublishingExtension> {
                publications {
                    create<MavenPublication>("mavenJava") {
                        from(components["java"])

                        pom {
                            name.set(project.name)
                            description.set(projectDescription(project.name))
                            url.set("https://github.com/CamilYed/clickhouse-r2dbc-reactive")
                            inceptionYear.set("2026")

                            licenses {
                                license {
                                    name.set("The Apache License, Version 2.0")
                                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                                    distribution.set("repo")
                                }
                            }

                            developers {
                                developer {
                                    id.set("CamilYed")
                                    name.set("CamilYed")
                                    url.set("https://github.com/CamilYed")
                                }
                            }

                            scm {
                                url.set("https://github.com/CamilYed/clickhouse-r2dbc-reactive")
                                connection.set(
                                    "scm:git:git://github.com/CamilYed/clickhouse-r2dbc-reactive.git"
                                )
                                developerConnection.set(
                                    "scm:git:ssh://git@github.com:CamilYed/clickhouse-r2dbc-reactive.git"
                                )
                            }
                        }
                    }
                }

                repositories {
                    maven {
                        name = "localBuild"
                        url = rootProject.layout.buildDirectory.dir("staging-deploy").get().asFile.toURI()
                    }

                    maven {
                        name = "centralSnapshots"
                        url = uri("https://central.sonatype.com/repository/maven-snapshots/")

                        mavenContent {
                            snapshotsOnly()
                        }

                        credentials {
                            username = providers.gradleProperty("centralUsername")
                                .orElse(providers.environmentVariable("CENTRAL_USERNAME"))
                                .orNull
                            password = providers.gradleProperty("centralPassword")
                                .orElse(providers.environmentVariable("CENTRAL_PASSWORD"))
                                .orNull
                        }
                    }
                }
            }

            val publishing = extensions.getByType(PublishingExtension::class.java)

            extensions.configure<SigningExtension> {
                val signingKey = providers.gradleProperty("signingKey")
                    .orElse(providers.environmentVariable("SIGNING_KEY"))
                    .orNull
                val signingPassword = providers.gradleProperty("signingPassword")
                    .orElse(providers.environmentVariable("SIGNING_PASSWORD"))
                    .orNull

                isRequired = isRemotePublishingRequested()

                if (!signingKey.isNullOrBlank()) {
                    useInMemoryPgpKeys(signingKey, signingPassword)
                }

                sign(publishing.publications)
            }
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()

        testLogging {
            events = setOf(
                TestLogEvent.PASSED,
                TestLogEvent.SKIPPED,
                TestLogEvent.FAILED
            )
            exceptionFormat = TestExceptionFormat.FULL
            showExceptions = true
            showCauses = true
            showStackTraces = true
            showStandardStreams = false
        }

        addTestListener(object : TestListener {
            override fun beforeSuite(suite: TestDescriptor) = Unit

            override fun beforeTest(testDescriptor: TestDescriptor) = Unit

            override fun afterTest(
                testDescriptor: TestDescriptor,
                result: TestResult
            ) = Unit

            override fun afterSuite(
                suite: TestDescriptor,
                result: TestResult
            ) {
                if (suite.parent == null) {
                    println()
                    println("Test result: ${result.resultType}")
                    println(
                        "Tests: ${result.testCount}, " +
                                "passed: ${result.successfulTestCount}, " +
                                "failed: ${result.failedTestCount}, " +
                                "skipped: ${result.skippedTestCount}"
                    )
                    println()
                }
            }
        })

        finalizedBy(tasks.named("jacocoTestReport"))
    }

    tasks.withType<JacocoReport>().configureEach {
        dependsOn(tasks.withType<Test>())

        reports {
            xml.required.set(true)
            html.required.set(true)
            csv.required.set(false)
        }
    }
}

sonar {
    properties {
        property("sonar.projectKey", "CamilYed_clickhouse-r2dbc-reactive")
        property("sonar.organization", "camilyed")
        property("sonar.host.url", "https://sonarcloud.io")
        property(
            "sonar.coverage.jacoco.xmlReportPaths",
            listOf(
                "clickhouse-r2dbc-reactive-core/build/reports/jacoco/test/jacocoTestReport.xml",
                "clickhouse-r2dbc-reactive-transport-http/build/reports/jacoco/test/jacocoTestReport.xml",
                "clickhouse-r2dbc-reactive-connector/build/reports/jacoco/test/jacocoTestReport.xml",
                "clickhouse-r2dbc-reactive-testkit/build/reports/jacoco/test/jacocoTestReport.xml",
                "clickhouse-r2dbc-reactive-integration-tests/build/reports/jacoco/test/jacocoTestReport.xml"
            ).joinToString(",")
        )
    }
}

fun projectDescription(projectName: String): String =
    when (projectName) {
        "clickhouse-r2dbc-reactive-core" ->
            "Transport-independent query and protocol core for the reactive ClickHouse R2DBC driver, reusing ClickHouse Java Client V2 components."
        "clickhouse-r2dbc-reactive-transport-http" ->
            "Non-blocking HTTP transport adapter for the reactive ClickHouse R2DBC driver."
        "clickhouse-r2dbc-reactive-connector" ->
            "Thin R2DBC SPI connector for ClickHouse, built on the reactive core and transport modules."
        "clickhouse-r2dbc-reactive-testkit" ->
            "Shared test infrastructure for the reactive ClickHouse R2DBC driver: a controlled " +
                "local server for transport contract tests, and a real-ClickHouse Testcontainers " +
                "DSL for integration tests."
        else ->
            "Reactive R2DBC driver for ClickHouse."
    }

fun Project.isRemotePublishingRequested(): Boolean =
    gradle.startParameter.taskNames.any { taskName ->
        taskName.contains("publish", ignoreCase = true) &&
                !taskName.contains("MavenLocal", ignoreCase = true) &&
                !taskName.contains("LocalBuild", ignoreCase = true)
    }

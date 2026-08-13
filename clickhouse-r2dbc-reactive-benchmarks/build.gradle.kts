plugins {
    `java-library`
    alias(libs.plugins.jmh)
}

// JMH benchmarks comparing this driver against client-v2, at multiple levels (raw transport,
// public R2DBC SPI, reactive-vs-blocking concurrency). See ROADMAP.md's Phase 5 section for the
// full design and rationale. Not published (see root build.gradle.kts's nonPublishedModules) and
// not part of `check`/`build` — run explicitly via `./gradlew :clickhouse-r2dbc-reactive-benchmarks:jmh`.
//
// Benchmark sources live under src/jmh/java (the me.champeau.jmh plugin's convention), which
// extends this module's main source set, so shared setup code (dataset seeding, container
// lifecycle) can live in src/main/java and be reused by every benchmark class without duplication.
dependencies {
    implementation(project(":clickhouse-r2dbc-reactive-core"))
    implementation(project(":clickhouse-r2dbc-reactive-transport-http"))
    implementation(project(":clickhouse-r2dbc-reactive-connector"))
    implementation(project(":clickhouse-r2dbc-reactive-testkit"))

    implementation(libs.reactor.core)
    implementation(libs.r2dbc.spi)
    implementation(libs.r2dbc.pool)

    // The comparison baseline this whole module exists to measure against.
    implementation(libs.clickhouse.client.v2)

    implementation(libs.slf4j.api)
    runtimeOnly(libs.slf4j.simple)

    implementation(platform(libs.testcontainers.bom))
    implementation(libs.testcontainers.clickhouse)

    // Sub-operation latency percentiles JMH's own SampleTime mode can't give us: time-to-first-row
    // inside StreamingScanBenchmark today, intra-burst per-query latency inside the not-yet-built
    // concurrency/burst benchmark next - see Phase 5's "What's measured, and how" in ROADMAP.md.
    implementation(libs.hdrhistogram)

    compileOnly(libs.jspecify)
}

jmh {
    // Kept modest by default so a local "does this still compile and run" pass is fast; the
    // `default`/`large` dataset tiers (see ROADMAP.md) are set directly on each benchmark class's
    // `@Param` array today - there's no `-Pjmh.rows` override wired here (unlike `includes`/
    // `profilers` below), since the me.champeau.jmh plugin's `benchmarkParameters` shape wasn't
    // confirmed against a real build before this comment was corrected. Don't trust a `-Pjmh.rows`
    // flag until this comment says otherwise.
    warmupIterations.set(1)
    iterations.set(3)
    fork.set(1)
    resultFormat.set("JSON")
    failOnError.set(true)

    // The me.champeau.jmh plugin does NOT read `-P` project properties automatically - `includes`/
    // `profilers` are plain Gradle extension properties, configured only in this build script,
    // never from the command line, unless a project explicitly wires that itself (as below). Every
    // earlier `-Pjmh.includes=X`/`-Pjmh.profilers=gc` instruction given before this was added was a
    // silent no-op: the property was set but nothing in this file ever read it, so every `jmh` run
    // executed the entire benchmark suite with no profiler regardless of what was passed on the
    // command line. Caught when a run that should have taken seconds (one class) instead ran
    // everything (~17 minutes). Fixed here, not worked around by asking for different flags.
    if (project.hasProperty("jmh.includes")) {
        includes.set(listOf(project.property("jmh.includes") as String))
    }
    if (project.hasProperty("jmh.profilers")) {
        profilers.set((project.property("jmh.profilers") as String).split(","))
    }
    // Added 2026-08-13: a single fork + one warmup iteration (the defaults above) turned out to be
    // too thin once results needed to be trusted between separate runs, not just between iterations
    // within one run - the same method/tier combination gave meaningfully different B/op numbers
    // (e.g. ourDriver at 1M: ~848 B/row one run, ~808 B/row the next) with byte-for-byte identical
    // code and input data. A single fork can't distinguish "real per-tier structural cost" from
    // "this particular JVM instance's JIT/GC/TLAB state" - only multiple independent forks can. Not
    // defaulted higher because a full multi-fork run is slow; opt in explicitly when a result is
    // about to be trusted for an architecture decision, not for routine iteration.
    if (project.hasProperty("jmh.forks")) {
        fork.set((project.property("jmh.forks") as String).toInt())
    }
    if (project.hasProperty("jmh.warmupIterations")) {
        warmupIterations.set((project.property("jmh.warmupIterations") as String).toInt())
    }
}

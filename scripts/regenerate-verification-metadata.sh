#!/usr/bin/env bash
#
# Regenerates gradle/verification-metadata.xml so it has a checksum entry for every artifact
# this project's Gradle configurations currently resolve to.
#
# When to run this: any time a dependency version changes (a version-catalog bump, a Dependabot
# PR, a new dependency added) and the next build fails with "Dependency verification failed for
# configuration ..." for artifacts that are genuinely trustworthy (i.e. not an unexpected/rogue
# artifact - if in doubt, stop and ask before running this). See CONTRIBUTING.md.
#
# Requires: JDK 21, Docker running (the real-ClickHouse/transport-contract test suite uses
# Testcontainers, not mocks), network access to Maven Central - the same requirements as CI.
#
# History of two wrong shortcuts this script tried first, kept here so nobody reintroduces them:
#
# 1. --dry-run. Per Gradle's own dependency verification docs
#    (docs.gradle.org/current/userguide/dependency_verification.html, "Dry-Run Mode"), --dry-run
#    writes to a SEPARATE gradle/verification-metadata.dryrun.xml preview file, never to the real
#    gradle/verification-metadata.xml. Looked like it worked (fast, no errors) but never actually
#    updated the file the build reads.
#
# 2. Passing the cheap `help` task instead of a real build. --write-verification-metadata does
#    make Gradle resolve every resolvable configuration regardless of which task you ask for (see
#    the docs' "Generating Checksums and Signatures" section) - true, and it caught 11 of 12
#    missing artifacts after the client-v2 0.9.8 bump. But `junit-bom-5.11.0.module` (Gradle
#    Module Metadata, not the .pom) only got resolved when the real `clean build
#    jacocoTestReport` task graph actually ran in CI - exactly the caveat the same docs page
#    states plainly: "dependencies that are only resolved during task execution... may not be
#    included in the generated file." `help` is a heuristic, not a guarantee.
#
# So: this script now runs the exact same tasks CI runs (see .github/workflows/ci.yml), which is
# the only thing that's actually guaranteed to resolve everything CI itself needs - plus the
# clickhouse-r2dbc-reactive-benchmarks module's jmh source set explicitly, since that one is
# deliberately excluded from `build`/`check` (see that module's build.gradle.kts) and so would
# otherwise never get touched by mirroring CI alone.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Regenerating gradle/verification-metadata.xml (mirrors ci.yml's build+jacoco run, plus jmh)..."
./gradlew --write-verification-metadata sha256 clean build jacocoTestReport \
  :clickhouse-r2dbc-reactive-benchmarks:jmhClasses

echo
echo "Done. Review what changed before committing:"
echo "  git diff gradle/verification-metadata.xml"
echo
echo "Then confirm a real build is actually green:"
echo "  ./gradlew spotlessCheck clean build"

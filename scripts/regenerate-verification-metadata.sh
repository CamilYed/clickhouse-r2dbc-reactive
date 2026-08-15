#!/usr/bin/env bash
#
# Regenerates gradle/verification-metadata.xml so it has a checksum entry for every artifact
# every module's Gradle configurations currently resolve to - main, test, and the
# clickhouse-r2dbc-reactive-benchmarks module's jmh source set (deliberately excluded from
# `build`/`check`, see that module's build.gradle.kts, so a plain `clean build` regeneration
# alone always misses its jmhCompileClasspath/jmhRuntimeClasspath - the exact failure this
# script exists to stop recurring).
#
# When to run this: any time a dependency version changes (a version-catalog bump, a Dependabot
# PR, a new dependency added) and the next build fails with "Dependency verification failed for
# configuration ..." for artifacts that are genuinely trustworthy (i.e. not an unexpected/rogue
# artifact - if in doubt, stop and ask before running this). See CONTRIBUTING.md.
#
# Requires: JDK 21, network access to Maven Central - the same requirements as a normal build.
# Uses --dry-run: Gradle still resolves every configuration needed to compute the requested
# tasks' inputs (see docs.gradle.org's dependency verification troubleshooting guide), so
# checksums get captured without actually compiling/running anything - much faster, and doesn't
# need Docker/Testcontainers running for the real-ClickHouse test tasks.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Regenerating gradle/verification-metadata.xml (main/test/jmh classpaths)..."
./gradlew \
  --write-verification-metadata sha256 \
  clean build \
  :clickhouse-r2dbc-reactive-benchmarks:jmhClasses \
  --dry-run

echo
echo "Done. Review what changed before committing:"
echo "  git diff gradle/verification-metadata.xml"
echo
echo "Then confirm a real build is actually green:"
echo "  ./gradlew spotlessCheck clean build"

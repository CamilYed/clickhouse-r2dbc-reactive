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
# Requires: JDK 21, network access to Maven Central - the same requirements as a normal build.
#
# Deliberately no --dry-run: per Gradle's own dependency verification docs
# (docs.gradle.org/current/userguide/dependency_verification.html, "Dry-Run Mode"), --dry-run
# writes to a SEPARATE gradle/verification-metadata.dryrun.xml preview file, never to the real
# gradle/verification-metadata.xml - using it here was this script's own first-draft bug: it
# looked like it worked (fast, no errors) but never actually updated the file the build reads,
# so the exact same "Dependency verification failed" errors kept happening on the next real
# build. `help` is deliberately the task passed below, not `clean build`: passing
# --write-verification-metadata makes Gradle resolve every resolvable configuration in the
# project (root, every subproject, buildSrc, included builds, plugins - see the docs' "Generating
# Checksums and Signatures" section) regardless of which task you ask for, so a cheap task like
# `help` is enough - no need to actually compile, run tests, or have Docker/Testcontainers up.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Regenerating gradle/verification-metadata.xml (resolves every configuration in every module)..."
./gradlew --write-verification-metadata sha256 help

echo
echo "Done. Review what changed before committing:"
echo "  git diff gradle/verification-metadata.xml"
echo
echo "Then confirm a real build is actually green:"
echo "  ./gradlew spotlessCheck clean build"

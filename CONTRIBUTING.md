# Contributing

This project is in an early design and validation phase. See the README for current status and
the architecture direction.

## Engineering guidelines

[CLAUDE.md](CLAUDE.md) is the actual working agreement for code in this repository: language
style, TDD workflow, the black-box/no-Mockito testing rules, and the hexagonal architecture
boundaries between modules. Read it before writing code here — it's opinionated on purpose, and
a pull request that conflicts with it will be asked to either follow it or update it deliberately.

## Before opening a pull request

Docker must be running — most of the test suite (transport contract tests and real-ClickHouse
integration tests) uses Testcontainers, not mocks.

Run:

```bash
./gradlew spotlessCheck clean build
```

CI (`.github/workflows/ci.yml`) runs the same check, plus SonarCloud analysis, on every push to
`main` and every pull request.

### Dependency verification failures after a version bump

`gradle/verification-metadata.xml` (see [Gradle's dependency verification
docs](https://docs.gradle.org/current/userguide/dependency_verification.html)) pins a SHA-256
checksum for every artifact every module's Gradle configurations resolve. Bumping a dependency
version — by hand or via a Dependabot PR — almost always pulls in artifacts (the new version
itself, and often new/changed transitive dependencies) that file has no entry for yet, and the
next build fails with `Dependency verification failed for configuration ...` listing each one.

If the failing artifacts are genuinely trustworthy (the dependency you meant to bump, or its
real transitive deps — not something unexpected), regenerate the file:

```bash
./scripts/regenerate-verification-metadata.sh
```

Passing `--write-verification-metadata` makes Gradle resolve every resolvable configuration in
every module — root, every subproject (including the `clickhouse-r2dbc-reactive-benchmarks`
module's `jmh` source set, which is otherwise excluded from `build`/`check`), `buildSrc`, and
plugins — regardless of which task you ask for, so the script deliberately asks for the cheap
`help` task rather than a real `clean build`: no compiling, no tests, no Docker/Testcontainers
needed. Review `git diff gradle/verification-metadata.xml` before committing.

Deliberately not `--dry-run`: per Gradle's own docs, `--dry-run` writes to a separate
`gradle/verification-metadata.dryrun.xml` preview file, never to the real
`gradle/verification-metadata.xml` — a mistake this script's own first draft made (looked like it
worked, changed nothing).

`.github/workflows/verification-metadata.yml` runs this same script automatically on any pull
request (including Dependabot's) that touches `gradle/libs.versions.toml`, and pushes the
regenerated file back onto the PR branch — so an ordinary version bump usually needs no manual
step at all. It cannot trigger `ci.yml`'s own checks on the commit it pushes (a GitHub Actions
loop-prevention rule for `GITHUB_TOKEN`-authored pushes), so re-run CI manually once its commit
lands.

## Design discussion

The direction for this driver started as a public discussion with the ClickHouse team:
[ClickHouse/ClickHouse#113638](https://github.com/ClickHouse/ClickHouse/discussions/113638).
Please read it before proposing architectural changes, especially around transport, backpressure,
and cancellation semantics.

## Code style

Formatting is enforced with [Spotless](https://github.com/diffplug/spotless) (Google Java Format).
Run `./gradlew spotlessApply` to format before committing.

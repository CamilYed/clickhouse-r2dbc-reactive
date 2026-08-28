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

The script runs the exact same tasks CI runs (`clean build jacocoTestReport`, see
`.github/workflows/ci.yml`), plus the `clickhouse-r2dbc-reactive-benchmarks` module's `jmh`
source set explicitly (otherwise excluded from `build`/`check` — see that module's
`build.gradle.kts`). Needs Docker running, same as a normal build. Review `git diff
gradle/verification-metadata.xml` before committing.

Two wrong shortcuts this script tried first, in case either looks tempting to reintroduce:

- `--dry-run` — per Gradle's own docs, writes to a separate `gradle/verification-metadata.dryrun.xml`
  preview file, never to the real `gradle/verification-metadata.xml`. Looked like it worked
  (fast, no errors), changed nothing.
- The cheap `help` task instead of a real build — `--write-verification-metadata` does force
  Gradle to resolve every resolvable configuration regardless of which task you ask for, and this
  caught 11 of 12 missing artifacts after the client-v2 0.9.8 bump, but missed
  `junit-bom-5.11.0.module` (Gradle Module Metadata), which only got resolved when the real
  `clean build jacocoTestReport` task graph actually ran — the exact caveat Gradle's own docs
  state: dependencies "only resolved during task execution... may not be included" by a `help`-only
  run. `help` is a heuristic, not a guarantee; mirroring CI's real command is.

`.github/workflows/verification-metadata.yml` runs this same script automatically on any pull
request (including Dependabot's) that touches `gradle/libs.versions.toml`, and pushes the
regenerated file back onto the PR branch — so an ordinary version bump usually needs no manual
step at all. It cannot trigger `ci.yml`'s own checks on the commit it pushes (a GitHub Actions
loop-prevention rule for `GITHUB_TOKEN`-authored pushes), so re-run CI manually once its commit
lands.

## Cutting a release

`.github/workflows/release.yml` (`workflow_dispatch`, needs a maintainer to trigger it manually)
builds, signs, and publishes all four modules to Maven Central via the Central Portal, then — once
the deployment is confirmed `PUBLISHED` — creates a Git tag and a GitHub Release, so the Central
Portal version, the tag, and the Release always point at the same commit
([ROADMAP archive Phase 7 item 13](engineering/roadmap-archive.md#phase-7--operational-control--r2dbc-correctness-020)).

1. On `main`, move the release notes in [CHANGELOG.md](CHANGELOG.md) under a
   `## [X.Y.Z] — YYYY-MM-DD` heading and commit it — the release workflow reads this file for
   GitHub Release notes and fails before building or publishing if it can't find a heading matching
   the version you give it.
2. Trigger `release.yml` (Actions tab → "Release" → "Run workflow") with the version (e.g.
   `0.2.0`) and a `publishing_type`:
   - `AUTOMATIC` — Central Portal publishes as soon as validation passes; the workflow waits for
     `PUBLISHED`, then tags and creates the GitHub Release in the same run.
   - `USER_MANAGED` (default, safer for a first look at what's about to go live) — the workflow
     stops once the deployment is `VALIDATED` and prints a reminder to review it on
     [Central Portal](https://central.sonatype.com) and click Publish yourself. The tag/Release are
     **not** created in this run (the deployment isn't `PUBLISHED` yet) — re-run `release.yml` with
     the same version after publishing so it observes `PUBLISHED` and finishes tagging/releasing.
3. Confirm the new version's badge (top of README.md) picks it up — Central Portal's search-index
   sync (which the shields.io badges read from) can lag a few minutes to a few hours behind
   publication.

## Design discussion

The direction for this driver started as a public discussion with the ClickHouse team:
[ClickHouse/ClickHouse#113638](https://github.com/ClickHouse/ClickHouse/discussions/113638).
Please read it before proposing architectural changes, especially around transport, backpressure,
and cancellation semantics.

## Code style

Formatting is enforced with [Spotless](https://github.com/diffplug/spotless) (Google Java Format).
Run `./gradlew spotlessApply` to format before committing.

A pre-commit hook that runs `spotlessApply` on staged Java files automatically lives at
`.githooks/pre-commit` — it isn't wired in by default (a hook outside `.git/hooks` needs git told
where to look), so enable it once per clone:

```bash
git config core.hooksPath .githooks
```

After that, every commit touching a `.java` file runs Spotless first and re-stages whatever it
reformats, so a commit never lands with formatting Spotless would immediately rewrite anyway.

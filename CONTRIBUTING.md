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

## Design discussion

The direction for this driver started as a public discussion with the ClickHouse team:
[ClickHouse/ClickHouse#113638](https://github.com/ClickHouse/ClickHouse/discussions/113638).
Please read it before proposing architectural changes, especially around transport, backpressure,
and cancellation semantics.

## Code style

Formatting is enforced with [Spotless](https://github.com/diffplug/spotless) (Google Java Format).
Run `./gradlew spotlessApply` to format before committing.

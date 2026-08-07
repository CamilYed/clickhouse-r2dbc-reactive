# Contributing

This project is in an early design and validation phase. See the README for current status and
the architecture direction.

## Before opening a pull request

Run:

```bash
./gradlew spotlessCheck clean build
```

## Design discussion

The direction for this driver started as a public discussion with the ClickHouse team:
[ClickHouse/ClickHouse#113638](https://github.com/ClickHouse/ClickHouse/discussions/113638).
Please read it before proposing architectural changes, especially around transport, backpressure,
and cancellation semantics.

## Code style

Formatting is enforced with [Spotless](https://github.com/diffplug/spotless) (Google Java Format).
Run `./gradlew spotlessApply` to format before committing.

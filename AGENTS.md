# Agent instructions

See [CLAUDE.md](CLAUDE.md) for the full engineering guidelines (language style, architecture,
testing philosophy, CI gates). This project has a single source of truth for those rules to avoid
`AGENTS.md`/`CLAUDE.md` drifting apart — read `CLAUDE.md` before making changes.

Quick reminders most relevant to autonomous changes:

- `final` everywhere (locals, params, fields) unless there's a real reason not to.
- No Mockito, ever. Black-box tests with in-memory fakes, Given-When-Then, Test Data Builders,
  Ability pattern, custom AssertJ assertions.
- `./gradlew spotlessCheck clean build` must pass before considering a change done.
- Don't add performance/benchmark tooling yet — that phase hasn't started.

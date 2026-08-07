# Engineering guidelines

This file is the working agreement for anyone (human or agent) writing code in this repository.
It is intentionally opinionated. If a change conflicts with something here, either follow this
document or update it deliberately — don't silently drift from it.

See also: [README.md](README.md) for architecture/status, [CONTRIBUTING.md](CONTRIBUTING.md) for
the PR checklist.

## Contents

- [Language style](#language-style)
- [Architecture](#architecture)
- [Testing philosophy](#testing-philosophy)
- [Test building blocks](#test-building-blocks)
- [Test types and tools](#test-types-and-tools)
- [Formatting and CI gates](#formatting-and-ci-gates)
- [Performance testing](#performance-testing)

## Language style

- **All variables are `final` by default** — local variables, method parameters, and fields.
  Mutability must be an explicit, deliberate choice, not a default. If something needs to
  change after construction, that's a signal to reconsider the design (builder, new value,
  immutable update) before reaching for a mutable field.
- Prefer immutable value types (records where appropriate) for anything that crosses a module
  boundary (query requests, settings, decoded rows, R2DBC options).
- Clean Code basics apply throughout: small methods, one level of abstraction per method,
  intention-revealing names, no boolean/flag parameters that silently change behavior, no dead
  code, no commented-out code left behind. If a method needs a comment to explain *what* it does,
  it should probably be split or renamed instead.
- No `null` as a silent default — use `Optional`, sealed types, or explicit "not present" domain
  objects instead, especially at module boundaries (connector ↔ core ↔ transport).

## Architecture

- Domain/protocol logic (`clickhouse-r2dbc-reactive-core`) must stay independent of any specific
  transport or framework. It knows about ClickHouse concepts (queries, settings, `query_id`, row
  decoding), not about Netty, HTTP, or R2DBC types.
- Transport (`clickhouse-r2dbc-reactive-transport-http`) and the R2DBC SPI implementation
  (`clickhouse-r2dbc-reactive-connector`) are **adapters** in the Ports & Adapters (Hexagonal
  Architecture) sense — they plug into the core through small, explicit interfaces owned by the
  core, not the other way around.
- Use DDD tactical patterns (value objects, aggregates, domain events, ubiquitous language) where
  the domain complexity actually warrants it. Don't force DDD ceremony onto what is fundamentally
  a thin protocol/decoding layer — apply it where there is real domain logic to protect (e.g.
  connection/transaction lifecycle rules, cancellation semantics), not to every data holder.
- A module should not reach into another module's internals. If `connector` needs something from
  `core`, it goes through `core`'s public API, never through package-private/internal classes.

## Testing philosophy

This project follows the black-box, no-Mockito testing style described in:

- [Testy, które nie kłamią cz. 1: Czytelność i DSL](https://camilyed.github.io/pl/testy-ktore-nie-klamia/)
  ([EN: Tests That Don't Lie, Part 1](https://camilyed.github.io/en/tests-that-dont-lie/))
- [Testy, które nie kłamią cz. 2: Pułapka Mockito i implementacje In-Memory](https://camilyed.github.io/pl/testy-ktore-nie-klamia-cz2/)
  ([EN: Tests That Don't Lie, Part 2](https://camilyed.github.io/en/tests-that-dont-lie-part-2/))
- [Make your tests readable by example (Allegro Tech Blog)](https://blog.allegro.tech/2022/02/readable-tests-by-example.html)

The short version: a test should tell you *what* the system does, not *how* it does it
internally, and it should fail only when actual behavior changes — never because of a harmless
refactor.

### Development workflow: TDD

We write code test-first, red-green-refactor:

1. **Red.** Write one failing test that expresses the behavior you're about to add, in the
   black-box style below. Run it and confirm it fails for the right reason (compile error or
   assertion failure), not for an unrelated one.
2. **Green.** Write the smallest amount of production code that makes it pass. Resist adding
   behavior the test doesn't demand yet — that's the next test's job.
3. **Refactor.** With the test green, clean up (naming, duplication, extracting a builder/ability/
   custom assertion) without changing behavior. Re-run the test after every refactor step.

This applies at every level: a new class in `core`, a new contract-test scenario in `testkit`, a
new R2DBC SPI method in `connector`. If you catch yourself writing production code with no failing
test driving it, stop and write the test first. Skeleton/plumbing code with no behavior yet
(module `build.gradle.kts`, `package-info.java`) is the explicit exception — there's nothing to
red-green there.

### Hard rules

1. **No Mockito. Ever.** Do not add it to the version catalog, do not add it to any module's
   dependencies. `verify(...)`/`when(...)` tests interaction with a library call, not business
   outcome, and they break on harmless refactors (`save()` → `saveAll()`) while missing real bugs
   (wrong field assigned, but `save()` was still called).
2. **Black-box over white-box.** Treat the unit under test (e.g. a service backed by an in-memory
   repository) as one black box. Assert on the resulting *state*, not on which internal method was
   called how many times.
3. **In-memory fakes instead of mocks.** For collaborators (repositories, caches, senders), write
   a small in-memory implementation (`ConcurrentHashMap`-backed is usually enough) of the real
   interface. It behaves like the real thing, is fast, and doesn't need "feeding" with
   `when(...).thenReturn(...)`.
4. **Given-When-Then structure, explicit comments.** Every test is visually split into `// given`,
   `// when`, `// then`, with `// and` for logically grouped sub-steps within a section. No
   exceptions for "small" tests — small tests are exactly where the discipline is cheapest to keep.
5. **No shared mutable variables between `given` and `then`.** Don't define `var expectedName =
   "Jan"` and reuse it in the assertion — if someone changes the value at the top, the assertion
   silently stays green while checking the wrong thing. Use literals directly in both places.
6. **No loops or conditionals inside a test body.** A `for`/`if` in a test means you're writing an
   algorithm that itself needs testing. Push iteration/searching into AssertJ (`extracting`,
   `filteredOn`, `containsExactly`, `anySatisfy`) or a custom assertion class.
7. **Don't assert against generated response/DTO classes for contract-sensitive tests.** An IDE
   rename of a DTO field will happily "fix" the test along with the production code, and the test
   keeps passing while the actual wire contract broke. For anything that represents a public
   contract (R2DBC result rows, wire format), assert against the raw/structural representation
   (e.g. inspect decoded values by name/position from the actual protocol response), not a
   generated class that gets refactored in lockstep with the code under test.
8. **Isolation is mandatory.** Every test starts from a clean, known state. In-memory fakes get
   cleared before each test (`@BeforeEach`). No test may depend on execution order or leftover
   state from another test.

## Test building blocks

Use these three building blocks together; they compose.

### Test Data Builder

Static factory + fluent `withX(...)` + sane defaults + `build()`. Only override what matters for
the scenario at hand.

```java
public final class QueryRequestTestBuilder {

    private String sql = "SELECT 1";
    private Duration timeout = Duration.ofSeconds(5);

    private QueryRequestTestBuilder() {}

    public static QueryRequestTestBuilder aQueryRequest() {
        return new QueryRequestTestBuilder();
    }

    public QueryRequestTestBuilder withSql(final String sql) {
        this.sql = sql;
        return this;
    }

    public QueryRequestTestBuilder withTimeout(final Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public QueryRequest build() {
        return new QueryRequest(sql, timeout);
    }
}
```

### Ability (test DSL)

An interface with default methods that hides technical setup behind domain-readable verbs. Test
classes implement one or more `*Ability` interfaces instead of wiring infrastructure by hand.

```java
public interface QueryRepositoryAbility {

    InMemoryQueryLog queryLog();

    default void thereIsAPendingQuery(final QueryRequestTestBuilder request) {
        queryLog().record(request.build());
    }
}
```

### Custom (tailor-made) assertion

An `AbstractAssert` subclass built on AssertJ, so failures read like domain statements instead of
`expected true but was false`.

```java
public final class QueryResultAssert extends AbstractAssert<QueryResultAssert, QueryResult> {

    private QueryResultAssert(final QueryResult actual) {
        super(actual, QueryResultAssert.class);
    }

    public static QueryResultAssert assertThatResult(final QueryResult actual) {
        return new QueryResultAssert(actual);
    }

    public QueryResultAssert hasRowCount(final int expected) {
        isNotNull();
        assertThat(actual.rows()).hasSize(expected);
        return this;
    }

    public QueryResultAssert wasCancelled() {
        isNotNull();
        assertThat(actual.cancelled()).isTrue();
        return this;
    }
}
```

### Putting it together

```java
class QueryCancellationTest extends BaseUnitTest implements QueryRepositoryAbility {

    @Test
    void shouldReleaseBuffersWhenCancelledDuringStreaming() {
        // given
        thereIsAPendingQuery(aQueryRequest().withSql("SELECT * FROM events"));

        // when
        final var result = executor.executeAndCancelAfterFirstRow();

        // then
        assertThatResult(result)
                .wasCancelled()
                .hasRowCount(1);
    }
}
```

`BaseUnitTest` (per module, where needed) owns the shared in-memory fakes and clears them in
`@BeforeEach`, so `given`, `when`, and `then` all operate on the same single source of truth.

## Test types and tools

| Level | Where | Tooling |
| --- | --- | --- |
| Unit tests | every module, `src/test/java` | JUnit 5 + AssertJ + in-memory fakes. No Spring, no containers, no Mockito. |
| Transport contract tests | `clickhouse-r2dbc-reactive-transport-http`, using `clickhouse-r2dbc-reactive-testkit` | Controlled local server from `testkit`: delayed headers/body, fragmented rows, slow subscriber, pool saturation, cancellation at every stage (see README's testing strategy). |
| Integration tests | `clickhouse-r2dbc-reactive-connector` | Testcontainers with a real ClickHouse instance. No mocking of ClickHouse itself. |

`clickhouse-r2dbc-reactive-testkit` exists specifically so transport/connector contract tests
don't need Mockito or ad-hoc test servers duplicated per module — it is the one place that owns
the controlled local server and shared test infrastructure.

## Formatting and CI gates

Already wired in the root `build.gradle.kts`, modeled on
[`spring-reactive-transaction-boundary`](https://github.com/CamilYed/spring-reactive-transaction-boundary):

- **Spotless** (Google Java Format) — `./gradlew spotlessCheck` / `./gradlew spotlessApply`.
  Every `java-library` subproject gets this automatically; new modules don't need to opt in
  manually.
- **JaCoCo** — coverage report generated after every test run, wired into the SonarCloud
  properties in the root build file.
- **SonarQube/SonarCloud** — `./gradlew sonar` (CI runs this when `SONAR_TOKEN` is set).
- CI (`.github/workflows/ci.yml`) runs `spotlessCheck`, `clean build`, `jacocoTestReport`, and the
  Sonar analysis on every push/PR.

Before opening a PR: `./gradlew spotlessCheck clean build` must pass locally.

## Performance testing

Not now. Functional correctness, the transport contract-test matrix, and the ClickHouse
integration test matrix come first. Performance/benchmark work (throughput, p50/p95/p99 latency,
time to first row, allocation/memory under streaming, many-small-request workloads vs. large
streaming results — see README's "Performance and dependency impact" section) is a later, separate
phase once the driver is functionally solid. Don't add benchmarking infrastructure or dependencies
until that phase actually starts.

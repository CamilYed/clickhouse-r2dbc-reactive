# ClickHouse R2DBC Reactive — review i plan release 0.2.0

**Repozytorium:** `CamilYed/clickhouse-r2dbc-reactive`
**Stan przeglądu:** 2026-08-14
**Proponowany następny release:** `0.2.0 — Operational Control & R2DBC Correctness`

> Zewnętrzny przegląd kodu `0.1.0`, przyjęty jako źródło Phase 7 w [ROADMAP.md](../ROADMAP.md#phase-7--operational-control--r2dbc-correctness-020).
> Każde konkretne twierdzenie techniczne poniżej (stan `ClickHouseResult`/`ClickHouseRow`/
> `setStatementTimeout`/`Statement.add()`) zostało zweryfikowane względem rzeczywistego kodu przed
> przepisaniem do ROADMAP.md — ten plik jest zachowany w całości jako oryginalne źródło, ROADMAP.md
> jest wersją roboczą/aktualną tego planu.

## 1. Ocena ogólna

Projekt jest już znacznie dalej niż typowy proof of concept.

Obecnie ma między innymi:

- implementację głównych elementów R2DBC SPI,
- transport oparty o Reactor Netty,
- streaming wyników bez materializowania całego response body,
- streaming INSERT,
- cancellation z best-effort `KILL QUERY`,
- retry ograniczony do bezpieczniejszej fazy pre-send,
- Testcontainers z prawdziwym ClickHouse,
- kontrolowany transport testowy,
- benchmarki JMH i porównanie z oficjalnym klientem Java,
- optymalizację reprezentacji dekodowanego wiersza po wynikach benchmarków,
- publikację `0.1.0` do Maven Central.

Największą wartością kolejnego release'u nie byłoby dokładanie wielu nowych typów ClickHouse ani kolejnego transportu. Najpierw warto domknąć zachowanie produkcyjne: limity puli, timeouty, kontrakty R2DBC, obserwowalność i compatibility testing.

---

# 2. Najważniejsze braki

## P0 — powinno wejść do 0.2.0

### 2.1. Jawna konfiguracja wewnętrznej puli Reactor Netty

README bardzo dobrze opisuje problem dwóch warstw poolingu:

1. opcjonalny `io.r2dbc.pool.ConnectionPool`,
2. wewnętrzny Reactor Netty `ConnectionProvider`, który naprawdę posiada sockety HTTP.

Jednocześnie aktualna konfiguracja przez standardowy URL R2DBC nie udostępnia pełnego sterowania tą drugą warstwą.

W kodzie istnieje możliwość podania `maxConnections` w części konstruktorów transportu, ale standardowa ścieżka:

```text
r2dbc:clickhouse://...
```

nie wystawia kompletnego zestawu limitów.

Brakuje przede wszystkim:

- `transportMaxConnections`,
- `transportPendingAcquireMaxCount`,
- `transportPendingAcquireTimeout`,
- `transportMaxIdleTime`,
- `transportMaxLifeTime`.

To powinien być jeden z głównych punktów 0.2.0.

### Dlaczego to ważne

Genezą projektu jest między innymi uniknięcie sytuacji, w której reaktywna aplikacja ma z pozoru kontrolowany R2DBC pool, ale niżej znajduje się kolejna niewidoczna kolejka requestów.

Jeżeli wewnętrzny `ConnectionProvider` pozostaje na defaultach Reactor Netty, użytkownik nie kontroluje w pełni:

- liczby fizycznych połączeń,
- wielkości kolejki oczekujących,
- czasu oczekiwania na socket,
- recyklingu połączeń.

To jest bardziej istotne produkcyjnie niż dodanie kolejnych typów ClickHouse.

### Proponowana implementacja

Wprowadzić jeden obiekt:

```java
record TransportPoolOptions(
        int maxConnections,
        int pendingAcquireMaxCount,
        Duration pendingAcquireTimeout,
        Duration maxIdleTime,
        Duration maxLifeTime
) {}
```

Nazwy publicznych opcji R2DBC warto prefiksować `transport...`, aby nie myliły się z:

```properties
spring.r2dbc.pool.max-size
```

Przykład:

```text
r2dbc:clickhouse://localhost:8123/default
    ?transportMaxConnections=16
    &transportPendingAcquireMaxCount=64
    &transportPendingAcquireTimeout=2s
    &transportMaxIdleTime=30s
    &transportMaxLifeTime=10m
```

Następnie wszystkie ścieżki tworzenia `ConnectionProvider` powinny przechodzić przez jeden builder/factory.

### Acceptance criteria

- wszystkie opcje dostępne przez `ConnectionFactoryOptions`,
- wszystkie opcje dostępne przez URL R2DBC,
- walidacja zakresów przy tworzeniu factory,
- brak cichego fallbacku dla błędnej wartości,
- test pokazujący odrzucenie requestu po przekroczeniu `pendingAcquireMaxCount`,
- test `pendingAcquireTimeout`,
- test maksymalnej liczby aktywnych połączeń,
- dokumentacja zależności outer R2DBC pool vs inner transport pool.

---

## 2.2. `Connection.setStatementTimeout(Duration)`

Aktualnie `ClickHouseConnection.setStatementTimeout(...)` zwraca `UnsupportedOperationException`.

ClickHouse ma ustawienie serwerowe:

```text
max_execution_time
```

więc istnieje naturalne mapowanie semantyki R2DBC na ClickHouse.

### Propozycja

Dodać:

```java
connection.setStatementTimeout(Duration.ofSeconds(5));
```

i przekazywać odpowiednie ustawienie do zapytania.

Dodatkowo można mieć default konfiguracyjny:

```text
statementTimeout=5s
```

Nie utożsamiałbym tego z Reactor Netty `responseTimeout`.

To są różne rzeczy:

- `statementTimeout` — limit wykonania zapytania,
- `responseTimeout` — zachowanie transportu HTTP.

### Acceptance criteria

- timeout ustawiony na `Connection` jest dziedziczony przez nowe statementy,
- timeout można nadpisać tylko tam, gdzie SPI/driver na to pozwala,
- `Duration.ZERO` ma jasno zdefiniowaną semantykę,
- test z realnym ClickHouse i celowo długim zapytaniem,
- poprawne mapowanie wyjątku do R2DBC,
- dokumentacja różnicy między server execution timeout a network timeout.

---

## 2.3. Współdzielony stan konsumpcji `Result`

`ClickHouseResult` chroni wynik przed wielokrotnym skonsumowaniem przez `AtomicBoolean`.

Jednak wynik utworzony przez:

```java
result.filter(...)
```

powinien być widokiem tego samego `Result`, a nie posiadać niezależny stan konsumpcji.

Aktualna implementacja sama sygnalizuje ten problem w komentarzu/Javadoc.

### Propozycja

Wydzielić:

```java
final class ResultConsumption {
    private final AtomicBoolean consumed = new AtomicBoolean();

    void acquire() {
        if (!consumed.compareAndSet(false, true)) {
            throw new IllegalStateException("Result has already been consumed");
        }
    }
}
```

i przekazywać ten sam obiekt do:

- oryginalnego `Result`,
- wszystkich `filter(...)` views.

### Acceptance criteria

Poniższe kombinacje mają być jednoznacznie obsłużone:

```java
result.map(...);
result.getRowsUpdated();
```

```java
var filtered = result.filter(...);
filtered.map(...);
result.map(...);
```

```java
result.map(...);
result.filter(...);
```

Testy powinny sprawdzać zachowanie zgodne z R2DBC SPI.

---

## 2.4. Konwersje w `Row.get(..., Class<T>)`

Aktualna implementacja typed getterów w praktyce opiera się głównie na castowaniu.

Przykładowo:

```java
row.get("x", Long.class)
```

dla wartości zdekodowanej jako `Integer` może zakończyć się `ClassCastException`.

Dla własnego API to może być akceptowalne, ale dla sterownika R2DBC warto mieć kontrolowaną warstwę konwersji.

### Proponowany pierwszy zakres

Nie robić od razu „konwertujemy wszystko do wszystkiego”.

W 0.2.0 wystarczy dobrze przetestowana macierz:

#### Numeric

- Byte
- Short
- Integer
- Long
- Float
- Double
- BigInteger
- BigDecimal

Z:

- range checking,
- bez silent overflow,
- przewidywalnym błędem konwersji.

#### Pozostałe

Tam, gdzie semantyka jest jednoznaczna:

- String,
- UUID,
- LocalDate,
- LocalDateTime,
- Instant / OffsetDateTime zależnie od mapowania typu ClickHouse.

### Ważne

Konwersja nie może być rozsiana po `ClickHouseRow`.

Warto wydzielić:

```java
final class ClickHouseValueConverter
```

lub:

```java
interface ValueConverter
```

z testami tabelarycznymi.

---

## 2.5. `Statement.add()` — correctness first

`Statement.add()` jest nadal niewspierane.

Nie wiązałbym pierwszej implementacji z koniecznością tworzenia zoptymalizowanego multi-row SQL.

Najpierw warto poprawnie zaimplementować semantykę R2DBC.

### Proponowany model

Każde:

```java
statement.bind(...).add();
```

robi snapshot aktualnego zestawu bindings.

`execute()` wykonuje zestawy sekwencyjnie:

```java
Flux.fromIterable(bindingSets)
    .concatMap(this::executeBindingSet);
```

i emituje osobny `Result` zgodnie z kontraktem.

### Dlaczego `concatMap`

Na pierwszy release:

- przewidywalna kolejność,
- prosta semantyka błędu,
- brak nagłego zwiększenia concurrency,
- mniejsza powierzchnia problemów z częściowym powodzeniem.

Później można benchmarkować batching/coalescing.

Dla dużych insertów nadal powinno być rekomendowane:

```java
insertStreaming(...)
```

czyli dedykowana ścieżka wydajnościowa.

---

# 3. P1 — bardzo wartościowe dla 0.2.0

## 3.1. Observability jako część architektury, nie dodatek

ROADMAP wspomina metrics/logging, ale ten obszar nadal jest słabiej rozwinięty niż sam transport.

Warto dodać neutralne API obserwowalności, bez twardego uzależnienia core od Micrometera.

Przykład:

```java
interface DriverObservationListener {
    void queryStarted(QueryContext context);
    void queryCompleted(QueryContext context, QueryMetrics metrics);
    void queryFailed(QueryContext context, Throwable error);
    void queryCancelled(QueryContext context);
}
```

### Minimalne dane

- `query_id`,
- operation kind: SELECT / INSERT / DDL / OTHER,
- czas oczekiwania na połączenie,
- czas całego requestu,
- czas do first row,
- rows,
- bytes,
- retries,
- cancellation,
- timeout,
- active transport connections,
- pending transport acquisitions.

### Bezpieczeństwo

Domyślnie nie logować:

- pełnego SQL,
- wartości bindów,
- credentials.

Można logować hash/fingerprint zapytania.

### Moduły

Core:

```text
clickhouse-r2dbc-reactive
```

Opcjonalnie później:

```text
clickhouse-r2dbc-reactive-micrometer
```

---

## 3.2. Jawne ownership schedulera dekodera RowBinary

`RowBinaryDecoder` korzysta z `Schedulers.boundedElastic()` podczas tworzenia readera, ponieważ bridge do strumienia ma blokujący interfejs odczytu.

To wymaga bardzo świadomego ownership.

Najważniejsze pytanie do przetestowania:

> Czy każdy odczyt kolejnego wiersza na pewno wykonuje się poza Netty event-loop, także gdy downstream wysyła request z innego wątku?

Nie zakładałbym tego wyłącznie na podstawie miejsca, w którym powstaje reader.

### Co bym zrobił

Wariant docelowy:

- driver-owned bounded scheduler,
- konfigurowalna maksymalna liczba workerów,
- konfigurowalna kolejka,
- scheduler zamykany razem z zasobem, który go posiada,
- żadnego przypadkowego używania globalnego shared `boundedElastic`.

### Testy

Dodać test, który failuje, jeżeli blokujący read odbywa się na:

```text
reactor-http-nio-*
```

Dodatkowo test:

- slow subscriber,
- cancellation podczas decode,
- wiele równoległych dużych wyników,
- bounded memory.

Nie twierdzę, że obecna implementacja na pewno wykonuje row decode na event-loop. Twierdzę, że kontrakt schedulera powinien być jawny i przetestowany, a nie zależeć od incidental Reactor threading.

---

## 3.3. Oficjalny R2DBC TestKit / compatibility lane

Repo ma już dużo sensownych testów w stylu TCK.

Dodałbym osobny compatibility lane oparty o oficjalne narzędzia/testy R2DBC tam, gdzie mają zastosowanie.

Dla ClickHouse należy jawnie opisać wyjątki:

- transactions — unsupported by design,
- savepoints — unsupported by design,
- generated keys — jeżeli nie mają sensu w danym modelu,
- batch semantics — zależnie od etapu implementacji.

Celem nie jest „zielone 100% za wszelką cenę”.

Celem jest:

> wiemy dokładnie, gdzie zachowanie drivera odpowiada SPI, a gdzie ClickHouse świadomie nie posiada danej semantyki.

---

## 3.4. CI compatibility matrix

Aktualny CI jest sensowny dla bieżącego developmentu, ale przed szerszym użyciem warto rozdzielić:

### PR lane

Szybki:

- Java 21,
- Spotless,
- unit,
- integration,
- leak checks.

### Nightly / scheduled

Cięższy:

- minimalna wspierana wersja ClickHouse,
- aktualna stabilna/LTS,
- najnowsza wspierana,
- dodatkowy runtime JDK, jeśli deklarujesz kompatybilność,
- JMH smoke/regression,
- większe concurrency tests.

Nie robiłbym pełnego benchmarku JMH jako twardego gate dla każdego PR — szum pomiarowy da za dużo false positives.

---

## 3.5. Netty leak detection w testach

ROADMAP już wskazuje leak detection jako ważny temat.

Warto mieć osobny test task/lane uruchamiany z agresywnym leak detectorem Netty.

Szczególnie dla scenariuszy:

- cancellation,
- disconnect mid-response,
- decoder failure,
- timeout,
- retry,
- downstream cancellation po kilku rekordach.

To jest krytyczne dla drivera strumieniującego `ByteBuf`.

---

# 4. P2 — dobry kierunek po 0.2.0

## 4.1. Spring integration bez udawania, że generic `.bind()` działa

Obecny README uczciwie dokumentuje problem.

Spring `DatabaseClient` potrzebuje `BindMarkersFactory`, natomiast ClickHouse native placeholder zawiera typ:

```sql
{id:UInt64}
```

Generic R2DBC marker nie niesie tej informacji.

Nie próbowałbym „naprawiać” tego przez:

- składanie SQL z literalami,
- regex replacement,
- własne escaping hacks.

To łatwo psuje:

- bezpieczeństwo,
- typowanie,
- String/DateTime/Array/Tuple,
- query plan semantics.

### Lepszy kierunek

Osobny, opcjonalny moduł integracyjny dla Spring:

```text
clickhouse-r2dbc-reactive-spring
```

który nie udaje pełnej transparentności, tylko daje ergonomiczną ścieżkę dla natywnych ClickHouse placeholders.

Równolegle można rozważyć dyskusję/upstream contribution do Spring Framework/Data R2DBC, jeżeli da się zaproponować rozszerzalny mechanizm.

To raczej temat 0.3.x niż 0.2.0.

---

## 4.2. `Dynamic` i `Variant`

Są jawnie niewspierane.

Nie dawałbym im wysokiego priorytetu tylko po to, żeby zwiększyć listę typów.

Warto dodać dopiero gdy:

- pojawi się realny użytkownik/use case,
- mapping będzie stabilny,
- będzie jasny oczekiwany Java representation.

---

## 4.3. Retry po błędach serwera

Aktualna polityka pre-send retry jest konserwatywna i dzięki temu bezpieczniejsza.

Nie dodawałbym automatycznie retry dla:

- INSERT,
- DDL,
- mutating operations

na podstawie samego server error code.

Najpierw potrzebny byłby jawny model:

```text
RetrySafety / Idempotency
```

Na przykład:

```java
enum RetryMode {
    NONE,
    PRE_SEND_ONLY,
    IDEMPOTENT_READS
}
```

Dopiero później server-code-aware retry dla bezpiecznych przypadków.

---

## 4.4. Nowy transport TCP / HTTP2

Nie robiłbym tego teraz.

Aktualne benchmarki pokazują, że obecny pipeline HTTP + RowBinary jest już konkurencyjny.

Nowy transport ma sens dopiero wtedy, gdy profiler/benchmark pokaże konkretny bottleneck, którego nie da się usunąć w obecnej architekturze.

---

# 5. Rzeczy, które poprawiłbym w obecnym repo bez zmiany feature set

## 5.1. Usunąć drift między README i ROADMAP

README mówi już o opublikowanym `0.1.0`.

Wciąż jest tekst sugerujący breaking changes „before a 0.1.0 release”.

ROADMAP nadal zawiera starszy etap benchmarków, podczas gdy README pokazuje już wyniki 3-fork.

Drobne, ale ważne: dokumentacja release'u powinna mieć jedno źródło prawdy.

### Propozycja

- `README.md` — stan użytkowy i quick start,
- `ROADMAP.md` — tylko przyszłość + completed milestones,
- `CHANGELOG.md` — historia release'ów,
- `docs/architecture.md` — trwałe decyzje architektoniczne,
- `docs/benchmarks.md` — metodologia i wyniki benchmarków.

---

## 5.2. Ograniczyć constructor explosion w transporcie

`ClickHouseHttpTransport` już ma dużo wariantów konfiguracji.

Dodanie:

- pool settings,
- timeout settings,
- metrics,
- scheduler settings

może szybko zwielokrotnić konstruktory.

Przed dodaniem nowych opcji warto przejść na jeden config object.

Przykład:

```java
record ClickHouseTransportConfig(
        URI endpoint,
        Credentials credentials,
        TransportPoolOptions pool,
        TransportTimeoutOptions timeouts,
        RetryOptions retry,
        TlsOptions tls
) {}
```

To jest refactor uzasadniony bezpośrednio przez nowe wymagania — nie „sprzątanie dla sprzątania”.

---

## 5.3. Release automation

Aktualny release workflow publikuje artefakty do Maven Central.

Dodałbym po udanym publish:

- tag Git,
- GitHub Release,
- release notes z changeloga,
- checksums,
- w kolejnym kroku SBOM/provenance.

Dobrze, żeby wersja w Maven Central, tag i GitHub Release zawsze wskazywały ten sam commit.

---

# 6. Proponowany zakres `0.2.0`

## Nazwa

```text
0.2.0 — Operational Control & R2DBC Correctness
```

## Must have

1. Exposed Reactor Netty transport pool limits.
2. Statement timeout.
3. Shared `Result` consumption state.
4. Row typed conversions.
5. Correctness-first `Statement.add()`.
6. Netty leak-detection test lane.
7. R2DBC compatibility/TestKit lane.
8. Documentation of double-pool behaviour.

## Should have

9. Driver observability SPI.
10. Basic lifecycle logging with `query_id`.
11. Decoder scheduler ownership + event-loop safety tests.
12. CI compatibility matrix.
13. Release tag + GitHub Release automation.

## Could have

14. Optional Micrometer adapter.
15. Query fingerprint for logs/metrics.
16. Benchmark tuning of RowBinary response chunk demand.

---

# 7. Czego NIE wkładałbym do 0.2.0

Żeby release nie zamienił się w półroczny rewrite:

- native ClickHouse TCP transport,
- HTTP/2 tylko „bo może być szybsze”,
- pełne rozwiązanie Spring `DatabaseClient.bind()`,
- `Dynamic`,
- `Variant`,
- rozbudowane automatic retries dla zapisów,
- transaction emulation,
- wielki refactor modułów,
- własny ORM/query DSL.

0.2.0 powinno zwiększyć **przewidywalność produkcyjną**, nie powierzchnię funkcji.

---

# 8. Proponowana kolejność prac dla Claude

Każdy punkt osobny branch/PR. Bez poprawiania niezwiązanych fragmentów.

## PR 1 — Result consumption correctness

Scope:

- shared consumption state,
- testy `filter/map/getRowsUpdated`,
- żadnych innych refactorów.

## PR 2 — Row conversions

Scope:

- `ClickHouseValueConverter`,
- numeric conversions,
- range checks,
- test matrix.

## PR 3 — Statement.add()

Scope:

- binding snapshots,
- sequential execution,
- Result per binding set,
- error semantics,
- bez optymalizacji batch SQL.

## PR 4 — Statement timeout

Scope:

- `setStatementTimeout`,
- `max_execution_time`,
- integration test,
- exception mapping.

## PR 5 — Transport pool options

Scope:

- config object,
- R2DBC URL options,
- `ConnectionProvider.builder`,
- active/pending/acquire timeout tests.

## PR 6 — Decoder scheduler contract

Scope:

- jawny scheduler ownership,
- test event-loop safety,
- cancellation/leak tests.

## PR 7 — Observability

Scope:

- neutral listener/recorder,
- query lifecycle,
- pool acquisition timing,
- brak Micrometer dependency w core.

## PR 8 — R2DBC compatibility + CI matrix

Scope:

- TestKit/TCK lane,
- documented unsupported capabilities,
- ClickHouse compatibility matrix.

## PR 9 — Release/documentation

Scope:

- README/ROADMAP sync,
- CHANGELOG,
- migration notes z 0.1.0,
- tag/GitHub Release workflow.

---

# 9. Gotowe issue titles

1. `Expose Reactor Netty connection pool limits through R2DBC options`
2. `Implement R2DBC statement timeout using ClickHouse max_execution_time`
3. `Share Result consumption state across filtered Result views`
4. `Add controlled typed value conversions for Row.get(Class<T>)`
5. `Implement correctness-first Statement.add() binding sets`
6. `Make RowBinary decoder scheduler ownership explicit`
7. `Add Netty event-loop blocking and ByteBuf leak tests`
8. `Introduce transport/query observability SPI`
9. `Add official R2DBC compatibility test lane`
10. `Add ClickHouse/JDK compatibility matrix`
11. `Synchronize README, ROADMAP and benchmark status`
12. `Create Git tag and GitHub Release after Central publication`

---

# 10. Definition of Done dla 0.2.0

Release uznałbym za gotowy, gdy:

- [ ] użytkownik kontroluje fizyczny transport pool bez własnego konstruktora,
- [ ] pending queue transportu jest ograniczona i ma timeout,
- [ ] statement timeout działa na realnym ClickHouse,
- [ ] `Result` ma jednoznaczną single-consumption semantics,
- [ ] typed `Row.get` ma kontrolowane konwersje,
- [ ] `Statement.add()` działa poprawnie,
- [ ] cancellation/timeout/error nie zostawiają `ByteBuf` leaks,
- [ ] jest test chroniący Netty event loop przed blocking decode,
- [ ] wiadomo, które R2DBC compatibility cases są wspierane, a które świadomie nie,
- [ ] README opisuje outer R2DBC pool i inner transport pool,
- [ ] release ma changelog, tag i identyfikowalny commit,
- [ ] benchmark baseline jest zapisany, ale nie jest flaky PR gate.

---

# 11. Release notes — szkic

## clickhouse-r2dbc-reactive 0.2.0

This release focuses on production control and R2DBC correctness rather than expanding the protocol surface.

### Highlights

- configurable Reactor Netty transport pool limits,
- bounded pending connection acquisition,
- statement timeout support backed by ClickHouse execution limits,
- improved R2DBC `Result` single-consumption semantics,
- controlled typed value conversion in `Row`,
- support for multiple binding sets through `Statement.add()`,
- stronger event-loop and Netty buffer leak tests,
- expanded R2DBC compatibility coverage,
- clearer operational documentation for double-layer pooling.

### Why this release

The 0.1.x line established the fully reactive HTTP/RowBinary pipeline, streaming SELECT/INSERT, cancellation, retries and the initial production-oriented benchmark baseline.

0.2.0 makes those capabilities easier to operate predictably under load.

---

# 12. Zasady dla Claude podczas implementacji

Możesz wkleić poniższe jako stałą instrukcję przy kolejnych taskach:

```text
Work only on the requested issue.

Do not perform unrelated cleanup, renaming, package moves, formatting changes
outside touched code, or speculative refactoring.

Before changing production code:
1. identify the existing contract,
2. add or update focused tests that demonstrate the missing behavior,
3. implement the smallest change that satisfies them.

Preserve:
- end-to-end non-blocking transport semantics,
- bounded memory,
- cancellation propagation,
- single-use R2DBC semantics,
- existing public API unless the issue explicitly requires a change.

Never use:
- block(),
- join(),
- get(),
- Thread.sleep(),
- unbounded buffering,
- blocking HTTP clients,
- silent fallback after invalid configuration.

For any change touching Netty ByteBuf:
- test cancellation,
- error path,
- resource release.

For any change touching concurrency:
- make queue/limit ownership explicit,
- test saturation,
- test cancellation while pending.

Run the existing formatting, unit, integration and verification tasks before
considering the issue complete.

Do not optimize before correctness unless the issue is explicitly a benchmark task.
```

---

# 13. Najważniejsza decyzja architektoniczna na teraz

Gdybym miał wybrać tylko jeden obszar na kolejny release, byłby to:

> **Expose and test the real transport admission-control boundary.**

R2DBC `ConnectionPool` nie jest fizycznym HTTP connection poolem tego drivera.

To Reactor Netty `ConnectionProvider` decyduje o:

- fizycznych socketach,
- oczekujących requestach,
- acquire timeout,
- reuse.

Te limity powinny być publicznym, testowalnym kontraktem sterownika.

To najbardziej odróżni ten projekt od implementacji, która jest „reactive” tylko na poziomie typów `Publisher`, ale pod obciążeniem posiada niewidoczną i niekontrolowaną warstwę transportową.

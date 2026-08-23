---
layout: home

hero:
  name: clickhouse-r2dbc-reactive
  text: A fully reactive R2DBC driver for ClickHouse
  tagline: Non-blocking from the socket up — its own small Reactor Netty transport, not client-v2's blocking HTTP client.
  actions:
    - theme: brand
      text: Get started
      link: /guide/spring-boot
    - theme: alt
      text: Architecture
      link: /architecture/overview
    - theme: alt
      text: View on GitHub
      link: https://github.com/CamilYed/clickhouse-r2dbc-reactive

features:
  - title: Non-blocking transport
    details: Built on Reactor Netty from the socket up. Reuses client-v2's public row-decoding classes only — its HTTP client is confirmed blocking and never called.
  - title: Streaming, not buffering
    details: Response bodies are decoded and emitted as a bounded, backpressure-aware stream of rows, not aggregated into memory first.
  - title: Real cancellation
    details: Disposing a subscription tears down the connection and sends a best-effort KILL QUERY, not just a client-side unsubscribe.
  - title: R2DBC SPI, verified
    details: Runs the official R2DBC Technology Compatibility Kit against a real ClickHouse server, not just a compatibility claim.
  - title: Spring WebFlux ready
    details: A full hexagonal Spring Boot + WebFlux demo, including the DatabaseClient / BindMarkersFactory gap and how to work around it.
  - title: One physical pool
    details: A factory-owned Reactor Netty connection pool; R2DBC Connection objects are cheap logical handles over it. io.r2dbc.pool stays available as an explicit opt-in.
---

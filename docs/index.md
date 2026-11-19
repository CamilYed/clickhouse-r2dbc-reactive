---
layout: home

hero:
  name: clickhouse-r2dbc-reactive
  text: A fully reactive R2DBC driver for ClickHouse
  tagline: Non-blocking transport, streaming results, bounded backpressure, and real cancellation.
  actions:
    - theme: brand
      text: Get started
      link: /guide/spring-boot
    - theme: alt
      text: Configuration
      link: /reference/configuration
    - theme: alt
      text: GitHub
      link: https://github.com/CamilYed/clickhouse-r2dbc-reactive
features:
  - title: Reactive by design
    details: Reactor Netty handles network I/O without wrapping blocking HTTP calls in futures.
  - title: Streaming and bounded
    details: Rows flow on demand without aggregating the complete response in memory.
  - title: Cancellation that propagates
    details: Cancelling a subscription releases transport resources and attempts to stop the server query.
  - title: Standard R2DBC integration
    details: Use the R2DBC SPI directly or integrate it with Spring WebFlux and DatabaseClient.
---

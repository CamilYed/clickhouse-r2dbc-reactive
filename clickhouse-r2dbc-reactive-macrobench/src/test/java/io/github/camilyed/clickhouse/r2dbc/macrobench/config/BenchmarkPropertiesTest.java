package io.github.camilyed.clickhouse.r2dbc.macrobench.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BenchmarkPropertiesTest {

  @Test
  void shouldDefaultPoolSizeToEightWhenNotConfigured() {
    // given / when
    final BenchmarkProperties properties =
        new BenchmarkProperties("dual", 100_000, 100_000, null, false);

    // then
    assertThat(properties.poolSize()).isEqualTo(8);
  }

  @Test
  void shouldKeepAnExplicitlyConfiguredPoolSize() {
    // given / when
    final BenchmarkProperties properties =
        new BenchmarkProperties("dual", 100_000, 100_000, 32, false);

    // then
    assertThat(properties.poolSize()).isEqualTo(32);
  }

  @Test
  void shouldDefaultBackendToDualWhenBlank() {
    // given / when
    final BenchmarkProperties properties =
        new BenchmarkProperties("  ", 100_000, 100_000, 8, false);

    // then
    assertThat(properties.backend()).isEqualTo("dual");
  }

  @Test
  void shouldDefaultRowCountsToOneHundredThousandWhenNonPositive() {
    // given / when
    final BenchmarkProperties properties = new BenchmarkProperties("dual", 0, -1, 8, false);

    // then
    assertThat(properties.pointRows()).isEqualTo(100_000);
    assertThat(properties.analyticsRows()).isEqualTo(100_000);
  }

  @Test
  void shouldDefaultUnpinR2dbcPoolToFalse() {
    // given / when
    final BenchmarkProperties properties =
        new BenchmarkProperties("dual", 100_000, 100_000, 8, false);

    // then
    assertThat(properties.unpinR2dbcPool()).isFalse();
  }

  @Test
  void shouldKeepAnExplicitlyConfiguredUnpinR2dbcPool() {
    // given / when
    final BenchmarkProperties properties =
        new BenchmarkProperties("dual", 100_000, 100_000, 8, true);

    // then
    assertThat(properties.unpinR2dbcPool()).isTrue();
  }
}

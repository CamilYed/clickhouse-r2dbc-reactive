package io.github.camilyed.clickhouse.r2dbc.macrobench.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BackendTest {

  @Test
  void shouldParsePlainR2dbc() {
    // given
    final String value = "r2dbc";

    // when
    final Backend parsed = Backend.fromProperty(value);

    // then
    assertThat(parsed).isEqualTo(Backend.R2DBC);
  }

  @Test
  void shouldParseHyphenatedClientV2() {
    // given
    final String value = "client-v2";

    // when
    final Backend parsed = Backend.fromProperty(value);

    // then
    assertThat(parsed).isEqualTo(Backend.CLIENT_V2);
  }

  @Test
  void shouldParseDualCaseInsensitively() {
    // given
    final String value = "DUAL";

    // when
    final Backend parsed = Backend.fromProperty(value);

    // then
    assertThat(parsed).isEqualTo(Backend.DUAL);
  }

  @Test
  void shouldRejectAnUnknownBackendName() {
    // given
    final String value = "postgres";

    // when / then
    assertThatThrownBy(() -> Backend.fromProperty(value))
        .isInstanceOf(IllegalArgumentException.class);
  }
}

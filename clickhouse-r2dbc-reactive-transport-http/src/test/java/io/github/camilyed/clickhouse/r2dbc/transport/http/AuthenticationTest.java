package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AuthenticationTest {

  @Test
  void shouldRedactThePasswordInBasicAuthenticationToString() {
    // given
    final Authentication authentication = Authentication.basic("alice", "super-secret");

    // when
    final String text = authentication.toString();

    // then
    assertThat(text).contains("alice").doesNotContain("super-secret");
  }

  @Test
  void shouldRedactTheKeyInUserKeyAuthenticationToString() {
    // given
    final Authentication authentication = Authentication.userKey("alice", "super-secret-key");

    // when
    final String text = authentication.toString();

    // then
    assertThat(text).contains("alice").doesNotContain("super-secret-key");
  }

  @Test
  void shouldNotRedactAnythingForNoneAuthenticationToString() {
    // given
    final Authentication authentication = Authentication.none();

    // when
    final String text = authentication.toString();

    // then
    assertThat(text).isEqualTo("None[]");
  }
}

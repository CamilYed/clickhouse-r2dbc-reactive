package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SqlFingerprintTest {

  @Test
  void shouldProduceTheSameFingerprintForTheSameSqlText() {
    // given
    final String sql = "SELECT * FROM events WHERE id = {id:UInt32}";

    // when
    final SqlFingerprint first = SqlFingerprint.of(sql);
    final SqlFingerprint second = SqlFingerprint.of(sql);

    // then
    assertThat(first).isEqualTo(second);
  }

  @Test
  void shouldProduceDifferentFingerprintsForDifferentSqlText() {
    // when
    final SqlFingerprint first = SqlFingerprint.of("SELECT 1");
    final SqlFingerprint second = SqlFingerprint.of("SELECT 2");

    // then
    assertThat(first).isNotEqualTo(second);
  }

  @Test
  void shouldProduceDifferentFingerprintsForWhitespaceOnlyDifferences() {
    // when
    final SqlFingerprint first = SqlFingerprint.of("SELECT 1");
    final SqlFingerprint second = SqlFingerprint.of("SELECT  1");

    // then
    assertThat(first).isNotEqualTo(second);
  }

  @Test
  void shouldNeverContainTheOriginalSqlText() {
    // given
    final String sql = "SELECT password FROM secrets";

    // when
    final SqlFingerprint fingerprint = SqlFingerprint.of(sql);

    // then
    assertThat(fingerprint.value()).doesNotContain("password", "secrets", "SELECT");
  }

  @Test
  void shouldExposeTheFingerprintAsItsStringRepresentation() {
    // given
    final SqlFingerprint fingerprint = SqlFingerprint.of("SELECT 1");

    // when / then
    assertThat(fingerprint.toString()).isEqualTo(fingerprint.value());
  }

  @Test
  void shouldRejectAnEmptyValue() {
    // when / then
    assertThatThrownBy(() -> new SqlFingerprint("")).isInstanceOf(IllegalArgumentException.class);
  }
}

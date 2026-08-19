package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TransportOptionsTest {

  @Test
  void shouldHaveNoPoolOptionsConfiguredByDefault() {
    // when
    final TransportOptions options = TransportOptions.defaults();

    // then
    assertThat(options.maxConnections()).isNull();
    assertThat(options.pendingAcquireMaxCount()).isNull();
    assertThat(options.pendingAcquireTimeout()).isNull();
    assertThat(options.maxIdleTime()).isNull();
    assertThat(options.maxLifeTime()).isNull();
  }

  @Test
  void shouldCarryAConfiguredMaxConnectionsUnchanged() {
    // when
    final TransportOptions options = TransportOptions.defaults().withMaxConnections(5);

    // then
    assertThat(options.maxConnections()).isEqualTo(5);
  }

  @Test
  void shouldRejectAZeroMaxConnections() {
    // given
    final TransportOptions options = TransportOptions.defaults();

    // when / then
    assertThatThrownBy(() -> options.withMaxConnections(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectANegativeMaxConnections() {
    // given
    final TransportOptions options = TransportOptions.defaults();

    // when / then
    assertThatThrownBy(() -> options.withMaxConnections(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldAcceptAPositivePendingAcquireMaxCount() {
    // when
    final TransportOptions options = TransportOptions.defaults().withPendingAcquireMaxCount(10);

    // then
    assertThat(options.pendingAcquireMaxCount()).isEqualTo(10);
  }

  @Test
  void shouldAcceptMinusOneAsAnUnboundedPendingAcquireMaxCount() {
    // when
    final TransportOptions options = TransportOptions.defaults().withPendingAcquireMaxCount(-1);

    // then
    assertThat(options.pendingAcquireMaxCount()).isEqualTo(-1);
  }

  @Test
  void shouldRejectAZeroPendingAcquireMaxCount() {
    // given
    final TransportOptions options = TransportOptions.defaults();

    // when / then
    assertThatThrownBy(() -> options.withPendingAcquireMaxCount(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectAPendingAcquireTimeoutLessThanZero() {
    // given
    final TransportOptions options = TransportOptions.defaults();

    // when / then
    assertThatThrownBy(() -> options.withPendingAcquireTimeout(Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectANegativeMaxIdleTime() {
    // given
    final TransportOptions options = TransportOptions.defaults();

    // when / then
    assertThatThrownBy(() -> options.withMaxIdleTime(Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectANegativeMaxLifeTime() {
    // given
    final TransportOptions options = TransportOptions.defaults();

    // when / then
    assertThatThrownBy(() -> options.withMaxLifeTime(Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}

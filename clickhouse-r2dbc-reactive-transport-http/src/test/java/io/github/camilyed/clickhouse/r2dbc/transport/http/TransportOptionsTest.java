package io.github.camilyed.clickhouse.r2dbc.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
    final Duration negativeDuration = Duration.ofSeconds(-1);

    // when / then
    assertThatThrownBy(() -> options.withPendingAcquireTimeout(negativeDuration))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectANegativeMaxIdleTime() {
    // given
    final TransportOptions options = TransportOptions.defaults();
    final Duration negativeDuration = Duration.ofSeconds(-1);

    // when / then
    assertThatThrownBy(() -> options.withMaxIdleTime(negativeDuration))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectANegativeMaxLifeTime() {
    // given
    final TransportOptions options = TransportOptions.defaults();
    final Duration negativeDuration = Duration.ofSeconds(-1);

    // when / then
    assertThatThrownBy(() -> options.withMaxLifeTime(negativeDuration))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldBeEqualToItself() {
    // given
    final TransportOptions options = aFullyPopulatedTransportOptions();

    // when / then
    assertThat(options).isEqualTo(options);
  }

  @Test
  void shouldNotBeEqualToNull() {
    // given
    final TransportOptions options = aFullyPopulatedTransportOptions();

    // when / then
    assertThat(options).isNotEqualTo(null);
  }

  @Test
  void shouldNotBeEqualToAValueOfADifferentType() {
    // given
    final TransportOptions options = aFullyPopulatedTransportOptions();

    // when / then
    assertThat(options).isNotEqualTo("not a TransportOptions");
  }

  @Test
  void shouldBeEqualWhenEveryFieldMatchesEvenWithASeparateCertificateArrayInstance() {
    // given
    final TransportOptions first =
        aFullyPopulatedTransportOptions().withTrustedCertificatePem(new byte[] {1, 2, 3});
    final TransportOptions second =
        aFullyPopulatedTransportOptions().withTrustedCertificatePem(new byte[] {1, 2, 3});

    // when / then
    assertThat(first).isEqualTo(second);
  }

  @Test
  void shouldHaveTheSameHashCodeForEqualInstancesWithSeparateCertificateArrayInstances() {
    // given
    final TransportOptions first =
        aFullyPopulatedTransportOptions().withTrustedCertificatePem(new byte[] {1, 2, 3});
    final TransportOptions second =
        aFullyPopulatedTransportOptions().withTrustedCertificatePem(new byte[] {1, 2, 3});

    // when / then
    assertThat(first).hasSameHashCodeAs(second);
  }

  @Test
  void shouldIncludeTheCertificateContentInToString() {
    // given
    final TransportOptions options =
        aFullyPopulatedTransportOptions().withTrustedCertificatePem(new byte[] {1, 2, 3});

    // when / then
    assertThat(options.toString()).contains("trustedCertificatePem=[1, 2, 3]");
  }

  @ParameterizedTest
  @MethodSource("singleFieldMutations")
  void shouldNotBeEqualWhenExactlyOneFieldDiffers(final UnaryOperator<TransportOptions> mutation) {
    // given
    final TransportOptions base = aFullyPopulatedTransportOptions();

    // when
    final TransportOptions mutated = mutation.apply(base);

    // then
    assertThat(base).isNotEqualTo(mutated);
  }

  private static Stream<UnaryOperator<TransportOptions>> singleFieldMutations() {
    return Stream.of(
        options -> options.withAuthentication(Authentication.userKey("other-user", "other-key")),
        options -> options.withResponseTimeout(Duration.ofSeconds(99)),
        options -> options.withConnectTimeout(Duration.ofSeconds(99)),
        options -> options.withTrustedCertificatePem(new byte[] {9, 9, 9}),
        options -> options.withRetryPolicy(new RetryPolicy(9, Duration.ofMillis(9))),
        options -> options.withMaxConnections(999),
        options -> options.withPendingAcquireMaxCount(999),
        options -> options.withPendingAcquireTimeout(Duration.ofSeconds(999)),
        options -> options.withMaxIdleTime(Duration.ofSeconds(999)),
        options -> options.withMaxLifeTime(Duration.ofSeconds(999)));
  }

  private static TransportOptions aFullyPopulatedTransportOptions() {
    return TransportOptions.defaults()
        .withAuthentication(Authentication.basic("user", "password"))
        .withResponseTimeout(Duration.ofSeconds(5))
        .withConnectTimeout(Duration.ofSeconds(3))
        .withTrustedCertificatePem(new byte[] {1, 2, 3})
        .withRetryPolicy(new RetryPolicy(2, Duration.ofMillis(10)))
        .withMaxConnections(8)
        .withPendingAcquireMaxCount(16)
        .withPendingAcquireTimeout(Duration.ofSeconds(1))
        .withMaxIdleTime(Duration.ofSeconds(30))
        .withMaxLifeTime(Duration.ofMinutes(5));
  }
}

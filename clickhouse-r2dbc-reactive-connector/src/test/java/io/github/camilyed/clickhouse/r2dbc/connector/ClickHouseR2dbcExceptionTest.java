package io.github.camilyed.clickhouse.r2dbc.connector;

import static org.assertj.core.api.Assertions.assertThat;

import com.clickhouse.client.api.ServerException;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class ClickHouseR2dbcExceptionTest {

  @Test
  void shouldReturnTheSameInstanceWhenAlreadyAnR2dbcException() {
    // given
    final R2dbcException original = new R2dbcBadGrammarException("bad SQL");

    // when
    final R2dbcException wrapped = ClickHouseR2dbcException.wrap(original);

    // then
    assertThat(wrapped).isSameAs(original);
  }

  @Test
  void shouldUseTheServerExceptionsCodeAndMessageWhenFoundInTheCauseChain() {
    // given
    final ServerException serverException =
        new ServerException(60, "Table not found", 404, "query-id-1");
    final Throwable failure =
        new RuntimeException(
            "Failed to read header", new IOException("upstream error", serverException));

    // when
    final R2dbcException wrapped = ClickHouseR2dbcException.wrap(failure);

    // then
    assertThat(wrapped.getErrorCode()).isEqualTo(60);
    assertThat(wrapped.getMessage()).isEqualTo("Table not found");
    assertThat(wrapped.getCause()).isSameAs(failure);
  }

  @Test
  void shouldFallBackToAGenericErrorCodeWhenNoServerExceptionIsInTheChain() {
    // given
    final Throwable failure = new IOException("connection reset");

    // when
    final R2dbcException wrapped = ClickHouseR2dbcException.wrap(failure);

    // then
    assertThat(wrapped.getErrorCode()).isZero();
    assertThat(wrapped.getMessage()).isEqualTo("connection reset");
    assertThat(wrapped.getCause()).isSameAs(failure);
  }
}

package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class EmptyRowBinaryReaderTest {

  @Test
  void shouldHaveNoColumnsAndNoRows() {
    // given
    final var reader = new EmptyRowBinaryReader(new ByteArrayInputStream(new byte[0]));

    // when
    final boolean hasNext = reader.hasNext();

    // then
    assertThat(reader.columns()).isEmpty();
    assertThat(hasNext).isFalse();
  }

  @Test
  void shouldRejectNextRowValuesCall() {
    // given
    final var reader = new EmptyRowBinaryReader(new ByteArrayInputStream(new byte[0]));

    // when / then
    assertThatThrownBy(reader::nextRowValues).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldCloseTheUnderlyingStream() throws Exception {
    // given
    final var closeTrackingStream = new CloseTrackingInputStream();
    final var reader = new EmptyRowBinaryReader(closeTrackingStream);

    // when
    reader.close();

    // then
    assertThat(closeTrackingStream.wasClosed()).isTrue();
  }

  /** A no-byte input stream that remembers whether {@link #close()} was called. */
  private static final class CloseTrackingInputStream extends ByteArrayInputStream {

    private boolean closed;

    private CloseTrackingInputStream() {
      super(new byte[0]);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }

    boolean wasClosed() {
      return closed;
    }
  }
}

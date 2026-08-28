package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;

import com.clickhouse.data.ClickHouseColumn;
import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.io.ByteArrayInputStream;
import java.io.SequenceInputStream;
import org.junit.jupiter.api.Test;

class RowBinaryHeaderTest {

  @Test
  void shouldReadColumnNamesAndTypesFromAOneColumnHeader() throws Exception {
    // given
    final var in =
        new ByteArrayInputStream(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes());

    // when
    final RowBinaryHeader header = RowBinaryHeader.readFrom(in);

    // then
    assertThat(header.present()).isTrue();
    assertThat(header.columns()).extracting(ClickHouseColumn::getColumnName).containsExactly("1");
    assertThat(header.columns())
        .extracting(ClickHouseColumn::getOriginalTypeName)
        .containsExactly("UInt8");
  }

  @Test
  void shouldReportNoHeaderPresentWhenTheStreamEndsBeforeAnyHeaderByte() throws Exception {
    // given - an empty body, e.g. a DDL statement's response
    final var in = new ByteArrayInputStream(new byte[0]);

    // when
    final RowBinaryHeader header = RowBinaryHeader.readFrom(in);

    // then
    assertThat(header.present()).isFalse();
    assertThat(header.columns()).isEmpty();
  }

  @Test
  void shouldCaptureExactlyTheBytesConsumedWhileParsingTheHeader() throws Exception {
    // given
    final byte[] wireBytes = RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes();
    final var in = new ByteArrayInputStream(wireBytes);

    // when
    final RowBinaryHeader header = RowBinaryHeader.readFrom(in);

    // then - the row byte(s) after the header were not consumed, only the header itself
    final int rowByteCount = wireBytes.length - header.rawBytes().length;
    assertThat(rowByteCount).isEqualTo(1); // one UInt8 row byte
    assertThat(in.read()).isEqualTo(0x01); // the row byte is still there, unconsumed
  }

  @Test
  void shouldLetTheCapturedHeaderBytesBeReplayedFollowedByTheRemainingStream() throws Exception {
    // given
    final byte[] wireBytes = RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes();
    final var in = new ByteArrayInputStream(wireBytes);
    final RowBinaryHeader header = RowBinaryHeader.readFrom(in);

    // when - replay the captured header bytes, then continue from where parsing left off
    final var replayed = new SequenceInputStream(new ByteArrayInputStream(header.rawBytes()), in);
    final byte[] replayedInFull = replayed.readAllBytes();

    // then - byte-for-byte identical to the original, untouched wire bytes
    assertThat(replayedInFull).isEqualTo(wireBytes);
  }
}

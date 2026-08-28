package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class RowBinaryWireFormatTest {

  @Test
  void shouldReadASingleByteVarUInt() throws Exception {
    // given
    final var in = new ByteArrayInputStream(new byte[] {0x01});

    // when
    final long value = RowBinaryWireFormat.readVarUInt(in);

    // then
    assertThat(value).isEqualTo(1L);
  }

  @Test
  void shouldReadAMultiByteVarUIntWithContinuationBit() throws Exception {
    // given - 300 encodes as 0xAC 0x02 (LEB128)
    final var in = new ByteArrayInputStream(new byte[] {(byte) 0xAC, 0x02});

    // when
    final long value = RowBinaryWireFormat.readVarUInt(in);

    // then
    assertThat(value).isEqualTo(300L);
  }

  @Test
  void shouldReadAnEmptyLengthPrefixedString() throws Exception {
    // given
    final var in = new ByteArrayInputStream(new byte[] {0x00});

    // when
    final String value = RowBinaryWireFormat.readString(in);

    // then
    assertThat(value).isEmpty();
  }

  @Test
  void shouldReadAUtf8LengthPrefixedString() throws Exception {
    // given
    final var in = new ByteArrayInputStream(new byte[] {0x03, 'a', 'b', 'c'});

    // when
    final String value = RowBinaryWireFormat.readString(in);

    // then
    assertThat(value).isEqualTo("abc");
  }

  @Test
  void shouldReadALittleEndianSignedShort() throws Exception {
    // given - -1 as int16 little-endian
    final var in = new ByteArrayInputStream(new byte[] {(byte) 0xFF, (byte) 0xFF});

    // when
    final short value = RowBinaryWireFormat.readShortLE(in, new byte[2]);

    // then
    assertThat(value).isEqualTo((short) -1);
  }

  @Test
  void shouldReadALittleEndianSignedInt() throws Exception {
    // given - 258 as int32 little-endian
    final var in = new ByteArrayInputStream(new byte[] {0x02, 0x01, 0x00, 0x00});

    // when
    final int value = RowBinaryWireFormat.readIntLE(in, new byte[4]);

    // then
    assertThat(value).isEqualTo(258);
  }

  @Test
  void shouldReadALittleEndianSignedLong() throws Exception {
    // given - -1 as int64 little-endian (all 0xFF)
    final var in =
        new ByteArrayInputStream(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });

    // when
    final long value = RowBinaryWireFormat.readLongLE(in, new byte[8]);

    // then
    assertThat(value).isEqualTo(-1L);
  }

  @Test
  void shouldWidenAnUnsignedInt64LittleEndianValueToABigIntegerBeyondLongRange() throws Exception {
    // given - UInt64 max value (2^64 - 1), all 0xFF little-endian - not representable as a signed
    // long
    final var in =
        new ByteArrayInputStream(
            new byte[] {
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
              (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });

    // when
    final BigInteger value = RowBinaryWireFormat.readUnsignedBigIntegerLE(in, new byte[8], 8);

    // then
    assertThat(value).isEqualTo(new BigInteger("18446744073709551615"));
  }

  @Test
  void shouldReadASignedBigIntegerPreservingNegativeValues() throws Exception {
    // given - -1 as a 16-byte little-endian two's complement value (all 0xFF)
    final byte[] allOnes = new byte[16];
    java.util.Arrays.fill(allOnes, (byte) 0xFF);
    final var in = new ByteArrayInputStream(allOnes);

    // when
    final BigInteger value = RowBinaryWireFormat.readSignedBigIntegerLE(in, new byte[16], 16);

    // then
    assertThat(value).isEqualTo(BigInteger.valueOf(-1));
  }

  @Test
  void shouldThrowEofExceptionWhenTheStreamEndsMidValue() {
    // given
    final var in = new ByteArrayInputStream(new byte[] {0x00});

    // when / then
    assertThatThrownBy(() -> RowBinaryWireFormat.readIntLE(in, new byte[4]))
        .isInstanceOf(EOFException.class);
  }
}

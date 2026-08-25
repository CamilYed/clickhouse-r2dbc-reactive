package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class ColumnDecoderTest {

  private final byte[] scratch = new byte[32];

  @Test
  void shouldDecodeInt8AsASignedByte() throws Exception {
    // given
    final var in = new ByteArrayInputStream(new byte[] {(byte) 0xFF});

    // when
    final Object value = ScalarColumnDecoder.INT8.decode(in, scratch);

    // then
    assertThat(value).isInstanceOf(Byte.class).isEqualTo((byte) -1);
  }

  @Test
  void shouldDecodeUInt8AsAWidenedShort() throws Exception {
    // given - 255, would be -1 as a signed byte
    final var in = new ByteArrayInputStream(new byte[] {(byte) 0xFF});

    // when
    final Object value = ScalarColumnDecoder.UINT8.decode(in, scratch);

    // then
    assertThat(value).isInstanceOf(Short.class).isEqualTo((short) 255);
  }

  @Test
  void shouldDecodeUInt16AsAWidenedInteger() throws Exception {
    // given - 65535 little-endian, would be -1 as a signed short
    final var in = new ByteArrayInputStream(new byte[] {(byte) 0xFF, (byte) 0xFF});

    // when
    final Object value = ScalarColumnDecoder.UINT16.decode(in, scratch);

    // then
    assertThat(value).isInstanceOf(Integer.class).isEqualTo(65535);
  }

  @Test
  void shouldDecodeUInt32AsAWidenedLong() throws Exception {
    // given - 4294967295 little-endian, would be -1 as a signed int
    final var in =
        new ByteArrayInputStream(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    // when
    final Object value = ScalarColumnDecoder.UINT32.decode(in, scratch);

    // then
    assertThat(value).isInstanceOf(Long.class).isEqualTo(4294967295L);
  }

  @Test
  void shouldDecodeUInt64AsAnUnsignedBigIntegerBeyondLongRange() throws Exception {
    // given - UInt64 max value
    final byte[] allOnes = new byte[8];
    java.util.Arrays.fill(allOnes, (byte) 0xFF);
    final var in = new ByteArrayInputStream(allOnes);

    // when
    final Object value = ScalarColumnDecoder.UINT64.decode(in, scratch);

    // then
    assertThat(value)
        .isInstanceOf(BigInteger.class)
        .isEqualTo(new BigInteger("18446744073709551615"));
  }

  @Test
  void shouldDecodeBoolAsTrueForByteValueOne() throws Exception {
    // given
    final var in = new ByteArrayInputStream(new byte[] {0x01});

    // when
    final Object value = ScalarColumnDecoder.BOOL.decode(in, scratch);

    // then
    assertThat(value).isEqualTo(true);
  }

  @Test
  void shouldDecodeStringAsAVarintLengthPrefixedUtf8Value() throws Exception {
    // given
    final var in = new ByteArrayInputStream(new byte[] {0x02, 'h', 'i'});

    // when
    final Object value = ScalarColumnDecoder.STRING.decode(in, scratch);

    // then
    assertThat(value).isEqualTo("hi");
  }

  @Test
  void shouldDecodeADecimal64BackedByALongReconstructedWithScale() throws Exception {
    // given - 123456 as an int64 little-endian, scale 2 -> 1234.56
    final var in =
        new ByteArrayInputStream(
            new byte[] {0x40, (byte) 0xE2, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00});

    // when
    final Object value = new DecimalColumnDecoder(8, 2).decode(in, scratch);

    // then
    assertThat(value).isInstanceOf(BigDecimal.class);
    assertThat((BigDecimal) value).isEqualByComparingTo("1234.56");
  }

  @Test
  void shouldDecodeADecimal32BackedByAnIntReconstructedWithScale() throws Exception {
    // given - 12345 as an int32 little-endian, scale 2 -> 123.45
    final var in = new ByteArrayInputStream(new byte[] {0x39, 0x30, 0x00, 0x00});

    // when
    final Object value = new DecimalColumnDecoder(4, 2).decode(in, scratch);

    // then
    assertThat((BigDecimal) value).isEqualByComparingTo("123.45");
  }

  @Test
  void shouldDecodeAWideDecimal128BackedByABigInteger() throws Exception {
    // given - -100 as a 16-byte little-endian two's complement value, scale 0
    final byte[] negativeOneHundred = new byte[16];
    java.util.Arrays.fill(negativeOneHundred, (byte) 0xFF);
    negativeOneHundred[0] = (byte) 0x9C; // low byte of -100 two's complement
    final var in = new ByteArrayInputStream(negativeOneHundred);

    // when
    final Object value = new DecimalColumnDecoder(16, 0).decode(in, scratch);

    // then
    assertThat((BigDecimal) value).isEqualByComparingTo("-100");
  }

  @Test
  void shouldDecodeAFixedStringOfExactlyItsDeclaredLength() throws Exception {
    // given - FixedString(5) storing "hi" padded with three trailing NUL bytes
    final byte[] fixedStringBytes = {'h', 'i', 0x00, 0x00, 0x00};
    final var in = new ByteArrayInputStream(fixedStringBytes);

    // when
    final Object value = new FixedStringColumnDecoder(5).decode(in, scratch);

    // then - decodes the exact same 5 bytes UTF-8 gave it, byte-for-byte
    assertThat(value)
        .isEqualTo(new String(fixedStringBytes, java.nio.charset.StandardCharsets.UTF_8));
  }
}

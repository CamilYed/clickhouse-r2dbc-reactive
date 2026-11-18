package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedResult;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

final class IncrementalRowBinaryDecoder {

  private IncrementalRowBinaryDecoder() {}

  static Flux<RowBinaryDecodeSignal> decode(final Flux<ByteBuffer> source) {
    return Flux.defer(
        () -> {
          final Parser parser = new Parser();
          return source.concatMap(parser::decode, 1).concatWith(Flux.defer(parser::finish));
        });
  }

  static Mono<DecodedResult> decodeResult(final Flux<ByteBuffer> source) {
    return Mono.create(
        sink -> {
          final DecodedResultHandoff handoff = new DecodedResultHandoff(sink);
          sink.onCancel(handoff::cancel);
          decode(source).subscribe(handoff);
        });
  }

  private static final class DecodedResultHandoff
      implements Subscriber<RowBinaryDecodeSignal>, Publisher<DecodedRow>, Subscription {

    private final MonoSink<DecodedResult> resultSink;
    private final AtomicBoolean rowsSubscribed = new AtomicBoolean();

    private volatile @Nullable Subscription upstream;
    private volatile @Nullable Subscriber<? super DecodedRow> rowsDownstream;
    private volatile boolean schemaReceived;
    private volatile boolean cancelled;

    private DecodedResultHandoff(final MonoSink<DecodedResult> resultSink) {
      this.resultSink = resultSink;
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
      if (upstream != null) {
        subscription.cancel();
        return;
      }
      upstream = subscription;
      if (cancelled) {
        subscription.cancel();
      } else {
        subscription.request(1);
      }
    }

    @Override
    public void onNext(final RowBinaryDecodeSignal signal) {
      if (!schemaReceived && signal instanceof RowBinaryDecodeSignal.Schema schema) {
        schemaReceived = true;
        resultSink.success(new DecodedResult(schema.columns(), Flux.from(this)));
        return;
      }
      if (signal instanceof RowBinaryDecodeSignal.Row row) {
        final Subscriber<? super DecodedRow> downstream = rowsDownstream;
        if (downstream == null) {
          cancel();
          resultSink.error(new IllegalStateException("Row arrived before rows were subscribed"));
        } else {
          downstream.onNext(row.value());
        }
        return;
      }
      cancel();
      resultSink.error(new IllegalStateException("RowBinary schema was emitted more than once"));
    }

    @Override
    public void onError(final Throwable failure) {
      final Subscriber<? super DecodedRow> downstream = rowsDownstream;
      if (!schemaReceived || downstream == null) {
        resultSink.error(failure);
      } else {
        downstream.onError(failure);
      }
    }

    @Override
    public void onComplete() {
      final Subscriber<? super DecodedRow> downstream = rowsDownstream;
      if (!schemaReceived) {
        resultSink.success(new DecodedResult(List.of(), Flux.empty()));
      } else if (downstream != null) {
        downstream.onComplete();
      }
    }

    @Override
    public void subscribe(final Subscriber<? super DecodedRow> subscriber) {
      if (!rowsSubscribed.compareAndSet(false, true)) {
        subscriber.onSubscribe(EmptySubscription.INSTANCE);
        subscriber.onError(new IllegalStateException("Decoded rows can only be consumed once"));
        return;
      }
      rowsDownstream = subscriber;
      subscriber.onSubscribe(this);
    }

    @Override
    public void request(final long demand) {
      if (demand <= 0) {
        cancel();
        final Subscriber<? super DecodedRow> downstream = rowsDownstream;
        if (downstream != null) {
          downstream.onError(new IllegalArgumentException("Demand must be positive"));
        }
        return;
      }
      final Subscription subscription = upstream;
      if (subscription != null) {
        subscription.request(demand);
      }
    }

    @Override
    public void cancel() {
      cancelled = true;
      final Subscription subscription = upstream;
      if (subscription != null) {
        subscription.cancel();
      }
    }
  }

  private enum EmptySubscription implements Subscription {
    INSTANCE;

    @Override
    public void request(final long demand) {}

    @Override
    public void cancel() {}
  }

  private enum Stage {
    COLUMN_COUNT,
    COLUMN_NAMES,
    COLUMN_TYPES,
    ROWS
  }

  private static final class Parser {

    private static final int MAX_COLUMN_COUNT = 65_536;
    private static final int MAX_HEADER_STRING_BYTES = 1024 * 1024;
    private static final int MAX_VALUE_BYTES = 16 * 1024 * 1024;

    private final VarUIntDecoder varUInt = new VarUIntDecoder();
    private final LengthPrefixedStringDecoder string =
        new LengthPrefixedStringDecoder(MAX_HEADER_STRING_BYTES);
    private final List<String> columnNames = new ArrayList<>();
    private final List<String> columnTypes = new ArrayList<>();

    private Stage stage = Stage.COLUMN_COUNT;
    private int columnCount;
    private int rowColumnIndex;
    private Object[] rowValues = new Object[0];
    private IncrementalValueDecoder[] columnDecoders = new IncrementalValueDecoder[0];

    private Flux<RowBinaryDecodeSignal> decode(final ByteBuffer chunk) {
      return Flux.generate(
          sink -> {
            final Optional<RowBinaryDecodeSignal> signal = decodeNext(chunk);
            if (signal.isPresent()) {
              sink.next(signal.get());
            } else {
              sink.complete();
            }
          });
    }

    private Optional<RowBinaryDecodeSignal> decodeNext(final ByteBuffer source) {
      while (source.hasRemaining()) {
        final Optional<RowBinaryDecodeSignal> signal =
            switch (stage) {
              case COLUMN_COUNT -> decodeColumnCount(source);
              case COLUMN_NAMES -> decodeColumnName(source);
              case COLUMN_TYPES -> decodeColumnType(source);
              case ROWS -> decodeRow(source);
            };
        if (signal.isPresent()) {
          return signal;
        }
      }
      return Optional.empty();
    }

    private Optional<RowBinaryDecodeSignal> decodeColumnCount(final ByteBuffer source) {
      final OptionalLong decoded = varUInt.decode(source);
      if (decoded.isEmpty()) {
        return Optional.empty();
      }
      final long value = decoded.getAsLong();
      if (value > MAX_COLUMN_COUNT) {
        throw new IllegalArgumentException("RowBinary column count exceeds limit: " + value);
      }
      columnCount = (int) value;
      rowValues = new Object[columnCount];
      columnDecoders = new IncrementalValueDecoder[columnCount];
      stage = columnCount == 0 ? Stage.ROWS : Stage.COLUMN_NAMES;
      if (columnCount == 0) {
        return Optional.of(new RowBinaryDecodeSignal.Schema(List.of()));
      }
      return Optional.empty();
    }

    private Optional<RowBinaryDecodeSignal> decodeColumnName(final ByteBuffer source) {
      final Optional<String> decoded = string.decode(source);
      decoded.ifPresent(columnNames::add);
      if (columnNames.size() == columnCount) {
        stage = Stage.COLUMN_TYPES;
      }
      return Optional.empty();
    }

    private Optional<RowBinaryDecodeSignal> decodeColumnType(final ByteBuffer source) {
      final Optional<String> decoded = string.decode(source);
      decoded.ifPresent(columnTypes::add);
      if (columnTypes.size() != columnCount) {
        return Optional.empty();
      }
      final List<ColumnDescriptor> columns = new ArrayList<>(columnCount);
      for (int index = 0; index < columnCount; index++) {
        final String type = columnTypes.get(index);
        columnDecoders[index] = decoderFor(type);
        columns.add(new ColumnDescriptor(columnNames.get(index), type));
      }
      stage = Stage.ROWS;
      return Optional.of(new RowBinaryDecodeSignal.Schema(columns));
    }

    private Optional<RowBinaryDecodeSignal> decodeRow(final ByteBuffer source) {
      final DecodeProgress decoded = columnDecoders[rowColumnIndex].decode(source);
      if (decoded == NeedMore.INSTANCE) {
        return Optional.empty();
      }
      rowValues[rowColumnIndex] = ((DecodedValue) decoded).value();
      rowColumnIndex++;
      if (rowColumnIndex != columnCount) {
        return Optional.empty();
      }
      final DecodedRow row = new DecodedRow(rowValues);
      rowValues = new Object[columnCount];
      rowColumnIndex = 0;
      return Optional.of(new RowBinaryDecodeSignal.Row(row));
    }

    private static IncrementalValueDecoder decoderFor(final String type) {
      if (type.startsWith("Nullable(") && type.endsWith(")")) {
        return new NullableDecoder(
            decoderFor(type.substring("Nullable(".length(), type.length() - 1)));
      }
      if ("Int32".equals(type)) {
        return new Int32Decoder();
      }
      return switch (type) {
        case "Int8" -> new FixedWidthDecoder(1, bytes -> bytes[0]);
        case "UInt8" -> new UInt8Decoder();
        case "Int16" -> new FixedWidthDecoder(2, bytes -> (short) readIntLE(bytes));
        case "UInt16" -> new FixedWidthDecoder(2, bytes -> readIntLE(bytes) & 0xFFFF);
        case "UInt32" -> new FixedWidthDecoder(4, bytes -> readIntLE(bytes) & 0xFFFFFFFFL);
        case "Int64" -> new FixedWidthDecoder(8, Parser::readLongLE);
        case "UInt64" -> new UInt64Decoder();
        case "Int128", "Int256" ->
            new FixedWidthDecoder(integerByteWidth(type), bytes -> new BigInteger(reverse(bytes)));
        case "UInt128", "UInt256" ->
            new FixedWidthDecoder(
                integerByteWidth(type), bytes -> new BigInteger(1, reverse(bytes)));
        case "Float32" -> new FixedWidthDecoder(4, bytes -> Float.intBitsToFloat(readIntLE(bytes)));
        case "Float64" ->
            new FixedWidthDecoder(8, bytes -> Double.longBitsToDouble(readLongLE(bytes)));
        case "Bool" -> new FixedWidthDecoder(1, bytes -> bytes[0] == 1);
        case "String" -> new StringDecoder(MAX_VALUE_BYTES);
        default -> decoderForParameterizedType(type);
      };
    }

    private static IncrementalValueDecoder decoderForParameterizedType(final String type) {
      if (type.startsWith("FixedString(") && type.endsWith(")")) {
        final int length =
            Integer.parseInt(type.substring("FixedString(".length(), type.length() - 1));
        if (length < 0 || length > MAX_VALUE_BYTES) {
          throw new IllegalArgumentException("FixedString length exceeds limit: " + length);
        }
        return new FixedWidthDecoder(length, bytes -> new String(bytes, StandardCharsets.UTF_8));
      }
      return DecimalDecoder.forType(type);
    }

    private static int integerByteWidth(final String type) {
      return type.endsWith("128") ? 16 : 32;
    }

    private static int readIntLE(final byte[] bytes) {
      int value = 0;
      for (int index = bytes.length - 1; index >= 0; index--) {
        value = (value << 8) | (bytes[index] & 0xFF);
      }
      return value;
    }

    private static long readLongLE(final byte[] bytes) {
      long value = 0;
      for (int index = bytes.length - 1; index >= 0; index--) {
        value = (value << 8) | (bytes[index] & 0xFFL);
      }
      return value;
    }

    private static byte[] reverse(final byte[] bytes) {
      final byte[] reversed = new byte[bytes.length];
      for (int index = 0; index < bytes.length; index++) {
        reversed[index] = bytes[bytes.length - 1 - index];
      }
      return reversed;
    }

    private Flux<RowBinaryDecodeSignal> finish() {
      if (stage == Stage.COLUMN_COUNT && varUInt.isIdle()) {
        return Flux.empty();
      }
      if (stage != Stage.ROWS || rowColumnIndex != 0 || currentColumnHasPartialValue()) {
        return Flux.error(
            new IllegalArgumentException("Truncated RowBinaryWithNamesAndTypes response"));
      }
      return Flux.empty();
    }

    private boolean currentColumnHasPartialValue() {
      return columnCount > 0 && !columnDecoders[rowColumnIndex].isIdle();
    }
  }

  private interface IncrementalValueDecoder {

    DecodeProgress decode(ByteBuffer source);

    boolean isIdle();
  }

  private sealed interface DecodeProgress permits DecodedValue, NeedMore {}

  private record DecodedValue(@Nullable Object value) implements DecodeProgress {}

  private enum NeedMore implements DecodeProgress {
    INSTANCE
  }

  private static final class UInt8Decoder implements IncrementalValueDecoder {

    @Override
    public DecodeProgress decode(final ByteBuffer source) {
      if (!source.hasRemaining()) {
        return NeedMore.INSTANCE;
      }
      return new DecodedValue((short) (source.get() & 0xFF));
    }

    @Override
    public boolean isIdle() {
      return true;
    }
  }

  private static final class StringDecoder implements IncrementalValueDecoder {

    private final LengthPrefixedStringDecoder delegate;

    private StringDecoder(final int maximumBytes) {
      delegate = new LengthPrefixedStringDecoder(maximumBytes);
    }

    @Override
    public DecodeProgress decode(final ByteBuffer source) {
      final Optional<String> decoded = delegate.decode(source);
      return decoded.<DecodeProgress>map(DecodedValue::new).orElse(NeedMore.INSTANCE);
    }

    @Override
    public boolean isIdle() {
      return delegate.isIdle();
    }
  }

  private static final class UInt64Decoder implements IncrementalValueDecoder {

    private final byte[] littleEndianBytes = new byte[Long.BYTES];
    private final byte[] bigEndianBytes = new byte[Long.BYTES];

    private int offset;

    @Override
    public DecodeProgress decode(final ByteBuffer source) {
      final int copied = Math.min(source.remaining(), littleEndianBytes.length - offset);
      source.get(littleEndianBytes, offset, copied);
      offset += copied;
      if (offset != littleEndianBytes.length) {
        return NeedMore.INSTANCE;
      }
      for (int index = 0; index < littleEndianBytes.length; index++) {
        bigEndianBytes[index] = littleEndianBytes[littleEndianBytes.length - 1 - index];
      }
      offset = 0;
      return new DecodedValue(new BigInteger(1, bigEndianBytes));
    }

    @Override
    public boolean isIdle() {
      return offset == 0;
    }
  }

  private static final class FixedWidthDecoder implements IncrementalValueDecoder {

    private final byte[] bytes;
    private final Function<byte[], Object> mapping;

    private int offset;

    private FixedWidthDecoder(final int width, final Function<byte[], Object> mapping) {
      bytes = new byte[width];
      this.mapping = mapping;
    }

    @Override
    public DecodeProgress decode(final ByteBuffer source) {
      final int copied = Math.min(source.remaining(), bytes.length - offset);
      source.get(bytes, offset, copied);
      offset += copied;
      if (offset != bytes.length) {
        return NeedMore.INSTANCE;
      }
      final Object value = mapping.apply(bytes);
      offset = 0;
      return new DecodedValue(value);
    }

    @Override
    public boolean isIdle() {
      return offset == 0;
    }
  }

  private static final class Int32Decoder implements IncrementalValueDecoder {

    private final byte[] bytes = new byte[Integer.BYTES];

    private int offset;

    @Override
    public DecodeProgress decode(final ByteBuffer source) {
      final int copied = Math.min(source.remaining(), bytes.length - offset);
      source.get(bytes, offset, copied);
      offset += copied;
      if (offset != bytes.length) {
        return NeedMore.INSTANCE;
      }
      final int value =
          (bytes[0] & 0xFF)
              | ((bytes[1] & 0xFF) << 8)
              | ((bytes[2] & 0xFF) << 16)
              | ((bytes[3] & 0xFF) << 24);
      offset = 0;
      return new DecodedValue(value);
    }

    @Override
    public boolean isIdle() {
      return offset == 0;
    }
  }

  private static final class NullableDecoder implements IncrementalValueDecoder {

    private final IncrementalValueDecoder delegate;

    private boolean valueFollows;

    private NullableDecoder(final IncrementalValueDecoder delegate) {
      this.delegate = delegate;
    }

    @Override
    public DecodeProgress decode(final ByteBuffer source) {
      if (!valueFollows) {
        if (!source.hasRemaining()) {
          return NeedMore.INSTANCE;
        }
        final int marker = source.get() & 0xFF;
        if (marker == 1) {
          return new DecodedValue(null);
        }
        if (marker != 0) {
          throw new IllegalArgumentException("Invalid RowBinary Nullable marker: " + marker);
        }
        valueFollows = true;
      }
      final DecodeProgress decoded = delegate.decode(source);
      if (decoded instanceof DecodedValue) {
        valueFollows = false;
      }
      return decoded;
    }

    @Override
    public boolean isIdle() {
      return !valueFollows && delegate.isIdle();
    }
  }

  private static final class DecimalDecoder implements IncrementalValueDecoder {

    private static final Pattern TYPE = Pattern.compile("Decimal\\((\\d+),\\s*(\\d+)\\)");
    private static final Pattern NAMED_TYPE = Pattern.compile("Decimal(32|64|128|256)\\((\\d+)\\)");

    private final byte[] littleEndianBytes;
    private final int scale;

    private int offset;

    private DecimalDecoder(final int byteWidth, final int scale) {
      littleEndianBytes = new byte[byteWidth];
      this.scale = scale;
    }

    private static DecimalDecoder forType(final String type) {
      final Matcher matcher = TYPE.matcher(type);
      if (matcher.matches()) {
        final int precision = Integer.parseInt(matcher.group(1));
        final int scale = Integer.parseInt(matcher.group(2));
        return new DecimalDecoder(byteWidthFor(precision), scale);
      }
      final Matcher namedMatcher = NAMED_TYPE.matcher(type);
      if (namedMatcher.matches()) {
        final int width = Integer.parseInt(namedMatcher.group(1)) / Byte.SIZE;
        final int scale = Integer.parseInt(namedMatcher.group(2));
        return new DecimalDecoder(width, scale);
      }
      throw new UnsupportedOperationException(
          "Incremental RowBinary decoder does not support type: " + type);
    }

    private static int byteWidthFor(final int precision) {
      if (precision <= 9) {
        return Integer.BYTES;
      }
      if (precision <= 18) {
        return Long.BYTES;
      }
      if (precision <= 38) {
        return 16;
      }
      if (precision <= 76) {
        return 32;
      }
      throw new UnsupportedOperationException(
          "Incremental RowBinary decoder does not support Decimal precision: " + precision);
    }

    @Override
    public DecodeProgress decode(final ByteBuffer source) {
      final int copied = Math.min(source.remaining(), littleEndianBytes.length - offset);
      source.get(littleEndianBytes, offset, copied);
      offset += copied;
      if (offset != littleEndianBytes.length) {
        return NeedMore.INSTANCE;
      }
      final byte[] bigEndianBytes = new byte[littleEndianBytes.length];
      for (int index = 0; index < littleEndianBytes.length; index++) {
        bigEndianBytes[index] = littleEndianBytes[littleEndianBytes.length - 1 - index];
      }
      offset = 0;
      return new DecodedValue(new BigDecimal(new BigInteger(bigEndianBytes), scale));
    }

    @Override
    public boolean isIdle() {
      return offset == 0;
    }
  }

  private static final class VarUIntDecoder {

    private long value;
    private int shift;

    private OptionalLong decode(final ByteBuffer source) {
      while (source.hasRemaining()) {
        final int current = source.get() & 0xFF;
        value |= (long) (current & 0x7F) << shift;
        if ((current & 0x80) == 0) {
          final long decoded = value;
          value = 0;
          shift = 0;
          return OptionalLong.of(decoded);
        }
        shift += 7;
        if (shift >= Long.SIZE) {
          throw new IllegalArgumentException("RowBinary VarUInt exceeds 64 bits");
        }
      }
      return OptionalLong.empty();
    }

    private boolean isIdle() {
      return shift == 0;
    }
  }

  private static final class LengthPrefixedStringDecoder {

    private final int maximumBytes;
    private final VarUIntDecoder length = new VarUIntDecoder();

    private byte[] bytes = new byte[0];
    private int offset;
    private boolean lengthDecoded;

    private LengthPrefixedStringDecoder(final int maximumBytes) {
      this.maximumBytes = maximumBytes;
    }

    private Optional<String> decode(final ByteBuffer source) {
      if (!lengthDecoded) {
        final OptionalLong decodedLength = length.decode(source);
        if (decodedLength.isEmpty()) {
          return Optional.empty();
        }
        final long value = decodedLength.getAsLong();
        if (value > maximumBytes) {
          throw new IllegalArgumentException("RowBinary string length exceeds limit: " + value);
        }
        bytes = new byte[(int) value];
        lengthDecoded = true;
      }
      final int copied = Math.min(source.remaining(), bytes.length - offset);
      source.get(bytes, offset, copied);
      offset += copied;
      if (offset != bytes.length) {
        return Optional.empty();
      }
      final String decoded = new String(bytes, StandardCharsets.UTF_8);
      bytes = new byte[0];
      offset = 0;
      lengthDecoded = false;
      return Optional.of(decoded);
    }

    private boolean isIdle() {
      return !lengthDecoded && length.isIdle();
    }
  }
}

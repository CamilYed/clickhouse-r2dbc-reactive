package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import reactor.core.publisher.Flux;

final class IncrementalRowBinaryDecoder {

  private IncrementalRowBinaryDecoder() {}

  static Flux<RowBinaryDecodeSignal> decode(final Flux<ByteBuffer> source) {
    return Flux.defer(
        () -> {
          final Parser parser = new Parser();
          return source.concatMap(parser::decode, 1).concatWith(Flux.defer(parser::finish));
        });
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

    private final VarUIntDecoder varUInt = new VarUIntDecoder();
    private final LengthPrefixedStringDecoder string =
        new LengthPrefixedStringDecoder(MAX_HEADER_STRING_BYTES);
    private final List<String> columnNames = new ArrayList<>();
    private final List<String> columnTypes = new ArrayList<>();

    private Stage stage = Stage.COLUMN_COUNT;
    private int columnCount;
    private int rowColumnIndex;
    private Object[] rowValues = new Object[0];

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
        if (!"UInt8".equals(type)) {
          throw new UnsupportedOperationException(
              "Incremental RowBinary decoder does not support type: " + type);
        }
        columns.add(new ColumnDescriptor(columnNames.get(index), type));
      }
      stage = Stage.ROWS;
      return Optional.of(new RowBinaryDecodeSignal.Schema(columns));
    }

    private Optional<RowBinaryDecodeSignal> decodeRow(final ByteBuffer source) {
      rowValues[rowColumnIndex] = (short) (source.get() & 0xFF);
      rowColumnIndex++;
      if (rowColumnIndex != columnCount) {
        return Optional.empty();
      }
      final DecodedRow row = new DecodedRow(rowValues);
      rowValues = new Object[columnCount];
      rowColumnIndex = 0;
      return Optional.of(new RowBinaryDecodeSignal.Row(row));
    }

    private Flux<RowBinaryDecodeSignal> finish() {
      if (stage == Stage.COLUMN_COUNT && columnNames.isEmpty() && columnTypes.isEmpty()) {
        return Flux.empty();
      }
      if (stage != Stage.ROWS || rowColumnIndex != 0) {
        return Flux.error(
            new IllegalArgumentException("Truncated RowBinaryWithNamesAndTypes response"));
      }
      return Flux.empty();
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
  }
}

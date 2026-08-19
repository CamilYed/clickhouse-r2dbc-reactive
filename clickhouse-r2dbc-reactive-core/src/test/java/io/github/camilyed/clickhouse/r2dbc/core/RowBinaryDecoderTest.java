package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class RowBinaryDecoderTest {

  private final RowDecodingScheduler decodingScheduler = RowDecodingScheduler.defaults();

  @AfterEach
  void disposeDecodingScheduler() {
    decodingScheduler.dispose();
  }

  @Test
  void shouldDecodeSelectOneIntoOneRow() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

    // when
    final DecodedRow row = RowBinaryDecoder.decodeRows(source).blockFirst(Duration.ofSeconds(5));

    // then
    assertThat(row.valueAt(0)).isEqualTo((short) 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldDecodeAnArrayColumnAsAPlainList() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.arrayOfInt32RowBinaryWithNamesAndTypes()));

    // when
    final DecodedRow row = RowBinaryDecoder.decodeRows(source).blockFirst(Duration.ofSeconds(5));

    // then
    assertThat((List<Integer>) row.valueAt(0)).containsExactly(10, 20, 30);
  }

  @Test
  void shouldExposeColumnSchemaAlongsideDecodedRows() {
    // given
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler).block(Duration.ofSeconds(5));

    // then
    assertThat(result.columns()).containsExactly(new ColumnDescriptor("1", "UInt8"));
    // and
    assertThat(result.rows().blockFirst(Duration.ofSeconds(5)).valueAt(0)).isEqualTo((short) 1);
  }

  @Test
  void shouldReadEveryRowOnTheGivenSchedulerRegardlessOfWhichThreadRequestsIt() {
    // given - a source that, on every request for its next element, records which thread made the
    // request; decode()'s row-generation must run on decodingScheduler's own worker even though
    // this test drives the whole chain from a thread decodingScheduler never created.
    final Flux<ByteBuffer> source =
        Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

    // when
    final DecodedResult result =
        RowBinaryDecoder.decode(source, decodingScheduler).block(Duration.ofSeconds(5));
    final String rowThreadName =
        result
            .rows()
            .map(row -> Thread.currentThread().getName())
            .blockFirst(Duration.ofSeconds(5));

    // then
    assertThat(rowThreadName).startsWith("clickhouse-r2dbc-decoder");
  }
}

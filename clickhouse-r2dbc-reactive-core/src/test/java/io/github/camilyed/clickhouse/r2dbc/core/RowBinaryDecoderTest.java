package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.camilyed.clickhouse.r2dbc.core.fakes.RowBinaryFixtures;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class RowBinaryDecoderTest {

    @Test
    void shouldDecodeSelectOneIntoOneRow() {
        // given
        final Flux<ByteBuffer> source =
                Flux.just(ByteBuffer.wrap(RowBinaryFixtures.selectOneRowBinaryWithNamesAndTypes()));

        // when
        final Map<String, Object> row = RowBinaryDecoder.decodeRows(source).blockFirst(Duration.ofSeconds(5));

        // then
        assertThat(row).containsEntry("1", (short) 1);
    }
}

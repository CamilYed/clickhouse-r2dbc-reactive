package io.github.camilyed.clickhouse.r2dbc.core;

import java.nio.ByteBuffer;

/** What can come out of the upstream {@code Flux<ByteBuffer>}: a chunk, completion, or an error. */
sealed interface StreamSignal {

  record Data(ByteBuffer buffer) implements StreamSignal {}

  record Complete() implements StreamSignal {}

  record Error(Throwable cause) implements StreamSignal {}
}

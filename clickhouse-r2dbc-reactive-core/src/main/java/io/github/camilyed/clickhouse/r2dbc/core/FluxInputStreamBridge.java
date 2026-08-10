package io.github.camilyed.clickhouse.r2dbc.core;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * Adapts a push-based {@code Flux<ByteBuffer>} into a pull-based, blocking {@link InputStream}.
 *
 * <p>Subscribes to the source with a fixed amount of outstanding demand and never requests more
 * than that; the queue backing this bridge therefore never holds more items than the demand
 * allows, so the thread delivering {@code onNext} (typically a Netty event loop thread) is never
 * blocked. Only the thread calling {@link #read()}/{@link #read(byte[], int, int)} blocks — that
 * must be a dedicated worker thread, never the event loop.
 */
public final class FluxInputStreamBridge extends InputStream {

    private final BlockingQueue<StreamSignal> queue;
    private final QueueingSubscriber subscriber;

    private ByteBuffer current = ByteBuffer.allocate(0);
    private boolean finished;

    private FluxInputStreamBridge(final Flux<ByteBuffer> source, final int demand) {
        this.queue = new LinkedBlockingQueue<>(demand + 1);
        this.subscriber = new QueueingSubscriber(queue, demand);
        source.subscribe(subscriber);
    }

    /** Subscribes to {@code source} and returns an {@link InputStream} reading its bytes. */
    public static FluxInputStreamBridge subscribeTo(final Flux<ByteBuffer> source, final int demand) {
        return new FluxInputStreamBridge(source, demand);
    }

    @Override
    public int read() throws IOException {
        final byte[] singleByte = new byte[1];
        final int bytesRead = read(singleByte, 0, 1);
        return bytesRead == -1 ? -1 : singleByte[0] & 0xFF;
    }

    @Override
    public int read(final byte[] destination, final int offset, final int length) throws IOException {
        if (finished) {
            return -1;
        }
        if (!current.hasRemaining() && !fillCurrent()) {
            return -1;
        }
        final int toCopy = Math.min(length, current.remaining());
        current.get(destination, offset, toCopy);
        if (!current.hasRemaining()) {
            subscriber.requestOne();
        }
        return toCopy;
    }

    private boolean fillCurrent() throws IOException {
        try {
            final StreamSignal signal = queue.take();
            return switch (signal) {
                case StreamSignal.Data(ByteBuffer buffer) -> {
                    current = buffer;
                    yield true;
                }
                case StreamSignal.Complete ignored -> {
                    finished = true;
                    yield false;
                }
                case StreamSignal.Error(Throwable cause) -> {
                    finished = true;
                    throw new IOException("Upstream Flux signalled an error", cause);
                }
            };
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for data", e);
        }
    }

    @Override
    public void close() {
        subscriber.dispose();
    }

    private static final class QueueingSubscriber extends BaseSubscriber<ByteBuffer> {

        private final BlockingQueue<StreamSignal> queue;
        private final int demand;

        private QueueingSubscriber(final BlockingQueue<StreamSignal> queue, final int demand) {
            this.queue = queue;
            this.demand = demand;
        }

        @Override
        protected void hookOnSubscribe(final Subscription subscription) {
            request(demand);
        }

        @Override
        protected void hookOnNext(final ByteBuffer value) {
            queue.add(new StreamSignal.Data(value));
        }

        @Override
        protected void hookOnComplete() {
            queue.add(new StreamSignal.Complete());
        }

        @Override
        protected void hookOnError(final Throwable throwable) {
            queue.add(new StreamSignal.Error(throwable));
        }

        private void requestOne() {
            request(1);
        }
    }
}
package io.github.camilyed.clickhouse.r2dbc.connector;

import io.github.camilyed.clickhouse.r2dbc.core.ClickHouseQuery;
import io.github.camilyed.clickhouse.r2dbc.transport.http.ClickHouseHttpTransport;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * A logical connection to ClickHouse over its HTTP interface.
 *
 * <p>ClickHouse's HTTP interface has no persistent per-connection session and no ACID
 * transactions in the sense R2DBC's {@code Connection} contract assumes — every query is an
 * independent HTTP request over a shared, pooled {@link ClickHouseHttpTransport}. This class is
 * therefore always in auto-commit mode, and every transaction/savepoint-related method either
 * fails clearly with {@link UnsupportedOperationException} or, where the R2DBC spec explicitly
 * allows it, no-ops — see each method's Javadoc for which. This is a deliberate design decision
 * (see ROADMAP.md's Phase 3 notes on "explicit unsupported-transaction-semantics handling"), not
 * an oversight: a caller that assumes real transactional guarantees here would be wrong to, and
 * this class fails loudly rather than silently pretending otherwise.
 *
 * <p>{@link #close()} only marks this logical connection closed; it does not tear down {@code
 * transport}, since the same transport (and its underlying connection pool) is shared by every
 * {@link Connection} a {@link ClickHouseConnectionFactory} produces.
 */
final class ClickHouseConnection implements Connection {

    private final ClickHouseHttpTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ClickHouseConnection(final ClickHouseHttpTransport transport) {
        this.transport = transport;
    }

    @Override
    public Publisher<Void> beginTransaction() {
        return Mono.error(new UnsupportedOperationException("ClickHouse does not support transactions"));
    }

    @Override
    public Publisher<Void> beginTransaction(final TransactionDefinition definition) {
        return Mono.error(new UnsupportedOperationException("ClickHouse does not support transactions"));
    }

    @Override
    public Publisher<Void> close() {
        return Mono.fromRunnable(() -> closed.set(true));
    }

    @Override
    public Publisher<Void> commitTransaction() {
        return Mono.error(new UnsupportedOperationException("ClickHouse does not support transactions"));
    }

    @Override
    public Batch createBatch() {
        throw new UnsupportedOperationException("Batches are not supported yet");
    }

    @Override
    public Publisher<Void> createSavepoint(final String name) {
        return Mono.error(new UnsupportedOperationException("ClickHouse does not support savepoints"));
    }

    @Override
    public Statement createStatement(final String sql) {
        if (sql == null) {
            throw new IllegalArgumentException("sql must not be null");
        }
        return new ClickHouseStatement(transport, sql);
    }

    @Override
    public boolean isAutoCommit() {
        return true;
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return ClickHouseConnectionMetadata.INSTANCE;
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return IsolationLevel.READ_COMMITTED;
    }

    @Override
    public Publisher<Void> releaseSavepoint(final String name) {
        // The R2DBC spec explicitly allows this: "Calling this for drivers not supporting
        // savepoint release results in a no-op."
        return Mono.empty();
    }

    @Override
    public Publisher<Void> rollbackTransaction() {
        return Mono.error(new UnsupportedOperationException("ClickHouse does not support transactions"));
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(final String name) {
        return Mono.error(new UnsupportedOperationException("ClickHouse does not support savepoints"));
    }

    @Override
    public Publisher<Void> setAutoCommit(final boolean autoCommit) {
        if (!autoCommit) {
            return Mono.error(
                    new UnsupportedOperationException("ClickHouse connections are always auto-commit"));
        }
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setLockWaitTimeout(final Duration timeout) {
        return Mono.error(new UnsupportedOperationException("Lock wait timeout is not supported yet"));
    }

    @Override
    public Publisher<Void> setStatementTimeout(final Duration timeout) {
        return Mono.error(new UnsupportedOperationException("Statement timeout is not supported yet"));
    }

    @Override
    public Publisher<Void> setTransactionIsolationLevel(final IsolationLevel isolationLevel) {
        return Mono.error(
                new UnsupportedOperationException("ClickHouse does not support configurable isolation levels"));
    }

    @Override
    public Publisher<Boolean> validate(final ValidationDepth depth) {
        if (closed.get()) {
            return Mono.just(false);
        }
        if (depth == ValidationDepth.LOCAL) {
            return Mono.just(true);
        }
        return transport.query(ClickHouseQuery.of("SELECT 1"))
                .aggregate()
                .asByteArray()
                .thenReturn(true)
                .onErrorReturn(false);
    }
}

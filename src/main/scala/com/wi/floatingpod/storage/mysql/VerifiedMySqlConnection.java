package com.wi.floatingpod.storage.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

/** JVM package-private JDBC access, rather than Scala-only visibility on a public bytecode getter. */
public final class VerifiedMySqlConnection implements AutoCloseable {
    private final Connection connection;
    private final MySqlConnectionIdentity identity;
    private final MySqlSessionTerminationBarrier barrier;

    VerifiedMySqlConnection(Connection connection, MySqlConnectionIdentity identity) {
        this.connection = Objects.requireNonNull(connection, "opened MySQL session");
        this.identity = Objects.requireNonNull(identity, "verified MySQL identity");
        this.barrier = new MySqlSessionTerminationBarrier(this);
    }

    // Intentionally not public: neither Java nor Scala consumers outside the product package get raw SQL.
    Connection connection() { return connection; }
    public MySqlConnectionIdentity identity() { return identity; }

    /** A fixed actual query protocol; namespace configuration is not physical/source authority. */
    public MySqlSessionBarrierAcquisition acquireSessionBarrier(
            MySqlSchemaAuditExpectation expectation, MySqlAuditBootScope scope, Duration wait) throws SQLException {
        return barrier.acquire(expectation, scope, wait);
    }

    void readmitSessionBarrier(MySqlSchemaAuditExpectation expectation, MySqlAuditBootScope scope) {
        barrier.requireHeld(expectation, scope);
    }

    /** Immutable fixed observations only; no JDBC resource or arbitrary-query callback is returned. */
    public MySqlSessionObservation observeSession(MySqlAuditBootScope scope) { return barrier.observe(scope); }

    /** Owner call observations remain distinct from service-side termination and settlement. */
    public MySqlSessionBarrierCloseObservation sessionBarrierCloseObservation() { return barrier.closeObservation(); }

    /** Revokes local capability before close, but does not certify server termination. */
    @Override public void close() throws SQLException {
        barrier.beginPhysicalClose();
        connection.close();
    }

    @Override public String toString() { return "VerifiedMySqlConnection[redacted]"; }
}

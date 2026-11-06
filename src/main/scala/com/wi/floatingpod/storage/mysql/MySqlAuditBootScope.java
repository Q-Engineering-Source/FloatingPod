package com.wi.floatingpod.storage.mysql;

import java.util.UUID;
import scala.Option;

/** Immutable scope identity; real JVM package-private raw tokens are not a public SQL escape. */
public final class MySqlAuditBootScope {
    private final Object connectionToken;
    private final MySqlConnectionIdentity connectionIdentity;
    private final Option<VerifiedMySqlConnection> verifiedConnection;
    private final Thread ownerThread;
    private final UUID bootUuid;
    private final long bootGeneration;
    private final long connectionGeneration;

    MySqlAuditBootScope(Object connectionToken, MySqlConnectionIdentity connectionIdentity,
            Option<VerifiedMySqlConnection> verifiedConnection, Thread ownerThread,
            UUID bootUuid, long bootGeneration, long connectionGeneration) {
        require(connectionToken != null, "MySQL connection identity token is required");
        require(connectionIdentity != null, "verified MySQL server identity is required");
        require(verifiedConnection != null, "verified MySQL connection option is required");
        if (verifiedConnection.isDefined()) {
            var verified = verifiedConnection.get();
            require(verified != null && verified.connection() != null, "owned MySQL connection must have a JDBC session");
            require(verified.connection() == connectionToken && verified.identity() == connectionIdentity,
                    "MySQL connection scope identity is inconsistent");
        }
        require(ownerThread != null, "owned IO thread identity is required");
        require(bootUuid != null, "game boot identity is required");
        require(bootGeneration > 0 && connectionGeneration > 0, "boot and connection generations must be positive");
        this.connectionToken = connectionToken;
        this.connectionIdentity = connectionIdentity;
        this.verifiedConnection = verifiedConnection;
        this.ownerThread = ownerThread;
        this.bootUuid = bootUuid;
        this.bootGeneration = bootGeneration;
        this.connectionGeneration = connectionGeneration;
    }

    public static MySqlAuditBootScope forOwnedIo(VerifiedMySqlConnection verified, Thread ownerThread,
            UUID bootUuid, long bootGeneration, long connectionGeneration) {
        require(verified != null && verified.connection() != null, "verified MySQL connection with JDBC session is required");
        return new MySqlAuditBootScope(verified.connection(), verified.identity(), Option.apply(verified),
                ownerThread, bootUuid, bootGeneration, connectionGeneration);
    }

    Object connectionToken() { return connectionToken; }
    MySqlConnectionIdentity connectionIdentity() { return connectionIdentity; }
    Option<VerifiedMySqlConnection> verifiedConnection() { return verifiedConnection; }
    Thread ownerThread() { return ownerThread; }
    public UUID bootUuid() { return bootUuid; }
    public long bootGeneration() { return bootGeneration; }
    public long connectionGeneration() { return connectionGeneration; }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException("requirement failed: " + message);
    }
    @Override public String toString() { return "MySqlAuditBootScope[boot=" + bootUuid
            + ", bootGeneration=" + bootGeneration + ", connectionGeneration=" + connectionGeneration + "]"; }
}

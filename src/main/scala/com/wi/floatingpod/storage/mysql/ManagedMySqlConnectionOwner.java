package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.bootstrap.ManagedDatabaseConfiguration;
import com.wi.floatingpod.config.OwnedIoSecretFiles;
import com.wi.sqljdbcdriverlib.SqlJdbcDistribution;
import com.wi.sqljdbcdriverlib.core.BackendId;
import com.wi.sqljdbcdriverlib.mysql.MySqlCredentialSnapshot;
import com.wi.sqljdbcdriverlib.mysql.MySqlCredentialSource;
import com.wi.sqljdbcdriverlib.mysql.MySqlEndpoint;
import com.wi.sqljdbcdriverlib.mysql.MySqlOpenRequest;
import com.wi.sqljdbcdriverlib.mysql.MySqlSecretSnapshot;
import com.wi.sqljdbcdriverlib.mysql.MySqlSecretSource;
import com.wi.sqljdbcdriverlib.mysql.MySqlTimeouts;
import com.wi.sqljdbcdriverlib.mysql.MySqlTrust;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * 初始化 worker 在 open 前登记的连接责任；不公开 raw JDBC，也不取得业务 writer 或 Ready。
 * 初次 TLS 会话的 UUID 只是待持久绑定的目标事实，绝不是独立静态结构批准。
 */
final class ManagedMySqlConnectionOwner implements AutoCloseable {
    enum State { NEW, OPENING, OPEN, FAILED, CLOSE_FAILED, CLOSED }

    private final Thread owner = Thread.currentThread();
    private final ManagedDatabaseConfiguration configuration;
    private final MySqlCredentialSource credentials;
    private final MySqlSecretSource trustSecret;
    private final Callable<Connection> transport;
    private State state = State.NEW;
    private Connection raw;
    private VerifiedMySqlConnection verified;
    private String lockName;
    private boolean lockAttempted;
    private boolean lockReleaseAttempted;
    private boolean lockReleased;
    private long startedAtNanos;
    private boolean closeAttempted;
    private Throwable closeFailure;

    ManagedMySqlConnectionOwner(ManagedDatabaseConfiguration configuration) {
        this(configuration, null);
    }

    /** 包内 seam 仅替换传输；真实身份、锁与清理责任仍走同一实现。 */
    ManagedMySqlConnectionOwner(ManagedDatabaseConfiguration configuration, Callable<Connection> transport) {
        this.configuration = Objects.requireNonNull(configuration, "managed database configuration");
        this.credentials = () -> {
            requireOwner();
            if (!configuration.usesPasswordFile())
                return configuration.withPassword(value -> new MySqlCredentialSnapshot(configuration.username(), value));
            try {
                return OwnedIoSecretFiles.withSecret(owner, configuration.passwordFile().orElseThrow(),
                    value -> new MySqlCredentialSnapshot(configuration.username(), value));
            } catch (IOException failure) {
                throw new IllegalStateException("Managed installer credential acquisition failed", failure);
            }
        };
        this.trustSecret = () -> {
            requireOwner();
            try {
                return OwnedIoSecretFiles.withSecret(owner, configuration.trustStorePasswordFile().orElseThrow(),
                    MySqlSecretSnapshot::new);
            } catch (IOException failure) {
                throw new IllegalStateException("Managed database trust secret acquisition failed", failure);
            }
        };
        this.transport = transport == null ? () -> SqlJdbcDistribution.backends().open(BackendId.MYSQL, request()) : transport;
    }

    private MySqlOpenRequest request() {
        return MySqlOpenRequest.builder(new MySqlEndpoint(configuration.host(), configuration.port(), configuration.database()), credentials)
            .timeouts(new MySqlTimeouts(Duration.ofMillis(configuration.connectTimeoutMillis()),
                Duration.ofMillis(configuration.socketTimeoutMillis())))
            .trust(MySqlTrust.store(configuration.trustStore().orElseThrow(), MySqlTrust.StoreType.PKCS12, trustSecret))
            .build();
    }

    MySqlConnectionIdentity open(UUID previouslyBoundServer) throws Exception {
        requireOwner();
        if (state != State.NEW) throw new IllegalStateException("Managed installer open is one-shot");
        state = State.OPENING;
        startedAtNanos = System.nanoTime();
        try {
            raw = transport.call();
            if (raw == null) throw new SQLException("Managed installer transport returned no connection");
            UUID server;
            String principal;
            try (PreparedStatement query = raw.prepareStatement("SELECT @@GLOBAL.server_uuid, DATABASE(), CURRENT_USER()")) {
                query.setQueryTimeout(queryTimeoutSeconds());
                try (ResultSet rows = query.executeQuery()) {
                    if (!rows.next()) throw rejected("Managed database identity is missing");
                    String uuid = rows.getString(1);
                    if (uuid == null || !uuid.matches("[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"))
                        throw rejected("Managed database UUID is malformed");
                    server = UUID.fromString(uuid);
                    if (server.equals(new UUID(0L, 0L))) throw rejected("Managed database UUID is zero");
                    if (!configuration.database().equals(rows.getString(2)))
                        throw rejected("Managed database selected catalog differs from configuration");
                    principal = rows.getString(3);
                    if (principal == null || !principal.startsWith(configuration.username() + "@"))
                        throw rejected("Managed installer effective account differs from configuration");
                    if (rows.next()) throw rejected("Managed database identity is ambiguous");
                }
            }
            if (previouslyBoundServer != null && !previouslyBoundServer.equals(server))
                throw rejected("Managed database server differs from persisted installation identity");
            MySqlConnectionSettings settings = MySqlConnectionSettings$.MODULE$.apply(
                configuration.host(), configuration.port(), configuration.database(), server, principal,
                credentials, configuration.trustStore().orElseThrow(), trustSecret,
                Duration.ofMillis(configuration.connectTimeoutMillis()), Duration.ofMillis(configuration.socketTimeoutMillis()),
                new MySqlDurabilityExpectation(1, 1L));
            // 复用版本、InnoDB、持久性、RC/strict/UTC及TLS检查；未通过前不发送DDL。
            MySqlConnectionIdentity identity = new MySqlConnectionVerification(settings).verifySession(raw);
            verified = new VerifiedMySqlConnection(raw, identity);
            raw = null;
            state = State.OPEN;
            return identity;
        } catch (Throwable primary) {
            state = State.FAILED;
            try { close(); }
            catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
            return ManagedMySqlConnectionOwner.<RuntimeException, MySqlConnectionIdentity>rethrow(primary);
        }
    }

    /** 同一个目标的不同安装/Boot使用同一锁；大小写归并只会保守串行，不允许绕过互斥。 */
    static String initializationLockName(UUID server, String schema) {
        Objects.requireNonNull(server);
        if (schema == null || !schema.matches("[A-Za-z0-9_]{1,64}")) throw new IllegalArgumentException("Safe schema required");
        try {
            byte[] identity = (server + "\n" + schema.toLowerCase(java.util.Locale.ROOT) + "\nFloatingPod-managed-init-v1\n")
                .getBytes(StandardCharsets.US_ASCII);
            return "fp:init:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(identity)).substring(0, 56);
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    void acquireInitializationLock() throws SQLException {
        requireOpen();
        if (lockAttempted) throw new IllegalStateException("Managed initialization lock acquisition is one-shot");
        lockAttempted = true;
        String name = initializationLockName(verified.identity().serverUuid(), configuration.database());
        try (PreparedStatement query = connection().prepareStatement("SELECT GET_LOCK(?, ?)")) {
            query.setQueryTimeout(queryTimeoutSeconds());
            query.setString(1, name);
            query.setDouble(2, configuration.lockTimeoutMillis() / 1000D);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) throw rejected("Managed initialization lock returned no result");
                int result = rows.getInt(1);
                boolean missing = rows.wasNull();
                // 在后续read/close失败前保留实际可能取得的锁责任。
                if (!missing && result == 1) lockName = name;
                if (missing || result != 1) throw rejected("Managed initialization lock was not acquired within its bound");
                if (rows.next()) throw rejected("Managed initialization lock result is ambiguous");
            }
        }
    }

    void releaseInitializationLock() throws SQLException {
        requireOpen();
        if (lockName == null) throw new IllegalStateException("No confirmed managed initialization lock");
        if (lockReleaseAttempted) throw new IllegalStateException("Managed initialization lock release is one-shot");
        lockReleaseAttempted = true;
        try (PreparedStatement query = connection().prepareStatement("SELECT RELEASE_LOCK(?)")) {
            query.setQueryTimeout(queryTimeoutSeconds());
            query.setString(1, lockName);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) throw rejected("Managed initialization lock release returned no result");
                int result = rows.getInt(1);
                if (rows.wasNull() || result != 1 || rows.next()) throw rejected("Managed initialization lock release is unconfirmed");
            }
        }
        lockName = null;
        lockReleased = true;
    }

    Connection connection() { requireOpen(); return verified.connection(); }
    MySqlConnectionIdentity identity() { requireOpen(); return verified.identity(); }
    State state() { requireOwner(); return state; }
    boolean retainsConnection() { requireOwner(); return raw != null || verified != null; }
    boolean lockReleaseConfirmed() { requireOwner(); return lockReleased; }
    /** GET_LOCK 已确认后即登记责任，即使结果集清理失败也不能丢失该事实。不是新的活性证明。 */
    boolean ownsInitializationLock() { requireOwner(); return lockName != null && state != State.CLOSED; }
    /** 未确认的 RELEASE_LOCK 不可盲重发；调用方保留失败并继续关闭原连接。 */
    boolean initializationLockReleaseAttempted() { requireOwner(); return lockReleaseAttempted; }

    /** 仅作为本次已验证通道的来源地址事实；发布策略据此限制新账号的 Host。 */
    String connectedClientHost() throws SQLException, IOException {
        requireHeldInitializationLock();
        try (PreparedStatement query = connection().prepareStatement("SELECT USER()")) {
            query.setQueryTimeout(queryTimeoutSeconds());
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) throw rejected("Managed connected caller is missing");
                String value;
                try (Reader reader = rows.getCharacterStream(1)) {
                    if (reader == null) throw rejected("Managed connected caller is NULL");
                    StringBuilder text = new StringBuilder(288);
                    char[] block = new char[288];
                    try {
                        int read;
                        while ((read = reader.read(block)) != -1) {
                            if (read == 0 || text.length() + read > 288)
                                throw rejected("Managed connected caller exceeds its identity bound");
                            text.append(block, 0, read);
                        }
                        value = text.toString();
                    } finally { java.util.Arrays.fill(block, '\0'); }
                }
                if (rows.next()) throw rejected("Managed connected caller is ambiguous");
                String prefix = configuration.username() + "@";
                if (!value.startsWith(prefix)) throw rejected("Managed connected caller differs from the configured installer");
                String host = value.substring(prefix.length());
                if (!host.matches("[A-Za-z0-9_.:-]{1,255}"))
                    throw rejected("Managed connected client host cannot be bound to an exact account Host");
                return host;
            }
        }
    }

    /** 每个安装效应/结构读取前核对同一实际会话仍持有原目标锁。 */
    void requireHeldInitializationLock() throws SQLException {
        requireOpen();
        if (lockName == null || lockReleaseAttempted)
            throw new IllegalStateException("Original managed initialization lock is unavailable");
        try (PreparedStatement query = connection().prepareStatement("SELECT IS_USED_LOCK(?) = CONNECTION_ID()")) {
            query.setQueryTimeout(queryTimeoutSeconds());
            query.setString(1, lockName);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) throw rejected("Managed initialization lock ownership is missing");
                int held = rows.getInt(1);
                if (rows.wasNull() || held != 1 || rows.next())
                    throw rejected("Managed initialization lock is no longer held by the original session");
            }
        }
    }

    int queryTimeoutSeconds() throws SQLTimeoutException {
        long remaining = Duration.ofMillis(configuration.initializationTimeoutMillis()).toNanos()
            - (System.nanoTime() - startedAtNanos);
        if (remaining <= 0L) throw new SQLTimeoutException("Managed initialization deadline expired");
        return Math.max(1, Math.toIntExact((remaining + 999_999_999L) / 1_000_000_000L));
    }
    private void requireOwner() {
        if (Thread.currentThread() != owner) throw new IllegalStateException("Exact managed initialization worker required");
    }
    private void requireOpen() {
        requireOwner();
        if (state != State.OPEN || verified == null) throw new IllegalStateException("Managed installer connection is unavailable");
    }
    private static SQLException rejected(String safeMessage) { return new SQLNonTransientConnectionException(safeMessage); }

    /** close返回不被写成服务端终止证明；close失败时原handle仍由本owner保留。 */
    @Override public void close() throws Exception {
        requireOwner();
        if (state == State.CLOSED) return;
        if (closeAttempted) {
            if (closeFailure != null) ManagedMySqlConnectionOwner.<RuntimeException, Void>rethrow(closeFailure);
            return;
        }
        closeAttempted = true;
        try {
            if (verified != null) verified.close();
            else if (raw != null) raw.close();
            verified = null;
            raw = null;
            state = State.CLOSED;
        } catch (Throwable failure) {
            closeFailure = failure;
            state = State.CLOSE_FAILED;
            ManagedMySqlConnectionOwner.<RuntimeException, Void>rethrow(failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable, R> R rethrow(Throwable failure) throws T { throw (T) failure; }
}

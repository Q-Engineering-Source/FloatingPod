package com.wi.floatingpod.storage.mysql;

import com.wi.sqljdbcdriverlib.SqlJdbcDistribution;
import com.wi.sqljdbcdriverlib.core.BackendId;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/** 唯一公开 driver issuer。公开 API 不接受或返回 raw JDBC 会话。 */
public final class MySqlConnectionSource {
    public static final String SupportedServerVersion = "9.7.2";
    public static final String ExpectedReadCommitted = "READ-COMMITTED";
    public static final String ExpectedStrictSqlMode = "STRICT_ALL_TABLES";
    public static final String ExpectedUtcTimeZone = "+00:00";
    private final MySqlConnectionSettings settings;
    private final Callable<Connection> driverOpen;

    public MySqlConnectionSource(MySqlConnectionSettings settings) {
        this(settings, () -> SqlJdbcDistribution.backends().open(BackendId.MYSQL, settings.openRequest()));
    }

    // 同包装配 seam 仅替换 driver transport；身份核验和资源责任仍执行真实路径。
    MySqlConnectionSource(MySqlConnectionSettings settings, Callable<Connection> driverOpen) {
        if (settings == null) throw new IllegalArgumentException("requirement failed: MySQL connection settings are required");
        if (driverOpen == null) throw new IllegalArgumentException("driver opener required");
        this.settings = settings;
        this.driverOpen = driverOpen;
    }

    public VerifiedMySqlConnection open() throws SQLException {
        SourceUnknownInspectionOpen owner = newOwnedSessionOpen();
        try { return owner.openOnce(); }
        catch (Throwable primary) {
            // 兼容 lexical caller 也必须随原异常保留尚未转交的实际资源；不伪造关闭成功。
            if (owner.hasRawHandle()) primary.addSuppressed(new FailedOpenOwnership(owner));
            return MySqlConnectionSource.<RuntimeException, VerifiedMySqlConnection>rethrow(primary);
        }
    }

    static final class FailedOpenOwnership extends IllegalStateException {
        private final SourceUnknownInspectionOpen owner;
        private FailedOpenOwnership(SourceUnknownInspectionOpen owner) {
            super("MySQL pre-handoff connection ownership retained; not admission");
            this.owner = owner;
        }
        SourceUnknownInspectionOpen retainedOwner() { return owner; }
    }

    /** 调用者须在 openOnce 前登记此 owner；失败 raw handle 不取得 UNKNOWN、native 或 admission 权限。 */
    static final class SourceUnknownInspectionOpen {
        enum Stage { NEW, DRIVER_OPEN_ATTEMPTED, DRIVER_RETURNED, VERIFY_ATTEMPTED, VERIFIED, TRANSFERRED, FAILED }
        private final MySqlConnectionSource issuer;
        private Stage stage = Stage.NEW;
        private Connection rawHandle;
        private VerifiedMySqlConnection verified;
        private boolean closeAttempted;
        private boolean closeReturned;
        private Throwable primary;

        private SourceUnknownInspectionOpen(MySqlConnectionSource issuer) {
            if (issuer == null) throw new IllegalArgumentException("requirement failed: exact MySQL source issuer is required");
            this.issuer = issuer;
        }

        synchronized VerifiedMySqlConnection openOnce() throws SQLException {
            if (stage != Stage.NEW) throw new IllegalStateException("UNKNOWN inspection source open is one-shot", primary);
            stage = Stage.DRIVER_OPEN_ATTEMPTED;
            try {
                Connection opened = issuer.driverOpen.call();
                rawHandle = opened;
                stage = Stage.DRIVER_RETURNED;
                if (opened == null) throw new SQLException("UNKNOWN inspection driver returned no JDBC handle");
                stage = Stage.VERIFY_ATTEMPTED;
                MySqlConnectionIdentity identity = new MySqlConnectionVerification(issuer.settings).verifySession(opened);
                verified = new VerifiedMySqlConnection(opened, identity);
                stage = Stage.VERIFIED;
                rawHandle = null;
                stage = Stage.TRANSFERRED;
                return verified;
            } catch (Throwable failure) {
                primary = failure;
                stage = Stage.FAILED;
                closeRawOnce(failure);
                return MySqlConnectionSource.<RuntimeException, VerifiedMySqlConnection>rethrow(failure);
            }
        }

        private void closeRawOnce(Throwable original) {
            if (rawHandle == null || closeAttempted) return;
            closeAttempted = true;
            try {
                rawHandle.close();
                closeReturned = true;
            } catch (Throwable cleanup) {
                if (cleanup != original) original.addSuppressed(cleanup);
            }
        }

        synchronized Stage stage() { return stage; }
        synchronized boolean hasRawHandle() { return rawHandle != null; }
        synchronized boolean closeAttempted() { return closeAttempted; }
        synchronized boolean closeReturned() { return closeReturned; }
        synchronized boolean rawConnectionClosed() throws SQLException {
            if (rawHandle == null) throw new IllegalStateException("no original untransferred JDBC handle");
            return rawHandle.isClosed();
        }
        synchronized Throwable primaryFailure() { return primary; }
        synchronized VerifiedMySqlConnection transferredVerified() {
            return stage == Stage.TRANSFERRED ? verified : null;
        }
    }

    SourceUnknownInspectionOpen newOwnedSessionOpen() {
        return new SourceUnknownInspectionOpen(this);
    }

    SourceUnknownInspectionOpen newSourceUnknownInspectionOpen() {
        return new SourceUnknownInspectionOpen(this);
    }

    /** 保留历史类型名；通用 caller-retained raw open 不授 UNKNOWN、native 或 admission 资格。 */
    SourceUnknownInspectionOpen newDbFirstSessionOpen() {
        return new SourceUnknownInspectionOpen(this);
    }

    // JVM package-private：外部消费者不能用任意 Connection 制造 owned wrapper。
    VerifiedMySqlConnection verifyAndTransfer(Connection connection) throws SQLException {
        if (connection == null) throw new IllegalArgumentException("requirement failed: opened MySQL connection is required");
        try {
            MySqlConnectionIdentity identity = new MySqlConnectionVerification(settings).verifySession(connection);
            return new VerifiedMySqlConnection(connection, identity);
        } catch (Throwable primary) {
            try { connection.close(); }
            catch (Throwable cleanup) {
                if (cleanup != primary) primary.addSuppressed(cleanup);
            }
            // Scala 核验可能抛出未在 JVM throws 表声明的 Throwable；不更换原类型、cause 或堆栈。
            return MySqlConnectionSource.<RuntimeException, VerifiedMySqlConnection>rethrow(primary);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable, R> R rethrow(Throwable primary) throws T {
        throw (T) primary;
    }
}

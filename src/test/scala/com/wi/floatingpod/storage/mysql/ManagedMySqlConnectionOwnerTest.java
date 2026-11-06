package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.bootstrap.ManagedDatabaseConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 直接检查真实owner的传输失败/线程责任；不连接SQL，不作为TLS或GET_LOCK运行证据。 */
class ManagedMySqlConnectionOwnerTest {
    @TempDir Path root;

    private ManagedDatabaseConfiguration configuration() throws Exception {
        Path file = root.resolve("config/floatingpod/database.properties");
        Files.createDirectories(file.getParent());
        Files.writeString(file, """
            DBHost=127.0.0.1
            Port=33317
            Database=fp_managed_test
            Username=installer
            Password=never-echo-this-secret
            TrustStore=private/test.p12
            TrustStorePasswordFile=private/test.password
            """);
        return ManagedDatabaseConfiguration.load(root, file);
    }

    @Test void preservesOriginalTransportFailureAndDoesNotRetryOpen() throws Exception {
        SQLException original = new SQLException("transport failed", "08006", 17);
        AtomicInteger calls = new AtomicInteger();
        try (var configuration = configuration();
             var owner = new ManagedMySqlConnectionOwner(configuration, () -> {
                 calls.incrementAndGet();
                 throw original;
             })) {
            assertSame(original, assertThrows(SQLException.class, () -> owner.open(null)));
            assertEquals(ManagedMySqlConnectionOwner.State.CLOSED, owner.state());
            assertFalse(owner.retainsConnection());
            assertFalse(owner.lockReleaseConfirmed(), "failed open must not certify lock release");
            assertThrows(IllegalStateException.class, () -> owner.open(null));
            assertEquals(1, calls.get());
        }
    }

    @Test void wrongThreadCannotStartTransportOrChangeOriginalOwner() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (var configuration = configuration();
             var owner = new ManagedMySqlConnectionOwner(configuration, () -> {
                 calls.incrementAndGet();
                 throw new AssertionError("wrong thread must not reach transport");
             })) {
            Thread foreign = Thread.ofPlatform().unstarted(() -> {
                try { owner.open(null); }
                catch (Throwable observed) { failure.set(observed); }
            });
            foreign.start();
            foreign.join();
            assertInstanceOf(IllegalStateException.class, failure.get());
            assertEquals(0, calls.get());
            assertEquals(ManagedMySqlConnectionOwner.State.NEW, owner.state());
        }
    }

    @Test void sameDatabaseHasStableBoundedLockIndependentOfInstallationAndBoot() {
        UUID server = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d");
        String name = ManagedMySqlConnectionOwner.initializationLockName(server, "floatingpod");
        assertEquals(64, name.length());
        assertEquals(name, ManagedMySqlConnectionOwner.initializationLockName(server, "FLOATINGPOD"));
        assertNotEquals(name, ManagedMySqlConnectionOwner.initializationLockName(server, "other"));
        assertNotEquals(name, ManagedMySqlConnectionOwner.initializationLockName(UUID.randomUUID(), "floatingpod"));
        assertThrows(IllegalArgumentException.class,
            () -> ManagedMySqlConnectionOwner.initializationLockName(server, "bad;USE mysql"));
    }
}

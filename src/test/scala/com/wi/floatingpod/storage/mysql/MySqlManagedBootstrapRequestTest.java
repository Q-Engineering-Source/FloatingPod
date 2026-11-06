package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.bootstrap.EarlyDependencyResolver;
import com.wi.floatingpod.bootstrap.ManagedBootstrapPolicy;
import com.wi.floatingpod.bootstrap.ManagedDatabaseConfiguration;
import com.wi.floatingpod.bootstrap.ProductionBootstrapControl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MySqlManagedBootstrapRequestTest {
    @TempDir Path temporary;

    private ManagedDatabaseConfiguration configuration() throws Exception {
        Path file = temporary.resolve("config/floatingpod/database.properties");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "DBHost=127.0.0.1\nPort=33317\nDatabase=fp_request_test\nUsername=installer\n" +
                "PasswordFile=absent-installer.secret\nTrustStore=absent-trust.p12\n" +
                "TrustStorePasswordFile=absent-trust.secret\n", StandardCharsets.UTF_8);
        return ManagedDatabaseConfiguration.load(temporary, file);
    }

    private EarlyDependencyResolver.ExpectedPins pins() {
        return new EarlyDependencyResolver.ExpectedPins(EarlyDependencyResolver.SQL_OUTER, Optional.empty(), Optional.empty());
    }

    private ProductionBootstrapControl.DbFirstIdentity identity() {
        return new ProductionBootstrapControl.DbFirstIdentity(UUID.randomUUID(), 1, 1, UUID.randomUUID(), UUID.randomUUID());
    }

    @Test void controlThreadCannotStartSqlPreparationAndOwnedCloseHasNoManagedStorageSideEffect() throws Exception {
        ManagedDatabaseConfiguration configuration = configuration();
        var request = new MySqlManagedBootstrapRequest(configuration, temporary.resolve("world"), identity(),
                ManagedBootstrapPolicy.policy(configuration.initializationTimeoutMillis()), pins());
        Path managed = temporary.resolve("config/floatingpod/managed");

        assertThrows(IllegalStateException.class, request::prepareOnOwnedIoThread);
        assertFalse(Files.exists(managed));
        assertTrue(request.failure().isEmpty());
        assertFalse(request.resourcesRetained());
        assertThrows(IllegalStateException.class, request::preparedControl);

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try { request.closeOnOwnedIoThread(); }
            catch (Throwable failure) { closeFailure.set(failure); }
        }, "managed-request-test-worker");
        worker.start();
        worker.join();

        assertNull(closeFailure.get());
        assertFalse(request.resourcesRetained());
        assertFalse(Files.exists(managed));
        assertFalse(Files.exists(temporary.resolve("absent-installer.secret")));
        assertFalse(Files.exists(temporary.resolve("absent-trust.p12")));
        assertFalse(Files.exists(temporary.resolve("absent-trust.secret")));
    }
}

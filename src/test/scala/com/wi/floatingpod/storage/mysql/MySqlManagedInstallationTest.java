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

import static org.junit.jupiter.api.Assertions.*;

class MySqlManagedInstallationTest {
    @TempDir Path temporary;

    @Test void closeBeforePrepareRetiresOnlyConfigurationAndCreatesNoConnectionOrInstallationFiles() throws Exception {
        Path file = temporary.resolve("config/floatingpod/database.properties");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "DBHost=127.0.0.1\nPort=33317\nDatabase=fp_installation_test\nUsername=installer\n" +
                "PasswordFile=absent-installer.secret\nTrustStore=absent-trust.p12\n" +
                "TrustStorePasswordFile=absent-trust.secret\n", StandardCharsets.UTF_8);
        ManagedDatabaseConfiguration configuration = ManagedDatabaseConfiguration.load(temporary, file);
        var identity = new ProductionBootstrapControl.DbFirstIdentity(UUID.randomUUID(), 1, 1, UUID.randomUUID(), UUID.randomUUID());
        var policy = ManagedBootstrapPolicy.policy(configuration.initializationTimeoutMillis());
        var pins = new EarlyDependencyResolver.ExpectedPins(EarlyDependencyResolver.SQL_OUTER, Optional.empty(), Optional.empty());
        var installation = new MySqlManagedInstallation(configuration, temporary.resolve("world"), identity, policy, pins);
        Path managed = temporary.resolve("config/floatingpod/managed");

        assertFalse(installation.resourcesRetained());
        assertDoesNotThrow(installation::closeOnOwnedIoThread);
        assertDoesNotThrow(installation::closeOnOwnedIoThread);
        assertFalse(installation.resourcesRetained());
        assertFalse(Files.exists(managed));
        assertFalse(Files.exists(temporary.resolve("absent-installer.secret")));
        assertFalse(Files.exists(temporary.resolve("absent-trust.p12")));
        assertFalse(Files.exists(temporary.resolve("absent-trust.secret")));
    }
}

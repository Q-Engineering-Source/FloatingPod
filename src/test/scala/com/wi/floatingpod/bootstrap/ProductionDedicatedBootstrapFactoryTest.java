package com.wi.floatingpod.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

/** Java early constructor/actual owner guard；不伪造 late Fresh success 或 transfer receipt。 */
class ProductionDedicatedBootstrapFactoryTest {
    @TempDir Path temporary;
    private DedicatedBootstrapBridge.Limits limits(){return new DedicatedBootstrapBridge.Limits(Duration.ofSeconds(2),Duration.ofSeconds(2),Duration.ofSeconds(2));}
    @Test void constructionDoesNotReadMissingControl() {
        var contract=new ProductionBootstrapControl.LaunchContract(temporary.resolve("absent-control"),"A".repeat(64));
        assertNotNull(new ProductionDedicatedBootstrapFactory(contract,limits()));
        assertThrows(NullPointerException.class,()->new ProductionDedicatedBootstrapFactory((ProductionBootstrapControl.LaunchContract)null,limits()));
    }
    @Test void actualBridgeWithoutControlOwnershipRejectsBeforeControlRead() {
        var factory=new ProductionDedicatedBootstrapFactory(new ProductionBootstrapControl.LaunchContract(temporary.resolve("absent-control"),"A".repeat(64)),limits());
        var installed=DedicatedBootstrapBridge.install((actual,control)->{throw new AssertionError("not entered");},limits());
        try {
            var bridge=DedicatedBootstrapBridge.create(new Object(),()->{throw new AssertionError("not entered");});
            assertThrows(IllegalStateException.class,()->factory.open(new Object(),bridge));
            assertEquals(DedicatedBootstrapBridge.Phase.NEW,bridge.phase());
        } finally {DedicatedBootstrapBridge.uninstall(installed);}
    }
    @Test void managedFactoryDoesNotAcquireCredentialsOrStartInitializationBeforeOwnedPreparation() throws Exception {
        var file=temporary.resolve("config/floatingpod/database.properties");
        Files.createDirectories(file.getParent());
        Files.writeString(file,"DBHost=127.0.0.1\nPort=33317\nDatabase=fp_factory_test\nUsername=installer\n"+
                "PasswordFile=absent-installer.password\nTrustStore=absent-trust.p12\n"+
                "TrustStorePasswordFile=absent-trust.password\n",StandardCharsets.UTF_8);
        try(var config=ManagedDatabaseConfiguration.load(temporary,file)) {
            var selected=ManagedBootstrapPolicy.limits(config.initializationTimeoutMillis());
            var factory=new ProductionDedicatedBootstrapFactory(config,selected);
            var installed=DedicatedBootstrapBridge.install((actual,control)->{throw new AssertionError("not entered");},selected);
            try {
                var bridge=DedicatedBootstrapBridge.create(new Object(),()->{throw new AssertionError("not entered");});
                assertThrows(IllegalStateException.class,()->factory.open(new Object(),bridge));
                assertEquals(DedicatedBootstrapBridge.Phase.NEW,bridge.phase());
                assertFalse(Files.exists(temporary.resolve("absent-installer.password")));
                assertFalse(Files.exists(temporary.resolve("config/floatingpod/managed")));
            } finally {DedicatedBootstrapBridge.uninstall(installed);}
        }
    }
    @Test void schema5LateEntryRejectsMissingActualControlBeforeAnyAssembly() {
        assertThrows(IllegalArgumentException.class,()->ProductionDedicatedBootstrapCallbacks$.MODULE$
                .openDeploymentReceipt(new Object(),null,null,null));
        assertThrows(IllegalArgumentException.class,()->ProductionDedicatedBootstrapCallbacks$.MODULE$
                .commandDeploymentReceipt(null,temporary.resolve("absent-world")));
    }
    @Test void missingClaimedHolderNeverPublishesOrdinaryRuntimeOrReady() {
        assertThrows(IllegalStateException.class,()->ProductionBootstrapInstallation.requireClaimedCallbacks(new Object()));
    }
    @Test void schema6LateEntryAndRuntimeRejectMissingOriginalOwnerBeforeAssembly() {
        assertThrows(IllegalArgumentException.class,()->ProductionDbFirstBootstrapCallbacks$.MODULE$
                .open(new Object(),null,null,null));
        assertThrows(IllegalArgumentException.class,()->com.wi.floatingpod.network.DbFirstDedicatedRuntime$.MODULE$
                .claimed(null,new Object()));
    }
}

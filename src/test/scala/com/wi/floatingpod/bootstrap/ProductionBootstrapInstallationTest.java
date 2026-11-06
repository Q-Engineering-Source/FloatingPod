package com.wi.floatingpod.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import static org.junit.jupiter.api.Assertions.*;

/** 实际 Java binding/create 状态机；不启动线程、不读取 control、不是 runtime/SQL 验收。 */
class ProductionBootstrapInstallationTest {
    @TempDir Path temporary;
    private DedicatedBootstrapBridge.Limits limits() {
        return new DedicatedBootstrapBridge.Limits(Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1));
    }
    private ProductionBootstrapControl.LaunchContract contract(String name) {
        return new ProductionBootstrapControl.LaunchContract(Path.of(name).toAbsolutePath().normalize(), "A".repeat(64));
    }
    @Test void installationPrecedesActualCreateAndIdenticalContractInstallsOnce() {
        var state = new ProductionBootstrapInstallation.InstallationState();
        var selected = contract("missing-control");
        var limit = limits();
        DedicatedBootstrapBridge.Installation binding = null;
        try {
            var first = state.create(selected, limit, new Object(), () -> fail("original start must not run"));
            binding = state.ensure(selected, limit);
            assertEquals(DedicatedBootstrapBridge.Phase.NEW, first.phase());
            assertNull(first.controlThread());
            assertSame(binding, state.ensure(selected, limit));
            var second = state.create(selected, limit, new Object(), () -> fail("original start must not run"));
            assertNotSame(first, second);
            assertEquals(DedicatedBootstrapBridge.Phase.NEW, second.phase());
            assertThrows(IllegalStateException.class, () -> state.create(contract("changed-control"), limit, new Object(), () -> fail()));
            assertSame(binding, state.ensure(selected, limit));
        } finally { if (binding != null) DedicatedBootstrapBridge.uninstall(binding); }
    }
    @Test void existingBridgeInstallationRejectsBeforeCreateWithoutReplacingResponsibility() {
        var limit = limits();
        var existing = DedicatedBootstrapBridge.install((server, control) -> { throw new AssertionError("factory must not open"); }, limit);
        try {
            var state = new ProductionBootstrapInstallation.InstallationState();
            var failure = assertThrows(IllegalStateException.class, () -> state.create(contract("absent"), limit, null, null));
            assertEquals("Bootstrap factory already installed", failure.getMessage());
            assertTrue(failure.getStackTrace().length > 0);
            // null server/start would fail create: reaching install failure proves create was short-circuited.
            var retained = DedicatedBootstrapBridge.create(new Object(), () -> fail());
            assertEquals(DedicatedBootstrapBridge.Phase.NEW, retained.phase());
        } finally { DedicatedBootstrapBridge.uninstall(existing); }
    }
    @Test @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void ordinaryDefaultReportsMissingConfigurationAndRejectsMixedOrUnknownModesBeforeStart() {
        var names = new String[]{"user.dir", "fp.bootstrap.mode", "fp.bootstrap.controlPath",
                "fp.bootstrap.controlSha256", "fp.bootstrap.preparationMillis", "fp.bootstrap.claimMillis",
                "fp.bootstrap.finalizationMillis"};
        var before = new HashMap<String,String>();
        for (var key : names) before.put(key, System.getProperty(key));
        try {
            for (var key : names) System.clearProperty(key);
            System.setProperty("user.dir", temporary.toString());
            var missing = assertThrows(IllegalStateException.class, () ->
                    ProductionBootstrapInstallation.createInstalled(new Object(), () -> fail("must not start")));
            assertTrue(missing.getMessage().contains("config"));
            assertNotNull(missing.getCause(), "the original configuration failure must be retained");
            System.setProperty("fp.bootstrap.controlPath", "legacy-control");
            var mixed = assertThrows(IllegalStateException.class, () ->
                    ProductionBootstrapInstallation.createInstalled(new Object(), () -> fail("must not start")));
            assertTrue(mixed.getMessage().contains("cannot be mixed"));
            System.clearProperty("fp.bootstrap.controlPath");
            System.setProperty("fp.bootstrap.mode", "unknown");
            assertThrows(IllegalArgumentException.class, () ->
                    ProductionBootstrapInstallation.createInstalled(new Object(), () -> fail("must not start")));
        } finally {
            for (var key : names) {
                var previous = before.get(key);
                if (previous == null) System.clearProperty(key); else System.setProperty(key, previous);
            }
        }
    }
}

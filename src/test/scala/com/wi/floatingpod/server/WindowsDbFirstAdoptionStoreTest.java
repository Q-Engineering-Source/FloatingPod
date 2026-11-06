package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 使用真实临时世界与 Windows native HANDLE 验证 DB-first 来源及 artifact store。 */
class WindowsDbFirstAdoptionStoreTest {
    private static final String CONFIGURATION_SHA = "ab".repeat(32);
    private static final String SECURITY_GUARD_SHA = "cd".repeat(32);
    private static final long MAX_BUDGET = 8L * 1024L * 1024L;

    /** 只记录 store 实际调用的动态费用，不伪造 native 行为或 holder 固定成本。 */
    private static final class Budget implements SourceForestAllocationBudget {
        private final SourceLimitProfile profile = SourceLimitProfile.of(
            1_048_576, 65_536, 65_536, 64, 4096, 1L << 24, 65_536, 65_536);
        private long held;
        private long peak;
        private long totalReserved;
        private long totalReleased;
        private int reserveCount;
        private int releaseCount;
        private int overReleaseAttempts;
        private int overLimitAttempts;

        @Override public SourceLimitProfile exactProfile() { return profile; }

        @Override public void reserveTemporary(long bytes, String context) throws IOException {
            if (bytes < 0L || bytes > MAX_BUDGET - held) {
                overLimitAttempts++;
                throw new IOException("FirstBinding6 test ledger rejected an over-limit reservation");
            }
            held = Math.addExact(held, bytes);
            peak = Math.max(peak, held);
            totalReserved = Math.addExact(totalReserved, bytes);
            reserveCount++;
        }

        @Override public void releaseTemporary(long bytes, String context) throws IOException {
            if (bytes < 0L || bytes > held) {
                overReleaseAttempts++;
                throw new IOException("FirstBinding6 test ledger rejected an over-return");
            }
            held -= bytes;
            totalReleased = Math.addExact(totalReleased, bytes);
            releaseCount++;
        }
    }

    @TempDir Path temporaryRoot;

    private static DbFirstWorldAdoptionPlanData plan() {
        return new DbFirstWorldAdoptionPlanData(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            UUID.randomUUID(), UUID.randomUUID(), 2, UUID.randomUUID(), "floatingpod_rbss_v1", 6,
            CONFIGURATION_SHA, UUID.randomUUID());
    }

    private static byte[] markerBytes(UUID binding) {
        return WindowsMarkerIo.markerBytes(binding, 1);
    }

    private static Path artifactPath(Path data, DbFirstWorldAdoptionPlanData plan) {
        return data.resolve(".floatingpod-adoption6-" + plan.bootstrapOperationUuid() + "-source.bin");
    }

    private static WindowsMarkerIo.FileIdentity nativeFileIdentity(WindowsMarkerIo io, Path path,
            long expectedLength, UUID operation) throws IOException {
        try (WindowsMarkerIo.Handle handle = io.open(path, WindowsMarkerIo.READ, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation)) {
            io.metadata(handle, false, expectedLength);
            return io.identity(handle);
        }
    }

    private static Set<String> childNames(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths.map(path -> path.getFileName().toString()).collect(Collectors.toUnmodifiableSet());
        }
    }

    private static byte[] artifactBody() {
        byte[] body = new byte[257];
        for (int index = 0; index < body.length; index++) body[index] = (byte) (index * 37 + 11);
        return body;
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void assertSettled(Budget budget) {
        assertEquals(0L, budget.held);
        assertEquals(budget.totalReserved, budget.totalReleased);
        assertTrue(budget.reserveCount > 0);
        assertTrue(budget.releaseCount > 0);
        assertTrue(budget.peak <= MAX_BUDGET);
        assertEquals(0, budget.overReleaseAttempts);
        assertEquals(0, budget.overLimitAttempts);
    }

    @Test void exclusiveCustodyAndRestartInventoryKeepExactBootAndReadbackNames() throws Exception {
        Path world = Files.createDirectory(temporaryRoot.resolve("restartworld"));
        Files.createDirectory(world.resolve("data"));
        var plan = plan();
        UUID boot = UUID.randomUUID();
        var budget = new Budget();
        var contenderBudget = new Budget();
        byte[] body = artifactBody();
        var source = WindowsDbFirstAdoptionStore.Key.of(WindowsDbFirstAdoptionStore.Role.SOURCE);
        var owner = new WindowsDbFirstAdoptionStore.Key(WindowsDbFirstAdoptionStore.Role.OWNER, boot);
        var intent = new WindowsDbFirstAdoptionStore.Key(WindowsDbFirstAdoptionStore.Role.HANDOFF_INTENT, boot);
        var readback = new WindowsDbFirstAdoptionStore.Key(WindowsDbFirstAdoptionStore.Role.READBACK, boot, sha256(body));
        String sourceFileId;
        try (var io = new WindowsMarkerIo()) {
            try (var first = new WindowsDbFirstAdoptionStore(io, budget, world, plan,
                    new WindowsWorldProvisioner.BootIdentity(boot, 1))) {
                first.initialize();
                sourceFileId = first.publish(source, body).fileId();
                first.publish(owner, body);
                first.publish(intent, body);
                first.publish(readback, body);
                assertEquals(Set.of(source, owner, intent, readback), Set.copyOf(first.discoverKeys()));
                try (var contender = new WindowsDbFirstAdoptionStore(io, contenderBudget, world, plan,
                        new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2))) {
                    var rejected = assertThrows(WindowsMarkerIo.NativeFailure.class, contender::initialize);
                    assertEquals(32, rejected.win32Error(), "the actual guard HANDLE excludes a second physical owner");
                }
                assertEquals(4, io.ownedHandleCount());
                assertSettled(contenderBudget);
            }
            assertSettled(budget);
            try (var restarted = new WindowsDbFirstAdoptionStore(io, budget, world, plan,
                    new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 3))) {
                restarted.initialize();
                assertEquals(Set.of(source, owner, intent, readback), Set.copyOf(restarted.discoverKeys()));
                try (var actual = restarted.read(source)) {
                    assertEquals(sourceFileId, actual.acknowledgement().fileId());
                    assertArrayEquals(body, actual.decode(bytes -> bytes.clone()));
                }
            }
            assertEquals(0, io.ownedHandleCount());
        }
        assertSettled(budget);
    }

    @Test void capturesRealExistingWorldAndRepeatedArtifactReadsRebarriersAndCloseSettleAllFees() throws Exception {
        Path world = Files.createDirectory(temporaryRoot.resolve("existingworld"));
        Path data = Files.createDirectory(world.resolve("data"));
        Path untouchedWorldFile = world.resolve("world-state.bin");
        byte[] untouchedWorldBytes = new byte[] {3, 1, 4, 1, 5, 9};
        Files.write(untouchedWorldFile, untouchedWorldBytes);
        UUID oldBinding = UUID.randomUUID();
        Path oldMarker = data.resolve("floatingpod-world-binding.bin");
        byte[] oldMarkerBytes = markerBytes(oldBinding);
        Files.write(oldMarker, oldMarkerBytes);
        Set<String> originalWorldEntries = childNames(world);

        DbFirstWorldAdoptionPlanData plan = plan();
        WindowsWorldProvisioner.BootIdentity boot = new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 1);
        UUID observationOperation = plan.bootstrapOperationUuid();
        byte[] body = artifactBody();
        WindowsDbFirstAdoptionStore.Key key = WindowsDbFirstAdoptionStore.Key.of(WindowsDbFirstAdoptionStore.Role.SOURCE);
        Path publishedArtifact = artifactPath(data, plan);
        Budget budget = new Budget();

        try (WindowsMarkerIo io = new WindowsMarkerIo();
             WindowsDbFirstAdoptionStore store = new WindowsDbFirstAdoptionStore(io, budget, world, plan, boot)) {
            WindowsMarkerIo.FileIdentity expectedParentId = io.captureDirectoryIdentity(world.getParent(), observationOperation);
            WindowsMarkerIo.FileIdentity expectedWorldId = io.captureDirectoryIdentity(world, observationOperation);
            WindowsMarkerIo.FileIdentity expectedDataId = io.captureDirectoryIdentity(data, observationOperation);
            WindowsMarkerIo.FileIdentity expectedMarkerId = nativeFileIdentity(io, oldMarker, 24L, observationOperation);
            Path guardPath = data.resolve(".floatingpod-adoption6.guard");

            store.initialize();
            assertEquals(4, io.ownedHandleCount());
            assertSettled(budget);
            WindowsMarkerIo.NativeFailure guardIsExclusive = assertThrows(WindowsMarkerIo.NativeFailure.class,
                () -> io.open(guardPath, WindowsMarkerIo.READ, 0, WindowsMarkerIo.OPEN_EXISTING,
                    WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, observationOperation));
            assertEquals(32, guardIsExclusive.win32Error());

            assertEquals(expectedDataId, store.dataDirectoryIdentity());
            var source = store.captureOriginalSource(CONFIGURATION_SHA, SECURITY_GUARD_SHA);
            store.requireOriginalSource(source);
            assertEquals(plan, source.plan());
            assertEquals(expectedParentId.versionedIdentity(), source.parentSaveFileId());
            assertEquals(expectedWorldId.versionedIdentity(), source.selectedSaveFileId());
            assertEquals(expectedDataId.versionedIdentity(), source.dataDirectoryFileId());
            assertNotNull(source.custodyFileId());
            assertTrue(source.oldMarker().present());
            assertEquals(expectedMarkerId.versionedIdentity(), source.oldMarker().fileId());
            assertEquals(1, source.oldMarker().format());
            assertEquals(oldBinding, source.oldMarker().markerUuid());
            assertEquals(expectedMarkerId, nativeFileIdentity(io, oldMarker, 24L, observationOperation));
            assertEquals(boot, source.creationBoot());
            assertEquals(CONFIGURATION_SHA, source.configurationSha256());
            assertEquals(SECURITY_GUARD_SHA, source.securityGuardSha256());
            assertEquals(4, io.ownedHandleCount());
            assertSettled(budget);

            var published = store.publish(key, body);
            assertEquals(key, published.key());
            assertEquals(body.length, published.byteLength());
            assertEquals(sha256(body), published.sha256());
            String publishedFileId = published.fileId();
            assertArrayEquals(body, Files.readAllBytes(publishedArtifact));
            assertEquals(publishedFileId, nativeFileIdentity(io, publishedArtifact, body.length, observationOperation).versionedIdentity());
            assertEquals(4, io.ownedHandleCount());
            assertSettled(budget);

            for (int iteration = 0; iteration < 24; iteration++) {
                WindowsDbFirstAdoptionStore.ReadArtifact read = store.read(key);
                assertNotNull(read);
                assertArrayEquals(body, read.decode(actual -> actual.clone()));
                WindowsDbFirstAdoptionStore.DurableArtifact acknowledgement = read.acknowledgement();
                assertEquals(publishedFileId, acknowledgement.fileId());
                read.close();
                assertSettled(budget);
                acknowledgement.rebarrier();
                assertSettled(budget);
                assertEquals(4, io.ownedHandleCount());
            }

            byte[] conflict = body.clone();
            conflict[0] ^= 1;
            assertThrows(IOException.class, () -> store.publish(key, conflict));
            assertArrayEquals(body, Files.readAllBytes(publishedArtifact));
            assertEquals(publishedFileId, nativeFileIdentity(io, publishedArtifact, body.length, observationOperation).versionedIdentity());
            assertSettled(budget);

            WindowsDbFirstAdoptionStore.ReadArtifact outstandingRead = store.read(key);
            assertNotNull(outstandingRead);
            assertTrue(budget.held > 0L);
            store.close();
            assertSettled(budget);
            assertEquals(0, io.ownedHandleCount());
            assertThrows(IOException.class, outstandingRead::acknowledgement);
            outstandingRead.close();
            assertSettled(budget);

            assertArrayEquals(oldMarkerBytes, Files.readAllBytes(oldMarker));
            assertEquals(expectedMarkerId, nativeFileIdentity(io, oldMarker, 24L, observationOperation));
            assertEquals(source.custodyFileId(), nativeFileIdentity(io, guardPath, 0L, observationOperation).versionedIdentity());
            assertArrayEquals(untouchedWorldBytes, Files.readAllBytes(untouchedWorldFile));
            assertEquals(originalWorldEntries, childNames(world));
            assertEquals(Set.of("floatingpod-world-binding.bin", ".floatingpod-adoption6.guard", publishedArtifact.getFileName().toString()),
                childNames(data));
        }
        assertSettled(budget);
    }

    @Test void corruptedPartialArtifactRefusesReadAndStoreCloseSettlesPendingHandleAndPins() throws Exception {
        Path world = Files.createDirectory(temporaryRoot.resolve("existingworld-corrupt-artifact"));
        Path data = Files.createDirectory(world.resolve("data"));
        Path untouchedWorldFile = world.resolve("world-state.bin");
        byte[] untouchedWorldBytes = new byte[] {2, 7, 1, 8, 2, 8};
        Files.write(untouchedWorldFile, untouchedWorldBytes);
        UUID oldBinding = UUID.randomUUID();
        Path oldMarker = data.resolve("floatingpod-world-binding.bin");
        byte[] oldMarkerBytes = markerBytes(oldBinding);
        Files.write(oldMarker, oldMarkerBytes);

        DbFirstWorldAdoptionPlanData plan = plan();
        WindowsWorldProvisioner.BootIdentity boot = new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 1);
        UUID operation = plan.bootstrapOperationUuid();
        WindowsDbFirstAdoptionStore.Key key = WindowsDbFirstAdoptionStore.Key.of(WindowsDbFirstAdoptionStore.Role.SOURCE);
        Path publishedArtifact = artifactPath(data, plan);
        Budget budget = new Budget();
        byte[] body = artifactBody();

        try (WindowsMarkerIo io = new WindowsMarkerIo();
             WindowsDbFirstAdoptionStore store = new WindowsDbFirstAdoptionStore(io, budget, world, plan, boot)) {
            WindowsMarkerIo.FileIdentity markerId = nativeFileIdentity(io, oldMarker, 24L, operation);
            store.initialize();
            var source = store.captureOriginalSource(CONFIGURATION_SHA, SECURITY_GUARD_SHA);
            store.requireOriginalSource(source);
            store.publish(key, body);
            assertArrayEquals(body, Files.readAllBytes(publishedArtifact));
            assertEquals(4, io.ownedHandleCount());

            // 截断成无效空 artifact；实际 OPEN_EXISTING HANDLE 必须由 store.close 收尾。
            Files.write(publishedArtifact, new byte[0]);
            assertThrows(IOException.class, () -> store.read(key));
            assertEquals(5, io.ownedHandleCount());
            assertEquals(0L, budget.held);
            WindowsMarkerIo.NativeFailure stillHeld = assertThrows(WindowsMarkerIo.NativeFailure.class,
                () -> io.open(publishedArtifact, WindowsMarkerIo.READ, 0, WindowsMarkerIo.OPEN_EXISTING,
                    WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation));
            assertEquals(32, stillHeld.win32Error());

            store.close();

            assertEquals(0, io.ownedHandleCount());
            assertSettled(budget);
            assertArrayEquals(new byte[0], Files.readAllBytes(publishedArtifact));
            assertArrayEquals(oldMarkerBytes, Files.readAllBytes(oldMarker));
            assertEquals(markerId, nativeFileIdentity(io, oldMarker, 24L, operation));
            assertArrayEquals(untouchedWorldBytes, Files.readAllBytes(untouchedWorldFile));
            try (WindowsMarkerIo.Handle reopened = io.open(publishedArtifact, WindowsMarkerIo.READ, 0,
                    WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation)) {
                assertNotNull(reopened);
            }
            assertEquals(0, io.ownedHandleCount());
        }
        assertSettled(budget);
    }
}

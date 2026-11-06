package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 Windows x64 native 行为测试；当前由 Main 集成运行，本文件编写阶段不运行 JVM。 */
class WindowsDbFirstMarkerAdoptionTest {
    private static final class Budget implements SourceForestAllocationBudget {
        private final SourceLimitProfile profile = SourceLimitProfile.of(64, 64, 64, 64, 64, 64, 64, 64);
        private long held;
        private int reserves;
        private int releases;

        @Override public SourceLimitProfile exactProfile() { return profile; }

        @Override public void reserveTemporary(long bytes, String context) throws IOException {
            if (bytes <= 0) throw new IOException("test reservation must be positive");
            held = Math.addExact(held, bytes);
            reserves++;
        }

        @Override public void releaseTemporary(long bytes, String context) throws IOException {
            if (bytes <= 0 || bytes > held) throw new IOException("test reservation release is unbalanced");
            held -= bytes;
            releases++;
        }
    }

    @TempDir Path temporaryRoot;

    private record Seed(Path parent, Path canonical, WindowsMarkerIo.FileIdentity parentIdentity,
                        UUID oldBinding, byte[] oldBytes, WindowsMarkerIo.FileIdentity oldIdentity) { }

    private Seed seedOldMarker(WindowsMarkerIo io, String directoryName) throws Exception {
        Path parent = Files.createDirectory(temporaryRoot.resolve(directoryName));
        Path canonical = parent.resolve(WindowsDbFirstMarkerAdoption.CANONICAL_MARKER_BASENAME);
        UUID oldBinding = UUID.randomUUID();
        byte[] oldBytes = WindowsMarkerIo.markerBytes(oldBinding, 1);
        Files.write(canonical, oldBytes);
        UUID observationOperation = UUID.randomUUID();
        WindowsMarkerIo.FileIdentity parentIdentity = io.captureDirectoryIdentity(parent, observationOperation);
        WindowsMarkerIo.FileIdentity oldIdentity = readIdentity(io, canonical, observationOperation);
        return new Seed(parent, canonical, parentIdentity, oldBinding, oldBytes, oldIdentity);
    }

    private static WindowsMarkerIo.FileIdentity readIdentity(WindowsMarkerIo io, Path path, UUID operation) throws IOException {
        try (WindowsMarkerIo.Handle handle = io.open(path, WindowsMarkerIo.READ, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation)) {
            io.metadata(handle, false);
            return io.identity(handle);
        }
    }

    private static WindowsDbFirstMarkerAdoption.Request request(Seed seed, UUID operation, UUID newBinding) {
        return new WindowsDbFirstMarkerAdoption.Request(seed.canonical(), newBinding, operation,
            seed.parentIdentity(), new WindowsDbFirstMarkerAdoption.ExpectedOldMarker(seed.oldIdentity(), seed.oldBytes()));
    }

    private static Path backupPath(Seed seed, UUID operation) {
        return seed.parent().resolve(WindowsDbFirstMarkerAdoption.preservedOldBasename(operation));
    }

    @Test void preservesOldFileIdAndBytesBeforeReturningRealNewMarkerAck() throws Exception {
        Seed seed;
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            seed = seedOldMarker(io, "first-binding");
            Budget budget = new Budget();
            UUID operation = UUID.randomUUID(), newBinding = UUID.randomUUID();
            var request = request(seed, operation, newBinding);

            var data = new WindowsDbFirstMarkerAdoption(io, budget).adopt(request);

            assertEquals(WindowsDbFirstMarkerAdoption.Outcome.OLD_MARKER_PRESERVED_NOW, data.outcome());
            assertSame(request, data.request());
            assertEquals(operation, data.markerPublicationAck().operationUuid());
            assertEquals(newBinding, data.markerPublicationAck().bindingUuid());
            assertFalse(data.markerPublicationAck().retainedExisting());
            assertEquals(seed.parentIdentity(), data.markerPublicationAck().parentIdentity());
            assertArrayEquals(WindowsMarkerIo.markerBytes(newBinding, 2), Files.readAllBytes(seed.canonical()));
            assertArrayEquals(seed.oldBytes(), Files.readAllBytes(backupPath(seed, operation)));
            assertEquals(seed.oldIdentity(), readIdentity(io, backupPath(seed, operation), operation));
            assertEquals(0L, budget.held);
            assertEquals(1, budget.reserves);
            assertEquals(1, budget.releases);
        }
    }

    @Test void resumesAfterNativeOldMoveWhenCanonicalIsMissing() throws Exception {
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            Seed seed = seedOldMarker(io, "partial-preservation");
            Budget budget = new Budget();
            UUID operation = UUID.randomUUID(), newBinding = UUID.randomUUID();
            Path backup = backupPath(seed, operation);

            budget.reserveTemporary(WindowsDbFirstMarkerAdoption.NATIVE_WORKSPACE_BYTES, "test native adoption move");
            try (WindowsMarkerIo.Handle parent = io.directory(seed.parent(), operation);
                 WindowsMarkerIo.Handle old = io.open(seed.canonical(),
                     WindowsMarkerIo.READ | WindowsMarkerIo.WRITE | WindowsMarkerIo.DELETE, 0,
                     WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation)) {
                io.flushChecked(old);
                io.renameHeldMarkerNoReplace(old, parent,
                    WindowsDbFirstMarkerAdoption.preservedOldBasename(operation), seed.oldIdentity(), seed.parentIdentity());
            } finally {
                budget.releaseTemporary(WindowsDbFirstMarkerAdoption.NATIVE_WORKSPACE_BYTES, "test native adoption move");
            }
            assertFalse(Files.exists(seed.canonical()));
            assertArrayEquals(seed.oldBytes(), Files.readAllBytes(backup));

            var data = new WindowsDbFirstMarkerAdoption(io, budget).adopt(request(seed, operation, newBinding));

            assertEquals(WindowsDbFirstMarkerAdoption.Outcome.OLD_MARKER_ALREADY_PRESERVED, data.outcome());
            assertFalse(data.markerPublicationAck().retainedExisting());
            assertArrayEquals(seed.oldBytes(), Files.readAllBytes(backup));
            assertEquals(seed.oldIdentity(), readIdentity(io, backup, operation));
            assertArrayEquals(WindowsMarkerIo.markerBytes(newBinding, 2), Files.readAllBytes(seed.canonical()));
            assertEquals(0L, budget.held);
        }
    }

    @Test void refusesWrongOldTokenWithoutMovingOrRewritingCanonical() throws Exception {
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            Seed seed = seedOldMarker(io, "wrong-old-token");
            Budget budget = new Budget();
            UUID operation = UUID.randomUUID();
            byte[] wrongExpectedBytes = WindowsMarkerIo.markerBytes(UUID.randomUUID(), 1);
            var request = new WindowsDbFirstMarkerAdoption.Request(seed.canonical(), UUID.randomUUID(), operation,
                seed.parentIdentity(), new WindowsDbFirstMarkerAdoption.ExpectedOldMarker(seed.oldIdentity(), wrongExpectedBytes));

            assertThrows(IOException.class, () -> new WindowsDbFirstMarkerAdoption(io, budget).adopt(request));

            assertArrayEquals(seed.oldBytes(), Files.readAllBytes(seed.canonical()));
            assertFalse(Files.exists(backupPath(seed, operation)));
            assertEquals(0L, budget.held);
            assertEquals(1, budget.reserves);
            assertEquals(1, budget.releases);
        }
    }

    @Test void refusesPreservedSiblingCollisionAndLeavesBothNativeObjectsUntouched() throws Exception {
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            Seed seed = seedOldMarker(io, "backup-collision");
            Budget budget = new Budget();
            UUID operation = UUID.randomUUID();
            Path backup = backupPath(seed, operation);
            byte[] collisionBytes = WindowsMarkerIo.markerBytes(UUID.randomUUID(), 2);
            Files.write(backup, collisionBytes);
            WindowsMarkerIo.FileIdentity collisionIdentity = readIdentity(io, backup, operation);
            var request = request(seed, operation, UUID.randomUUID());

            assertThrows(IOException.class, () -> new WindowsDbFirstMarkerAdoption(io, budget).adopt(request));

            assertArrayEquals(seed.oldBytes(), Files.readAllBytes(seed.canonical()));
            assertArrayEquals(collisionBytes, Files.readAllBytes(backup));
            assertEquals(seed.oldIdentity(), readIdentity(io, seed.canonical(), operation));
            assertEquals(collisionIdentity, readIdentity(io, backup, operation));
            assertEquals(0L, budget.held);
        }
    }

    @Test void rebarriersAlreadyNewCanonicalOnlyAfterVerifyingOldBackup() throws Exception {
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            Seed seed = seedOldMarker(io, "rebarrier");
            Budget budget = new Budget();
            UUID operation = UUID.randomUUID(), newBinding = UUID.randomUUID();
            var request = request(seed, operation, newBinding);
            var adopter = new WindowsDbFirstMarkerAdoption(io, budget);
            adopter.adopt(request);
            WindowsMarkerIo.FileIdentity preservedIdentity = readIdentity(io, backupPath(seed, operation), operation);

            var replay = adopter.adopt(request);

            assertEquals(WindowsDbFirstMarkerAdoption.Outcome.CANONICAL_ALREADY_NEW, replay.outcome());
            assertTrue(replay.markerPublicationAck().retainedExisting());
            assertEquals(seed.oldIdentity(), preservedIdentity);
            assertEquals(seed.oldIdentity(), readIdentity(io, backupPath(seed, operation), operation));
            assertArrayEquals(seed.oldBytes(), Files.readAllBytes(backupPath(seed, operation)));
            assertArrayEquals(WindowsMarkerIo.markerBytes(newBinding, 2), Files.readAllBytes(seed.canonical()));
            assertEquals(0L, budget.held);
            assertEquals(2, budget.reserves);
            assertEquals(2, budget.releases);
        }
    }

    @Test void explicitOldMarkerAbsencePublishesAndRebarriersWithoutInventingBackup() throws Exception {
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            Path parent = Files.createDirectory(temporaryRoot.resolve("first-binding-without-old-marker"));
            Path canonical = parent.resolve(WindowsDbFirstMarkerAdoption.CANONICAL_MARKER_BASENAME);
            UUID operation = UUID.randomUUID(), newBinding = UUID.randomUUID();
            var request = new WindowsDbFirstMarkerAdoption.Request(canonical, newBinding, operation,
                io.captureDirectoryIdentity(parent, operation), WindowsDbFirstMarkerAdoption.ExpectedAbsence.INSTANCE);
            Budget budget = new Budget();
            var adopter = new WindowsDbFirstMarkerAdoption(io, budget);

            var first = adopter.adopt(request);
            var replay = adopter.adopt(request);

            assertEquals(WindowsDbFirstMarkerAdoption.Outcome.OLD_MARKER_ABSENT, first.outcome());
            assertFalse(first.markerPublicationAck().retainedExisting());
            assertEquals(WindowsDbFirstMarkerAdoption.Outcome.CANONICAL_ALREADY_NEW, replay.outcome());
            assertTrue(replay.markerPublicationAck().retainedExisting());
            assertArrayEquals(WindowsMarkerIo.markerBytes(newBinding, 2), Files.readAllBytes(canonical));
            assertFalse(Files.exists(parent.resolve(WindowsDbFirstMarkerAdoption.preservedOldBasename(operation))));
            assertEquals(0L, budget.held);
            assertEquals(2, budget.reserves);
            assertEquals(2, budget.releases);
        }
    }

    @Test void refusesToContinueWhenExpectedOldMarkerAndItsRecoverySiblingAreBothMissing() throws Exception {
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            Path parent = Files.createDirectory(temporaryRoot.resolve("missing-old-marker-evidence"));
            Path canonical = parent.resolve(WindowsDbFirstMarkerAdoption.CANONICAL_MARKER_BASENAME);
            Path receiptOnlyFile = parent.resolve("receipt-only.bin");
            byte[] oldBytes = WindowsMarkerIo.markerBytes(UUID.randomUUID(), 1);
            Files.write(receiptOnlyFile, oldBytes);
            UUID operation = UUID.randomUUID();
            var oldIdentity = readIdentity(io, receiptOnlyFile, operation);
            var parentIdentity = io.captureDirectoryIdentity(parent, operation);
            var request = new WindowsDbFirstMarkerAdoption.Request(canonical, UUID.randomUUID(), operation,
                parentIdentity, new WindowsDbFirstMarkerAdoption.ExpectedOldMarker(oldIdentity, oldBytes));
            Budget budget = new Budget();

            assertThrows(IOException.class, () -> new WindowsDbFirstMarkerAdoption(io, budget).adopt(request));

            assertFalse(Files.exists(canonical));
            assertFalse(Files.exists(parent.resolve(WindowsDbFirstMarkerAdoption.preservedOldBasename(operation))));
            assertArrayEquals(oldBytes, Files.readAllBytes(receiptOnlyFile));
            assertEquals(0L, budget.held);
        }
    }
}

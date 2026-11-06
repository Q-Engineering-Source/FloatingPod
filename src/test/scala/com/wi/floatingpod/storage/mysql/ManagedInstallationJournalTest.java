package com.wi.floatingpod.storage.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ManagedInstallationJournalTest {
    @TempDir Path directory;

    private static String sha(String value) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static ManagedInstallationJournal.Identity identity() {
        return new ManagedInstallationJournal.Identity(UUID.fromString("10000000-0000-0000-0000-000000000001"),
                UUID.fromString("20000000-0000-0000-0000-000000000002"), "floatingpod", sha("config"), sha("recipe"));
    }
    private static List<ManagedInstallationJournal.Step> steps() {
        return List.of(new ManagedInstallationJournal.Step("table.manifest", sha("manifest")),
                new ManagedInstallationJournal.Step("seed.writer", sha("writer-seed")));
    }

    @Test void pendingIntentSurvivesRestartAndRequiresExplicitEffectConfirmation() throws Exception {
        Path file = directory.resolve(ManagedInstallationJournal.FILE_NAME);
        try (var journal = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            assertEquals(ManagedInstallationJournal.Phase.READY_FOR_STEP, journal.snapshot().phase());
            journal.recordIntent(0);
            assertEquals(ManagedInstallationJournal.Phase.INTENT_REQUIRES_INSPECTION, journal.snapshot().phase());
            assertEquals(0, journal.snapshot().pendingIntentIndex().orElseThrow());
        }
        try (var resumed = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            var snapshot = resumed.snapshot();
            assertEquals(ManagedInstallationJournal.Phase.INTENT_REQUIRES_INSPECTION, snapshot.phase());
            assertEquals(0, snapshot.pendingIntentIndex().orElseThrow());
            assertThrows(IllegalStateException.class, () -> resumed.recordIntent(1));
            resumed.confirmEffect(0);
            assertEquals(1, resumed.snapshot().nextStepIndex());
            assertEquals(ManagedInstallationJournal.Phase.READY_FOR_STEP, resumed.snapshot().phase());
            resumed.recordIntent(1);
            resumed.confirmEffect(1);
            assertEquals(ManagedInstallationJournal.Phase.ALL_STEPS_CONFIRMED, resumed.snapshot().phase());
        }
        assertTrue(Files.size(file) < ManagedInstallationJournal.MAX_BYTES);
    }

    @Test void exactIdentityAndOrderedPlanAreRequiredToResume() throws Exception {
        try (var journal = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            journal.recordIntent(0);
        }
        Path file = directory.resolve(ManagedInstallationJournal.FILE_NAME);
        long originalSize = Files.size(file);
        var wrongIdentity = new ManagedInstallationJournal.Identity(identity().installationId(), identity().serverId(),
                "other_schema", identity().configurationSha256(), identity().recipeSha256());
        assertThrows(IOException.class, () -> ManagedInstallationJournal.openOrCreate(directory, wrongIdentity, steps()));
        var changedSteps = List.of(new ManagedInstallationJournal.Step("table.manifest", sha("changed")), steps().get(1));
        assertThrows(IOException.class, () -> ManagedInstallationJournal.openOrCreate(directory, identity(), changedSteps));
        assertEquals(originalSize, Files.size(file), "mismatch checks must not replace or mutate pending evidence");
    }

    @Test void sealAndLocalBundlePublicationAreDistinctOrderedAcknowledgments() throws Exception {
        try (var journal = ManagedInstallationJournal.openOrCreate(directory, identity(), List.of(steps().getFirst()))) {
            assertThrows(IllegalStateException.class, journal::recordSealConfirmed);
            journal.recordIntent(0);
            journal.confirmEffect(0);
            journal.recordSealConfirmed();
            assertEquals(ManagedInstallationJournal.Phase.SEALED, journal.snapshot().phase());
            assertThrows(IllegalStateException.class, () -> journal.recordIntent(0));
        }
        try (var resumed = ManagedInstallationJournal.openOrCreate(directory, identity(), List.of(steps().getFirst()))) {
            assertEquals(ManagedInstallationJournal.Phase.SEALED, resumed.snapshot().phase());
            resumed.recordBundlePublished();
            assertEquals(ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED, resumed.snapshot().phase());
        }
        try (var finalRead = ManagedInstallationJournal.openOrCreate(directory, identity(), List.of(steps().getFirst()))) {
            assertEquals(ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED, finalRead.snapshot().phase());
            assertThrows(IllegalStateException.class, finalRead::recordBundlePublished);
        }
    }

    @Test void aTornTrailingRecordKeepsTheVerifiedIntentAndIsTruncatedBeforeNextAppend() throws Exception {
        Path file = directory.resolve(ManagedInstallationJournal.FILE_NAME);
        try (var journal = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            journal.recordIntent(0);
        }
        long validSize = Files.size(file);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.position(channel.size());
            channel.write(ByteBuffer.wrap(new byte[]{0, 0, 0, 69, 1, 0}));
            channel.force(true);
        }
        try (var resumed = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            assertEquals(ManagedInstallationJournal.Phase.INTENT_REQUIRES_INSPECTION, resumed.snapshot().phase());
            assertTrue(resumed.snapshot().tornSuffixNeedsRecovery());
            resumed.confirmEffect(0);
            assertFalse(resumed.snapshot().tornSuffixNeedsRecovery());
            assertEquals(validSize + Integer.BYTES + 1L + Integer.BYTES + 32L + 32L, Files.size(file));
        }
    }

    @Test void completeRecordDigestCorruptionIsRejectedAndNotDiscardedAsATornTail() throws Exception {
        Path file = directory.resolve(ManagedInstallationJournal.FILE_NAME);
        try (var journal = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            journal.recordIntent(0);
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long last = channel.size() - 1;
            ByteBuffer value = ByteBuffer.allocate(1);
            assertEquals(1, channel.read(value, last));
            value.flip();
            byte corrupt = (byte) (value.get() ^ 0x40);
            channel.write(ByteBuffer.wrap(new byte[]{corrupt}), last);
            channel.force(true);
        }
        assertThrows(IOException.class, () -> ManagedInstallationJournal.openOrCreate(directory, identity(), steps()));
    }

    @Test void exclusiveLocalFileLockRejectsASecondLiveOwner() throws Exception {
        try (var first = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            assertThrows(IOException.class, () -> ManagedInstallationJournal.openOrCreate(directory, identity(), steps()));
            assertEquals(ManagedInstallationJournal.Phase.READY_FOR_STEP, first.snapshot().phase());
        }
        try (var afterRelease = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            assertEquals(ManagedInstallationJournal.Phase.READY_FOR_STEP, afterRelease.snapshot().phase());
        }
    }

    @Test void rejectsInvalidPlanBoundsAndWrongOwnerThread() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new ManagedInstallationJournal.Step("bad/key", sha("x")));
        assertThrows(IllegalArgumentException.class, () -> new ManagedInstallationJournal.Step("x", "A".repeat(64)));
        assertThrows(IllegalArgumentException.class, () -> ManagedInstallationJournal.openOrCreate(directory, identity(),
                java.util.Collections.nCopies(ManagedInstallationJournal.MAX_STEPS + 1, steps().getFirst())));
        try (var journal = ManagedInstallationJournal.openOrCreate(directory, identity(), steps())) {
            var wrongThread = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            Thread thread = Thread.ofPlatform().unstarted(() -> {
                try { journal.snapshot(); } catch (Throwable failure) { wrongThread.set(failure); }
            });
            thread.start(); thread.join();
            assertInstanceOf(IllegalStateException.class, wrongThread.get());
        }
    }

    @Test void existingEmptyJournalIsNotMistakenForPermissionToMintANewIdentity() throws Exception {
        Path file = directory.resolve(ManagedInstallationJournal.FILE_NAME);
        Files.createFile(file);
        assertThrows(IOException.class, () -> ManagedInstallationJournal.openOrCreate(directory, identity(), steps()));
        assertEquals(0L, Files.size(file));
    }
}

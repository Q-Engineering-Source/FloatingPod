package com.wi.floatingpod.storage.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 只验证真实私有目录与进度日志行为；不代表 MySQL 或服务器验收。 */
class ManagedInstallationBundleTest {
    @TempDir Path instance;

    private static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static ManagedInstallationIdentity.Record identity() {
        return new ManagedInstallationIdentity.Record(
                UUID.fromString("20000000-0000-0000-0000-000000000001"),
                UUID.fromString("20000000-0000-0000-0000-000000000002"),
                UUID.fromString("20000000-0000-0000-0000-000000000003"),
                UUID.fromString("20000000-0000-0000-0000-000000000004"),
                UUID.fromString("20000000-0000-0000-0000-000000000005"),
                UUID.fromString("20000000-0000-0000-0000-000000000006"),
                UUID.fromString("20000000-0000-0000-0000-000000000007"),
                UUID.fromString("20000000-0000-0000-0000-000000000008"),
                "floatingpod_live", "a".repeat(64), "b".repeat(64));
    }

    private static ManagedInstallationJournal journal(ManagedInstallationFiles files,
            ManagedInstallationIdentity.Record identity) throws IOException {
        var key = new ManagedInstallationJournal.Identity(identity.installationUuid(), identity.serverUuid(),
                identity.schema(), identity.configurationSha256(), identity.recipeSha256());
        var step = new ManagedInstallationJournal.Step("schema.sealed", "c".repeat(64));
        return ManagedInstallationJournal.openOrCreate(files.directory(), key, List.of(step));
    }

    private static ManagedInstallationBundle.Payload payload(String source) throws Exception {
        byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
        return new ManagedInstallationBundle.Payload(bytes, sha(bytes));
    }

    private static ManagedInstallationBundle.Loaded publish(ManagedInstallationFiles files,
            ManagedInstallationJournal journal, ManagedInstallationIdentity.Record identity) throws Exception {
        return ManagedInstallationBundle.publish(files, journal, identity,
                payload("baseline DATA"), payload("runtime contract DATA"), payload("source capture DATA"),
                payload("metadata DATA"), "d".repeat(64));
    }

    private static void seal(ManagedInstallationJournal journal) throws IOException {
        journal.recordIntent(0);
        journal.confirmEffect(0);
        journal.recordSealConfirmed();
    }

    @Test void refusesBeforeJournalSealWithoutPublishingAnyPayloadOrPointer() throws Exception {
        var files = ManagedInstallationFiles.open(instance);
        var identity = identity();
        try (var journal = journal(files, identity)) {
            assertThrows(IllegalStateException.class, () -> publish(files, journal, identity));
            for (String name : List.of(ManagedInstallationBundle.BASELINE_FILE,
                    ManagedInstallationBundle.RUNTIME_CONTRACT_FILE, ManagedInstallationBundle.SOURCE_CAPTURE_FILE,
                    ManagedInstallationBundle.RUNTIME_METADATA_FILE, ManagedInstallationBundle.PENDING_POINTER_FILE,
                    ManagedInstallationBundle.ACTIVE_POINTER_FILE))
                assertFalse(Files.exists(files.directory().resolve(name)));
        }
    }

    @Test void verifiesEveryInputActualHashBeforeWritingAnyPayload() throws Exception {
        var files = ManagedInstallationFiles.open(instance);
        var identity = identity();
        try (var journal = journal(files, identity)) {
            seal(journal);
            byte[] wrong = "baseline bytes with a detached digest".getBytes(StandardCharsets.UTF_8);
            var failure = assertThrows(IOException.class, () -> ManagedInstallationBundle.publish(files, journal,
                    identity, new ManagedInstallationBundle.Payload(wrong, "e".repeat(64)),
                    payload("runtime contract DATA"), payload("source capture DATA"), payload("metadata DATA"),
                    "d".repeat(64)));
            assertNotNull(failure.getMessage());
            assertFalse(Files.exists(files.directory().resolve(ManagedInstallationBundle.BASELINE_FILE)));
            assertFalse(Files.exists(files.directory().resolve(ManagedInstallationBundle.ACTIVE_POINTER_FILE)));
        }
    }

    @Test void publishesAllPayloadsThenActivePointerAndLoadsTheSameImmutableIdentity() throws Exception {
        var files = ManagedInstallationFiles.open(instance);
        var identity = identity();
        try (var journal = journal(files, identity)) {
            seal(journal);
            var first = publish(files, journal, identity);
            assertEquals(ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED, journal.snapshot().phase());
            assertTrue(Files.exists(first.baselinePath()));
            assertTrue(Files.exists(first.runtimeContractPath()));
            assertTrue(Files.exists(first.sourceCapturePath()));
            assertTrue(Files.exists(first.metadataPath()));
            assertTrue(Files.exists(files.directory().resolve(ManagedInstallationBundle.PENDING_POINTER_FILE)));
            assertTrue(Files.exists(files.directory().resolve(ManagedInstallationBundle.ACTIVE_POINTER_FILE)));
            assertEquals(first, ManagedInstallationBundle.load(files, identity).orElseThrow());
            assertEquals(first, publish(files, journal, identity)); // sealed, identical retry is verification-only.
        }
    }

    @Test void identityDriftAndPayloadDriftRejectWithoutReplacingExistingFiles() throws Exception {
        var files = ManagedInstallationFiles.open(instance);
        var identity = identity();
        try (var journal = journal(files, identity)) {
            seal(journal);
            var published = publish(files, journal, identity);
            var changedIdentity = new ManagedInstallationIdentity.Record(
                    UUID.fromString("20000000-0000-0000-0000-000000000011"), identity.serverUuid(), identity.bindingUuid(),
                    identity.provisioningUuid(), identity.creationOperationUuid(), identity.bootstrapOperationUuid(),
                    identity.prepareWriteUuid(), identity.certifyWriteUuid(), identity.schema(),
                    identity.configurationSha256(), identity.recipeSha256());
            assertThrows(IOException.class, () -> ManagedInstallationBundle.load(files, changedIdentity));
            byte[] changed = "foreign baseline".getBytes(StandardCharsets.UTF_8);
            Files.write(published.baselinePath(), changed);
            assertThrows(IOException.class, () -> ManagedInstallationBundle.load(files, identity));
            var failure = assertThrows(IOException.class, () -> publish(files, journal, identity));
            assertNotNull(failure.getMessage());
            assertArrayEquals(changed, Files.readAllBytes(published.baselinePath()));
        }
    }

    @Test void corruptPointerAndForeignActivePointerFailClosedWithoutOverwrite() throws Exception {
        var files = ManagedInstallationFiles.open(instance);
        var identity = identity();
        try (var journal = journal(files, identity)) {
            seal(journal);
            publish(files, journal, identity);
            Path active = files.directory().resolve(ManagedInstallationBundle.ACTIVE_POINTER_FILE);
            byte[] corrupt = "corrupt pointer\n".getBytes(StandardCharsets.US_ASCII);
            Files.write(active, corrupt);
            assertThrows(IOException.class, () -> ManagedInstallationBundle.load(files, identity));
            assertArrayEquals(corrupt, Files.readAllBytes(active));
        }
    }

    @Test void foreignActivePointerIsNeverReplaced() throws Exception {
        var files = ManagedInstallationFiles.open(instance);
        var identity = identity();
        byte[] foreign = "foreign\n".getBytes(StandardCharsets.US_ASCII);
        files.publishNewFile(ManagedInstallationBundle.ACTIVE_POINTER_FILE, foreign);
        try (var journal = journal(files, identity)) {
            seal(journal);
            assertThrows(IOException.class, () -> publish(files, journal, identity));
            assertArrayEquals(foreign, Files.readAllBytes(files.directory().resolve(ManagedInstallationBundle.ACTIVE_POINTER_FILE)));
            assertFalse(Files.exists(files.directory().resolve(ManagedInstallationBundle.BASELINE_FILE)));
        }
    }

}

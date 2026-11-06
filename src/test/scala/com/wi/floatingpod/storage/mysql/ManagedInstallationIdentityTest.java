package com.wi.floatingpod.storage.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ManagedInstallationIdentityTest {
    @TempDir Path instanceRoot;

    private static String sha(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static UUID server() { return UUID.fromString("20000000-0000-0000-0000-000000000002"); }
    private static String configuration() { return sha("non-secret connection semantics"); }
    private static String recipe() { return sha("compiled full27 recipe"); }
    private static ManagedInstallationFiles files(Path root) throws IOException { return ManagedInstallationFiles.open(root); }
    private static Path identityFile(Path root) { return root.resolve("config/floatingpod/managed").resolve(ManagedInstallationIdentity.FILE_NAME); }

    @Test void firstCreationPersistsOneDistinctIdentityAndRestartReadsExactlyTheSameRecord() throws Exception {
        var managed = files(instanceRoot);
        var created = ManagedInstallationIdentity.openOrCreate(managed, server(), "floatingpod", configuration(), recipe());
        byte[] first = Files.readAllBytes(identityFile(instanceRoot));
        try {
            var ids = Set.of(created.installationUuid(), created.serverUuid(), created.bindingUuid(), created.provisioningUuid(),
                    created.creationOperationUuid(), created.bootstrapOperationUuid(), created.prepareWriteUuid(), created.certifyWriteUuid());
            assertEquals(8, ids.size());
            assertEquals(server(), created.serverUuid());
            assertEquals("floatingpod", created.schema());
            assertEquals(configuration(), created.configurationSha256());
            assertEquals(recipe(), created.recipeSha256());
            var reopened = ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), recipe());
            assertEquals(created, reopened);
            assertArrayEquals(first, Files.readAllBytes(identityFile(instanceRoot)), "reopen must not rewrite the stable identity");
        } finally { Arrays.fill(first, (byte) 0); }
    }

    @Test void serverSchemaConfigurationOrRecipeDriftIsRejectedWithoutReissuingOrRewritingIdentity() throws Exception {
        var created = ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), recipe());
        Path path = identityFile(instanceRoot);
        byte[] before = Files.readAllBytes(path);
        try {
            assertThrows(IOException.class, () -> ManagedInstallationIdentity.openOrCreate(files(instanceRoot),
                    UUID.fromString("30000000-0000-0000-0000-000000000003"), "floatingpod", configuration(), recipe()));
            assertThrows(IOException.class, () -> ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "other_db", configuration(), recipe()));
            assertThrows(IOException.class, () -> ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", sha("changed config"), recipe()));
            assertThrows(IOException.class, () -> ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), sha("changed recipe")));
            assertEquals(created, ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), recipe()));
            assertArrayEquals(before, Files.readAllBytes(path));
        } finally { Arrays.fill(before, (byte) 0); }
    }

    @Test void pendingJournalWithoutIdentityBlocksCreationOfAnyReplacementIdentity() throws Exception {
        var managed = files(instanceRoot);
        Path directory = managed.directory();
        try (var journal = ManagedInstallationJournal.openOrCreate(directory, new ManagedInstallationJournal.Identity(
                UUID.fromString("10000000-0000-0000-0000-000000000001"), server(), "floatingpod", configuration(), recipe()),
                java.util.List.of(new ManagedInstallationJournal.Step("first.table", sha("ddl"))))) {
            journal.recordIntent(0);
        }
        assertTrue(Files.exists(directory.resolve(ManagedInstallationJournal.FILE_NAME)));
        assertThrows(IOException.class, () -> ManagedInstallationIdentity.openOrCreate(managed, server(), "floatingpod", configuration(), recipe()));
        assertFalse(Files.exists(identityFile(instanceRoot)));
    }

    @Test void corruptOrNoncanonicalIdentityIsRejectedWithoutMutation() throws Exception {
        ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), recipe());
        Path path = identityFile(instanceRoot);
        long originalSize = Files.size(path);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long last = channel.size() - 2; // change a checksum digit, retain complete framing
            ByteBuffer one = ByteBuffer.allocate(1);
            assertEquals(1, channel.read(one, last));
            one.flip();
            channel.write(ByteBuffer.wrap(new byte[]{(byte) (one.get() ^ 1)}), last);
            channel.force(true);
        }
        assertThrows(IOException.class, () -> ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), recipe()));
        assertEquals(originalSize, Files.size(path));
    }

    @Test void noIdentityIsCreatedWhenExpectedInputsAreInvalid() throws Exception {
        var managed = files(instanceRoot);
        assertThrows(IllegalArgumentException.class, () -> ManagedInstallationIdentity.openOrCreate(managed, server(), "bad/schema", configuration(), recipe()));
        assertThrows(IllegalArgumentException.class, () -> ManagedInstallationIdentity.openOrCreate(managed, server(), "floatingpod", "A".repeat(64), recipe()));
        assertFalse(Files.exists(identityFile(instanceRoot)));
    }

    @Test void identityFileWithBroadenedPosixPermissionsIsRejected() throws Exception {
        ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), recipe());
        Path path = identityFile(instanceRoot);
        if (Files.getFileAttributeView(path, java.nio.file.attribute.PosixFileAttributeView.class) == null) return;
        Files.setPosixFilePermissions(path, java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
        assertThrows(IOException.class, () -> ManagedInstallationIdentity.openOrCreate(files(instanceRoot), server(), "floatingpod", configuration(), recipe()));
    }
}

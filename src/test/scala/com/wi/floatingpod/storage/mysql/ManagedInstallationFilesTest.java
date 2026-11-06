package com.wi.floatingpod.storage.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ManagedInstallationFilesTest {
    @TempDir Path root;

    @Test void runtimePasswordIsGeneratedPrivateAndReusedWithoutRotation() throws Exception {
        var files = ManagedInstallationFiles.open(root);
        Path first = files.ensureRuntimePassword("fp_s6_runtime");
        byte[] original = Files.readAllBytes(first);
        try {
            assertTrue(first.startsWith(files.directory()));
            assertTrue(first.getFileName().toString().matches("runtime-fp_s6_runtime\\.secret"));
            assertEquals(44, original.length);
            assertEquals('\n', original[43]);
            boolean upper = false, lower = false, digit = false, special = false;
            for (int i = 0; i < 43; i++) {
                int value = original[i] & 0xff;
                assertTrue(value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
                        || value >= '0' && value <= '9' || value == '-' || value == '_');
                upper |= value >= 'A' && value <= 'Z';
                lower |= value >= 'a' && value <= 'z';
                digit |= value >= '0' && value <= '9';
                special |= value == '-' || value == '_';
            }
            assertTrue(upper && lower && digit && special, "new passwords include the default MEDIUM character classes");
            verifyPrivateFile(first);
            assertEquals(first, files.ensureRuntimePassword("fp_s6_runtime"));
            var restarted = ManagedInstallationFiles.open(root);
            assertEquals(first, restarted.ensureRuntimePassword("fp_s6_runtime"));
            byte[] reused = Files.readAllBytes(first);
            try { assertArrayEquals(original, reused, "reopen must reuse the same credential bytes"); }
            finally { Arrays.fill(reused, (byte) 0); }
        } finally { Arrays.fill(original, (byte) 0); }
    }

    @Test void persistedBundleUsesCreateNewAndNeverReplacesExistingBytes() throws Exception {
        var files = ManagedInstallationFiles.open(root);
        byte[] input = "non-secret immutable metadata".getBytes(StandardCharsets.UTF_8);
        Path path = files.publishNewFile("managed-k.json", input);
        byte[] before = Files.readAllBytes(path);
        try {
            assertArrayEquals(input, before);
            verifyPrivateFile(path);
            IOException collision = assertThrows(IOException.class, () -> files.publishNewFile("managed-k.json", new byte[]{1, 2, 3}));
            assertNotNull(collision.getMessage());
            assertArrayEquals(before, Files.readAllBytes(path), "collision must not replace existing bytes");
        } finally { Arrays.fill(before, (byte) 0); Arrays.fill(input, (byte) 0); }
    }

    @Test void unsafePreexistingManagedDirectoryIsRejectedAndLeftUntouched() throws Exception {
        Path directory = root.resolve("config").resolve("floatingpod").resolve("managed");
        Files.createDirectories(directory);
        if (Files.getFileAttributeView(root, java.nio.file.attribute.PosixFileAttributeView.class) != null) {
            Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxr-xr-x"));
            assertThrows(IOException.class, () -> ManagedInstallationFiles.open(root));
            assertEquals(PosixFilePermissions.fromString("rwxr-xr-x"), Files.getPosixFilePermissions(directory));
        } else {
            // A default Windows directory includes inherited ACEs; the strict existing-directory check must reject it.
            assertThrows(IOException.class, () -> ManagedInstallationFiles.open(root));
        }
        assertEquals(0, childCount(directory));
    }

    @Test void symlinkedManagedDirectoryIsRejectedWhenTheFilesystemAllowsCreatingOne() throws Exception {
        Path outside = Files.createDirectory(root.resolve("outside"));
        Path parent = Files.createDirectories(root.resolve("config").resolve("floatingpod"));
        try { Files.createSymbolicLink(parent.resolve("managed"), outside); }
        catch (UnsupportedOperationException | IOException | SecurityException unavailable) { return; }
        assertThrows(IOException.class, () -> ManagedInstallationFiles.open(root));
        assertEquals(0, childCount(outside));
    }

    @Test void malformedExistingSecretFailsWithoutRotation() throws Exception {
        var files = ManagedInstallationFiles.open(root);
        Path path = files.directory().resolve("runtime-fp_s6_runtime.secret");
        byte[] malformed = "not-a-runtime-password\n".getBytes(StandardCharsets.UTF_8);
        files.publishNewFile("runtime-fp_s6_runtime.secret", malformed);
        Arrays.fill(malformed, (byte) 0);
        byte[] before = Files.readAllBytes(path);
        try {
            assertThrows(IOException.class, () -> files.ensureRuntimePassword("fp_s6_runtime"));
            assertArrayEquals(before, Files.readAllBytes(path));
        } finally { Arrays.fill(before, (byte) 0); }
    }

    @Test void bundleNameAndSizeAreValidatedBeforeCreatingAnyFile() throws Exception {
        var files = ManagedInstallationFiles.open(root);
        assertThrows(IllegalArgumentException.class, () -> files.publishNewFile("../secret", new byte[]{1}));
        assertThrows(IllegalArgumentException.class, () -> files.publishNewFile("empty.bin", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> files.publishNewFile("large.bin",
                new byte[ManagedInstallationFiles.MAX_BUNDLE_BYTES + 1]));
        assertEquals(0, childCount(files.directory()));
    }

    private static void verifyPrivateFile(Path path) throws IOException {
        var posix = Files.getFileAttributeView(path, java.nio.file.attribute.PosixFileAttributeView.class);
        if (posix != null) {
            assertEquals(PosixFilePermissions.fromString("rw-------"), posix.readAttributes().permissions());
        } else {
            var acl = Files.getFileAttributeView(path, java.nio.file.attribute.AclFileAttributeView.class);
            assertNotNull(acl);
            assertFalse(acl.getAcl().isEmpty());
            assertTrue(acl.getAcl().stream().allMatch(entry -> entry.type() == java.nio.file.attribute.AclEntryType.ALLOW));
            assertTrue(acl.getAcl().stream().allMatch(entry -> entry.flags().isEmpty()));
            var lookup = path.getFileSystem().getUserPrincipalLookupService();
            var trusted = new java.util.HashSet<java.nio.file.attribute.UserPrincipal>();
            trusted.add(lookup.lookupPrincipalByName(ProcessHandle.current().info().user().orElseThrow()));
            trusted.add(lookup.lookupPrincipalByName("NT AUTHORITY\\SYSTEM"));
            trusted.add(lookup.lookupPrincipalByName("BUILTIN\\Administrators"));
            assertEquals(trusted, acl.getAcl().stream().map(java.nio.file.attribute.AclEntry::principal)
                    .collect(java.util.stream.Collectors.toSet()));
            assertEquals(trusted.size(), acl.getAcl().size());
        }
    }

    private static long childCount(Path directory) throws IOException {
        try (Stream<Path> children = Files.list(directory)) { return children.count(); }
    }
}

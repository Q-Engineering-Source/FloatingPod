package com.wi.floatingpod.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class OwnedIoSecretFilesTest {
    @TempDir Path temporary;
    private Path file(byte[] bytes) throws IOException {
        Path path = temporary.resolve("synthetic-secret.txt").toAbsolutePath(); Files.write(path, bytes); return path;
    }
    private OwnedIoSecretFiles.Buffers buffers() {
        return new OwnedIoSecretFiles.Buffers(new byte[OwnedIoSecretFiles.MAX_SECRET_BYTES + 1], new char[OwnedIoSecretFiles.MAX_SECRET_BYTES]);
    }
    private void assertZero(OwnedIoSecretFiles.Buffers buffers) {
        assertArrayEquals(new byte[buffers.bytes().length], buffers.bytes());
        assertArrayEquals(new char[buffers.chars().length], buffers.chars());
    }

    @Test void freshReadsPreserveOtherWhitespaceAndEraseAllTemporaryArraysAfterConsumerClone() throws Exception {
        Path path = file("  合成\tsecret  \n".getBytes(StandardCharsets.UTF_8));
        OwnedIoSecretFiles.Buffers buffers = buffers(); AtomicReference<char[]> passed = new AtomicReference<>();
        char[] clone = OwnedIoSecretFiles.withSecret(Thread.currentThread(), path, chars -> { passed.set(chars); return chars.clone(); }, buffers);
        assertArrayEquals("  合成\tsecret  ".toCharArray(), clone);
        assertArrayEquals(new char[passed.get().length], passed.get()); assertZero(buffers);
        Files.writeString(path, "new synthetic value", StandardCharsets.UTF_8);
        char[] fresh = OwnedIoSecretFiles.withSecret(Thread.currentThread(), path, chars -> chars.clone());
        assertArrayEquals("new synthetic value".toCharArray(), fresh);
        java.util.Arrays.fill(clone, '\0'); java.util.Arrays.fill(fresh, '\0');
    }

    @Test void consumerFailureRetainsSameCauseAndErasesTemps() throws Exception {
        Path path = file("synthetic".getBytes(StandardCharsets.UTF_8));
        RuntimeException original = new IllegalStateException("consumer failure");
        OwnedIoSecretFiles.Buffers buffers = buffers(); AtomicReference<char[]> passed = new AtomicReference<>();
        assertSame(original, assertThrows(IllegalStateException.class, () -> OwnedIoSecretFiles.withSecret(Thread.currentThread(), path,
                chars -> { passed.set(chars); throw original; }, buffers)));
        assertArrayEquals(new char[passed.get().length], passed.get()); assertZero(buffers);
    }

    @Test void invalidUtf8BomEmptyMultilineAndOverLimitRejectAndEraseBuffers() throws Exception {
        byte[][] rejected = { new byte[0], "\n".getBytes(StandardCharsets.UTF_8), "\ufeffsecret".getBytes(StandardCharsets.UTF_8),
                "secret\r\n".getBytes(StandardCharsets.UTF_8), "secret\n\n".getBytes(StandardCharsets.UTF_8),
                "first\nsecond".getBytes(StandardCharsets.UTF_8), new byte[OwnedIoSecretFiles.MAX_SECRET_BYTES + 1] };
        for (byte[] content : rejected) {
            Path path = file(content); OwnedIoSecretFiles.Buffers buffers = buffers();
            assertThrows(IOException.class, () -> OwnedIoSecretFiles.withSecret(Thread.currentThread(), path, chars -> fail("Invalid secret reached consumer"), buffers));
            assertZero(buffers);
        }
        Path malformed = file(new byte[] {(byte) 0xc3, 0x28}); OwnedIoSecretFiles.Buffers buffers = buffers();
        assertThrows(CharacterCodingException.class, () -> OwnedIoSecretFiles.withSecret(Thread.currentThread(), malformed, chars -> chars.clone(), buffers));
        assertZero(buffers);
    }

    @Test void exactThreadCheckFailsBeforeAnyFileReadAndBoundaryAllows4096Bytes() throws Exception {
        Path missing = temporary.resolve("does-not-exist").toAbsolutePath();
        Thread other = new Thread(() -> { }, "not-owned-io");
        assertThrows(IllegalStateException.class, () -> OwnedIoSecretFiles.withSecret(other, missing, chars -> fail("Wrong actor consumer")));
        byte[] maximum = new byte[OwnedIoSecretFiles.MAX_SECRET_BYTES]; java.util.Arrays.fill(maximum, (byte) 'x');
        Path path = file(maximum);
        Integer length = OwnedIoSecretFiles.withSecret(Thread.currentThread(), path, chars -> chars.length);
        assertEquals(OwnedIoSecretFiles.MAX_SECRET_BYTES, length.intValue());
        assertThrows(IOException.class, () -> OwnedIoSecretFiles.withSecret(Thread.currentThread(), missing, chars -> fail("Missing file consumer")));
    }
}

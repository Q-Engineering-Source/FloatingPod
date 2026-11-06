package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.sqljdbcdriverlib.mysql.MySqlCredentialSource;
import com.wi.sqljdbcdriverlib.mysql.MySqlSecretSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class MySqlFileSourcesTest {
    @TempDir Path temporary;

    @Test void realPublicSnapshotsAcquireFreshAndCloseWithoutReflectingPrivateFields() throws Exception {
        Path path = temporary.resolve("synthetic.txt").toAbsolutePath(); Files.writeString(path, "synthetic\n", StandardCharsets.UTF_8);
        MySqlRuntimeMetadata.FileReference reference = MySqlRuntimeMetadata.fileReference("file:" + path);
        MySqlCredentialSource credentials = MySqlFileSources.credentials(Thread.currentThread(), "fixture_user", reference);
        MySqlSecretSource trust = MySqlFileSources.trustSecret(Thread.currentThread(), reference);
        try (var first = credentials.acquire(); var secret = trust.acquire()) {
            assertEquals("MySqlCredentialSnapshot[redacted]", first.toString()); assertEquals("MySqlSecretSnapshot[redacted]", secret.toString());
        }
        Files.writeString(path, "", StandardCharsets.UTF_8);
        MySqlFileSources.SecretAcquisitionFailure failed = assertThrows(MySqlFileSources.SecretAcquisitionFailure.class, credentials::acquire);
        assertInstanceOf(IOException.class, failed.getCause());
        assertEquals("Owned-IO secret file acquisition failed", failed.getMessage());
        Files.writeString(path, "updated synthetic", StandardCharsets.UTF_8);
        try (var next = credentials.acquire(); var secret = trust.acquire()) { assertNotNull(next); assertNotNull(secret); }
        assertEquals("MySqlFileCredentialSource[redacted]", credentials.toString());
        assertEquals("MySqlFileTrustSecretSource[redacted]", trust.toString());
    }

    @Test void onlyExactActualIoThreadCanAcquireAndReadFailuresRetainCause() throws Exception {
        Path path = temporary.resolve("synthetic.txt").toAbsolutePath(); Files.writeString(path, "synthetic", StandardCharsets.UTF_8);
        AtomicReference<MySqlCredentialSource> source = new AtomicReference<>(); AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread io = new Thread(() -> {
            try {
                source.set(MySqlFileSources.credentials(Thread.currentThread(), "fixture_user", MySqlRuntimeMetadata.fileReference("file:" + path)));
                try (var snapshot = source.get().acquire()) { assertNotNull(snapshot); }
            } catch (Throwable failure) { outcome.set(failure); }
        }, "selected-owned-io");
        io.start(); io.join(2000); assertFalse(io.isAlive()); assertNull(outcome.get());
        assertThrows(IllegalStateException.class, source.get()::acquire);
        MySqlSecretSource missing = MySqlFileSources.trustSecret(Thread.currentThread(), MySqlRuntimeMetadata.fileReference("file:" + temporary.resolve("missing").toAbsolutePath()));
        MySqlFileSources.SecretAcquisitionFailure failure = assertThrows(MySqlFileSources.SecretAcquisitionFailure.class, missing::acquire);
        assertInstanceOf(IOException.class, failure.getCause()); assertNotNull(failure.getCause().getStackTrace());
    }
}

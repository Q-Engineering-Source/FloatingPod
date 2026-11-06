package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.config.MySqlRuntimeMetadata.FileReference;
import com.wi.floatingpod.config.OwnedIoSecretFiles;
import com.wi.sqljdbcdriverlib.mysql.MySqlCredentialSnapshot;
import com.wi.sqljdbcdriverlib.mysql.MySqlCredentialSource;
import com.wi.sqljdbcdriverlib.mysql.MySqlSecretSnapshot;
import com.wi.sqljdbcdriverlib.mysql.MySqlSecretSource;
import java.io.IOException;
import java.util.Objects;

/** Late driver-bound Sources；不由 early metadata/Mixin 静态引用，完全不缓存 secret。 */
public final class MySqlFileSources {
    public static final class SecretAcquisitionFailure extends IllegalStateException {
        private SecretAcquisitionFailure(IOException original) { super("Owned-IO secret file acquisition failed", original); }
    }
    private MySqlFileSources() { }
    public static MySqlCredentialSource credentials(Thread exactOwnedIo, String username, FileReference reference) {
        Objects.requireNonNull(exactOwnedIo); Objects.requireNonNull(reference); Objects.requireNonNull(username);
        if (username.isBlank() || username.codePoints().anyMatch(point -> point < 32 || point == 127))
            throw new IllegalArgumentException("Credential username is malformed");
        return new MySqlCredentialSource() {
            @Override public MySqlCredentialSnapshot acquire() {
                try {
                    // 真实 public ctor 防御clone；finally擦除传入的临时byte/char buffers。
                    return OwnedIoSecretFiles.withSecret(exactOwnedIo, reference.absolutePath(),
                            secret -> new MySqlCredentialSnapshot(username, secret));
                } catch (IOException original) { throw new SecretAcquisitionFailure(original); }
            }
            @Override public String toString() { return "MySqlFileCredentialSource[redacted]"; }
        };
    }
    public static MySqlSecretSource trustSecret(Thread exactOwnedIo, FileReference reference) {
        Objects.requireNonNull(exactOwnedIo); Objects.requireNonNull(reference);
        return new MySqlSecretSource() {
            @Override public MySqlSecretSnapshot acquire() {
                try { return OwnedIoSecretFiles.withSecret(exactOwnedIo, reference.absolutePath(), MySqlSecretSnapshot::new); }
                catch (IOException original) { throw new SecretAcquisitionFailure(original); }
            }
            @Override public String toString() { return "MySqlFileTrustSecretSource[redacted]"; }
        };
    }
}

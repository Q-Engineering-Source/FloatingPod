package com.wi.floatingpod.storage.mysql;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/** Pure configuration identity shared by SQL and native anchor code; never a source/Fresh capability. */
public final class MySqlSessionBarrierNamespace {
    private static final String DOMAIN_VERSION = "FP_B1_SESSION_NAMESPACE_BE_V3";
    private static final String APP_ID = "com.wi.floatingpod";
    private static final Pattern SCHEMA = Pattern.compile("[A-Za-z0-9_]{1,64}");
    private final String lockName;

    private MySqlSessionBarrierNamespace(String lockName) { this.lockName = lockName; }

    /** Exact B1 Plan ASCII schema contract; never trim, fold case or normalize an identity. */
    public static MySqlSessionBarrierNamespace forIdentity(
            UUID serverUuid, String schema, UUID provisioningUuid, UUID bindingUuid) {
        Objects.requireNonNull(serverUuid, "namespace server UUID");
        Objects.requireNonNull(provisioningUuid, "namespace provisioning UUID");
        Objects.requireNonNull(bindingUuid, "namespace binding UUID");
        if (schema == null || !SCHEMA.matcher(schema).matches()) {
            throw new IllegalArgumentException("exact safe ASCII schema required for namespace");
        }
        try {
            var buffer = new ByteArrayOutputStream();
            var out = new DataOutputStream(buffer);
            text(out, DOMAIN_VERSION);
            text(out, APP_ID);
            uuid(out, serverUuid);
            text(out, schema);
            uuid(out, provisioningUuid);
            uuid(out, bindingUuid);
            out.flush();
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(buffer.toByteArray());
            String name = "fpb1:" + Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
            if (name.length() != 48 || !name.chars().allMatch(c -> c <= 127)) {
                throw new IllegalStateException("48 ASCII namespace invariant failed");
            }
            return new MySqlSessionBarrierNamespace(name);
        } catch (IOException | NoSuchAlgorithmException cause) {
            throw new IllegalStateException("namespace canonical encoding or SHA-256 failed", cause);
        }
    }

    // Protocol: big-endian int32 UTF-8 byte length followed by exact bytes; UUID MSB then LSB int64.
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 256) throw new IllegalArgumentException("namespace canonical text bound exceeded");
        out.writeInt(bytes.length);
        out.write(bytes);
    }
    private static void uuid(DataOutputStream out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    public String lockName() { return lockName; }
    @Override public String toString() { return "MySqlSessionBarrierNamespace[redacted]"; }
}

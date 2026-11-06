package com.wi.floatingpod.server;

import java.io.IOException;
import java.util.Arrays;

/** 已固定 DATA 的 exact prefix/suffix 算法；不持有 HANDLE、不授予 native 写入或 ACK。 */
public final class B1HandoffExactPrefix {
    private B1HandoffExactPrefix() {}
    /** complete164 actual 必须是 C[0:k] ++ B[k:164]，选择唯一规定的最大 k。 */
    public static int ownerOffset(byte[] before, byte[] target, byte[] actual) throws IOException {
        ownerLength(before); ownerLength(target); ownerLength(actual);
        for (int k = WindowsWorldProvisioner.OWNER_SIZE; k >= 0; k--) {
            boolean matches = true;
            for (int i = 0; i < actual.length; i++) {
                if (actual[i] != (i < k ? target[i] : before[i])) { matches = false; break; }
            }
            if (matches) return k;
        }
        throw new IOException("Guard is outside the exact original B-to-C prefix family");
    }
    public static byte[] ownerSuffix(byte[] before, byte[] target, byte[] actual) throws IOException {
        int offset = ownerOffset(before, target, actual);
        return Arrays.copyOfRange(target, offset, target.length);
    }
    /** 原 artifact temp 是其固定 expected fullbody 的严格前缀；完整 body 的 suffix 长度为零。 */
    public static int artifactOffset(byte[] expected, byte[] actual, long explicitLimit) throws IOException {
        if (expected == null || actual == null || explicitLimit <= 0 || expected.length <= 0 ||
            expected.length > explicitLimit || actual.length > expected.length)
            throw new IOException("Exact bounded original artifact prefix required");
        for (int i = 0; i < actual.length; i++)
            if (expected[i] != actual[i]) throw new IOException("Original artifact bytes differ from fixed fullbody prefix");
        return actual.length;
    }
    public static byte[] artifactSuffix(byte[] expected, byte[] actual, long explicitLimit) throws IOException {
        int offset = artifactOffset(expected, actual, explicitLimit);
        return Arrays.copyOfRange(expected, offset, expected.length);
    }
    private static void ownerLength(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length != WindowsWorldProvisioner.OWNER_SIZE)
            throw new IOException("Exact complete164 owner bytes required");
    }
}

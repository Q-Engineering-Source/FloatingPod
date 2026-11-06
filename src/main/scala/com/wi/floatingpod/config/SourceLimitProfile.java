package com.wi.floatingpod.config;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/** format3 固定的大端 LimitProfile；实例同时持有规范字节、摘要和由字节解码的八项预算。 */
public final class SourceLimitProfile {
    public static final String ENCODING = "FP_SOURCE_LIMIT_PROFILE_BE_V1";
    private static final byte[] TAG = ENCODING.getBytes(StandardCharsets.US_ASCII);
    private static final int ENCODED_LENGTH = Integer.BYTES + TAG.length + 8 * Long.BYTES;

    private final byte[] canonicalBytes;
    private final byte[] sha256;
    private final long maxRegionBytes;
    private final long maxChunkBytes;
    private final long maxDecompressedBytes;
    private final long maxNbtDepth;
    private final long maxMembers;
    private final long maxRetainedBytes;
    private final long maxSqlPacketBytes;
    private final long maxNativeArtifactBytes;

    private SourceLimitProfile(byte[] bytes, long[] values) {
        canonicalBytes = bytes.clone();
        sha256 = digest(canonicalBytes);
        maxRegionBytes = values[0];
        maxChunkBytes = values[1];
        maxDecompressedBytes = values[2];
        maxNbtDepth = values[3];
        maxMembers = values[4];
        maxRetainedBytes = values[5];
        maxSqlPacketBytes = values[6];
        maxNativeArtifactBytes = values[7];
    }

    /** 由八个调用方明确给出的正值构造唯一规范编码；不提供默认预算。 */
    public static SourceLimitProfile of(long maxRegionBytes, long maxChunkBytes,
            long maxDecompressedBytes, long maxNbtDepth, long maxMembers,
            long maxRetainedBytes, long maxSqlPacketBytes, long maxNativeArtifactBytes) {
        long[] values = {maxRegionBytes, maxChunkBytes, maxDecompressedBytes, maxNbtDepth,
                maxMembers, maxRetainedBytes, maxSqlPacketBytes, maxNativeArtifactBytes};
        validateValues(values);
        return new SourceLimitProfile(encode(values), values);
    }

    /** 从实际完整编码解码；拒绝错误 tag、错误长度、非正值和尾随字节。 */
    public static SourceLimitProfile decode(byte[] input) {
        if (input == null) throw invalid("LimitProfile canonical bytes are required");
        byte[] bytes = input.clone();
        if (bytes.length != ENCODED_LENGTH) throw invalid("LimitProfile canonical byte length is invalid");
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int tagLength = buffer.getInt();
        if (tagLength != TAG.length) throw invalid("LimitProfile encoding tag length is invalid");
        byte[] tag = new byte[tagLength];
        buffer.get(tag);
        if (!MessageDigest.isEqual(TAG, tag)) throw invalid("LimitProfile encoding tag is unsupported");
        long[] values = new long[8];
        for (int index = 0; index < values.length; index++) values[index] = buffer.getLong();
        if (buffer.hasRemaining()) throw invalid("LimitProfile encoding has trailing bytes");
        validateValues(values);
        byte[] recoded = encode(values);
        if (!MessageDigest.isEqual(bytes, recoded)) throw invalid("LimitProfile encoding is not canonical");
        return new SourceLimitProfile(bytes, values);
    }

    /** 同时核对调用方冻结的完整编码摘要，不接受 detached budgets 替代原字节。 */
    public static SourceLimitProfile decode(byte[] input, byte[] expectedSha256) {
        if (expectedSha256 == null || expectedSha256.length != 32)
            throw invalid("LimitProfile expected SHA-256 must contain exactly 32 bytes");
        SourceLimitProfile decoded = decode(input);
        if (!MessageDigest.isEqual(decoded.sha256, expectedSha256))
            throw invalid("LimitProfile canonical byte SHA-256 differs from its pinned identity");
        return decoded;
    }

    public long maxRegionBytes() { return maxRegionBytes; }
    public long maxChunkBytes() { return maxChunkBytes; }
    public long maxDecompressedBytes() { return maxDecompressedBytes; }
    public long maxNbtDepth() { return maxNbtDepth; }
    public long maxMembers() { return maxMembers; }
    public long maxRetainedBytes() { return maxRetainedBytes; }
    public long maxSqlPacketBytes() { return maxSqlPacketBytes; }
    public long maxNativeArtifactBytes() { return maxNativeArtifactBytes; }
    public byte[] canonicalBytes() { return canonicalBytes.clone(); }
    public byte[] sha256() { return sha256.clone(); }
    public String sha256Hex() { return HexFormat.of().formatHex(sha256); }

    @Override public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof SourceLimitProfile that)) return false;
        return MessageDigest.isEqual(canonicalBytes, that.canonicalBytes);
    }

    @Override public int hashCode() { return Arrays.hashCode(sha256); }
    @Override public String toString() { return "SourceLimitProfile[" + sha256Hex().substring(0, 12) + "…, budgets=8]"; }

    private static byte[] encode(long[] values) {
        validateValues(values);
        try {
            var bytes = new ByteArrayOutputStream(ENCODED_LENGTH);
            var output = new DataOutputStream(bytes);
            output.writeInt(TAG.length);
            output.write(TAG);
            for (long value : values) output.writeLong(value);
            output.flush();
            byte[] result = bytes.toByteArray();
            if (result.length != ENCODED_LENGTH) throw invalid("LimitProfile encoder produced an invalid length");
            return result;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory LimitProfile encoding failed", impossible);
        }
    }

    private static void validateValues(long[] values) {
        Objects.requireNonNull(values, "LimitProfile budget values");
        if (values.length != 8) throw invalid("LimitProfile requires exactly eight budgets");
        for (long value : values) if (value <= 0) throw invalid("LimitProfile budgets must be strictly positive");
    }

    private static byte[] digest(byte[] value) {
        try { return MessageDigest.getInstance("SHA-256").digest(value); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 is unavailable", impossible); }
    }

    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}

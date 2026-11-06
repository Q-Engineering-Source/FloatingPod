package com.wi.floatingpod.config;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

final class SourceLimitProfileTest {
    private static final String EXPECTED_CANONICAL_HEX =
            "0000001d46505F534F555243455F4C494D49545F50524F46494C455F42455F5631"
            + "0000000000000001"
            + "0000000000000002"
            + "0000000000000003"
            + "0000000000000004"
            + "0000000000000005"
            + "0102030405060708"
            + "0000000000000007"
            + "7fffffffffffffff";

    @Test
    void encodesExactBigEndianTagAndOrderedEightValues() {
        SourceLimitProfile profile = SourceLimitProfile.of(1L, 2L, 3L, 4L, 5L,
                0x0102030405060708L, 7L, Long.MAX_VALUE);
        byte[] expected = HexFormat.of().parseHex(EXPECTED_CANONICAL_HEX);
        assertEquals(97, expected.length);
        assertArrayEquals(expected, profile.canonicalBytes());
        assertEquals(1L, profile.maxRegionBytes());
        assertEquals(2L, profile.maxChunkBytes());
        assertEquals(3L, profile.maxDecompressedBytes());
        assertEquals(4L, profile.maxNbtDepth());
        assertEquals(5L, profile.maxMembers());
        assertEquals(0x0102030405060708L, profile.maxRetainedBytes());
        assertEquals(7L, profile.maxSqlPacketBytes());
        assertEquals(Long.MAX_VALUE, profile.maxNativeArtifactBytes());
    }

    @Test
    void decodesAndReencodesActualCanonicalBytesAndVerifiesTheActualDigest() throws Exception {
        SourceLimitProfile original = SourceLimitProfile.of(11L, 22L, 33L, 44L, 55L, 66L, 77L, 88L);
        byte[] bytes = original.canonicalBytes();
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        SourceLimitProfile decoded = SourceLimitProfile.decode(bytes, digest);
        assertEquals(original, decoded);
        assertArrayEquals(bytes, decoded.canonicalBytes());
        assertArrayEquals(digest, decoded.sha256());
        assertEquals(HexFormat.of().formatHex(digest), decoded.sha256Hex());
        assertNotEquals(original, SourceLimitProfile.of(11L, 22L, 33L, 44L, 55L, 66L, 77L, 89L));
    }

    @Test
    void callerArraysCannotMutateTheStoredProfileOrDigest() throws Exception {
        SourceLimitProfile original = SourceLimitProfile.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        byte[] supplied = original.canonicalBytes();
        byte[] expectedDigest = MessageDigest.getInstance("SHA-256").digest(supplied);
        SourceLimitProfile decoded = SourceLimitProfile.decode(supplied, expectedDigest);
        supplied[0] ^= 0x7f;
        byte[] exposedBytes = decoded.canonicalBytes();
        exposedBytes[0] ^= 0x7f;
        byte[] exposedDigest = decoded.sha256();
        exposedDigest[0] ^= 0x7f;
        assertArrayEquals(original.canonicalBytes(), decoded.canonicalBytes());
        assertArrayEquals(expectedDigest, decoded.sha256());
    }

    @Test
    void rejectsZeroAndNegativeValuesInEveryBudgetPosition() {
        for (int index = 0; index < 8; index++) {
            long[] values = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L};
            values[index] = 0L;
            assertThrows(IllegalArgumentException.class, () -> from(values));
            values[index] = -1L;
            assertThrows(IllegalArgumentException.class, () -> from(values));
        }
    }

    @Test
    void rejectsWrongTagLengthWrongTagTruncationTrailingBytesAndDigest() throws Exception {
        byte[] valid = SourceLimitProfile.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L).canonicalBytes();
        byte[] wrongLength = valid.clone();
        ByteBuffer.wrap(wrongLength).order(ByteOrder.BIG_ENDIAN).putInt(30);
        assertThrows(IllegalArgumentException.class, () -> SourceLimitProfile.decode(wrongLength));
        byte[] wrongTag = valid.clone();
        wrongTag[4] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SourceLimitProfile.decode(wrongTag));
        assertThrows(IllegalArgumentException.class, () -> SourceLimitProfile.decode(Arrays.copyOf(valid, valid.length - 1)));
        assertThrows(IllegalArgumentException.class, () -> SourceLimitProfile.decode(Arrays.copyOf(valid, valid.length + 1)));
        assertThrows(IllegalArgumentException.class, () -> SourceLimitProfile.decode(valid, new byte[32]));
        byte[] zero = valid.clone();
        ByteBuffer.wrap(zero).order(ByteOrder.BIG_ENDIAN).putLong(4 + SourceLimitProfile.ENCODING.length(), 0L);
        assertThrows(IllegalArgumentException.class, () -> SourceLimitProfile.decode(zero));
        byte[] negative = valid.clone();
        ByteBuffer.wrap(negative).order(ByteOrder.BIG_ENDIAN).putLong(4 + SourceLimitProfile.ENCODING.length(), -1L);
        assertThrows(IllegalArgumentException.class, () -> SourceLimitProfile.decode(negative));
    }

    private static SourceLimitProfile from(long[] values) {
        return SourceLimitProfile.of(values[0], values[1], values[2], values[3],
                values[4], values[5], values[6], values[7]);
    }
}

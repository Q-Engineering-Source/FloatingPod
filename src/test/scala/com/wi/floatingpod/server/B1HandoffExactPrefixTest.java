package com.wi.floatingpod.server;

import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@Tag("b1-two-slot-pure")
class B1HandoffExactPrefixTest {
    @Test void everyGuardCrashPrefixCompletesOnlyTheUnwrittenSuffix() throws Exception {
        byte[] b = new byte[164], c = new byte[164];
        for (int i = 0; i < 164; i++) { b[i] = (byte) i; c[i] = (byte) (i ^ 0x5a); }
        for (int k = 0; k <= 164; k++) {
            byte[] actual = b.clone(); System.arraycopy(c, 0, actual, 0, k);
            byte[] retained = actual.clone();
            assertEquals(k, B1HandoffExactPrefix.ownerOffset(b, c, actual));
            byte[] suffix = B1HandoffExactPrefix.ownerSuffix(b, c, actual);
            assertEquals(164 - k, suffix.length);
            System.arraycopy(suffix, 0, actual, k, suffix.length);
            assertArrayEquals(c, actual);
            assertArrayEquals(Arrays.copyOf(c, k), Arrays.copyOf(actual, k));
            assertArrayEquals(retained, hybrid(b, c, k));
        }
    }
    @Test void equalBytesChooseLargestOffsetIncludingAlreadyCompleteTarget() throws Exception {
        byte[] b = new byte[164], c = new byte[164]; c[80] = 1; c[120] = 2;
        assertEquals(80, B1HandoffExactPrefix.ownerOffset(b, c, b));
        byte[] prefix = hybrid(b, c, 81);
        assertEquals(120, B1HandoffExactPrefix.ownerOffset(b, c, prefix));
        assertEquals(164, B1HandoffExactPrefix.ownerOffset(b, c, c));
        assertEquals(0, B1HandoffExactPrefix.ownerSuffix(b, c, c).length);
    }
    @Test void foreignNonprefixAndWrongLengthsNeverYieldSuffix() {
        byte[] b = new byte[164], c = new byte[164]; Arrays.fill(c, (byte) 1);
        byte[] bad = b.clone(); bad[80] = 1;
        assertThrows(IOException.class, () -> B1HandoffExactPrefix.ownerSuffix(b, c, bad));
        assertThrows(IOException.class, () -> B1HandoffExactPrefix.ownerOffset(b, c, new byte[163]));
        assertThrows(IOException.class, () -> B1HandoffExactPrefix.ownerOffset(null, c, b));
    }
    @Test void artifactCreatePartialAndFullBodyPreserveExactExpectedBytes() throws Exception {
        byte[] expected = new byte[257];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i * 31);
        for (int k = 0; k <= expected.length; k++) {
            byte[] partial = Arrays.copyOf(expected, k), saved = partial.clone();
            assertEquals(k, B1HandoffExactPrefix.artifactOffset(expected, partial, 257));
            byte[] suffix = B1HandoffExactPrefix.artifactSuffix(expected, partial, 257);
            byte[] completed = new byte[257]; System.arraycopy(partial, 0, completed, 0, k);
            System.arraycopy(suffix, 0, completed, k, suffix.length);
            assertArrayEquals(expected, completed); assertArrayEquals(saved, partial);
        }
        byte[] corrupt = Arrays.copyOf(expected, 5); corrupt[4] ^= 1;
        assertThrows(IOException.class, () -> B1HandoffExactPrefix.artifactOffset(expected, corrupt, 257));
        assertThrows(IOException.class, () -> B1HandoffExactPrefix.artifactSuffix(expected, new byte[258], 257));
        assertThrows(IOException.class, () -> B1HandoffExactPrefix.artifactOffset(expected, new byte[0], 256));
    }
    private static byte[] hybrid(byte[] b, byte[] c, int k) {
        byte[] actual = b.clone(); System.arraycopy(c, 0, actual, 0, k); return actual;
    }
}

package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 真实MarkerIo READ/EOF/identity seam；不是完整cold lookup或SQL/native资格。 */
final class WindowsInventoryStreamingCompareNativeTest {
    @TempDir Path directory;
    private static byte[] body() {
        byte[] bytes = new byte[90000];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte)(i * 31 + i / 251);
        return bytes;
    }
    private void compare(byte[] actual, byte[] expected, boolean twice) throws Exception {
        try (var io = new WindowsMarkerIo()) {
            var path = directory.resolve("inventory-payload.bin");
            try (var writer = io.createMarkerHandle(path, UUID.randomUUID())) {
                io.writeWitnessArtifact(writer, actual, 131072L); io.flushChecked(writer);
            }
            try (var reader = io.open(path, WindowsMarkerIo.READ, 0, WindowsMarkerIo.OPEN_EXISTING,
                    WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, UUID.randomUUID())) {
                var identity = io.identity(reader);
                io.compareWitnessArtifactExact(reader, expected, identity, 131072L);
                if (twice) io.compareWitnessArtifactExact(reader, expected, identity, 131072L);
            }
        }
    }
    @Test void completeNinetyThousandBytesCompareTwiceWithSameNativeHandle() throws Exception {
        byte[] expected = body(); byte[] retainedBefore = expected.clone();
        compare(expected, expected, true);
        assertArrayEquals(retainedBefore, expected);
    }
    @Test void changedByteAcrossChunkBoundaryIsRejectedWithOriginalNativeStack() throws Exception {
        byte[] expected = body(), actual = expected.clone(); actual[65536] ^= 1;
        IOException caught = null;
        try { compare(actual, expected, false); fail("complete byte mismatch must fail"); }
        catch (IOException failure) { caught = failure; }
        assertNotNull(caught); assertTrue(caught.getStackTrace().length > 0);
    }
    @Test void changedLastByteIsNotHiddenByEqualLength() throws Exception {
        byte[] expected = body(), actual = expected.clone(); actual[actual.length - 1] ^= 1;
        assertThrows(IOException.class, () -> compare(actual, expected, false));
    }
    @Test void TrailingByteCannotPassCompleteEofAndMetadata() throws Exception {
        byte[] expected = body(); byte[] actual = java.util.Arrays.copyOf(expected, expected.length + 1);
        assertThrows(IOException.class, () -> compare(actual, expected, false));
    }
    @Test void TruncatedBodyCannotPassExactMetadata() throws Exception {
        byte[] expected = body(); byte[] actual = java.util.Arrays.copyOf(expected, expected.length - 1);
        assertThrows(IOException.class, () -> compare(actual, expected, false));
    }
    @Test void ForeignFileIdentityCannotBeBorrowedForEqualCompleteBytes() throws Exception {
        try (var io = new WindowsMarkerIo()) {
            var a = directory.resolve("a.bin"); var b = directory.resolve("b.bin"); byte[] expected = body();
            for (Path path : new Path[]{a, b}) try (var h = io.createMarkerHandle(path, UUID.randomUUID())) {
                io.writeWitnessArtifact(h, expected, 131072L); io.flushChecked(h);
            }
            WindowsMarkerIo.FileIdentity foreign;
            try (var h = io.open(a, WindowsMarkerIo.READ, 0, WindowsMarkerIo.OPEN_EXISTING,
                    WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, UUID.randomUUID())) { foreign = io.identity(h); }
            try (var h = io.open(b, WindowsMarkerIo.READ, 0, WindowsMarkerIo.OPEN_EXISTING,
                    WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, UUID.randomUUID())) {
                assertThrows(IOException.class, () -> io.compareWitnessArtifactExact(h, expected, foreign, 131072L));
            }
        }
    }
}

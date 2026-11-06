package com.wi.floatingpod.server;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 未执行候选：root 只能在单独获准的新临时目录、Java25 Windows x64/native-access 下执行。
 * 无 assumption/skip；平台缺失必须失败。NIO 仅测试建目录和观察字节，不作为 durable ACK。
 */
class WindowsCanonicalMarkerPublicationBarrierTest {
    @TempDir Path temporaryRoot;
    private static final UUID TOKEN = UUID.fromString("ba502fef-f31c-4bda-98f6-381a57ccb44c");
    private WindowsCanonicalMarkerPublicationBarrier.Request request(WindowsMarkerIo io, Path marker, UUID binding, int format) throws IOException {
        UUID operation = UUID.randomUUID();
        return new WindowsCanonicalMarkerPublicationBarrier.Request(marker, binding, format, operation,
            io.captureDirectoryIdentity(marker.getParent(), operation));
    }
    @Test void newPublicationAndExactReplayHaveRealSameFileIdentityAndDistinctOperations() throws Exception {
        Path parent = Files.createDirectory(temporaryRoot.resolve("world")), marker = parent.resolve("binding.marker");
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            var barrier = new WindowsCanonicalMarkerPublicationBarrier(io);
            var firstRequest = request(io, marker, TOKEN, 2);
            var first = barrier.publish(firstRequest);
            assertFalse(first.retainedExisting());
            assertEquals(firstRequest.operationUuid(), first.operationUuid());
            assertArrayEquals(WindowsMarkerIo.markerBytes(TOKEN, 2), Files.readAllBytes(marker));
            var replayRequest = request(io, marker, TOKEN, 2);
            var replay = barrier.publish(replayRequest);
            assertTrue(replay.retainedExisting());
            assertEquals(first.fileIdentity(), replay.fileIdentity());
            assertEquals(first.markerSha256(), replay.markerSha256());
            assertNotEquals(first.operationUuid(), replay.operationUuid());
            try (var listing = Files.list(parent)) { assertEquals(1, listing.count()); }
        }
    }
    @Test void differentTokenCannotOverwriteActualCanonicalBytes() throws Exception {
        Path parent = Files.createDirectory(temporaryRoot.resolve("world")), marker = parent.resolve("binding.marker");
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            var barrier = new WindowsCanonicalMarkerPublicationBarrier(io);
            var original = barrier.publish(request(io, marker, TOKEN, 2));
            assertThrows(IOException.class, () -> barrier.publish(request(io, marker, UUID.randomUUID(), 2)));
            assertArrayEquals(WindowsMarkerIo.markerBytes(TOKEN, 2), Files.readAllBytes(marker));
            var reread = barrier.publish(request(io, marker, TOKEN, 2));
            assertEquals(original.fileIdentity(), reread.fileIdentity());
        }
    }
    @Test void restoredFormatOneIsRetainedAndNotMigrated() throws Exception {
        Path parent = Files.createDirectory(temporaryRoot.resolve("restore")), marker = parent.resolve("binding.marker");
        Files.write(marker, WindowsMarkerIo.markerBytes(TOKEN, 1));
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            var barrier = new WindowsCanonicalMarkerPublicationBarrier(io);
            assertTrue(barrier.publish(request(io, marker, TOKEN, 1)).retainedExisting());
            assertThrows(IOException.class, () -> barrier.publish(request(io, marker, TOKEN, 2)));
            assertArrayEquals(WindowsMarkerIo.markerBytes(TOKEN, 1), Files.readAllBytes(marker));
        }
    }
    @Test void actualNoReplaceMoveRejectsExistingTargetAndKeepsBothObjects() throws Exception {
        Path source = temporaryRoot.resolve("source"), target = temporaryRoot.resolve("target");
        byte[] sourceBytes = WindowsMarkerIo.markerBytes(TOKEN, 2), targetBytes = WindowsMarkerIo.markerBytes(UUID.randomUUID(), 2);
        Files.write(source, sourceBytes); Files.write(target, targetBytes);
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            var failure = assertThrows(WindowsMarkerIo.NativeFailure.class,
                () -> io.moveWriteThroughNoReplace(source, target, UUID.randomUUID()));
            assertEquals("MoveFileExW", failure.api());
            assertTrue(failure.win32Error() == 80 || failure.win32Error() == 183);
            assertArrayEquals(sourceBytes, Files.readAllBytes(source)); assertArrayEquals(targetBytes, Files.readAllBytes(target));
        }
    }
    @Test void flushOnActualReadOnlyHandleFailsAtNativeApi() throws Exception {
        Path file = temporaryRoot.resolve("readonly"); Files.write(file, WindowsMarkerIo.markerBytes(TOKEN, 2));
        try (WindowsMarkerIo io = new WindowsMarkerIo(); var handle = io.open(file, WindowsMarkerIo.READ, 0,
             WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, UUID.randomUUID())) {
            var failure = assertThrows(WindowsMarkerIo.NativeFailure.class, () -> io.flushChecked(handle));
            assertEquals("FlushFileBuffers", failure.api()); assertNotEquals(0, failure.win32Error());
        }
    }
    @Test void fileIdentityDistinguishesRealFilesEvenWithSameBytes() throws Exception {
        Path first = temporaryRoot.resolve("first"), second = temporaryRoot.resolve("second");
        Files.write(first, WindowsMarkerIo.markerBytes(TOKEN, 2)); Files.write(second, WindowsMarkerIo.markerBytes(TOKEN, 2));
        try (WindowsMarkerIo io = new WindowsMarkerIo();
             var a = io.open(first, WindowsMarkerIo.READ, 0, 3, WindowsMarkerIo.OPEN_REPARSE, UUID.randomUUID());
             var b = io.open(second, WindowsMarkerIo.READ, 0, 3, WindowsMarkerIo.OPEN_REPARSE, UUID.randomUUID())) {
            assertNotEquals(io.identity(a), io.identity(b));
            assertEquals(io.identity(a), io.identity(a));
        }
    }
    @Test void wrongCapturedParentIdentityFailsBeforePublication() throws Exception {
        Path parent = Files.createDirectory(temporaryRoot.resolve("world")), other = Files.createDirectory(temporaryRoot.resolve("other"));
        Path marker = parent.resolve("binding.marker"); UUID operation = UUID.randomUUID();
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            var request = new WindowsCanonicalMarkerPublicationBarrier.Request(marker, TOKEN, 2, operation,
                io.captureDirectoryIdentity(other, operation));
            assertThrows(IOException.class, () -> new WindowsCanonicalMarkerPublicationBarrier(io).publish(request));
            assertFalse(Files.exists(marker));
            try (var listing = Files.list(parent)) { assertEquals(0, listing.count()); }
        }
    }
    @Test void truncatedOrTrailingCanonicalIsExplicitFailureWithoutOverwrite() throws Exception {
        Path marker = temporaryRoot.resolve("binding.marker"); byte[] bad = new byte[25]; Files.write(marker, bad);
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            assertThrows(IOException.class, () -> new WindowsCanonicalMarkerPublicationBarrier(io).publish(request(io, marker, TOKEN, 2)));
            assertArrayEquals(bad, Files.readAllBytes(marker));
        }
    }
    @Test void markerEncodingAndNativeLayoutsHaveExactWidthsAndOrder() throws Exception {
        byte[] bytes = WindowsMarkerIo.markerBytes(TOKEN, 2); assertEquals(24, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        assertEquals(0x46505742, buffer.getInt()); assertEquals(2, buffer.getInt());
        assertEquals(TOKEN.getMostSignificantBits(), buffer.getLong()); assertEquals(TOKEN.getLeastSignificantBits(), buffer.getLong());
        assertEquals(24, WindowsMarkerIo.FILE_ID.byteSize()); assertEquals(8, WindowsMarkerIo.FILE_ID.byteAlignment());
        assertEquals(24, WindowsMarkerIo.STANDARD.byteSize()); assertEquals(8, WindowsMarkerIo.ATTRIBUTE_TAG.byteSize());
        byte[] id = new byte[16]; id[15] = 1; var identity = new WindowsMarkerIo.FileIdentity(-1L, id); id[15] = 2;
        assertEquals("FP_WIN32_FILE_ID_V1;volume=ffffffffffffffff;id=00000000000000000000000000000001", identity.versionedIdentity());
    }
    @Test void boundedUtf16AndProgressRejectRealMalformedValues() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment path = WindowsMarkerIo.wide(arena, "C:\\test\\世界");
            assertEquals('C', path.get(ValueLayout.JAVA_CHAR, 0));
            assertEquals('\0', path.get(ValueLayout.JAVA_CHAR, path.byteSize() - 2));
            assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.wide(arena, "a\0b"));
            assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.wide(arena, "\ud800"));
        }
        assertEquals(3, WindowsMarkerIo.progress(3, 24));
        assertThrows(IOException.class, () -> WindowsMarkerIo.progress(0, 24));
        assertThrows(IOException.class, () -> WindowsMarkerIo.progress(25, 24));
        assertThrows(IOException.class, () -> WindowsCanonicalMarkerPublicationBarrier.requireExact(new byte[24], new byte[25]));
        assertThrows(IOException.class, () -> WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent("X:\\world", "X:\\world2\\marker"));
    }
    @Test void translatedUnexpectedFailureRetainsOriginalCauseAndSuppressedCleanup() {
        IllegalStateException original = new IllegalStateException("synthetic responsible-boundary defect");
        IOException cleanup = new IOException("synthetic cleanup failure"); original.addSuppressed(cleanup);
        IOException translated = WindowsMarkerIo.ioFailure("direct-test", original);
        assertSame(original, translated.getCause()); assertSame(cleanup, translated.getCause().getSuppressed()[0]);
        assertTrue(translated.getCause().getStackTrace().length > 0);
    }
}

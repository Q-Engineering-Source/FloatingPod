package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 未执行真实Windows native tests；无模拟ACK，无skip，无SQL/Ready主张。 */
@Tag("b1-certify-native")
class WindowsExistingMarkerRebarrierTest {
    @TempDir Path root;
    @Test void existingExactCanonicalRebarrierPreservesOriginalNativeIdentityAndFullBytes() throws Exception {
        Path parent=Files.createDirectory(root.resolve("data")); Path marker=parent.resolve("floatingpod-world-binding.bin");
        try (var io=new WindowsMarkerIo()) {
            UUID operation=UUID.randomUUID(),binding=UUID.randomUUID();
            var request=new WindowsCanonicalMarkerPublicationBarrier.Request(marker,binding,2,operation,io.captureDirectoryIdentity(parent,operation));
            var publisher=new WindowsCanonicalMarkerPublicationBarrier(io); var original=publisher.publish(request);
            var current=publisher.rebarrierExisting(request,original);
            assertTrue(current.retainedExisting()); assertEquals(original.fileIdentity(),current.fileIdentity());
            assertEquals(original.parentIdentity(),current.parentIdentity()); assertEquals(original.markerSha256(),current.markerSha256());
            assertArrayEquals(WindowsMarkerIo.markerBytes(binding,2),Files.readAllBytes(marker));
        }
    }
    @Test void missingOriginalCanonicalPreservesActualNativeCauseAndCannotRecreate() throws Exception {
        Path parent=Files.createDirectory(root.resolve("data")); Path marker=parent.resolve("floatingpod-world-binding.bin");
        try(var io=new WindowsMarkerIo()) {
            UUID operation=UUID.randomUUID(); var request=new WindowsCanonicalMarkerPublicationBarrier.Request(marker,UUID.randomUUID(),2,operation,io.captureDirectoryIdentity(parent,operation));
            var publisher=new WindowsCanonicalMarkerPublicationBarrier(io); var original=publisher.publish(request);
            Files.delete(marker); // 测试移除已关闭marker；production不执行pathDelete。
            var failure=assertThrows(WindowsMarkerIo.NativeFailure.class,()->publisher.rebarrierExisting(request,original));
            assertEquals(2,failure.win32Error()); assertFalse(Files.exists(marker));
            try(var children=Files.list(parent)){assertEquals(0L,children.count());}
        }
    }
    @Test void conflictingExistingCanonicalIsRetainedWithoutRewriteOrAck() throws Exception {
        Path parent=Files.createDirectory(root.resolve("data")); Path marker=parent.resolve("floatingpod-world-binding.bin");
        try(var io=new WindowsMarkerIo()) {
            UUID operation=UUID.randomUUID(); var request=new WindowsCanonicalMarkerPublicationBarrier.Request(marker,UUID.randomUUID(),2,operation,io.captureDirectoryIdentity(parent,operation));
            var publisher=new WindowsCanonicalMarkerPublicationBarrier(io); var original=publisher.publish(request);
            byte[] conflict=WindowsMarkerIo.markerBytes(UUID.randomUUID(),2); Files.write(marker,conflict);
            assertThrows(IOException.class,()->publisher.rebarrierExisting(request,original));
            assertArrayEquals(conflict,Files.readAllBytes(marker));
        }
    }
}

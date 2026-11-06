package com.wi.floatingpod.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** Root 必须在已审 Java25 Windows x64/native-access JVM 中执行；本候选未运行。 */
@Tag("full20-witness-native")
class WindowsFull20WitnessArtifactNativeTest {
    @TempDir Path temporary;
    @Test void originalNativeHandleWritesAndReadsFullPayloadAboveReceiptCap() throws Exception {
        byte[] payload = new byte[90000];
        for(int i=0;i<payload.length;i++) payload[i]=(byte)i;
        try(var io = new WindowsMarkerIo(); var handle = io.createMarkerHandle(temporary.resolve("witness.bin"),UUID.randomUUID())) {
            io.writeWitnessArtifact(handle,payload,100000);
            io.flushChecked(handle);
            var identity = io.identity(handle);
            assertArrayEquals(payload,io.readWitnessArtifactExact(handle,payload.length,100000));
            assertEquals(identity,io.identity(handle));
            assertThrows(IOException.class, () -> io.readWitnessArtifactExact(handle,payload.length-1,100000));
            assertEquals(identity,io.identity(handle));
        }
    }
    @Test void artifactBoundRejectsBeforeWriteWithoutChangingActualHandleLength() throws Exception {
        try(var io = new WindowsMarkerIo(); var handle = io.createMarkerHandle(temporary.resolve("bound.bin"),UUID.randomUUID())) {
            assertThrows(IllegalArgumentException.class, () -> io.writeWitnessArtifact(handle,new byte[70000],65536));
            assertEquals(0,io.payloadLength(handle));
            byte[] exact = new byte[65536];
            io.writeWitnessArtifact(handle,exact,65536);
            assertArrayEquals(exact,io.readWitnessArtifactExact(handle,65536,65536));
        }
    }
    @Test void foreignActualThreadCannotWriteTheOwnedOriginalHandle() throws Exception {
        try(var io = new WindowsMarkerIo(); var handle = io.createMarkerHandle(temporary.resolve("owner.bin"),UUID.randomUUID())) {
            var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            Thread foreign = new Thread(() -> {
                try { io.writeWitnessArtifact(handle,new byte[90000],100000); }
                catch(Throwable cause) { failure.set(cause); }
            },"actual-foreign-witness-owner");
            Throwable primary=null;
            try {
                foreign.start();foreign.join(3000);
                assertFalse(foreign.isAlive(),"actual foreign helper must exit within bounded test window");
                assertInstanceOf(IllegalStateException.class,failure.get());
                assertEquals(0,io.payloadLength(handle));
            } catch(Throwable cause) { primary=cause;throw cause; }
            finally {
                try {
                    if(foreign.isAlive()) {
                        foreign.interrupt();foreign.join(3000);
                        if(foreign.isAlive())throw new IOException("Actual foreign helper did not exit after bounded cleanup");
                    }
                } catch(Throwable cleanup) {
                    if(primary!=null) { if(cleanup!=primary)primary.addSuppressed(cleanup); }
                    else throw cleanup;
                }
            }
        }
    }
}

package com.wi.floatingpod.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 真native volume GUID回归；不把finalPath交Java Path parser，不构造假ACK。未执行。 */
@Tag("b1-certify-native")
class WindowsNativeLegacySlotProbeTest {
    @TempDir Path temporary;
    private String nativeWorld(WindowsMarkerIo io,Path world,UUID operation)throws Exception {
        try(var handle=io.directory(world,operation)){return io.finalPath(handle);}
    }
    /** 原365源码在actual publishCreatedSource→requireDataLocation处复现同一InvalidPathException。 */
    @Test void actualAnchorPublicationRebarrierAndLegacyConflictUseVolumeGuidWithoutPathParsing()throws Exception {
        Path root=Files.createDirectory(temporary.resolve("anchor-regression"));
        String javaExecutable=System.getProperty("fp.b1.certify.child.java"),cp=System.getProperty("fp.b1.certify.child.classpath");
        if(javaExecutable==null||cp==null||!Path.of(javaExecutable).isAbsolute()||!Files.isRegularFile(Path.of(javaExecutable))||cp.isBlank())
            throw new java.io.IOException("Root must supply actual Java25 and frozen V4 regression classpath");
        Process child=new ProcessBuilder(javaExecutable,"--enable-native-access=ALL-UNNAMED","-cp",cp,AnchorChild.class.getName(),root.toString()).redirectErrorStream(true).start();
        try {
            child.getOutputStream().close();
            if(!child.waitFor(20,java.util.concurrent.TimeUnit.SECONDS))throw new java.io.IOException("Native GUID regression child timeout");
            String output=new String(child.getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(0,child.exitValue(),output);
        } finally {
            if(child.isAlive()){child.destroyForcibly();if(!child.waitFor(5,java.util.concurrent.TimeUnit.SECONDS))throw new java.io.IOException("Native GUID regression child cleanup timeout");}
        }
    }
    public static final class AnchorChild {
        public static void main(String[] args)throws Exception {
            if(args.length!=1)throw new IllegalArgumentException("Exact test root required");Path root=Path.of(args[0]);
            Path metadata=B1SourceAnchorFixture.writeMetadata(root);var io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);
            var created=provisioner.createFresh(root.resolve("world"),B1SourceAnchorFixture.plan(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),1));
            var anchors=new WindowsB1SourceAnchorStore(io);
            var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(com.wi.floatingpod.config.MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());
            var ack=anchors.publishCreatedSource(location,created);assertTrue(ack.verifiedInput().worldNativePath().startsWith("\\\\?\\Volume{"));
            var live=anchors.rebarrierExisting(location,ack.verifiedInput());assertEquals(ack.anchorSha256(),live.anchorSha256());
            Path legacy=root.resolve("world/.floatingpod-b1-resume-v3.tmp");Files.write(legacy,new byte[]{3,4,5});
            assertThrows(java.io.IOException.class,()->anchors.rebarrierExisting(location,live.verifiedInput()));
            assertArrayEquals(new byte[]{3,4,5},Files.readAllBytes(legacy));assertTrue(Files.exists(root.resolve("config/runtime.properties.b1-source-v4")));
            // Actual custody stays process-lifetime owned; no early cleanup or IOExit/termination proof.
        }
    }
    @Test void actualVolumeGuidWorldAbsentLegacyNamesDoNotCreateFiles()throws Exception {
        Path world=Files.createDirectory(temporary.resolve("world"));UUID operation=UUID.randomUUID();
        try(var io=new WindowsMarkerIo()) {
            String nativePath=nativeWorld(io,world,operation);assertTrue(nativePath.startsWith("\\\\?\\Volume{"));
            WindowsB1SourceAnchorStore.refuseLegacySlots(io,nativePath,operation);
            try(var children=Files.list(world)){assertEquals(0L,children.count());}
        }
    }
    @Test void actualVolumeGuidExistingLegacySlotIsRefusedAndPreserved()throws Exception {
        Path world=Files.createDirectory(temporary.resolve("world"));Path legacy=world.resolve(".floatingpod-b1-resume-v3.tmp");
        Files.write(legacy,new byte[]{7,8,9});UUID operation=UUID.randomUUID();
        try(var io=new WindowsMarkerIo()) {
            String nativePath=nativeWorld(io,world,operation);
            assertThrows(java.io.IOException.class,()->WindowsB1SourceAnchorStore.refuseLegacySlots(io,nativePath,operation));
            assertArrayEquals(new byte[]{7,8,9},Files.readAllBytes(legacy));
            try(var handle=io.openExistingNativeLegacySlot(nativePath,".floatingpod-b1-resume-v3.tmp",operation)) {
                WindowsB1SourceAnchorStore.requireExactNativeChild(nativePath,io.finalPath(handle),".floatingpod-b1-resume-v3.tmp");
            }
        }
    }
    @Test void missingNativeParentError3CannotBeInterpretedAsAbsentLegacyFile()throws Exception {
        Path world=Files.createDirectory(temporary.resolve("world"));UUID operation=UUID.randomUUID();
        try(var io=new WindowsMarkerIo()) {
            String absentParent=nativeWorld(io,world,operation)+"\\missing-parent";
            var failure=assertThrows(WindowsMarkerIo.NativeFailure.class,()->WindowsB1SourceAnchorStore.refuseLegacySlots(io,absentParent,operation));
            assertEquals(3,failure.win32Error());assertFalse(Files.exists(world.resolve("missing-parent")));
        }
    }
    @Test void actualNativeWorldCannotOpenArbitraryNamesTraversalOrAliases()throws Exception {
        Path world=Files.createDirectory(temporary.resolve("world"));UUID operation=UUID.randomUUID();
        try(var io=new WindowsMarkerIo()) {
            String nativePath=nativeWorld(io,world,operation);
            for(String name:new String[]{"other.bin",".floatingpod-b1-resume-v4.0","..\\.floatingpod-b1-resume-v3.0",".floatingpod-b1-resume-v3.0 "})
                assertThrows(IllegalArgumentException.class,()->io.openExistingNativeLegacySlot(nativePath,name,operation));
            for(String parent:new String[]{nativePath+"\\..",nativePath+"\\alias.",nativePath+"\\",nativePath+"\\\\child"})
                assertThrows(IllegalArgumentException.class,()->io.openExistingNativeLegacySlot(parent,".floatingpod-b1-resume-v3.0",operation));
            try(var children=Files.list(world)){assertEquals(0L,children.count());}
        }
    }
}

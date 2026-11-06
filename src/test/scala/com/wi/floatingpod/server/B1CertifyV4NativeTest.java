package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real native files/custody/anchor/slots in bounded isolated child. SQL images are synthetic facts only. */
@Tag("b1-certify-native")
class B1CertifyV4NativeTest {
    @TempDir Path temporary;
    private String child(String mode)throws Exception {
        Path root=Files.createDirectory(temporary.resolve(mode));
        String java=System.getProperty("fp.b1.certify.child.java"),cp=System.getProperty("fp.b1.certify.child.classpath");
        if(java==null||cp==null||!Path.of(java).isAbsolute()||!Files.isRegularFile(Path.of(java))||cp.isBlank())
            throw new IOException("Root must supply actual Java25 and frozen V4 child classpath");
        Process p=new ProcessBuilder(java,"--enable-native-access=ALL-UNNAMED","-cp",cp,Child.class.getName(),mode,root.toString()).redirectErrorStream(true).start();
        try {p.getOutputStream().close();if(!p.waitFor(45,TimeUnit.SECONDS))throw new IOException("V4 native child exceeded bounded window");
            String output=new String(p.getInputStream().readAllBytes(),StandardCharsets.UTF_8);assertEquals(0,p.exitValue(),output);return output;
        }finally{if(p.isAlive()){p.destroyForcibly();if(!p.waitFor(5,TimeUnit.SECONDS))throw new IOException("V4 child cleanup timeout");}}
    }
    @Test void actualSeq1Retire0IntentSeq2Retire1CertifiedSeq3()throws Exception{assertTrue(child("rotate").contains("V4_NATIVE_CHAIN=true"));}
    @Test void everyLegacyAnchorAndTemporaryNameIsRejectedAndPreserved()throws Exception{assertTrue(child("legacy-anchor").contains("V4_LEGACY_ANCHOR_REJECTED=true"));}
    @Test void everyLegacySlotAndTemporaryNameIsRejectedAndPreserved()throws Exception{assertTrue(child("legacy-slot").contains("V4_LEGACY_SLOT_REJECTED=true"));}
    @Test void mixedV4WithLegacyAnchorBlocksReadAndRebarrierWithoutDeletion()throws Exception{assertTrue(child("mixed-anchor").contains("V4_MIXED_ANCHOR_REJECTED=true"));}
    @Test void mixedV4WithLegacySlotBlocksReadAndRebarrierWithoutDeletion()throws Exception{assertTrue(child("mixed-slot").contains("V4_MIXED_SLOT_REJECTED=true"));}
    @Test void v4FilenameCannotReadOrRebarrierV3Envelope()throws Exception{assertTrue(child("old-bytes").contains("V4_OLD_BYTES_REJECTED=true"));}
    public static final class Child {
        private record Actual(WindowsMarkerIo io,WindowsWorldProvisioner provisioner,WindowsWorldProvisioner.ProvisionedWorld world,
                WindowsB1SourceAnchorStore anchors,WindowsB1SourceAnchorStore.ControlledAnchorLocation location,
                WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor,WindowsB1TwoSlotStore slots,WindowsB1TwoSlotStore.SourceScope scope,
                FullStateAssertions genesis,Path root) { }
        private static Actual fixture(Path root,boolean publish)throws Exception {
            Files.createDirectories(root);Path metadata=B1SourceAnchorFixture.writeMetadata(root);
            var io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var boot=new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),1);
            var world=provisioner.createFresh(root.resolve("world"),B1SourceAnchorFixture.plan(),boot);var anchors=new WindowsB1SourceAnchorStore(io);
            var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());
            var anchor=publish?anchors.publishCreatedSource(location,world):null;var slots=new WindowsB1TwoSlotStore(io);
            var scope=publish?slots.bindSource(anchors,location,anchor,world.bootWorldCustodyProof()):null;
            var process=io.captureCurrentProcessIdentity(B1SourceAnchorFixture.plan().bootstrapOperationUuid());
            var owner=new NativeOwnerAssertions(process.pid(),process.creationFileTime(),boot.bootId(),boot.generation());
            return new Actual(io,provisioner,world,anchors,location,anchor,slots,scope,B1TwoSlotFixture.genesis(owner),root);
        }
        public static void main(String[] args)throws Exception {
            if(args.length!=2)throw new IllegalArgumentException("Exact mode/root required");String mode=args[0];Path root=Path.of(args[1]);
            if(mode.equals("legacy-anchor")) {
                for(int v=1;v<=3;v++)for(String tail:new String[]{"",".tmp"}) {
                    var f=fixture(root.resolve("v"+v+(tail.isEmpty()?"canonical":"tmp")),false);Path legacy=f.root.resolve("config/runtime.properties.b1-source-v"+v+tail);
                    Files.write(legacy,new byte[]{1,2,3});assertThrows(IOException.class,()->f.anchors.publishCreatedSource(f.location,f.world));
                    assertThrows(IOException.class,()->f.anchors.readExpectedSource(f.location));assertArrayEquals(new byte[]{1,2,3},Files.readAllBytes(legacy));
                    assertFalse(Files.exists(f.root.resolve("config/runtime.properties.b1-source-v4")));
                }
                System.out.println("V4_LEGACY_ANCHOR_REJECTED=true");return;
            }
            if(mode.equals("legacy-slot")) {
                for(int v=1;v<=3;v++)for(String tail:new String[]{".0",".1",".tmp"}) {
                    var f=fixture(root.resolve("v"+v+tail),false);Path legacy=f.root.resolve("world/.floatingpod-b1-resume-v"+v+tail);
                    Files.write(legacy,new byte[]{1,2,3});assertThrows(IOException.class,()->f.anchors.publishCreatedSource(f.location,f.world));
                    assertArrayEquals(new byte[]{1,2,3},Files.readAllBytes(legacy));assertFalse(Files.exists(f.root.resolve("config/runtime.properties.b1-source-v4")));
                }
                System.out.println("V4_LEGACY_SLOT_REJECTED=true");return;
            }
            var f=fixture(root,true);var g=f.slots.publishGenesis(f.scope,f.genesis);
            if(mode.equals("mixed-anchor")) {
                Path old=root.resolve("config/runtime.properties.b1-source-v3");Files.write(old,new byte[]{4,5});
                assertThrows(IOException.class,()->f.anchors.readExpectedSource(f.location));assertThrows(IOException.class,()->f.anchors.rebarrierExisting(f.location,f.anchor.verifiedInput()));
                assertArrayEquals(new byte[]{4,5},Files.readAllBytes(old));assertTrue(Files.exists(root.resolve("config/runtime.properties.b1-source-v4")));
                System.out.println("V4_MIXED_ANCHOR_REJECTED=true");return;
            }
            if(mode.equals("mixed-slot")) {
                Path old=root.resolve("world/.floatingpod-b1-resume-v3.tmp");Files.write(old,new byte[]{4,5});
                assertThrows(IOException.class,()->f.slots.readCurrent(f.scope));assertThrows(IOException.class,()->f.slots.rebarrierCurrent(f.scope,g.facts()));
                assertArrayEquals(new byte[]{4,5},Files.readAllBytes(old));assertTrue(Files.exists(root.resolve("world/.floatingpod-b1-resume-v4.0")));
                System.out.println("V4_MIXED_SLOT_REJECTED=true");return;
            }
            if(mode.equals("old-bytes")) {
                Path path=root.resolve("world/.floatingpod-b1-resume-v4.0");byte[] old=Files.readAllBytes(path);ByteBuffer.wrap(old).putLong(0x4650423152535633L).putInt(3);Files.write(path,old);
                assertThrows(IOException.class,()->f.slots.readCurrent(f.scope));assertThrows(IOException.class,()->f.slots.rebarrierCurrent(f.scope,g.facts()));assertArrayEquals(old,Files.readAllBytes(path));
                Path anchor=root.resolve("config/runtime.properties.b1-source-v4");byte[] oldAnchor=Files.readAllBytes(anchor);ByteBuffer.wrap(oldAnchor).putLong(0x46504231414e4333L).putInt(3);Files.write(anchor,oldAnchor);
                assertThrows(IOException.class,()->f.anchors.readExpectedSource(f.location));assertThrows(IOException.class,()->f.anchors.rebarrierExisting(f.location,f.anchor.verifiedInput()));assertArrayEquals(oldAnchor,Files.readAllBytes(anchor));
                System.out.println("V4_OLD_BYTES_REJECTED=true");return;
            }
            if(!mode.equals("rotate"))throw new IllegalArgumentException("Unknown V4 child mode");
            var prepared=B1NormalCertifyFixture.prepared(f.genesis);var p=f.slots.publishNext(f.scope,g,Kind.PREPARED_CONFIRMED,prepared);assertEquals(1,p.facts().record().sequence());
            var location=f.provisioner.ensureCanonicalMarkerLocation(f.world.bootWorldCustodyProof());var publisher=new WindowsCanonicalMarkerPublicationBarrier(f.io);
            var ack=publisher.publish(location.barrierRequest());var current=publisher.rebarrierExisting(location.barrierRequest(),ack);assertEquals(ack.fileIdentity(),current.fileIdentity());
            var facts=new MarkerAssertions(WindowsMarkerIo.markerBytes(ack.bindingUuid(),ack.physicalFormat()),ack.fileIdentity().versionedIdentity(),ack.parentIdentity().versionedIdentity(),
                ack.durabilityProtocol(),f.world.bootWorldCustodyProof().generation(),("TEST_ONLY_PUBLIC_ACK_FACTS:"+ack.operationUuid()+":"+ack.bindingUuid()+":"+ack.markerSha256()).getBytes(StandardCharsets.UTF_8));
            var intent=B1NormalCertifyFixture.intent(prepared,facts);var pair=f.slots.readCurrent(f.scope);
            var retire0=f.slots.retireInactiveSlot(f.scope,p,pair.inactive());assertEquals(0,retire0.absentSlot());
            var i=f.slots.publishNext(f.scope,retire0.retainedRecord(),Kind.NATIVE_ACK_CERTIFY_INTENT,intent);assertEquals(2,i.facts().record().sequence());
            pair=f.slots.readCurrent(f.scope);var retire1=f.slots.retireInactiveSlot(f.scope,i,pair.inactive());assertEquals(1,retire1.absentSlot());
            var c=f.slots.publishNext(f.scope,retire1.retainedRecord(),Kind.CERTIFIED,B1NormalCertifyFixture.certified(intent));assertEquals(3,c.facts().record().sequence());
            var read=f.slots.readCurrent(f.scope);assertEquals(3,read.current().record().sequence());assertEquals(2,read.inactive().record().sequence());
            assertArrayEquals(c.facts().canonicalBytes(),f.slots.rebarrierCurrent(f.scope,c.facts()).facts().canonicalBytes());
            assertNull(c.facts().record().assertions().pendingAttempt());assertNull(c.facts().record().assertions().latestSettlement());assertNotNull(c.facts().record().assertions().acknowledgedCertifyWitness());
            System.out.println("V4_NATIVE_CHAIN=true");
            // Actual current process custody deliberately retained; normal exit is not an IOExit or SQL proof.
        }
    }
}

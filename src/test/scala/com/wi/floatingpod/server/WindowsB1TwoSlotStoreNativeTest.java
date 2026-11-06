package com.wi.floatingpod.server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native binary store tests only. SQL images are explicitly synthetic caller assertions. */
@Tag("b1-two-slot-native")
class WindowsB1TwoSlotStoreNativeTest {
    @TempDir Path temporary;
    private String child(String mode)throws Exception {
        Path root=Files.createDirectory(temporary.resolve(mode));B1SourceAnchorFixture.writeMetadata(root);
        String java=System.getProperty("fp.b1.two.slot.child.java"),classpath=System.getProperty("fp.b1.two.slot.child.classpath");
        if(java==null||classpath==null||!Path.of(java).isAbsolute()||!Files.isRegularFile(Path.of(java))||classpath.isBlank())
            throw new IOException("Root must supply actual Java25 and frozen reviewed two-slot child classpath");
        Process p=new ProcessBuilder(java,"--enable-native-access=ALL-UNNAMED","-cp",classpath,Child.class.getName(),mode,root.toString()).redirectErrorStream(true).start();
        try{p.getOutputStream().close();if(!p.waitFor(20,TimeUnit.SECONDS))throw new IOException("Two-slot child exceeded bounded window");
            String output=new String(p.getInputStream().readAllBytes(),StandardCharsets.UTF_8);assertEquals(0,p.exitValue(),"Child failure: "+output);return output;
        }finally{if(p.isAlive()){p.destroyForcibly();if(!p.waitFor(5,TimeUnit.SECONDS))throw new IOException("Two-slot child failed bounded cleanup");}}
    }
    @Test void actualGenesisTwoSlotsCheckedRetirementAndThirdPublication()throws Exception{assertTrue(child("rotate").contains("NATIVE_ROTATION=true"));}
    @Test void unknownFixedTemporaryIsPreservedWithoutGenesisAck()throws Exception{assertTrue(child("temporary").contains("TEMP_RETAINED=true"));}
    @Test void actualOldVersionNameCannotBeTreatedAsEmptyGenesis()throws Exception{assertTrue(child("legacy").contains("LEGACY_REFUSED=true"));}
    @Test void currentAnchorCannotBePassedAsInactiveDeletionTarget()throws Exception{assertTrue(child("current-refusal").contains("CURRENT_RETAINED=true"));}
    @Test void changedInactiveFullBytesCannotBeDeleted()throws Exception{assertTrue(child("inactive-conflict").contains("INACTIVE_CONFLICT_RETAINED=true"));}
    @Test void twoCanonicalButNonconsecutiveRecordsAreConflict()throws Exception{assertTrue(child("nonconsecutive").contains("NONCONSECUTIVE_REFUSED=true"));}
    @Test void wrongWorkerCannotReadOrPublish()throws Exception{assertTrue(child("wrong-worker").contains("WRONG_WORKER_REFUSED=true"));}
    @Test void inventedCurrentProcessOwnerCannotIssueGenesis()throws Exception{assertTrue(child("wrong-owner").contains("OWNER_REFUSED=true"));}
    @Test void actualSingleSlotReadbackAndRebarrierPreservesNativeIdentity()throws Exception{assertTrue(child("single-rebarrier").contains("SINGLE_REBARRIER=true"));}
    /** Isolated actual native process; normal exit is not an IOExit/termination capability. */
    public static final class Child {
        public static void main(String[] args)throws Exception {
            if(args.length!=2)throw new IllegalArgumentException("Exact mode/root required");String mode=args[0];Path root=Path.of(args[1]);
            if(!java.util.Set.of("rotate","temporary","legacy","current-refusal","inactive-conflict","nonconsecutive","wrong-worker","wrong-owner","single-rebarrier").contains(mode))
                throw new IllegalArgumentException("Unknown native two-slot mode");
            WindowsMarkerIo io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var boot=new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),1);
            var created=provisioner.createFresh(root.resolve("world"),B1SourceAnchorFixture.plan(),boot);
            var anchors=new WindowsB1SourceAnchorStore(io);Path metadata=root.resolve("config/runtime.properties");
            var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());
            var anchor=anchors.publishCreatedSource(location,created);var store=new WindowsB1TwoSlotStore(io);
            var scope=store.bindSource(anchors,location,anchor,created.bootWorldCustodyProof());
            var process=io.captureCurrentProcessIdentity(B1SourceAnchorFixture.plan().creationOperationUuid());
            var owner=new NativeOwnerAssertions(process.pid(),process.creationFileTime(),boot.bootId(),boot.generation());
            var assertions=B1TwoSlotFixture.genesis(owner);Path slot0=root.resolve("world/.floatingpod-b1-resume-v4.0"),slot1=root.resolve("world/.floatingpod-b1-resume-v4.1");
            if(mode.equals("temporary")){Path temp=root.resolve("world/.floatingpod-b1-resume-v4.tmp");Files.write(temp,new byte[]{1,2,3});
                assertThrows(IOException.class,()->store.publishGenesis(scope,assertions));assertArrayEquals(new byte[]{1,2,3},Files.readAllBytes(temp));assertFalse(Files.exists(slot0));
                System.out.println("TEMP_RETAINED=true");return;}
            if(mode.equals("legacy")){Path legacy=root.resolve("world/.floatingpod-b1-resume-v1.0");Files.write(legacy,new byte[]{4,5});
                assertThrows(IOException.class,()->store.publishGenesis(scope,assertions));assertArrayEquals(new byte[]{4,5},Files.readAllBytes(legacy));assertFalse(Files.exists(slot0));
                System.out.println("LEGACY_REFUSED=true");return;}
            if(mode.equals("wrong-worker")){var failure=new AtomicReference<Throwable>();Thread wrong=new Thread(()->{try{store.readCurrent(scope);}catch(Throwable t){failure.set(t);}});
                wrong.start();wrong.join(5000);assertFalse(wrong.isAlive());assertInstanceOf(IllegalStateException.class,failure.get());System.out.println("WRONG_WORKER_REFUSED=true");return;}
            if(mode.equals("wrong-owner")){assertThrows(IOException.class,()->store.publishGenesis(scope,B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner())));
                assertFalse(Files.exists(slot0));System.out.println("OWNER_REFUSED=true");return;}
            var genesis=store.publishGenesis(scope,assertions);assertEquals(0,genesis.facts().record().sequence());
            if(mode.equals("single-rebarrier")){var read=store.readCurrent(scope);assertNull(read.inactive());var ack=store.rebarrierCurrent(scope,read.current());
                assertEquals(genesis.facts().fileIdentity(),ack.facts().fileIdentity());assertArrayEquals(genesis.facts().canonicalBytes(),ack.facts().canonicalBytes());
                System.out.println("SINGLE_REBARRIER=true");return;}
            var prepared=B1TwoSlotFixture.prepared(assertions);
            if(mode.equals("nonconsecutive")){var bad=new B1ResumeRecord(3,WindowsWorldProvisioner.digest(genesis.facts().canonicalBytes()),Kind.DB_ACQUIRED,
                genesis.facts().record().sourceAnchorSha256(),genesis.facts().record().originalReceipt(),genesis.facts().record().barrierNamespace(),prepared);
                Files.write(slot1,B1ResumeRecordCodec.encode(bad));assertThrows(IOException.class,()->store.readCurrent(scope));System.out.println("NONCONSECUTIVE_REFUSED=true");return;}
            var next=store.publishNext(scope,genesis,Kind.PREPARED_CONFIRMED,prepared);var pair=store.readCurrent(scope);
            assertEquals(1,pair.current().record().sequence());assertEquals(0,pair.inactive().record().sequence());
            if(mode.equals("current-refusal")){byte[] before=Files.readAllBytes(slot1);assertThrows(IOException.class,()->store.retireInactiveSlot(scope,next,pair.current()));
                assertArrayEquals(before,Files.readAllBytes(slot1));assertTrue(Files.exists(slot0));System.out.println("CURRENT_RETAINED=true");return;}
            if(mode.equals("inactive-conflict")){byte[] changed=Files.readAllBytes(slot0);changed[40]^=1;Files.write(slot0,changed);
                assertThrows(IOException.class,()->store.retireInactiveSlot(scope,next,pair.inactive()));assertArrayEquals(changed,Files.readAllBytes(slot0));System.out.println("INACTIVE_CONFLICT_RETAINED=true");return;}
            assertThrows(IOException.class,()->store.publishNext(scope,next,Kind.DB_ACQUIRED,prepared));
            var retired=store.retireInactiveSlot(scope,next,pair.inactive());assertEquals(0,retired.absentSlot());assertFalse(Files.exists(slot0));
            var third=store.publishNext(scope,retired.retainedRecord(),Kind.DB_ACQUIRED,prepared);assertEquals(2,third.facts().record().sequence());
            var read=store.readCurrent(scope);assertEquals(2,read.current().record().sequence());assertEquals(1,read.inactive().record().sequence());
            assertTrue(Files.exists(slot0));assertTrue(Files.exists(slot1));System.out.println("NATIVE_ROTATION=true");
            // Actual successful custody remains process-lifetime-owned; no early cleanup or manufactured exit proof.
        }
    }
}

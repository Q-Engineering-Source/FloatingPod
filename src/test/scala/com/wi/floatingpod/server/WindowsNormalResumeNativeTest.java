package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** Actual isolated native children; SQL assertions stay explicitly test-only DATA. */
@Tag("b1-normal-resume-native")
final class WindowsNormalResumeNativeTest {
    @TempDir Path temporary;
    private final Map<Path, RetainedNativeFixtureSupervisor.Fixture> retained = new LinkedHashMap<>();


    void child(String mode,Path root)throws Exception {
        if(retained.containsKey(root)){retained.get(root).finish(mode);return;}
        String java=System.getProperty("fp.b1.resume.child.java"),cp=System.getProperty("fp.b1.resume.child.classpath");
        if(java==null||cp==null||!Path.of(java).isAbsolute()||cp.isBlank())throw new IOException("Root must freeze actual Java25/native child classpath");
        Process p=new ProcessBuilder(java,"--enable-native-access=ALL-UNNAMED","-cp",cp,Child.class.getName(),mode,root.toString()).redirectErrorStream(true).start();
        Throwable primary=null;
        try{p.getOutputStream().close();if(!p.waitFor(25,TimeUnit.SECONDS))throw new IOException("Native resume child exceeded25s");String output=new String(p.getInputStream().readAllBytes(),StandardCharsets.UTF_8);assertEquals(0,p.exitValue(),output);}
        catch(Throwable t){primary=t;throw t;}
        finally{if(p.isAlive())try{p.destroyForcibly();if(!p.waitFor(5,TimeUnit.SECONDS))throw new IOException("Native child cleanup timeout");}catch(Throwable close){if(primary!=null)primary.addSuppressed(close);else throw close;}}
    }
    Path fixture(String name)throws Exception {Path root=Files.createDirectory(temporary.resolve(name));B1SourceAnchorFixture.writeMetadata(root);retained.put(root,RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.resume.child.java"),System.getProperty("fp.b1.resume.child.classpath"),Child.class,root));return root;}
    @Test void exitedOldProcessInspectRetirePublishHandoffThenInstallWithoutChangingCreation()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){
        Path root=fixture("full");byte[] receipt=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.RECEIPT_NAME));byte[] owner=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME));child("resume",root);
        assertArrayEquals(receipt,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.RECEIPT_NAME)));assertFalse(java.util.Arrays.equals(owner,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME))));
        assertEquals(Kind.HANDOFF_PREPARED,B1ResumeRecordCodec.decode(Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.0"))).kind());
    
        }
    }
    @Test void wrongWorkerCannotInspectAndOriginalGuardRemainsExact()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("thread");byte[] owner=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME));child("wrong-worker",root);assertArrayEquals(owner,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME)));
        }
    }
    @Test void inventedSuccessorCannotPublishAndOwnerRemainsOriginal()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("owner");byte[] owner=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME));child("wrong-owner",root);assertArrayEquals(owner,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME)));
        }
    }
    @Test void existingUnknownTemporaryCannotIssueHandoffOrInstallOwner()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("temp");byte[] owner=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME));child("temporary",root);assertArrayEquals(owner,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME)));assertArrayEquals(new byte[]{4,5,6},Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.tmp")));
        }
    }
    @Test void actualLiveShareZeroGuardCannotBecomeResumeInspection()throws Exception {Path root=Files.createDirectory(temporary.resolve("live"));B1SourceAnchorFixture.writeMetadata(root);child("live-refusal",root);}
    public static final class Child {
        private static void runRetained(Path root,String javaExecutable,String cp)throws Exception {
            WindowsMarkerIo io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);
            Path metadata=root.resolve("config/runtime.properties");var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());var slots=new WindowsB1TwoSlotStore(io);
            try(var creator=RetainedNativeFixtureSupervisor.exitedCreator(io,javaExecutable,cp,Child.class,"create-hold",root)){
                RetainedNativeFixtureSupervisor.signal(root,".retained-supervisor-ready");String mode=RetainedNativeFixtureSupervisor.nextMode();if(mode==null)return;
            if(!java.util.Set.of("resume","wrong-worker","wrong-owner","temporary").contains(mode))throw new IllegalArgumentException("unknown mode");
            var expected=anchors.readExpectedSource(location);var anchor=anchors.rebarrierExisting(location,expected);byte[] originalOwner=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME));
            var inspection=creator.inspect(provisioner,root.resolve("world"),expected.expectedPlan(),expected.expectedReceiptSha256(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),2));assertArrayEquals(originalOwner,inspection.originalOwnerCanonicalBytes());inspection.validateInspection();assertFalse(inspection.ownerPublicationAttempted());
            if(mode.equals("wrong-worker")){AtomicReference<Throwable> failure=new AtomicReference<>();Thread wrong=new Thread(()->{try{inspection.validateInspection();}catch(Throwable t){failure.set(t);}});wrong.start();wrong.join(5000);assertFalse(wrong.isAlive());assertInstanceOf(IOException.class,failure.get());return;}
            var scope=slots.bindResumeSource(anchors,location,anchor,inspection);var read=slots.readCurrent(scope);var predecessor=slots.rebarrierCurrent(scope,read.current());assertThrows(IOException.class,()->slots.retireInactiveSlot(scope,predecessor,read.current()));
            var retired=slots.retireInactiveSlot(scope,predecessor,read.inactive());var old=retired.retainedRecord().facts().record().assertions();var target=inspection.targetOwner();var original=old.originalPrepareWitness();UUID operation=UUID.randomUUID();var ctx=new OperationAssertions(target.boot(),old.currentWriter().epoch(),old.currentWriter().binding(),operation,original.context().effects(),original.context().originalOperationFingerprint());
            var handoff=new AttemptAssertions(UUID.randomUUID(),Phase.HANDOFF,operation,target.generation(),UUID.randomUUID(),123,expected.barrierNamespace(),ctx,old.currentWriter(),old.targetWriter(),original.beforeGraph(),original.targetGraph());
            var state=new FullStateAssertions(target,inspection.previousOwner(),old.retirement(),original,old.currentWriter(),old.targetWriter(),handoff,old.latestSettlement(),old.marker(),old.acknowledgedCertifyWitness());
            if(mode.equals("wrong-owner")){var bad=new FullStateAssertions(inspection.previousOwner(),inspection.previousOwner(),state.retirement(),original,state.currentWriter(),state.targetWriter(),handoff,state.latestSettlement(),state.marker(),state.acknowledgedCertifyWitness());assertThrows(IOException.class,()->slots.publishHandoffPrepared(scope,retired.retainedRecord(),bad));inspection.validateInspection();return;}
            if(mode.equals("temporary")){Files.write(root.resolve("world/.floatingpod-b1-resume-v4.tmp"),new byte[]{4,5,6});assertThrows(IOException.class,()->slots.publishHandoffPrepared(scope,retired.retainedRecord(),state));assertFalse(inspection.ownerPublicationAttempted());return;}
            var ack=slots.publishHandoffPrepared(scope,retired.retainedRecord(),state);inspection.validateInspection();assertFalse(inspection.ownerPublicationAttempted());assertEquals(target,ack.record().assertions().currentOwner());assertEquals(inspection.previousOwner(),ack.record().assertions().previousOwner());
            var resumed=provisioner.installResumeOwner(inspection,ack);assertFalse(resumed.createdThisBoot());resumed.bootWorldCustodyProof().validateLive();assertEquals(target.boot(),resumed.bootWorldCustodyProof().bootId());assertArrayEquals(expected.originalReceiptCanonicalBytes(),resumed.freshProvisioningProof().receiptCanonicalBytes());assertThrows(IOException.class,inspection::validateInspection);assertThrows(IOException.class,()->provisioner.installResumeOwner(inspection,ack));
            }
        }
        public static void main(String[] args)throws Exception {
            if(args.length==4 && args[0].equals("retained-supervisor")){runRetained(Path.of(args[1]),args[2],args[3]);return;}
            if(args.length!=2)throw new IllegalArgumentException("exact mode/root");String mode=args[0];Path root=Path.of(args[1]);Path metadata=root.resolve("config/runtime.properties");
            WindowsMarkerIo io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());var slots=new WindowsB1TwoSlotStore(io);
            if(mode.equals("live-refusal")){var world=provisioner.createFresh(root.resolve("world"),B1SourceAnchorFixture.plan(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),1));var nativeFailure=assertThrows(WindowsMarkerIo.NativeFailure.class,()->provisioner.beginResumeInspection(root.resolve("world"),world.freshProvisioningProof().plan(),world.freshProvisioningProof().receiptSha256(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),2)));assertEquals(32,nativeFailure.win32Error());world.bootWorldCustodyProof().validateLive();return;}
            if(mode.equals("create")||mode.equals("create-hold")){var boot=new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),1);var world=provisioner.createFresh(root.resolve("world"),B1SourceAnchorFixture.plan(),boot);var ack=anchors.publishCreatedSource(location,world);var scope=slots.bindSource(anchors,location,ack,world.bootWorldCustodyProof());var process=io.captureCurrentProcessIdentity(B1SourceAnchorFixture.plan().creationOperationUuid());var state=B1TwoSlotFixture.genesis(new NativeOwnerAssertions(process.pid(),process.creationFileTime(),boot.bootId(),1));var genesis=slots.publishGenesis(scope,state);slots.publishNext(scope,genesis,Kind.PREPARED_CONFIRMED,B1TwoSlotFixture.prepared(state));if(mode.equals("create-hold"))RetainedNativeFixtureSupervisor.holdCreated(root,world);return;}
            throw new IllegalArgumentException("Resume modes require the exact retained supervisor");
        }
    }
}

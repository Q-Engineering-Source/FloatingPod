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
final class WindowsInstalledResumeRecordScopeNativeTest {
    @TempDir Path temporary;
    private final Map<Path, RetainedNativeFixtureSupervisor.Fixture> retained = new LinkedHashMap<>();


    void child(String mode,Path root)throws Exception {
        var fixture=retained.get(root);
        if(fixture==null)throw new IOException("Exact retained native supervisor fixture required");
        fixture.finish(mode);
    }
    Path fixture(String name)throws Exception {Path root=Files.createDirectory(temporary.resolve(name));B1SourceAnchorFixture.writeMetadata(root);retained.put(root,RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.resume.child.java"),System.getProperty("fp.b1.resume.child.classpath"),Child.class,root));return root;}
    @Test void actualInstalledCustodyRebarrierRetireAndPublishPreserveFullChain()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){
            Path root=fixture("installed");byte[] receipt=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.RECEIPT_NAME));
            child("installed",root);
            assertArrayEquals(receipt,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.RECEIPT_NAME)));
            assertEquals(Kind.DB_ACQUIRED,B1ResumeRecordCodec.decode(Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.1"))).kind());
        }
    }
    @Test void actualPostInstallIssuanceFailureRetainsCustodyAndOriginalAckWithoutRetry()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("issue");child("issue-failure",root);}
    }
    @Test void installedScopeRejectsWrongWorkerBeforeMutation()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("thread");child("installed-thread",root);}
    }
    @Test void actualPublicationConflictRetainsOriginalFailureAndInstalledCustody()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("failure");child("installed-failure",root);}
    }
    public static final class Child {
        private static void runRetained(Path root,String javaExecutable,String cp)throws Exception {
            WindowsMarkerIo io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);
            Path metadata=root.resolve("config/runtime.properties");var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());var slots=new WindowsB1TwoSlotStore(io);
            try(var creator=RetainedNativeFixtureSupervisor.exitedCreator(io,javaExecutable,cp,Child.class,"create-hold",root)){
                RetainedNativeFixtureSupervisor.signal(root,".retained-supervisor-ready");String mode=RetainedNativeFixtureSupervisor.nextMode();if(mode==null)return;
            if(!java.util.Set.of("installed","issue-failure","installed-thread","installed-failure").contains(mode))throw new IllegalArgumentException("unknown mode");
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
            if(mode.equals("issue-failure")){
                var installed=provisioner.installResumeOwner(inspection,ack);
                Files.write(root.resolve("world/.floatingpod-b1-resume-v4.tmp"),new byte[]{7,8,9});
                Throwable failure;
                try{ack.issueInstalledScope(inspection,installed);throw new AssertionError("issuance must reject temporary");}
                catch(IOException originalFailure){failure=originalFailure;}
                assertSame(installed,inspection.installedWorld());assertSame(ack,inspection.installedHandoffAcknowledgement());
                assertSame(failure,inspection.installedContinuationFailure());assertTrue(failure.getStackTrace().length>0);
                assertNull(inspection.installedRecordScope());installed.bootWorldCustodyProof().validateLive();
                assertThrows(IOException.class,()->provisioner.installResumeOwnerForRecords(inspection,ack));
                assertThrows(IOException.class,()->ack.issueInstalledScope(inspection,installed));
                assertSame(failure,inspection.installedContinuationFailure());return;
            }
            var installed=provisioner.installResumeOwnerForRecords(inspection,ack);
            assertSame(installed,inspection.installedRecordScope());assertSame(ack,inspection.installedHandoffAcknowledgement());
            var world=installed.installedWorld();assertSame(world,inspection.installedWorld());
            assertFalse(world.createdThisBoot());world.bootWorldCustodyProof().validateLive();
            assertArrayEquals(expected.originalReceiptCanonicalBytes(),world.freshProvisioningProof().receiptCanonicalBytes());
            assertArrayEquals(ack.canonicalBytes(),installed.initialRecord().facts().canonicalBytes());
            assertThrows(IOException.class,inspection::validateInspection);assertThrows(IOException.class,()->slots.readCurrent(scope));
            assertThrows(IOException.class,()->provisioner.installResumeOwnerForRecords(inspection,ack));
            if(mode.equals("installed-thread")){
                var failure=new AtomicReference<Throwable>();
                var wrong=new Thread(()->{try{slots.readCurrent(installed);}catch(Throwable originalFailure){failure.set(originalFailure);}});
                wrong.start();wrong.join(5000);assertFalse(wrong.isAlive());assertInstanceOf(IllegalStateException.class,failure.get());
                assertNull(installed.retainedFailure());assertArrayEquals(ack.canonicalBytes(),slots.readCurrent(installed).current().canonicalBytes());return;
            }
            if(mode.equals("installed-failure")){
                Files.write(root.resolve("world/.floatingpod-b1-resume-v4.tmp"),new byte[]{3,2,1});
                Throwable failure;
                try{slots.publishNext(installed,installed.initialRecord(),Kind.DB_ACQUIRED,state);throw new AssertionError("unknown temporary must reject publication");}
                catch(IOException originalFailure){failure=originalFailure;}
                assertSame(failure,installed.retainedFailure());assertSame(failure,inspection.retainedFailure());
                assertTrue(failure.getStackTrace().length>0);world.bootWorldCustodyProof().validateLive();
                assertThrows(IOException.class,()->slots.readCurrent(installed));
                assertArrayEquals(ack.canonicalBytes(),Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.0")));return;
            }
            var current=slots.readCurrent(installed);var durable=slots.rebarrierCurrent(installed,current.current());
            assertEquals(installed.initialRecord().facts().fileIdentity(),durable.facts().fileIdentity());
            var removed=slots.retireInactiveSlot(installed,durable,current.inactive());
            var next=slots.publishNext(installed,removed.retainedRecord(),Kind.DB_ACQUIRED,state);
            assertEquals(ack.record().sequence()+1,next.facts().record().sequence());
            assertArrayEquals(WindowsWorldProvisioner.digest(ack.canonicalBytes()),next.facts().record().previousRecordSha256());
            assertArrayEquals(ack.record().originalReceipt(),next.facts().record().originalReceipt());
            assertEquals(ack.record().barrierNamespace(),next.facts().record().barrierNamespace());
            assertSame(world,installed.installedWorld());assertNull(installed.retainedFailure());
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

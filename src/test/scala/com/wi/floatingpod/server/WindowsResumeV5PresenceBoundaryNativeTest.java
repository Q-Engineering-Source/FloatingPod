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
final class WindowsResumeV5PresenceBoundaryNativeTest {
    @TempDir Path temporary;
    private final Map<Path, RetainedNativeFixtureSupervisor.Fixture> retained = new LinkedHashMap<>();


    void child(String mode,Path root)throws Exception {
        var fixture=retained.get(root);
        if(fixture==null)throw new IOException("Exact retained native supervisor fixture required");
        fixture.finish(mode);
    }
    Path fixture(String name)throws Exception {Path root=Files.createDirectory(temporary.resolve(name));B1SourceAnchorFixture.writeMetadata(root);retained.put(root,RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.resume.child.java"),System.getProperty("fp.b1.resume.child.classpath"),Child.class,root));return root;}
    @Test void actualResumeScopeRejectsV5FixedBeforeV4Mutation() throws Exception {
        try (var owned = RetainedNativeFixtureSupervisor.scope(retained)) {
            Path root = fixture("v5-fixed"); child("v5-fixed", root);
        }
    }
    @Test void actualResumeScopeRejectsPendingV5BeforeV4Mutation() throws Exception {
        try (var owned = RetainedNativeFixtureSupervisor.scope(retained)) {
            Path root = fixture("v5-pending"); child("v5-pending", root);
        }
    }
    public static final class Child {
        private static void runRetained(Path root,String javaExecutable,String cp)throws Exception {
            WindowsMarkerIo io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);
            Path metadata=root.resolve("config/runtime.properties");var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());var slots=new WindowsB1TwoSlotStore(io);
            try(var creator=RetainedNativeFixtureSupervisor.exitedCreator(io,javaExecutable,cp,Child.class,"create-hold",root)){
                RetainedNativeFixtureSupervisor.signal(root,".retained-supervisor-ready");String mode=RetainedNativeFixtureSupervisor.nextMode();if(mode==null)return;
            if(!java.util.Set.of("v5-fixed","v5-pending").contains(mode))throw new IllegalArgumentException("unknown mode");
            var expected=anchors.readExpectedSource(location);var anchor=anchors.rebarrierExisting(location,expected);byte[] originalOwner=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME));
            var inspection=creator.inspect(provisioner,root.resolve("world"),expected.expectedPlan(),expected.expectedReceiptSha256(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),2));assertArrayEquals(originalOwner,inspection.originalOwnerCanonicalBytes());inspection.validateInspection();assertFalse(inspection.ownerPublicationAttempted());
            if(mode.equals("wrong-worker")){AtomicReference<Throwable> failure=new AtomicReference<>();Thread wrong=new Thread(()->{try{inspection.validateInspection();}catch(Throwable t){failure.set(t);}});wrong.start();wrong.join(5000);assertFalse(wrong.isAlive());assertInstanceOf(IOException.class,failure.get());return;}
            var scope=slots.bindResumeSource(anchors,location,anchor,inspection);var read=slots.readCurrent(scope);var predecessor=slots.rebarrierCurrent(scope,read.current());assertThrows(IOException.class,()->slots.retireInactiveSlot(scope,predecessor,read.current()));
            var old = predecessor.facts().record().assertions();
            var target = inspection.targetOwner();
            var original = old.originalPrepareWitness();
            UUID operation = UUID.randomUUID();
            var context = new OperationAssertions(target.boot(), old.currentWriter().epoch(),
                    old.currentWriter().binding(), operation, original.context().effects(),
                    original.context().originalOperationFingerprint());
            var handoff = new AttemptAssertions(UUID.randomUUID(), Phase.HANDOFF, operation,
                    target.generation(), UUID.randomUUID(), 123, expected.barrierNamespace(), context,
                    old.currentWriter(), old.targetWriter(), original.beforeGraph(), original.targetGraph());
            var targetState = new FullStateAssertions(target, inspection.previousOwner(), old.retirement(),
                    original, old.currentWriter(), old.targetWriter(), handoff, old.latestSettlement(),
                    old.marker(), old.acknowledgedCertifyWitness());
            assertEquals(target, targetState.currentOwner());
            assertEquals(inspection.previousOwner(), targetState.previousOwner());
            byte[] slot0 = Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.0"));
            byte[] slot1 = Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.1"));
            Path v5 = root.resolve("world/" + (mode.equals("v5-fixed")
                    ? ".floatingpod-b1-resume-v5.0" : ".floatingpod-b1-resume-v5.tmp"));
            // Presence DATA deliberately has no V5 ACK authority: it must block V4 mutation.
            byte[] pending = new byte[]{7, 5, 1}; Files.write(v5, pending);
            IOException retireFailure = assertThrows(IOException.class,
                    () -> slots.retireInactiveSlot(scope, predecessor, read.inactive()));
            assertEquals("Fixed B1 file exists; conflict retained: " + v5.getFileName(), retireFailure.getMessage());
            assertTrue(retireFailure.getStackTrace().length > 0);
            IOException publishFailure = assertThrows(IOException.class,
                    () -> slots.publishHandoffPrepared(scope, predecessor, targetState));
            assertEquals("Fixed B1 file exists; conflict retained: " + v5.getFileName(), publishFailure.getMessage());
            assertTrue(publishFailure.getStackTrace().length > 0);
            assertArrayEquals(slot0, Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.0")));
            assertArrayEquals(slot1, Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.1")));
            assertArrayEquals(pending, Files.readAllBytes(v5));
            assertArrayEquals(originalOwner, Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.GUARD_NAME)));
            assertFalse(inspection.ownerPublicationAttempted());
            return;
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

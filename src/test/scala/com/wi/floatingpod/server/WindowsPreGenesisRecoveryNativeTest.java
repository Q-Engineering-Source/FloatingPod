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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real separated native processes; every SQL context here is test-only DATA. */
@Tag("b1-pre-genesis-native")
final class WindowsPreGenesisRecoveryNativeTest {
    @TempDir Path temp;
    private final Map<Path, RetainedNativeFixtureSupervisor.Fixture> retained = new LinkedHashMap<>();


    void child(String mode,Path root)throws Exception {
        if(retained.containsKey(root)){retained.get(root).finish(mode);return;}
        String java=System.getProperty("fp.b1.pregenesis.child.java"),cp=System.getProperty("fp.b1.pregenesis.child.classpath");
        if(java==null||cp==null||!Path.of(java).isAbsolute()||cp.isBlank())throw new IOException("Root must supply frozen Java25/native child inputs");
        Process p=new ProcessBuilder(java,"--enable-native-access=ALL-UNNAMED","-cp",cp,Child.class.getName(),mode,root.toString()).redirectErrorStream(true).start();Throwable failure=null;
        try{p.getOutputStream().close();if(!p.waitFor(25,TimeUnit.SECONDS))throw new IOException("Pre-genesis child timeout25s");String out=new String(p.getInputStream().readAllBytes(),StandardCharsets.UTF_8);assertEquals(0,p.exitValue(),out);}
        catch(Throwable t){failure=t;throw t;}
        finally{if(p.isAlive())try{p.destroyForcibly();if(!p.waitFor(5,TimeUnit.SECONDS))throw new IOException("Pre-genesis cleanup timeout");}catch(Throwable cleanup){if(failure!=null)failure.addSuppressed(cleanup);else throw cleanup;}}
    }
    Path fixture(String name)throws Exception {Path root=Files.createDirectory(temp.resolve(name));B1SourceAnchorFixture.writeMetadata(root);retained.put(root,RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.pregenesis.child.java"),System.getProperty("fp.b1.pregenesis.child.classpath"),Child.class,root));assertFalse(Files.exists(root.resolve("world/.floatingpod-b1-resume-v4.0")));return root;}
    Path guard(Path root){return root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME);}
    @Test void anchoredNoGenesisCrashRecoversWithActualNativeAckBeforeInstallingOwner()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){
        Path root=fixture("positive");byte[] receipt=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.RECEIPT_NAME));byte[] old=Files.readAllBytes(guard(root));child("recover",root);
        assertArrayEquals(receipt,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.RECEIPT_NAME)));assertFalse(java.util.Arrays.equals(old,Files.readAllBytes(guard(root))));assertFalse(Files.exists(root.resolve("world/.floatingpod-b1-resume-v4.1")));
        B1ResumeRecord record=B1ResumeRecordCodec.decode(Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.0")));assertEquals(Kind.GENESIS,record.kind());assertEquals(0,record.sequence());assertArrayEquals(new byte[32],record.previousRecordSha256());assertNotNull(record.assertions().previousOwner());
    
        }
    }
    @Test void wrongSuccessorKeepsOriginalOwnerAndEmptySlots()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("owner");byte[] old=Files.readAllBytes(guard(root));child("wrong-owner",root);assertArrayEquals(old,Files.readAllBytes(guard(root)));assertFalse(Files.exists(root.resolve("world/.floatingpod-b1-resume-v4.0")));
        }
    }
    @Test void wrongFixedPrepareOperationKeepsOriginalOwnerAndEmptySlots()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("operation");byte[] old=Files.readAllBytes(guard(root));child("wrong-operation",root);assertArrayEquals(old,Files.readAllBytes(guard(root)));assertFalse(Files.exists(root.resolve("world/.floatingpod-b1-resume-v4.0")));
        }
    }
    @Test void unknownFixedTemporaryRemainsAndCannotIssueAck()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("temp");byte[] old=Files.readAllBytes(guard(root));Files.write(root.resolve("world/.floatingpod-b1-resume-v4.tmp"),new byte[]{7,8,9});child("conflict",root);assertArrayEquals(old,Files.readAllBytes(guard(root)));assertArrayEquals(new byte[]{7,8,9},Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.tmp")));
        }
    }
    @Test void anyExistingCanonicalRemainsAndCannotBeClaimedAsNewGenesis()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("canonical");byte[] old=Files.readAllBytes(guard(root));Files.write(root.resolve("world/.floatingpod-b1-resume-v4.1"),new byte[]{1,2,3});child("conflict",root);assertArrayEquals(old,Files.readAllBytes(guard(root)));assertArrayEquals(new byte[]{1,2,3},Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.1")));
        }
    }
    @Test void changedOldGuardCannotIssueInspectionOrNewOwner()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("guard");byte[] changed=Files.readAllBytes(guard(root));changed[0]^=1;Files.write(guard(root),changed);child("changed-guard",root);assertArrayEquals(changed,Files.readAllBytes(guard(root)));assertFalse(Files.exists(root.resolve("world/.floatingpod-b1-resume-v4.0")));
        }
    }
    @Test void durableAckCannotInstallForeignActualInspection()throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)){Path root=fixture("foreign");Path other=Files.createDirectory(root.resolve("foreign"));B1SourceAnchorFixture.writeMetadata(other);retained.get(root).prepareForeign();byte[] original=Files.readAllBytes(guard(other));child("foreign-ack",root);assertArrayEquals(original,Files.readAllBytes(guard(other)));
        }
    }
    public static final class Child {
        private static void runRetained(Path root,String javaExecutable,String cp)throws Exception {
            WindowsMarkerIo io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);
            Path metadata=root.resolve("config/runtime.properties");var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());var slots=new WindowsB1TwoSlotStore(io);
            try(var creator=RetainedNativeFixtureSupervisor.exitedCreator(io,javaExecutable,cp,Child.class,"anchor-hold",root)){
                RetainedNativeFixtureSupervisor.signal(root,".retained-supervisor-ready");String mode=RetainedNativeFixtureSupervisor.nextMode();
                RetainedNativeFixtureSupervisor.Creator foreignCreator=null;Throwable primary=null;
                try{
                    if("prepare-foreign".equals(mode)){
                        foreignCreator=RetainedNativeFixtureSupervisor.exitedCreator(io,javaExecutable,cp,Child.class,"anchor-hold",root.resolve("foreign"));
                        RetainedNativeFixtureSupervisor.signal(root.resolve("foreign"),".retained-supervisor-ready");mode=RetainedNativeFixtureSupervisor.nextMode();
                    }
                    if(mode==null)return;
            if(!java.util.Set.of("recover","wrong-owner","wrong-operation","conflict","changed-guard","foreign-ack").contains(mode))throw new IllegalArgumentException("unknown mode");
            var expected=anchors.readExpectedSource(location);var anchor=anchors.rebarrierExisting(location,expected);
            if(mode.equals("changed-guard")){assertThrows(IOException.class,()->creator.inspect(provisioner,root.resolve("world"),expected.expectedPlan(),expected.expectedReceiptSha256(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),2)));return;}
            var inspection=creator.inspect(provisioner,root.resolve("world"),expected.expectedPlan(),expected.expectedReceiptSha256(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),2));var scope=slots.bindResumeSource(anchors,location,anchor,inspection);var owner=inspection.targetOwner();var assertions=B1TwoSlotFixture.genesis(owner);
            assertions=new FullStateAssertions(owner,inspection.previousOwner(),null,null,assertions.currentWriter(),assertions.targetWriter(),assertions.pendingAttempt(),null,null,null);
            if(mode.equals("wrong-owner")){var bad=new FullStateAssertions(inspection.previousOwner(),inspection.previousOwner(),null,null,assertions.currentWriter(),assertions.targetWriter(),assertions.pendingAttempt(),null,null,null);assertThrows(IOException.class,()->slots.publishPreGenesisRecovery(scope,bad));inspection.validateInspection();return;}
            if(mode.equals("wrong-operation")){var a=assertions.pendingAttempt();UUID wrong=UUID.randomUUID();var context=new OperationAssertions(owner.boot(),a.context().writerEpoch(),a.context().binding(),wrong,a.context().effects(),a.context().originalOperationFingerprint());var attempt=new AttemptAssertions(a.attemptId(),a.phase(),wrong,a.bootGeneration(),a.connectionGeneration(),a.connectionId(),a.namespace(),context,a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());var bad=new FullStateAssertions(owner,inspection.previousOwner(),null,null,assertions.currentWriter(),assertions.targetWriter(),attempt,null,null,null);assertThrows(IOException.class,()->slots.publishPreGenesisRecovery(scope,bad));inspection.validateInspection();return;}
            final var exact=assertions;
            if(mode.equals("conflict")){assertThrows(IOException.class,()->slots.publishPreGenesisRecovery(scope,exact));assertFalse(inspection.ownerPublicationAttempted());return;}
            var ack=slots.publishPreGenesisRecovery(scope,exact);inspection.validateInspection();assertFalse(inspection.ownerPublicationAttempted());assertEquals(owner,ack.record().assertions().currentOwner());assertEquals(inspection.previousOwner(),ack.record().assertions().previousOwner());
            if(mode.equals("foreign-ack")){Path other=root.resolve("foreign"),otherMetadata=other.resolve("config/runtime.properties");var secondLocation=anchors.captureLocation(otherMetadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(otherMetadata)),UUID.randomUUID());var secondExpected=anchors.readExpectedSource(secondLocation);anchors.rebarrierExisting(secondLocation,secondExpected);assertNotNull(foreignCreator);var second=foreignCreator.inspect(provisioner,other.resolve("world"),secondExpected.expectedPlan(),secondExpected.expectedReceiptSha256(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),3));assertThrows(IOException.class,()->provisioner.installResumeOwner(second,ack));assertFalse(second.ownerPublicationAttempted());return;}
            var resumed=provisioner.installResumeOwner(inspection,ack);assertFalse(resumed.createdThisBoot());resumed.bootWorldCustodyProof().validateLive();assertArrayEquals(expected.originalReceiptCanonicalBytes(),resumed.freshProvisioningProof().receiptCanonicalBytes());assertEquals(owner.boot(),resumed.bootWorldCustodyProof().bootId());assertThrows(IOException.class,()->provisioner.installResumeOwner(inspection,ack));
                }catch(Throwable failure){primary=failure;throw failure;}
                finally{if(foreignCreator!=null)try{foreignCreator.close();}catch(Throwable cleanup){if(primary!=null)primary.addSuppressed(cleanup);else throw cleanup;}}
            }
        }
        public static void main(String[] args)throws Exception {
            if(args.length==4 && args[0].equals("retained-supervisor")){runRetained(Path.of(args[1]),args[2],args[3]);return;}
            if(args.length!=2)throw new IllegalArgumentException("exact mode/root");String mode=args[0];Path root=Path.of(args[1]),metadata=root.resolve("config/runtime.properties");
            WindowsMarkerIo io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());var slots=new WindowsB1TwoSlotStore(io);
            if(mode.equals("anchor-stop")||mode.equals("anchor-hold")){var world=provisioner.createFresh(root.resolve("world"),B1SourceAnchorFixture.plan(),new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),1));anchors.publishCreatedSource(location,world);assertFalse(Files.exists(root.resolve("world/.floatingpod-b1-resume-v4.0")));if(mode.equals("anchor-hold"))RetainedNativeFixtureSupervisor.holdCreated(root,world);return;}
            throw new IllegalArgumentException("Recovery modes require the exact retained supervisor");
        }
    }
}

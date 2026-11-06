package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真 creator exit/retained observation/inspection；只测冷类型入口，不伪造会计或全 witness ACK。 */
@Tag("b1-normal-resume-native")
final class WindowsColdResumeScopeBoundaryNativeTest {
    @TempDir Path temporary;
    private final Map<Path,RetainedNativeFixtureSupervisor.Fixture> retained=new LinkedHashMap<>();
    private void run(String mode)throws Exception {
        try(var owned=RetainedNativeFixtureSupervisor.scope(retained)) {
            Path root=Files.createDirectory(temporary.resolve(mode));B1SourceAnchorFixture.writeMetadata(root);
            var fixture=RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.resume.child.java"),
                System.getProperty("fp.b1.resume.child.classpath"),Child.class,root);
            retained.put(root,fixture);fixture.finish(mode);
        }
    }
    @Test void coldScopeAcceptsActualInspectionWithV5WhileOrdinaryMutationScopeStillRejects()throws Exception {run("cold-type");}
    @Test void foreignWorkerCannotIssueColdScopeFromActualRetainedInspection()throws Exception {run("foreign-worker");}
    public static final class Child {
        private static void supervise(Path root,String java,String cp)throws Exception {
            var io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);
            var slots=new WindowsB1TwoSlotStore(io);Path metadata=root.resolve("config/runtime.properties");
            var location=anchors.captureLocation(metadata,WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());
            try(var creator=RetainedNativeFixtureSupervisor.exitedCreator(io,java,cp,Child.class,"create-hold",root)) {
                RetainedNativeFixtureSupervisor.signal(root,".retained-supervisor-ready");String mode=RetainedNativeFixtureSupervisor.nextMode();
                if(mode==null)return;if(!mode.equals("cold-type")&&!mode.equals("foreign-worker"))throw new IllegalArgumentException("Exact fixture mode required");
                var expected=anchors.readExpectedSource(location);var anchor=anchors.rebarrierExisting(location,expected);
                byte[] owner=Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME));
                var inspection=creator.inspect(provisioner,root.resolve("world"),expected.expectedPlan(),expected.expectedReceiptSha256(),
                    new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),2));
                byte[] v4a=Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.0"));
                byte[] v4b=Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.1"));
                Path v5=root.resolve("world/.floatingpod-b1-resume-v5.0");byte[] unfinished={7,5,1};Files.write(v5,unfinished);
                var oldFailure=assertThrows(IOException.class,()->slots.bindResumeSource(anchors,location,anchor,inspection));
                assertEquals("Fixed B1 file exists; conflict retained: .floatingpod-b1-resume-v5.0",oldFailure.getMessage());
                assertTrue(oldFailure.getStackTrace().length>0);
                if(mode.equals("foreign-worker")) {
                    var failure=new AtomicReference<Throwable>();
                    Thread foreign=new Thread(()->{try{slots.bindResumeSourceForColdLookup(anchors,location,anchor,inspection);}catch(Throwable cause){failure.set(cause);}},"cold-scope-foreign");
                    Throwable primary=null;
                    try {foreign.start();foreign.join(5000);assertFalse(foreign.isAlive());assertInstanceOf(IllegalStateException.class,failure.get());assertEquals("Exact two-slot owned worker required",failure.get().getMessage());assertTrue(failure.get().getStackTrace().length>0);}
                    catch(Throwable cause){primary=cause;throw cause;}
                    finally {
                        try {if(foreign.isAlive()){foreign.interrupt();foreign.join(5000);if(foreign.isAlive())throw new IOException("Foreign cold fixture thread still live");}}
                        catch(Throwable cleanup){if(primary==null)throw cleanup;if(cleanup!=primary)primary.addSuppressed(cleanup);}
                    }
                } else {
                    var cold=slots.bindResumeSourceForColdLookup(anchors,location,anchor,inspection);assertNotNull(cold);
                    assertNull(slots.retainedColdLookup(cold));
                    // 缺真 selected facts/accounting 明确拒绝；不能把 private scope 当作完整冷读取成功。
                    assertThrows(IOException.class,()->slots.inspectExistingV5(cold,null,null));assertNull(slots.retainedColdLookup(cold));
                }
                inspection.validateInspection();assertFalse(inspection.ownerPublicationAttempted());
                assertArrayEquals(owner,Files.readAllBytes(root.resolve("world/"+WindowsWorldProvisioner.GUARD_NAME)));
                assertArrayEquals(v4a,Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.0")));
                assertArrayEquals(v4b,Files.readAllBytes(root.resolve("world/.floatingpod-b1-resume-v4.1")));
                assertArrayEquals(unfinished,Files.readAllBytes(v5));
            }
        }
        public static void main(String[] args)throws Exception {
            if(args.length==4&&args[0].equals("retained-supervisor")){supervise(Path.of(args[1]),args[2],args[3]);return;}
            if(args.length!=2||!args[0].equals("create-hold"))throw new IllegalArgumentException("Exact creator mode/root required");
            Path root=Path.of(args[1]);var io=new WindowsMarkerIo();var provisioner=new WindowsWorldProvisioner(io);var anchors=new WindowsB1SourceAnchorStore(io);
            var location=anchors.captureLocation(root.resolve("config/runtime.properties"),WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(
                MySqlRuntimeMetadata.load(root.resolve("config/runtime.properties"))),UUID.randomUUID());
            var boot=new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),1);var world=provisioner.createFresh(root.resolve("world"),B1SourceAnchorFixture.plan(),boot);
            var anchor=anchors.publishCreatedSource(location,world);var slots=new WindowsB1TwoSlotStore(io);
            var scope=slots.bindSource(anchors,location,anchor,world.bootWorldCustodyProof());var process=io.captureCurrentProcessIdentity(B1SourceAnchorFixture.plan().creationOperationUuid());
            var state=B1TwoSlotFixture.genesis(new NativeOwnerAssertions(process.pid(),process.creationFileTime(),boot.bootId(),1));
            var genesis=slots.publishGenesis(scope,state);slots.publishNext(scope,genesis,Kind.PREPARED_CONFIRMED,B1TwoSlotFixture.prepared(state));
            RetainedNativeFixtureSupervisor.holdCreated(root,world);
        }
    }
}

package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Test-only supervisor: coordination files never authorize native exit or custody. */
final class RetainedNativeFixtureSupervisor {
    private static final int READINESS_OUTPUT_CAPTURE_LIMIT_BYTES = 64 * 1024;
    private RetainedNativeFixtureSupervisor() {}
    static void signal(Path root, String name) throws IOException {
        Files.writeString(root.resolve(name), "fixture coordination only", StandardCharsets.US_ASCII,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }
    private static void ready(JavaArgumentFileChildLauncher.StartedChild started, Path marker) throws Exception {
        Process child=started.process();
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
        while(!Files.exists(marker) && child.isAlive() && System.nanoTime()<deadline) Thread.sleep(10);
        if(!Files.isRegularFile(marker)) throw new IOException("Actual fixture child did not reach coordination stage: "+marker.getFileName());
    }
    /** Only called after the existing owned-child close/wait path; it adds bounded diagnostics to the original readiness failure. */
    private static void attachReadinessDiagnostics(JavaArgumentFileChildLauncher.StartedChild started, Throwable primary) {
        Process child=started.process();
        if(child.isAlive()) {
            primary.addSuppressed(new IOException("Actual fixture child remains alive after its owned close path; exit/output are unavailable"));
            return;
        }
        String exitCode="UNKNOWN";
        try { exitCode=Integer.toString(child.exitValue()); }
        catch(RuntimeException|Error failure) { primary.addSuppressed(failure); }
        String output="<not-captured>";
        try(var stream=child.getInputStream()) {
            byte[] captured=stream.readNBytes(READINESS_OUTPUT_CAPTURE_LIMIT_BYTES+1);
            boolean truncated=captured.length>READINESS_OUTPUT_CAPTURE_LIMIT_BYTES;
            if(truncated) captured=Arrays.copyOf(captured,READINESS_OUTPUT_CAPTURE_LIMIT_BYTES);
            output=new String(captured,StandardCharsets.UTF_8)
                +(truncated?"<fixture child output truncated after 65536 bytes>":"");
        } catch(IOException|RuntimeException|Error failure) { primary.addSuppressed(failure); }
        primary.addSuppressed(new IOException("Actual fixture child exitCode="+exitCode+"; mergedStdoutStderr="+output));
    }
    private static JavaArgumentFileChildLauncher.StartedChild spawn(String java, String cp, Class<?> main, String mode, Path root, boolean supervisor) throws IOException {
        if(java==null || cp==null || !Path.of(java).isAbsolute() || !Files.isRegularFile(Path.of(java)) || cp.isBlank())
            throw new IOException("Root must supply reviewed actual Java25 and frozen child classpath");
        var arguments=new ArrayList<String>(List.of(mode,root.toAbsolutePath().toString()));
        if(supervisor){arguments.add(java);arguments.add(cp);}
        return JavaArgumentFileChildLauncher.start(Path.of(java),cp,main.getName(),arguments,root.toAbsolutePath());
    }
    private static void rethrow(Throwable failure) throws Exception {
        if(failure instanceof Exception exception) throw exception;
        if(failure instanceof Error error) throw error;
        throw new AssertionError(failure);
    }
    /** Always releases stdin and physically waits this actual child; no kill or PID snapshot. */
    private static void closeActual(JavaArgumentFileChildLauncher.StartedChild started) throws Exception {
        Process child=started.process();
        Throwable first=null;boolean interrupted=false;
        try{child.getOutputStream().close();}catch(Throwable failure){first=failure;}
        for(;;){try{child.waitFor();break;}catch(InterruptedException failure){interrupted=true;if(first==null)first=failure;else first.addSuppressed(failure);}}
        try{started.deleteArgumentFileAfterExit();}catch(Throwable failure){if(first==null)first=failure;else first.addSuppressed(failure);}
        if(interrupted)Thread.currentThread().interrupt();
        if(first!=null)rethrow(first);
    }
    static final class Fixture implements AutoCloseable {
        private final JavaArgumentFileChildLauncher.StartedChild started;
        private final Process child; private final Path root;
        private Throwable readinessFailure;
        private boolean readinessDiagnosticsAttached;
        private boolean closed;
        private Fixture(JavaArgumentFileChildLauncher.StartedChild started,Path root){this.started=started;this.child=started.process();this.root=root;}
        void prepareForeign() throws Exception {
            child.getOutputStream().write("prepare-foreign\n".getBytes(StandardCharsets.US_ASCII));child.getOutputStream().flush();
            try { ready(started,root.resolve("foreign/.retained-supervisor-ready")); }
            catch(Throwable failure) { readinessFailure=failure; rethrow(failure); }
        }
        String finish(String mode) throws Exception {
            child.getOutputStream().write((mode+"\n").getBytes(StandardCharsets.US_ASCII));child.getOutputStream().flush();
            child.getOutputStream().close();
            if(!child.waitFor(25,TimeUnit.SECONDS))throw new IOException("Actual retained fixture supervisor observation timeout");
            String output=new String(child.getInputStream().readAllBytes(),StandardCharsets.UTF_8);
            assertEquals(0,child.exitValue(),output);return output;
        }
        @Override public void close() throws Exception {
            if(!closed) {
                try { closeActual(started); closed=true; }
                catch(Throwable cleanup) {
                    if(readinessFailure==null) rethrow(cleanup);
                    readinessFailure.addSuppressed(cleanup);
                }
            }
            if(readinessFailure!=null && !readinessDiagnosticsAttached) {
                attachReadinessDiagnostics(started,readinessFailure);
                readinessDiagnosticsAttached=true;
            }
        }
    }
    static AutoCloseable scope(Map<Path,Fixture> fixtures) {
        return () -> {
            Throwable first=null;
            for(var fixture:fixtures.values())try{fixture.close();}catch(Throwable failure){if(first==null)first=failure;else first.addSuppressed(failure);}
            if(first!=null)rethrow(first);
        };
    }
    static Fixture start(String java,String cp,Class<?> main,Path root) throws Exception {
        var started=spawn(java,cp,main,"retained-supervisor",root,true);
        Process process=started.process();
        Fixture fixture=new Fixture(started,root);
        try{ready(started,root.resolve(".retained-supervisor-ready"));return fixture;}
        catch(Throwable primary){fixture.readinessFailure=primary;try{fixture.close();}catch(Throwable cleanup){primary.addSuppressed(cleanup);}rethrow(primary);throw new AssertionError("unreachable");}
    }
    static final class Creator implements AutoCloseable {
        private final JavaArgumentFileChildLauncher.StartedChild started;
        private final Process child;private final WindowsMarkerIo.RetainedProcessObservation observation;
        private boolean consumed;
        private Creator(JavaArgumentFileChildLauncher.StartedChild started,WindowsMarkerIo.RetainedProcessObservation observation){this.started=started;this.child=started.process();this.observation=observation;}
        WindowsWorldProvisioner.ResumeInspection inspect(WindowsWorldProvisioner provisioner,Path world,
                WindowsWorldProvisioner.Plan plan,String sha,WindowsWorldProvisioner.BootIdentity boot) throws IOException {
            var result=provisioner.beginResumeInspection(world,plan,sha,boot,observation);
            assertInstanceOf(WindowsWorldProvisioner.ResumeInspectionReady.class,result);
            consumed=true;return ((WindowsWorldProvisioner.ResumeInspectionReady)result).inspection();
        }
        WindowsWorldProvisioner.ProvisionedWorld resume(WindowsWorldProvisioner provisioner,Path world,
                WindowsWorldProvisioner.Plan plan,String sha,WindowsWorldProvisioner.BootIdentity boot) throws IOException {
            var result=provisioner.resumeExisting(world,plan,sha,boot,observation);
            assertInstanceOf(WindowsWorldProvisioner.ResumeCustodyReady.class,result);
            consumed=true;return ((WindowsWorldProvisioner.ResumeCustodyReady)result).world();
        }
        @Override public void close() throws Exception {
            Throwable first=null;try{closeActual(started);}catch(Throwable failure){first=failure;}
            if(!consumed)try{observation.close();}catch(Throwable failure){if(first==null)first=failure;else first.addSuppressed(failure);}
            if(first!=null)rethrow(first);
        }
    }
    static Creator exitedCreator(WindowsMarkerIo io,String java,String cp,Class<?> main,String mode,Path root) throws Exception {
        var started=spawn(java,cp,main,mode,root,false);
        Process child=started.process();WindowsMarkerIo.RetainedProcessObservation observation=null;
        Throwable readinessFailure=null;
        try{
            try { ready(started,root.resolve(".retained-creator-ready")); }
            catch(Throwable failure) { readinessFailure=failure; throw failure; }
            observation=io.observeLiveProcessCandidate(child.pid(),B1SourceAnchorFixture.plan().bootstrapOperationUuid());
            assertFalse(observation.isSignaled());
            child.getOutputStream().close();
            if(!child.waitFor(20,TimeUnit.SECONDS))throw new IOException("Actual creator observation timeout");
            assertEquals(0,child.exitValue(),new String(child.getInputStream().readAllBytes(),StandardCharsets.UTF_8));
            assertTrue(observation.isSignaled());
            return new Creator(started,observation);
        }catch(Throwable primary){
            try{closeActual(started);}catch(Throwable cleanup){primary.addSuppressed(cleanup);}
            if(primary==readinessFailure) attachReadinessDiagnostics(started,primary);
            if(observation!=null)try{observation.close();}catch(Throwable cleanup){primary.addSuppressed(cleanup);}
            rethrow(primary);throw new AssertionError("unreachable");
        }
    }
    static void holdCreated(Path root,WindowsWorldProvisioner.ProvisionedWorld world) throws Exception {
        signal(root,".retained-creator-ready");System.in.read();world.bootWorldCustodyProof().validateLive();
    }
    static String nextMode() throws IOException {
        // One shared stream per supervisor; do not buffer across prepare-foreign handshake.
        var bytes=new ByteArrayOutputStream();int next;
        while((next=System.in.read())!=-1 && next!='\n'){if(bytes.size()>=64)throw new IOException("Bounded fixture mode required");bytes.write(next);}
        return bytes.size()==0 && next==-1 ? null : bytes.toString(StandardCharsets.US_ASCII);
    }
}

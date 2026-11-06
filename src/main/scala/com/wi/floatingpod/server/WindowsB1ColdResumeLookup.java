package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import static com.wi.floatingpod.server.B1ResumeRecord.*;

/** 原 inspection 的冷全字节 DATA 观测；不是安装、SQL、恢复或 Ready 能力。 */
public final class WindowsB1ColdResumeLookup {
    private static final String[] V5={".floatingpod-b1-resume-v5.0",".floatingpod-b1-resume-v5.1"};
    private static final String PENDING=".floatingpod-b1-resume-v5.tmp";
    public enum Status { RESOLVED_DATA, UNRESOLVED_DATA, FAILED_DATA }
    public enum UnresolvedReason { NONE, PENDING_V5, NO_V5, INSUFFICIENT_V5_HISTORY, MISSING_ARTIFACT }
    public static final class ArtifactObservation {
        private final B1Full20WitnessReference reference;
        private final WindowsMarkerIo.FileIdentity identity;
        private final byte[] bytes;
        private ArtifactObservation(B1Full20WitnessReference reference,Read read){this.reference=reference;identity=read.identity;bytes=read.first;}
        public B1Full20WitnessReference reference(){return reference;}
        public WindowsMarkerIo.FileIdentity identity(){return identity;}
        public long fullLength(){return bytes.length;}
    }
    public static final class Observation {
        private final WindowsB1ColdResumeLookup holder;
        private Observation(WindowsB1ColdResumeLookup holder){this.holder=holder;}
        public Status status(){holder.owner();if(holder.failure!=null)return Status.FAILED_DATA;if(!holder.completed)throw new IllegalStateException("Cold observation still pending");return holder.reason==UnresolvedReason.NONE?Status.RESOLVED_DATA:Status.UNRESOLVED_DATA;}
        public UnresolvedReason unresolvedReason(){holder.owner();return holder.reason;}
        public WindowsB1TwoSlotStore.ResumeStoredRecordFacts predecessor(){holder.owner();return holder.predecessor;}
        public B1ResumeRecordV5 currentRecord(){holder.owner();return holder.current==null?null:holder.current.record;}
        public List<ArtifactObservation> artifacts(){holder.owner();return holder.completeArtifacts;}
        public Throwable retainedFailure(){holder.owner();return holder.failure;}
    }
    private static final class Read {
        final String name;
        final MySqlFull20ColdReadAccounting accounting;
        WindowsMarkerIo.Handle handle;
        WindowsMarkerIo.FileIdentity identity;
        byte[] first,second;
        B1ResumeRecordV5 v5Record;
        Read(String name,MySqlFull20ColdReadAccounting accounting){this.name=name;this.accounting=accounting;}
    }
    private static final class V5Facts {
        final Read read;final B1ResumeRecordV5 record;
        V5Facts(Read read,B1ResumeRecordV5 record){this.read=read;this.record=record;}
    }
    private final WindowsB1TwoSlotStore issuer;
    private final WindowsB1TwoSlotStore.ColdResumeSourceScope source;
    private final WindowsB1TwoSlotStore.ResumeStoredRecordFacts predecessor;
    private final MySqlFull20ColdReadAccounting accounting;
    private final Thread thread=Thread.currentThread();
    private final ArrayList<Read> reads=new ArrayList<>(6);
    private final ArrayList<ArtifactObservation> artifacts=new ArrayList<>(4);
    private List<ArtifactObservation> completeArtifacts=List.of();
    private V5Facts current;
    private UnresolvedReason reason=UnresolvedReason.NONE;
    private Throwable failure;
    private byte[] predecessorBytes;
    private WindowsMarkerIo.Handle pendingHandle;
    private WindowsMarkerIo.FileIdentity pendingIdentity;
    private boolean attempted,completed;
    private final Observation result=new Observation(this);
    WindowsB1ColdResumeLookup(WindowsB1TwoSlotStore issuer,WindowsB1TwoSlotStore.ColdResumeSourceScope source,
            WindowsB1TwoSlotStore.ResumeStoredRecordFacts predecessor,MySqlFull20ColdReadAccounting accounting){
        this.issuer=issuer;this.source=source;this.predecessor=predecessor;this.accounting=accounting;
    }
    private void owner(){if(Thread.currentThread()!=thread)throw new IllegalStateException("Exact cold observation owner required");}
    public Observation observation(){owner();return result;}
    public Throwable retainedFailure(){owner();return failure;}
    void observe()throws IOException {
        owner();if(attempted)throw new IOException("Cold lookup is one-shot; original responsibility retained");attempted=true;
        try {
            accounting.requireOpen();accounting.beforeSelectedV4Copy(predecessor.canonicalByteLength());
            predecessorBytes=predecessor.canonicalBytes();issuer.rebarrierColdV4(source,predecessor);
            if(pendingExists()){issuer.rebarrierColdV4(source,predecessor);finish(UnresolvedReason.PENDING_V5);return;}
            V5Facts a=readV5(0),b=readV5(1);
            if(a==null&&b==null){issuer.rebarrierColdV4(source,predecessor);finish(UnresolvedReason.NO_V5);return;}
            current=a==null?b:b==null?a:a.record.transition().sequence()>b.record.transition().sequence()?a:b;
            if((a==null||b==null)&&current.record.transition().sequence()!=Math.addExact(predecessor.record().sequence(),1L)){
                requireBridgeData(current.record,predecessor.record(),predecessorBytes,predecessor.fileIdentity(),issuer.coldPreviousOwner(source));
                issuer.requireColdRecord(source,current.record.transition());issuer.rebarrierColdV4(source,predecessor);
                finish(UnresolvedReason.INSUFFICIENT_V5_HISTORY);return;
            }
            current=selectAndValidate(a,b,predecessor,predecessorBytes,issuer.coldPreviousOwner(source));
            issuer.requireColdRecord(source,current.record.transition());
            var state=current.record.transition().assertions();var refs=current.record.references();
            readArtifact(refs.originalPrepare(),state.originalPrepareWitness());
            readArtifact(refs.pending(),state.pendingAttempt());
            readArtifact(refs.settled(),state.latestSettlement()==null?null:state.latestSettlement().settledAttempt());
            readArtifact(refs.acknowledgedCertify(),state.acknowledgedCertifyWitness());
            issuer.rebarrierColdV4(source,predecessor);issuer.requireColdLive(source);
            finish(reason);
        } catch(Throwable primary){failure=primary;throw propagate(primary);}
    }
    private void finish(UnresolvedReason unresolved){
        reason=unresolved;completeArtifacts=List.copyOf(artifacts);completed=true;
    }
    private boolean pendingExists()throws IOException {
        var io=issuer.coldIo(source);
        try {
            pendingHandle=open(PENDING);if(pendingHandle==null)return false;
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.coldNativePath(source),io.finalPath(pendingHandle),PENDING);
            // 未完成 payload 不猜测、不删除；原 inspection/io 保留责任。
            pendingIdentity=io.identity(pendingHandle);io.closeChecked(pendingHandle);pendingHandle=null;return true;
        } catch(Throwable primary){WindowsMarkerIo.closeSuppressed(pendingHandle,primary);throw propagate(primary);}
    }
    private WindowsMarkerIo.Handle open(String name)throws IOException {
        try {return issuer.coldIo(source).open(issuer.coldDirectory(source).resolve(name),WindowsMarkerIo.READ,0,
            WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,issuer.coldOperation(source));}
        catch(WindowsMarkerIo.NativeFailure cause){if(cause.win32Error()==2)return null;throw cause;}
    }
    private Read read(String name,long expectedLength,WindowsMarkerIo.FileIdentity expectedIdentity,byte[] expectedSha,
            long limit)throws IOException {
        var budget=accounting.newReadAccounting();var read=new Read(name,budget);reads.add(read); // before OPEN 登记。
        var io=issuer.coldIo(source);
        try {
            read.handle=open(name);if(read.handle==null)return null;
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.coldNativePath(source),io.finalPath(read.handle),name);
            // Artifact 的完整长度来自已严格解码的引用；metadata 校验实际 EOF。
            // payloadLength 仅用于 <=64 KiB 的 V5 record，不能用于完整全20 witness。
            long length=expectedLength>0?expectedLength:io.payloadLength(read.handle);
            if(length<=0||length>limit||length>Integer.MAX_VALUE||expectedLength>0&&length!=expectedLength)
                throw new IOException("Cold complete file length differs or exceeds explicit bound");
            read.identity=io.identity(read.handle);
            if(expectedIdentity!=null&&!expectedIdentity.equals(read.identity))throw new IOException("Cold native file identity differs");
            io.metadata(read.handle,false,length);budget.beforeRead(length);
            read.first=io.readWitnessArtifactExact(read.handle,Math.toIntExact(length),limit);
            budget.retainReadBytes(read.first);
            read.second=io.readWitnessArtifactExact(read.handle,read.first.length,limit);
            if(!Arrays.equals(read.first,read.second)||!read.identity.equals(io.identity(read.handle))||
                    expectedSha!=null&&!Arrays.equals(expectedSha,WindowsWorldProvisioner.digest(read.first)))
                throw new IOException("Cold complete file bytes/digest/identity changed");
            issuer.requireColdLive(source);io.closeChecked(read.handle);read.handle=null;
            Arrays.fill(read.second,(byte)0);read.second=null;budget.nativeReadClosed();return read;
        } catch(Throwable primary){
            WindowsMarkerIo.closeSuppressed(read.handle,primary);
            // 保留 handle/两个 arrays/会计临时款及原 cause；失败不伪造 scratch 已回收。
            throw propagate(primary);
        }
    }
    private V5Facts readV5(int slot)throws IOException {
        var read=read(V5[slot],0,null,null,Math.min(accounting.artifactLimit(),B1ResumeRecordV5.MAX_RECORD_BYTES));
        if(read==null)return null;
        read.accounting.beforeV5Decode(read.first.length);
        var record=B1ResumeRecordV5.decodeData(read.first);read.v5Record=record;
        B1ResumeRecordCodec.exact(read.first,B1ResumeRecordV5.encodeData(record),"Cold V5 canonical bytes differ");
        if(record.transition().sequence()%2!=slot)throw new IOException("Cold V5 sequence in wrong fixed slot");
        return new V5Facts(read,record);
    }
    /** 真实读取和直接 DATA 回归共用；不发行 native ACK。 */
    private static V5Facts selectAndValidate(V5Facts a,V5Facts b,WindowsB1TwoSlotStore.ResumeStoredRecordFacts v4,byte[] v4Bytes,
            NativeOwnerAssertions actualPreviousOwner)throws IOException {
        if(a==null&&b==null)throw new IOException("Cold V5 absent");
        V5Facts current=a==null?b:b==null?a:a.record.transition().sequence()>b.record.transition().sequence()?a:b;
        if(a!=null&&b!=null){
            if(a.record.transition().sequence()==b.record.transition().sequence())throw new IOException("Cold V5 same-sequence conflict");
            var old=current==a?b:a;B1ResumeRecordV5.consecutive(old.record,old.read.first,current.record);
        }
        for(var facts:new V5Facts[]{a,b})if(facts!=null)requireBridgeData(facts.record,v4.record(),v4Bytes,v4.fileIdentity(),actualPreviousOwner);
        var first=current.record.transition().sequence()==Math.addExact(v4.record().sequence(),1L)?current:null;
        if(first==null&&a!=null&&a.record.transition().sequence()==Math.addExact(v4.record().sequence(),1L))first=a;
        if(first==null&&b!=null&&b.record.transition().sequence()==Math.addExact(v4.record().sequence(),1L))first=b;
        if(first!=null){
            B1ResumeRecordCodec.exact(WindowsWorldProvisioner.digest(v4Bytes),first.record.previousContainerSha256(),"Cold initial V5 container differs from V4");
            B1ResumeRecordCodec.consecutive(v4.record(),v4Bytes,first.record.transition());
        } else if(a==null||b==null)throw new IOException("Cold V5 history is insufficient without original first or consecutive pair");
        return current;
    }
    static void requireBridgeData(B1ResumeRecordV5 record,B1ResumeRecord v4,byte[] v4Bytes,String v4Identity,
            NativeOwnerAssertions previous)throws IOException {
        var handoff=v4.assertions().pendingAttempt();
        if(v4.kind()!=Kind.HANDOFF_PREPARED||handoff==null||handoff.phase()!=Phase.HANDOFF||
            v4.assertions().previousOwner()==null||handoff.bootGeneration()!=v4.assertions().currentOwner().generation()||
            !handoff.context().boot().equals(v4.assertions().currentOwner().boot())||record.bridgeSequence()!=v4.sequence()||
            !record.bridgeIdentity().versionedIdentity().equals(v4Identity)||
            !Arrays.equals(record.bridgeSha256(),WindowsWorldProvisioner.digest(v4Bytes))||
            !record.transition().assertions().currentOwner().equals(previous))
            throw new IOException("Cold V5 exact installed V4 bridge/previous owner differs");
        B1ResumeRecordCodec.exact(v4.sourceAnchorSha256(),record.transition().sourceAnchorSha256(),"Cold V5 anchor differs");
        B1ResumeRecordCodec.exact(v4.originalReceipt(),record.transition().originalReceipt(),"Cold V5 original receipt differs");
        if(!v4.barrierNamespace().equals(record.transition().barrierNamespace()))throw new IOException("Cold V5 namespace differs");
    }
    private void readArtifact(B1Full20WitnessReference ref,AttemptAssertions attempt)throws IOException {
        if((ref==null)!=(attempt==null))throw new IOException("Cold attempt/reference topology differs");if(ref==null)return;
        // 同一 immutable attempt 的多个角色是同一个完整 artifact，不递归复制其历史图。
        for(var existing:artifacts)if(existing.reference.attemptId().equals(ref.attemptId())){
            if(!existing.reference.exactlyEquals(ref))throw new IOException("Cold duplicate attempt reference differs");
            return;
        }
        if(!accounting.profileSha256().equals(HexFormat.of().formatHex(ref.profileSha256())))throw new IOException("Cold reference profile differs from successor root");
        var read=read(ref.basename(),ref.fullLength(),ref.fileIdentity(),ref.fullSha256(),accounting.artifactLimit());
        if(read==null){reason=UnresolvedReason.MISSING_ARTIFACT;return;}
        byte[] expected=B1ResumeRecordCodec.attemptBytes(attempt);
        read.accounting.decodeWitness(read.first,expected,predecessorBytes);
        artifacts.add(new ArtifactObservation(ref,read));completeArtifacts=List.copyOf(artifacts);
    }
    private static final class InventoryDraft {
        final long generation;
        WindowsMarkerIo.Handle handle;
        Throwable failure;
        boolean compared;
        InventoryDraft(long generation) { this.generation = generation; }
    }
    /** 私有发行的fresh lexical DATA；不签SQL、flush/durability或可写scope。 */
    public static final class NativeCurrentInventoryAck {
        private final WindowsB1ColdResumeLookup lookup;
        private final long generation;
        private boolean retired;
        private NativeCurrentInventoryAck(WindowsB1ColdResumeLookup lookup, long generation) {
            this.lookup = lookup; this.generation = generation;
        }
        public void requireCurrent() throws IOException { lookup.requireInventory(this); }
        public long generation() throws IOException { requireCurrent(); return generation; }
        public void retire() throws IOException { lookup.retireInventory(this); }
    }
    private InventoryDraft inventoryDraft;
    private NativeCurrentInventoryAck inventoryAck;

    /** 每次plan/execute均重新实际完整读；旧Observation的cached getters不能替此入口。 */
    public NativeCurrentInventoryAck revalidateCurrentInventory() throws IOException {
        owner();
        if (!completed || failure != null || reason != UnresolvedReason.NONE || inventoryDraft != null || inventoryAck != null)
            throw new IOException("Exact resolved cold lookup with no active inventory generation required", failure);
        long generation = 0;
        try {
            int worldLength = issuer.inventoryCapturedWorldLength(source, this, accounting);
            generation = accounting.beginInventoryPass(this, worldLength); // 首draft allocation/OPEN前。
            inventoryDraft = new InventoryDraft(generation);
            compareCompleteInventory(); // 正常返回时所有guard局部/Arena/handle引用已退出。
            inventoryDraft.compared = true;
            inventoryAck = new NativeCurrentInventoryAck(this, generation);
            return inventoryAck;
        } catch (Throwable primary) {
            failure = primary;
            if (inventoryDraft != null) inventoryDraft.failure = primary;
            if (generation != 0) try { accounting.failInventoryPass(this, generation, primary); }
            catch (Throwable secondary) { if (secondary != primary) primary.addSuppressed(secondary); }
            throw propagate(primary);
        }
    }
    private void compareCompleteInventory() throws IOException {
        issuer.inventoryBorrowedGuard(source, this, accounting);
        issuer.inventoryCompareV4AndAbsent(source, this, accounting);
        compareInventoryFile(PENDING, null); // fixed V5 temp必须真实absence。
        for (var read : reads) compareInventoryFile(read.name, read);
        issuer.inventoryBorrowedGuard(source, this, accounting);
    }
    private void compareInventoryFile(String name, Read known) throws IOException {
        var io = issuer.inventoryBorrowedIo(source, this, accounting);
        var draft = inventoryDraft;
        try {
            try {
                draft.handle = io.open(issuer.inventoryBorrowedDirectory(source, this, accounting).resolve(name),
                    WindowsMarkerIo.READ, 0, WindowsMarkerIo.OPEN_EXISTING,
                    WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE,
                    issuer.inventoryBorrowedOperation(source, this, accounting));
            } catch (WindowsMarkerIo.NativeFailure absent) {
                if (absent.win32Error() != 2) throw absent;
                draft.handle = null;
            }
            if (known == null || known.first == null) {
                if (draft.handle != null) throw new IOException("Inventory expected native absence differs");
                return;
            }
            if (draft.handle == null) throw new IOException("Inventory original complete payload disappeared");
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.inventoryBorrowedNativePath(source, this, accounting),
                io.finalPath(draft.handle), name);
            for (int pass = 0; pass < 2; pass++)
                io.compareWitnessArtifactExact(draft.handle, known.first, known.identity, accounting.artifactLimit());
            io.closeChecked(draft.handle); draft.handle = null;
        } catch (Throwable primary) {
            draft.failure = primary;
            WindowsMarkerIo.closeSuppressed(draft.handle, primary);
            // 即使close成功，failure draft/旧body/整个scratch仍由同lookup和actor保留。
            throw propagate(primary);
        }
    }
    private void requireInventory(NativeCurrentInventoryAck ack) throws IOException {
        owner();
        if (ack == null || ack.lookup != this || ack != inventoryAck || ack.retired || failure != null ||
                inventoryDraft == null || !inventoryDraft.compared || inventoryDraft.failure != null ||
                inventoryDraft.generation != ack.generation)
            throw new IOException("Exact fresh lexical inventory DATA generation required", failure);
        accounting.requireInventoryPass(this, ack.generation);
        try { compareCompleteInventory(); }
        catch (Throwable primary) {
            failure = primary; inventoryDraft.failure = primary;
            try { accounting.failInventoryPass(this, ack.generation, primary); }
            catch (Throwable secondary) { if (secondary != primary) primary.addSuppressed(secondary); }
            throw propagate(primary);
        }
    }
    private void retireInventory(NativeCurrentInventoryAck ack) throws IOException {
        requireInventory(ack);
        var draft = inventoryDraft;
        try {
            // 两compound对象512B保持retained；只释放已经返回/关闭并drop的真正临时workspace。
            inventoryDraft = null; inventoryAck = null; ack.retired = true;
            accounting.finishInventoryPass(this, ack.generation);
        } catch (Throwable primary) {
            inventoryDraft = draft; draft.failure = primary; failure = primary;
            throw propagate(primary);
        }
    }
    private static IOException propagate(Throwable primary){
        if(primary instanceof IOException io)return io;if(primary instanceof RuntimeException runtime)throw runtime;
        if(primary instanceof Error error)throw error;return new IOException("Cold native DATA boundary failed",primary);
    }
}

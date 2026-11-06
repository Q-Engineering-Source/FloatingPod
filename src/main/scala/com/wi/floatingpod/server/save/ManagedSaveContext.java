package com.wi.floatingpod.server.save;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import com.wi.floatingpod.config.SourceLimitProfile;
import java.util.ArrayDeque;
import net.minecraft.nbt.NBTBase;
import net.minecraft.nbt.NBTTagList;
import java.io.IOException;
import java.io.OutputStream;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTTagCompound;

/** 同原调用线程的短scope；不跨线程传game objects，不执行JDBC/native force。 */
public final class ManagedSaveContext implements AutoCloseable {
    private static final ThreadLocal<ManagedSaveContext> CURRENT=new ThreadLocal<>();
    public final ManagedSourceSaveLedger ledger;
    public ManagedSourceSaveLedger.Attempt attempt;
    private final ManagedSaveContext previous;
    private final Thread owner=Thread.currentThread();
    private boolean closed;
    private DataOutputStream stream;
    private boolean streamCloseAttempted;
    private ManagedSaveQueueObservation.Guard queueGuard;
    private ManagedSaveContext(ManagedSourceSaveLedger ledger,ManagedSourceSaveLedger.Attempt attempt) {
        this.ledger=ledger;this.attempt=attempt;previous=CURRENT.get();
        if(previous!=null)throw new IllegalStateException("Nested managed save/write scope unsupported; no proof issued");
        CURRENT.set(this);
    }
    public static ManagedSaveContext producer(ManagedSourceSaveLedger ledger,ManagedSourceSaveLedger.Attempt attempt) { return new ManagedSaveContext(ledger,attempt); }
    public static ManagedSaveContext writer(ManagedSourceSaveLedger ledger) { return new ManagedSaveContext(ledger,null); }
    public static ManagedSaveContext current() { return CURRENT.get(); }
    public static byte[] encode(NBTTagCompound compound,SourceLimitProfile profile) throws IOException {
        int maximum=Math.toIntExact(profile.maxDecompressedBytes());
        record Node(NBTBase tag,long depth) { }
        ArrayDeque<Node> pending=new ArrayDeque<>();pending.push(new Node(compound,1));long visited=0;
        long maximumMembers=profile.maxMembers();
        while(!pending.isEmpty()) {
            Node n=pending.pop();if(++visited>maximumMembers)throw new IOException("Full NBT observation exceeds explicit member budget");if(visited>maximum)throw new IOException("NBT observation node traversal exceeds byte-budget-derived bound");if(n.depth()>profile.maxNbtDepth())throw new IOException("Full NBT observation exceeds explicit depth budget");
            if(n.tag() instanceof NBTTagCompound c)for(String key:c.getKeySet()) {
                if(pending.size()>=maximumMembers-visited)throw new IOException("Full NBT observation exceeds explicit member budget");
                if(pending.size()>=maximum-visited)throw new IOException("NBT observation traversal exceeds byte-budget-derived bound");pending.push(new Node(c.getTag(key),n.depth()+1));
            }
            else if(n.tag() instanceof NBTTagList list)for(int i=0;i<list.tagCount();i++) {
                if(pending.size()>=maximumMembers-visited)throw new IOException("Full NBT observation exceeds explicit member budget");
                if(pending.size()>=maximum-visited)throw new IOException("NBT observation traversal exceeds byte-budget-derived bound");pending.push(new Node(list.get(i),n.depth()+1));
            }
        }
        ByteArrayOutputStream buffer=new ByteArrayOutputStream(Math.min(maximum,8192));
        OutputStream bounded=new OutputStream() {
            private int count;
            @Override public void write(int b)throws IOException{if(count>=maximum)throw new IOException("Full NBT observation exceeds explicit payload budget");buffer.write(b);count++;}
            @Override public void write(byte[] b,int off,int len)throws IOException{if(len>maximum-count)throw new IOException("Full NBT observation exceeds explicit payload budget");buffer.write(b,off,len);count+=len;}
        };
        try(DataOutputStream out=new DataOutputStream(bounded)){CompressedStreamTools.write(compound,out);}
        return buffer.toByteArray();
    }
    public void opened(DataOutputStream actual) { check();if(stream!=null)throw new IllegalStateException("Multiple output streams in one flight");ledger.openedOutput(attempt,actual);stream=actual; }
    public void closeReturned() { check();ledger.outputCloseReturned(attempt); }
    public void closeFailed() { check();if(stream!=null)ledger.retainAbandonedOutput(attempt,stream); }
    public void closing(DataOutputStream actual) { check();if(actual!=stream || streamCloseAttempted)throw new IllegalStateException("Foreign/repeated output close");streamCloseAttempted=true; }
    /** 编码失败时close会调用ChunkBuffer.write，不能用finally发布部分payload；强引用保留。 */
    public void retainUnclosedOutput() {
        check();if(stream!=null && !streamCloseAttempted)ledger.retainAbandonedOutput(attempt,stream);
    }
    /** Original writeNextIO selection is fenced only until its exact Map.remove, before Region/encoder work. */
    public void holdQueueGuard(ManagedSaveQueueObservation.Guard actual) {
        check();if(queueGuard!=null||actual==null)throw new IllegalStateException("One exact original queue-selection guard required");queueGuard=actual;
    }
    public boolean hasQueueGuard() { check();return queueGuard!=null; }
    public void releaseQueueGuard() {
        check();ManagedSaveQueueObservation.Guard actual=queueGuard;if(actual==null)throw new IllegalStateException("Original queue-selection guard is not held");
        queueGuard=null;actual.close();
    }
    /** 原fault是本次责任边界的primary；预算/recorder失败只作为次级诊断。 */
    public void recordFailure(Throwable actualCause)throws Throwable {
        java.util.Objects.requireNonNull(actualCause);
        try { check();if(attempt!=null)ledger.failure(attempt,actualCause);else ledger.closeFailure(actualCause); }
        catch(Throwable observation) { ManagedSourceSaveLedger.suppressSecondary(actualCause,observation);throw actualCause; }
    }
    /** 诊断预算失败也必须尝试保留actualstream；永不close部分encoder payload。 */
    public void failed(Throwable actualCause)throws Throwable {
        java.util.Objects.requireNonNull(actualCause);
        try { recordFailure(actualCause); }
        catch(Throwable observation) { ManagedSourceSaveLedger.suppressSecondary(actualCause,observation); }
        try { if(streamCloseAttempted)closeFailed();else retainUnclosedOutput(); }
        catch(Throwable retention) { ManagedSourceSaveLedger.suppressSecondary(actualCause,retention); }
        throw actualCause;
    }
    private void check(){if(closed || Thread.currentThread()!=owner || CURRENT.get()!=this)throw new IllegalStateException("Exact active save scope required");}
    @Override public void close(){
        check();Throwable cleanup=null;
        if(queueGuard!=null){ManagedSaveQueueObservation.Guard actual=queueGuard;queueGuard=null;try{actual.close();}catch(Throwable cause){cleanup=cause;}}
        closed=true;CURRENT.remove();
        if(cleanup instanceof RuntimeException cause)throw cause;
        if(cleanup instanceof Error cause)throw cause;
        if(cleanup!=null)throw new IllegalStateException("Original queue-selection guard cleanup failed",cleanup);
    }
}

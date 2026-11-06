package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.bootstrap.ManagedSourcePathCensus;
import com.wi.floatingpod.server.DedicatedWorldContext;
import com.wi.floatingpod.server.NormalRecallRecoverySourceCustody;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import net.minecraft.server.MinecraftServer;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/** 只把显式工作责任转交给调用者原有账本，不持有或创建容量。 */
public interface SourceForestAllocationBudget {
    SourceLimitProfile exactProfile();
    void reserveTemporary(long bytes, String context) throws IOException;
    void releaseTemporary(long bytes, String context) throws IOException;

    /** Separate balances never permit a holder to return another holder's reservation. */
    default void reserveSaveEntry(ManagedSourceSaveLedger ledger)throws IOException{throw new IllegalStateException("Original source save account required");}
    default void reserveReadEntry(SourceLoadManifest holder)throws IOException{throw new IllegalStateException("Original source read account required");}
    default SourceForestAllocationBudget isolate(){return this;}
    default void retire()throws IOException{}
    default SourceForestAllocationBudget forManifest(SourceLoadManifest holder){throw new IllegalStateException("Original source account required");}
    default SourceForestAllocationBudget forRead(SourceReadOperation operation){throw new IllegalStateException("Original source account required");}
    default SourceForestAllocationBudget forSave(ManagedSourceSaveLedger ledger){throw new IllegalStateException("Original source account required");}

    /** Only the original Session supplies root operations and actual transfer-state checks. */
    abstract class Root implements SourceForestAllocationBudget {
        private Account custody,partialAccount;private boolean custodyAttempted;
        private final List<Account> accounts=new ArrayList<>();
        private final SourceAccountBook accountBook=new SourceAccountBook(new SourceAccountBook.Fees(){
            @Override public void reserve(long bytes,String context)throws IOException{Root.this.reserveRoot(bytes,context);}
            @Override public void release(long bytes,String context)throws IOException{Root.this.releaseRoot(bytes,context);}
        });
        private volatile NormalRecallRecoverySourceCustody responsibility;
        private MinecraftServer custodyServer;private DedicatedWorldContext custodyContext;
        public final synchronized SourceForestAllocationBudget bindCustody(NormalRecallRecoverySourceCustody actual){
            requireMain();Objects.requireNonNull(actual);actual.requireAllocationOwner();
            if(!custodyAttempted||custodyServer==null||responsibility!=null)throw new IllegalStateException("Original prepared one-shot custody binding required");
            actual.requireAllocationBinding(this,custodyServer,custodyContext);
            var slot=accountBook.registerPrepaidCustody();
            responsibility=actual;partialAccount=new Account(this,actual,custodyContext,slot);custody=partialAccount;accounts.add(custody);return custody;
        }
        /**
         * A path census is a child of the exact preinitialized custody slot. The census itself owns
         * the lexical event guards; callers cannot turn the root bridge into a transferable balance.
         */
        public final synchronized Account isolateForColdCensus(
                NormalRecallRecoverySourceCustody exactCustody,ManagedSourcePathCensus exactCensus)throws IOException{
            requireMain();Objects.requireNonNull(exactCustody);Objects.requireNonNull(exactCensus);
            if(responsibility!=exactCustody||custody==null||custody.root!=this||custody.owner!=exactCustody||
                    custody.context!=custodyContext||!accountBook.contains(custody.slot))
                throw new IllegalStateException("Exact original preinitialized custody account required for cold census");
            exactCustody.requireBoundAllocation(this,custodyServer,custodyContext);
            exactCensus.requireExactCustody(exactCustody);
            custody.check();
            return retainAccount(exactCensus,custodyContext,custody,true);
        }
        private synchronized Account retainAccount(Object owner,DedicatedWorldContext context,Account parent,boolean retirable)throws IOException{
            var slot=accountBook.retain(parent==null?null:parent.slot,retirable);
            partialAccount=new Account(this,owner,context,slot);
            accounts.add(partialAccount);return partialAccount;
        }
        private synchronized void retireAccount(Account actual)throws IOException{
            if(actual.root!=this||!accounts.contains(actual))
                throw new IllegalStateException("Exact settled temporary source account required for retirement");
            accountBook.retire(actual.slot);
            accounts.remove(actual);if(partialAccount==actual)partialAccount=null;
        }
        private void requireResponsibility(){var actual=responsibility;if(actual!=null)actual.requireAllocationResponsibility();}

        protected abstract void requireMain();
        protected abstract void requireLive();
        protected abstract void reserveRoot(long bytes,String context)throws IOException;
        protected abstract void releaseRoot(long bytes,String context)throws IOException;
        @Override public final void reserveTemporary(long bytes,String context){throw new IllegalStateException("Root source bridge has no holder balance");}
        @Override public final void releaseTemporary(long bytes,String context){throw new IllegalStateException("Root source bridge has no holder balance");}
        public final synchronized void prepareCustody(MinecraftServer server,DedicatedWorldContext context)throws IOException{
            requireMain();Objects.requireNonNull(server);Objects.requireNonNull(context);
            if(Thread.currentThread()!=server.getServerThread()||context.actualServer()!=server)throw new IllegalStateException("Actual claimed source Main required");
            context.validateAboutToStart(server);
            if(custodyAttempted)throw new IllegalStateException("Original source custody account already prepared");
            custodyAttempted=true;custodyServer=server;custodyContext=context;
            reserveRoot(SourceAccountBook.CUSTODY_METADATA_BYTES,"original source custody/account/partial slots/stable cold observer BEFORE allocation");
            accountBook.custodyMetadataPrepaid();
        }

    }
    /** Constructor and guard selection are closed; callers cannot supply an authorizer or Thread. */
    final class Account implements SourceForestAllocationBudget {
        private final Root root;private volatile Object owner;private final DedicatedWorldContext context;
        private final SourceAccountBook.Slot slot;
        private Account(Root root,Object owner,DedicatedWorldContext context,SourceAccountBook.Slot slot){this.root=root;this.owner=owner;this.context=context;this.slot=slot;}
        private void active(){root.accountBook.requireActive(slot);}

        private void check(){
            active();root.requireLive();root.requireResponsibility();
            if(owner instanceof MinecraftServer server){root.requireMain();if(Thread.currentThread()!=server.getServerThread())throw new IllegalStateException("Actual source Main required");context.validateAboutToStart(server);}
            else if(owner instanceof NormalRecallRecoverySourceCustody actual){root.requireMain();actual.requireAllocationOwner();}
            else if(owner instanceof SourceLoadManifest manifest){if(manifest.mainAllocationActor())root.requireMain();manifest.requireAllocationOwner();}
            else if(owner instanceof SourceReadOperation operation){if(operation.mainAllocationActor())root.requireMain();operation.requireAllocationOwner();}
            else if(owner instanceof ManagedSourceSaveLedger ledger){if(ledger.mainAllocationActor())root.requireMain();ledger.requireAllocationOwner();}
            else if(owner instanceof ManagedSourcePathCensus census)census.requireAllocationOwner();
            else throw new IllegalStateException("Unknown original source account owner");
        }
        private synchronized Account child(Object actual,boolean temporary){synchronized(root){check();try{return root.retainAccount(actual,context,this,temporary);}catch(IOException cause){throw new UncheckedIOException(cause);}}}
        public SourceLimitProfile exactProfile(){check();return root.exactProfile();}
        public SourceForestAllocationBudget isolate(){return child(owner,true);}
        /**
         * A retained capture body gets its own child slot under the exact original save ledger. The
         * child remains governed by that ledger's real Main/IO allocation actor; the Main custody
         * account itself is never transferred to the asynchronous publisher.
         */
        public Account isolateForCapturedInput(ManagedSourceSaveLedger exactLedger){
            Objects.requireNonNull(exactLedger);
            synchronized(root){
                active();root.requireLive();root.requireResponsibility();
                if(owner!=exactLedger)throw new IllegalStateException("Exact original source save ledger account required");
                exactLedger.requireAllocationOwner();
                return child(exactLedger,true);
            }
        }
        public SourceForestAllocationBudget forManifest(SourceLoadManifest holder){return child(Objects.requireNonNull(holder),false);}
        public SourceForestAllocationBudget forRead(SourceReadOperation operation){return child(Objects.requireNonNull(operation),false);}
        public SourceForestAllocationBudget forSave(ManagedSourceSaveLedger ledger){return child(Objects.requireNonNull(ledger),false);}
        public synchronized void retire()throws IOException{synchronized(root){check();root.retireAccount(this);}}
        public synchronized void reserveSaveEntry(ManagedSourceSaveLedger ledger)throws IOException{synchronized(root){
            active();root.requireLive();root.requireResponsibility();
            if(owner!=ledger)throw new IllegalStateException("Foreign source save entry account");if(ledger.mainAllocationActor())root.requireMain();ledger.requireAllocationEntryActor();
            root.accountBook.reserve(slot,128L,"actual source lexical save entry BEFORE allocation");
        }}
        public synchronized void reserveReadEntry(SourceLoadManifest holder)throws IOException{synchronized(root){
            active();root.requireLive();root.requireResponsibility();
            if(owner!=holder)throw new IllegalStateException("Foreign source read entry account");if(holder.mainAllocationActor())root.requireMain();
            root.accountBook.reserve(slot,128L,"actual source lexical read entry BEFORE allocation");
        }}
        public synchronized void reserveTemporary(long bytes,String context)throws IOException{synchronized(root){
            check();root.accountBook.reserve(slot,bytes,context);
        }}
        public synchronized void releaseTemporary(long bytes,String context)throws IOException{synchronized(root){
            check();root.accountBook.release(slot,bytes,context);
        }}
    }

    /** 子作用域隔离费用；所有 child 最后消费成功结束后才退休本 scope 的 account。 */
    final class Scope implements AutoCloseable {
        private final SourceForestAllocationBudget adapter;
        private final Thread owner=Thread.currentThread();
        private long charged;private int children;private Scope parent;
        private boolean closed;private Throwable cleanupFailure;
        private Scope(SourceForestAllocationBudget adapter) { this.adapter=adapter; }

        public static Scope untracked() { return new Scope(null); }
        public static Scope open(SourceForestAllocationBudget adapter, SourceLimitProfile profile) {
            Objects.requireNonNull(adapter);Objects.requireNonNull(profile);
            if(adapter.exactProfile()!=profile)throw new IllegalArgumentException("Exact allocation profile instance required");
            adapter=adapter.isolate();
            adapter.reserveProfileCheck();
            try { adapter.reserveTemporary(64L,"source forest initial lexical scope"); }
            catch(IOException failure){throw new UncheckedIOException("Source forest scope allocation failed",failure);}
            Scope scope=new Scope(adapter);scope.charged=64L;return scope;
        }
        public Scope child() {
            checkOpen();SourceForestAllocationBudget childAdapter=adapter==null?null:adapter.isolate();
            if(childAdapter!=null)try{childAdapter.reserveTemporary(64L,"source forest child lexical scope BEFORE allocation");}
            catch(IOException failure){throw new UncheckedIOException("Source forest child scope allocation failed",failure);}
            children=Math.addExact(children,1);Scope child=new Scope(childAdapter);child.parent=this;child.charged=childAdapter==null?0L:64L;return child;
        }
        public void charge(long bytes,String context) {
            checkOpen();if(bytes<0)throw new IllegalArgumentException("Negative work reservation");
            if(adapter==null)return;
            long next=Math.addExact(charged,bytes);
            try { adapter.reserveTemporary(bytes,context); }
            catch(IOException failure) { throw new UncheckedIOException(context,failure); }
            charged=next;
        }
        private void checkOpen(){if(Thread.currentThread()!=owner||closed||cleanupFailure!=null)throw new IllegalStateException("Exact open allocation scope owner required",cleanupFailure);}
        @Override public void close() {
            checkOpen();if(children!=0)throw new IllegalStateException("Original lexical child consumers remain unsettled");
            try {
                if(adapter!=null){adapter.releaseTemporary(charged,"source forest lexical work");charged=0;adapter.retire();}
            }catch(IOException failure){cleanupFailure=failure;throw new UncheckedIOException("Source forest cleanup failed",failure);}
            catch(RuntimeException|Error failure){cleanupFailure=failure;throw failure;}
            charged=0;closed=true;if(parent!=null){parent.children--;parent=null;}
        }

    }
    /** 0-byte reservation verifies the adapter's actual owner/open boundary before allocation. */
    default void reserveProfileCheck() {
        try { reserveTemporary(0L,"source forest allocation adapter admission"); }
        catch(IOException failure) { throw new UncheckedIOException("Source forest adapter admission failed",failure); }
    }
}

/** 仅封装生产 Account 的数值费用与槽位生命周期，不授予 owner、Main 或 custody 权限。 */
final class SourceAccountBook {
    static final long ACCOUNT_METADATA_BYTES=128L;
    static final long CUSTODY_METADATA_BYTES=2304L;
    private static final String ACCOUNT_SLOT_CONTEXT="original source holder account/retention slot BEFORE allocation";
    private static final String ACCOUNT_RETIRE_CONTEXT="successful source account last consumer dropped";

    interface Fees {
        void reserve(long bytes,String context)throws IOException;
        void release(long bytes,String context)throws IOException;
    }

    static final class Slot {
        private final SourceAccountBook book;
        private Slot parent;
        private final boolean retirable;
        private long held;
        private int children;
        private volatile boolean retired;
        private volatile Throwable failure;
        private Slot(SourceAccountBook book,Slot parent,boolean retirable){this.book=book;this.parent=parent;this.retirable=retirable;}
    }

    private final Fees fees;
    private final List<Slot> slots=new ArrayList<>();
    // 对应 Root.partialAccount：登记或 Account 包装分配失败时仍保留最新槽，只有精确退休成功才清除。
    private Slot partialSlot;
    private boolean custodyMetadataAvailable,custodySlotRegistered;

    SourceAccountBook(Fees fees){this.fees=Objects.requireNonNull(fees);}

    synchronized void custodyMetadataPrepaid(){
        if(custodyMetadataAvailable||custodySlotRegistered)throw new IllegalStateException("Original source custody metadata fee already settled");
        custodyMetadataAvailable=true;
    }

    synchronized Slot registerPrepaidCustody(){
        if(!custodyMetadataAvailable||custodySlotRegistered)throw new IllegalStateException("Original source custody metadata fee must be settled exactly once");
        Slot slot=new Slot(this,null,false);partialSlot=slot;custodyMetadataAvailable=false;custodySlotRegistered=true;slots.add(slot);return slot;
    }

    synchronized Slot retain(Slot parent,boolean retirable)throws IOException{
        int nextChildren=0;
        if(parent!=null){requireActive(parent);nextChildren=Math.addExact(parent.children,1);}
        fees.reserve(ACCOUNT_METADATA_BYTES,ACCOUNT_SLOT_CONTEXT);
        Slot slot=new Slot(this,parent,retirable);partialSlot=slot;
        if(parent!=null)parent.children=nextChildren;
        slots.add(slot);
        return slot;
    }

    synchronized void requireActive(Slot slot){
        requireBook(slot);
        if(slot.retired||slot.failure!=null)throw new IllegalStateException("Source account retired or cleanup failed",slot.failure);
        if(!slots.contains(slot))throw new IllegalStateException("Source account slot is no longer retained");
    }

    synchronized void reserve(Slot slot,long bytes,String context)throws IOException{
        requireActive(slot);if(bytes<0)throw new IllegalArgumentException("Negative source reservation");
        long next=Math.addExact(slot.held,bytes);
        try{fees.reserve(bytes,context);}catch(IOException|RuntimeException|Error cause){slot.failure=cause;throw cause;}
        slot.held=next;
    }

    synchronized void release(Slot slot,long bytes,String context)throws IOException{
        requireActive(slot);if(bytes<0||bytes>slot.held)throw new IllegalStateException("Source release exceeds exact holder reservation");
        try{fees.release(bytes,context);}catch(IOException|RuntimeException|Error cause){slot.failure=cause;throw cause;}
        slot.held-=bytes;
    }

    synchronized void retire(Slot slot)throws IOException{
        requireActive(slot);
        if(!slot.retirable||slot.held!=0L||slot.children!=0)throw new IllegalStateException("Exact settled temporary source account required for retirement");
        try{fees.release(ACCOUNT_METADATA_BYTES,ACCOUNT_RETIRE_CONTEXT);}
        catch(IOException|RuntimeException|Error cause){slot.failure=cause;throw cause;}
        slots.remove(slot);if(partialSlot==slot)partialSlot=null;if(slot.parent!=null){slot.parent.children--;slot.parent=null;}slot.retired=true;
    }

    private void requireBook(Slot slot){if(slot==null||slot.book!=this)throw new IllegalStateException("Foreign source account slot");}

    synchronized int slotCount(){return slots.size();}
    synchronized boolean isPartialSlot(Slot slot){return partialSlot==slot;}
    synchronized boolean contains(Slot slot){return slot!=null&&slot.book==this&&slots.contains(slot);}
    synchronized long heldBytes(Slot slot){requireBook(slot);return slot.held;}
    synchronized int childCount(Slot slot){requireBook(slot);return slot.children;}
    synchronized Slot parentOf(Slot slot){requireBook(slot);return slot.parent;}
    synchronized Throwable failureCause(Slot slot){requireBook(slot);return slot.failure;}
}

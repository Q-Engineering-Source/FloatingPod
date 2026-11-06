package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;

/** 每 actual loader 一份 bounded async→Main 身份 holder；无UUID/entity global registry。 */
public final class SourceLoadManifest {
    private final Object loader,world;private final Thread main;private final SourceDomainIdentity identity;
    private final SourceLimitProfile limits;private final long operationBudget,initialReservation;
    private final IdentityHashMap<Object,SourceReadOperation> awaiting=new IdentityHashMap<>();
    private final List<SourceReadOperation> retained=new ArrayList<>();private final List<SourceReadOperation.Observation> completed=new ArrayList<>();
    private final java.util.Set<SourceReadOperation> rawObserved=java.util.Collections.newSetFromMap(new IdentityHashMap<>()),mainObserved=java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private long generation,reserved;private volatile Throwable primary;
    private SourceForestAllocationBudget allocation;
    private boolean stopping;
    private int activeEntries;
    private final ThreadLocal<ReadEntry> readEntry=new ThreadLocal<>();
    private final IdentityHashMap<SourceReadOperation,SourceForestAllocationBudget> operationAccounts=new IdentityHashMap<>();
    public final class ReadEntry implements AutoCloseable {
        private final Thread actor=Thread.currentThread();private boolean closed,started;
        private ReadEntry(){}
        public void close(){
            synchronized(SourceLoadManifest.this){
            if(closed||Thread.currentThread()!=actor||readEntry.get()!=this)throw new IllegalStateException("Exact original read entry required");
            if(allocation!=null){try{allocation.releaseTemporary(128L,"original source lexical read entry dropped");}catch(java.io.IOException cause){retainAllocationFailure(cause);throw new java.io.UncheckedIOException(cause);}}
            closed=true;readEntry.remove();activeEntries--;
            }
        }
    }
    public synchronized ReadEntry enterReadEntry(Object actualLoader,Object actualWorld){
        if(actualLoader!=loader||actualWorld!=world||readEntry.get()!=null)throw new IllegalStateException("Exact original loader read entry required");
        if(stopping)throw new IllegalStateException("Original source read admission is stopping");
        if(primary!=null)throw new IllegalStateException("Original source observer failed",primary);
        try{int next=Math.incrementExact(activeEntries);if(allocation!=null)allocation.reserveReadEntry(this);ReadEntry entry=new ReadEntry();readEntry.set(entry);activeEntries=next;return entry;}
        catch(java.io.IOException cause){retainAllocationFailure(cause);throw new java.io.UncheckedIOException(cause);}
        catch(RuntimeException|Error cause){retainAllocationFailure(cause);throw cause;}
    }
    private synchronized void retainAllocationFailure(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
    public void requireAllocationOwner(){if(Thread.currentThread()!=main&&readEntry.get()==null)throw new IllegalStateException("Exact Main or lexical source read required");}
    boolean mainAllocationActor(){return Thread.currentThread()==main;}
    void requireReadEntry(){if(readEntry.get()==null)throw new IllegalStateException("Original lexical read entry required");}
    void requireReadLoader(Object actualLoader){if(actualLoader!=loader)throw new IllegalStateException("Foreign source allocation loader");requireReadEntry();}

    private SourceDomainSession domain;
    private SourceDomainCapture.CatalogDomainObservation domainObservation;
    private SourceLoadManifest(Object loader,Object world,Thread main,SourceDomainIdentity identity,SourceLimitProfile limits){
        this.loader=Objects.requireNonNull(loader);this.world=Objects.requireNonNull(world);this.main=Objects.requireNonNull(main);this.identity=identity;this.limits=limits;
        operationBudget=limits.maxRetainedBytes();initialReservation=Math.addExact(4096L,Math.multiplyExact(limits.maxDecompressedBytes(),4L));
        if(initialReservation>limits.maxRetainedBytes())throw new IllegalArgumentException("Read observation reservation exceeds actual shared retained budget");
    }
    public static SourceLoadManifest install(MinecraftServer server,WorldServer world,AnvilChunkLoader loader,
            SourceDomainSession domain,SourceDomainCapture.CatalogDomainObservation observation){
        if(server==null||world==null||loader==null||Thread.currentThread()!=server.getServerThread()||world.getMinecraftServer()!=server||world.getChunkProvider().chunkLoader!=loader)
            throw new IllegalStateException("Exact actual Main/world/loader required for source install");
        domain.requireCurrent(observation);SourceLoadManifest holder=new SourceLoadManifest(loader,world,server.getServerThread(),domain.identity(),domain.limitsForObservation());
        holder.domain=domain;holder.domainObservation=observation;return holder;
    }
    /** 原 Recall root 先收 holder 费用，后分配并安装；不接受独立本地预算替代。 */
    public static SourceLoadManifest install(MinecraftServer server,WorldServer world,AnvilChunkLoader loader,
            SourceDomainSession domain,SourceDomainCapture.CatalogDomainObservation observation,SourceForestAllocationBudget allocation)throws java.io.IOException{
        Objects.requireNonNull(allocation);
        domain.requireCurrent(observation);
        if(allocation.exactProfile()!=domain.limitsForObservation())throw new IllegalArgumentException("Original source manifest profile required");
        allocation.reserveTemporary(512L,"original source loader manifest BEFORE allocation");
        SourceLoadManifest holder=install(server,world,loader,domain,observation);
        holder.allocation=allocation.forManifest(holder);return holder;
    }
    // package-only direct lifecycle seam; observes data, never represents Minecraft/native integration.
    static SourceLoadManifest observedFixture(Object loader,Object world,SourceLimitProfile limits){return new SourceLoadManifest(loader,world,Thread.currentThread(),SourceDomainIdentity.issue(java.util.UUID.randomUUID(),0),limits);}
    public void assertInstallation(Object actualLoader){assertMain();if(actualLoader!=loader)throw new IllegalStateException("Foreign source loader install");}
    public void assertMain(){if(Thread.currentThread()!=main)throw new IllegalStateException("Exact source Main required");}
    /** 封住新入口；已经接纳的原lexical entry仍能完成一次原read与Main handoff。 */
    public synchronized void beginStopping(Object actualLoader){assertInstallation(actualLoader);stopping=true;}
    /** 仅证明本地read处理已结束，不授予物理责任解除、卸载或账本退款。 */
    public synchronized void requireStoppedReads(Object actualLoader){
        if(!pollStoppedReads(actualLoader))throw new IllegalStateException("Original source reads remain active");
    }
    /** 非阻塞观察；原entry关闭或Main handoff尚未完成时返回false，不污染共享失败。 */
    public synchronized boolean pollStoppedReads(Object actualLoader){
        assertInstallation(actualLoader);
        if(!stopping||primary!=null)throw new IllegalStateException("Original source reads are not stopped or failed",primary);
        if(activeEntries!=0||rawObserved.size()!=retained.size()||!awaiting.isEmpty())return false;
        for(SourceReadOperation operation:retained)
            if(operation.exactReturnedRoot()!=null&&!mainObserved.contains(operation))
                return false;
        return true;
    }
    public void requireCurrentDomain(){assertMain();if(primary!=null)throw new IllegalStateException("Original source manifest failure retained",primary);if(domain!=null)domain.requireCurrent(domainObservation); }
    SourceReadOperation.DomainObservation domainProjection(){return domainObservation==null?
        new SourceReadOperation.DomainObservation(0,0,SourceDomainCapture.GenerationObservation.GENERATION_UNOBSERVED,List.of()):
        new SourceReadOperation.DomainObservation(domainObservation.observedGeneration(),domainObservation.registryGeneration(),domainObservation.generationObservation(),domainObservation.unresolved());}
    SourceLimitProfile limits(){return limits;}SourceDomainIdentity identity(){return identity;}long operationBudget(){return operationBudget;}
    public synchronized SourceReadOperation beginRead(Object actualLoader,Object actualWorld,int x,int z){
        if(actualLoader!=loader||actualWorld!=world)throw new IllegalStateException("Foreign source read loader/world");
        ReadEntry entry=readEntry.get();
        if((stopping&&entry==null)||(entry!=null&&(entry.closed||entry.started)))
            throw new IllegalStateException("Original source read admission ended or was consumed");
        if(primary!=null)throw new IllegalStateException("Source observer fault retained",primary);
        if(allocation!=null)requireReadEntry();
        if(entry!=null)entry.started=true;
        try{
        long next=Math.addExact(reserved,initialReservation);if(next>limits.maxRetainedBytes())throw new IllegalStateException("Source read operation reservation exhausted");
        chargeOriginal(initialReservation,"actual source read/partial holder BEFORE allocation");
        SourceReadOperation operation=new SourceReadOperation(this,world,x,z,Math.addExact(generation,1));generation++;reserved=next;retained.add(operation);if(allocation!=null)operationAccounts.put(operation,allocation.forRead(operation));return operation;
        }catch(RuntimeException|Error cause){retainAllocationFailure(cause);throw cause;}
    }
    synchronized void reserveAdditional(SourceReadOperation operation,long bytes){owned(operation);long next=Math.addExact(reserved,bytes);if(bytes<0||next>limits.maxRetainedBytes())throw new IllegalStateException("Source retained-byte budget exhausted");if(allocation!=null){try{operationAccounts.get(operation).reserveTemporary(bytes,"actual source read retained representation BEFORE allocation");}catch(java.io.IOException cause){throw new java.io.UncheckedIOException(cause);}}reserved=next;}
    public synchronized void rawFinished(SourceReadOperation operation,Throwable cause){
        owned(operation);if(!rawObserved.add(operation))throw new IllegalStateException("Source raw observation delivered twice");if(primary==null)primary=cause==null?operation.observation().primary():cause;Object root=operation.exactReturnedRoot();
        if(root!=null&&cause==null){if(awaiting.containsKey(root)){IllegalStateException conflict=new IllegalStateException("Async root already has another source operation");operation.recordFailure(conflict);if(primary==null)primary=conflict;completed.add(operation.observation());throw conflict;}awaiting.put(root,operation);}
        else completed.add(operation.observation());
    }
    public synchronized SourceReadOperation beginDecode(Object actualLoader,Object actualWorld,Object actualLevel,int x,int z){
        assertMain();if(actualLoader!=loader||actualWorld!=world)throw new IllegalStateException("Foreign source decode owner");
        SourceReadOperation found=null;Object root=null;
        for(var entry:awaiting.entrySet())if(entry.getValue().x()==x&&entry.getValue().z()==z&&entry.getValue().exactReturnedLevel()==actualLevel){
            if(found!=null)throw new IllegalStateException("Ambiguous exact source read root");found=entry.getValue();root=entry.getKey();}
        if(found==null){
            if(stopping)throw new IllegalStateException("Stopped source loader rejects an unaccepted Main decode");
            return null;
        }
        awaiting.remove(root);return found;
    }
    public synchronized void decoded(SourceReadOperation operation,Throwable cause){owned(operation);assertMain();if(!rawObserved.contains(operation)||!mainObserved.add(operation))throw new IllegalStateException("Exact once observed Main handoff required");var observed=operation.observation();if(primary==null)primary=cause==null?observed.primary():cause;completed.add(observed);}
    public synchronized List<SourceReadOperation.Observation> drainObserved(int maximum){
        assertMain();if(maximum<1)throw new IllegalArgumentException("Positive observation drain bound required");int n=Math.min(maximum,completed.size());
        List<SourceReadOperation.Observation> result=List.copyOf(completed.subList(0,n));completed.subList(0,n).clear();
        // Raw/body/stream obligations stay strongly retained; drain does not release reservation or physical responsibility.
        return result;
    }
    private void chargeOriginal(long bytes,String context){
        if(allocation==null)return;
        try{allocation.reserveTemporary(bytes,context);}catch(java.io.IOException cause){throw new java.io.UncheckedIOException(cause);}
    }
    private void owned(SourceReadOperation operation){if(!retained.contains(operation))throw new IllegalArgumentException("Foreign source operation");}
}

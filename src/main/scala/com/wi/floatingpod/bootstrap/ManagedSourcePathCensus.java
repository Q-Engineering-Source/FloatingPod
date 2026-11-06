package com.wi.floatingpod.bootstrap;

import com.wi.floatingpod.server.NormalRecallRecoverySourceCustody;
import com.wi.floatingpod.server.NormalRecallRecoverySourceCustody.ColdSourceObserver;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import com.wi.floatingpod.server.source.SourceLoadManifest;
import java.io.IOException;
import java.nio.file.Path;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Optional;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraft.world.storage.IThreadedFileIO;
import net.minecraft.world.storage.ThreadedFileIOBase;

/**
 * The original boot scope's charged, identity-based census for managed source paths.
 * It records actual live owners; it never makes queue emptiness or object collection a completion signal.
 */
public final class ManagedSourcePathCensus {
    public enum Phase { COLD, LOADED }
    public enum Admission { OPEN, CLOSING, EXCLUSIVE, FAILED }
    public enum Kind {
        FORGE_TASK, FORGE_CALLBACK, SOURCE_READ, FILE_IO_WORK, CAPTURE_JOB,
        PRODUCER, ATTEMPT, QUEUE_ENTRY, FLIGHT, OUTPUT, CACHE_LEASE
    }
    public enum State {
        CREATED, QUEUED, HELD, IN_FLIGHT, OUTPUT_HELD, READ_QUEUED, READ_RUNNING,
        RAW_NBT_HELD, MAIN_INSTALL, FULL_CALLBACK, REQUEUED, CANCEL_PENDING,
        SETTLED, FAILED
    }

    private static final long CENSUS_BASE_BYTES=512L;
    private static final long LOADER_BINDING_BYTES=256L;
    private static final long TOKEN_BYTES=192L;
    private static final long FORGE_CALL_BYTES=96L;
    private static final long EXECUTION_SCOPE_BYTES=64L;
    private static final String CENSUS_BASE_CONTEXT="original managed source census state BEFORE allocation";
    private static final String LOADER_BINDING_CONTEXT="original managed source loader namespace BEFORE registration";
    private static final String TOKEN_CONTEXT="actual source lifecycle token BEFORE registration";

    /**
     * A single value in ManagedBootSaveScope's existing loader identity map.  The census does not
     * keep a second loader registry.  COLD identity is this boot's one retained census; LOADED
     * identity is the actual Anvil loader key used by the custody LoaderSlot map.
     */
    public static final class PathBinding {
        private final ManagedSourcePathCensus census;
        private final Object actualLoader;
        private final Path loaderDirectory;
        private Object actualWorld;
        private int dimension;
        private Path verifiedWorldRoot;
        private Object domainLifetime;
        private SourceLoadManifest exactManifest;
        private long observerGeneration;
        private Token activeSchedulerToken;

        private PathBinding(ManagedSourcePathCensus census,Object actualLoader,Path loaderDirectory) {
            this.census=census;this.actualLoader=actualLoader;this.loaderDirectory=loaderDirectory;
            this.dimension=Integer.MIN_VALUE;this.observerGeneration=census.observerGeneration;
        }
        public Phase phase(){synchronized(census){return exactManifest==null?Phase.COLD:Phase.LOADED;}}
        /** Stable custody observer for COLD; the exact Anvil loader/LoaderSlot key for LOADED. */
        public Object observationId(){synchronized(census){
            if(exactManifest!=null)return actualLoader;
            if(census.coldObserver==null)
                throw census.retainFailure(new IllegalStateException("Exact Custody cold source observer is not attached to this census"));
            return census.coldObserver;
        }}
        public Object bootLifetime(){return census.scope.exactControl();}
        public Object custodyLifetime(){return census.custody;}
        /** SourceLoadManifest retains the actual SourceDomainSession and first-END observation. */
        public Object domainLifetime(){synchronized(census){return domainLifetime;}}
        public Object saveLoaderNamespace(){return actualLoader;}
        public Path worldRoot(){return census.scope.worldRoot();}
        public Path verifiedDimensionRoot(){synchronized(census){return verifiedWorldRoot;}}
        public Path loaderDirectory(){return loaderDirectory;}
        public int dimension(){synchronized(census){return dimension;}}
        public long censusEpoch(){synchronized(census){return census.epoch;}}
        public long observerGeneration(){synchronized(census){return observerGeneration;}}
        public Object actualLoader(){return actualLoader;}
        public Object actualWorld(){synchronized(census){return actualWorld;}}
        public boolean hasVerifiedWorld(){synchronized(census){return actualWorld!=null&&verifiedWorldRoot!=null&&dimension!=Integer.MIN_VALUE;}}
        public Token beginProducer(Object exactProducer){return census.beginProducer(this,exactProducer);}
        private boolean exactManifest(SourceLoadManifest value){return exactManifest==value;}
    }

    /** A unique obligation keyed by the exact live Java owner, never by an invented job UUID. */
    public static final class Token {
        private final ManagedSourcePathCensus census;
        private final Kind kind;
        private final PathBinding binding;
        private final long registrationEpoch;
        private final long registrationGeneration;
        private Object actualOwner;
        private Object actualTask;
        private Object actualQueueEntry;
        private Object actualAttempt;
        private Object actualPayload;
        private Object actualStream;
        private Object actualCacheLease;
        private Object actualChunkBuffer;
        private Object actualRegionFile;
        private Object actualLedgerSlot;
        private Object actualWorld;
        private int x,z;
        private Object exactMainLevel;
        private State state;
        private int children;
        private boolean schedulerRemoved;
        private boolean schedulerHoldRequested;
        private boolean awaitingSchedulerRemoval;
        private boolean taskMapRemoved;
        private boolean notPublishedSynchronous;
        private boolean flightActive;
        private boolean workerActive;
        private boolean workerRan;
        private boolean cancellationAcknowledged;
        private boolean cancelledBeforeRun;
        private State stateBeforeCancellation;
        private Token parent;
        private boolean syncHeld;
        private boolean callbackReturned;
        private boolean readReturned;
        private Throwable primary;
        private boolean closed;

        private Token(ManagedSourcePathCensus census,Kind kind,PathBinding binding,Token parent,
                      Object actualOwner,Object actualTask,Object actualWorld,int x,int z,State state) {
            this.census=census;this.kind=kind;this.binding=binding;this.actualOwner=actualOwner;
            this.parent=parent;
            this.actualTask=actualTask;this.actualWorld=actualWorld;this.x=x;this.z=z;this.state=state;
            this.registrationEpoch=census.epoch;this.registrationGeneration=binding==null?census.observerGeneration:binding.observerGeneration;
        }
        public Kind kind(){return kind;}
        public State state(){synchronized(census){return state;}}
        public PathBinding binding(){return binding;}
        public Object actualOwner(){synchronized(census){return actualOwner;}}
        public Object actualTask(){synchronized(census){return actualTask;}}
        public Object actualQueueEntry(){synchronized(census){return actualQueueEntry;}}
        public Object actualAttempt(){synchronized(census){return actualAttempt;}}
        public Object actualPayload(){synchronized(census){return actualPayload;}}
        public Object actualLedgerSlot(){synchronized(census){return actualLedgerSlot;}}
        public Object actualRegionFile(){synchronized(census){return actualRegionFile;}}
        public Object actualStream(){synchronized(census){return actualStream;}}
        public Object actualChunkBuffer(){synchronized(census){return actualChunkBuffer;}}
        public Object actualWorld(){synchronized(census){return actualWorld;}}
        public int x(){return x;}
        public int z(){return z;}
        public long registrationEpoch(){return registrationEpoch;}
        public long registrationGeneration(){return registrationGeneration;}
        public boolean hasCause(){synchronized(census){return primary!=null;}}
        public Throwable cause(){synchronized(census){return primary;}}
        public boolean isClosed(){synchronized(census){return closed;}}
        public boolean schedulerHoldRequested(Object exactTask){synchronized(census){census.requireTask(this,exactTask,"SCHEDULER_HOLD_QUERY");return schedulerHoldRequested;}}
        public Token beginCapture(Object exactJob,Object exactAttempt,Object actualNbt,Object exactChunkPos,Object exactLedgerSlot){
            return census.beginCapture(this,exactJob,exactAttempt,actualNbt,exactChunkPos,exactLedgerSlot);
        }

        public void bindTask(Object exactTask){census.bindTask(this,exactTask);}
        public void queuePublished(Object exactTask){census.queuePublished(this,exactTask);}
        public void schedulerStarted(Object exactTask){census.schedulerStarted(this,exactTask);}
        public void schedulerReturned(Object exactTask,boolean moreWork){census.schedulerReturned(this,exactTask,moreWork);}
        public void schedulerHeld(Object exactTask){census.schedulerHeld(this,exactTask);}
        public void schedulerRequeued(Object exactTask){census.schedulerRequeued(this,exactTask);}
        public void schedulerRemoved(Object exactTask){census.schedulerRemoved(this,exactTask);}
        public void syncAcquired(Object exactTask){census.syncAcquired(this,exactTask);}
        public void syncReturned(Object exactTask){census.syncReturned(this,exactTask);}
        public void callbackAdded(Object exactTask,Object exactRunnable){census.callbackAdded(this,exactTask,exactRunnable);}
        public void callbackRemoved(Object exactTask,Object exactRunnable,boolean removed){census.callbackRemoved(this,exactTask,exactRunnable,removed);}
        public void callbackStarted(Object exactTask,Object exactRunnable){census.callbackStarted(this,exactTask,exactRunnable);}
        public void callbackEnded(Object exactTask,Object exactRunnable,Throwable cause){census.callbackEnded(this,exactTask,exactRunnable,cause);}
        public void readStarted(Object exactLoader,Object exactWorld,int x,int z){census.readStarted(this,exactLoader,exactWorld,x,z);}
        public void rawOutput(Object exactLoader,Object exactResult,Object exactRoot){census.rawOutput(this,exactLoader,exactResult,exactRoot);}
        public void mainInstallStarted(Object exactWorld,Object exactRoot,Object exactLevel){census.mainInstallStarted(this,exactWorld,exactRoot,exactLevel);}
        public void mainInstallReturned(Object exactWorld,Object exactRoot,Throwable cause){census.mainInstallReturned(this,exactWorld,exactRoot,cause);}
        public void fullCallbackReturned(Object exactTask,Throwable cause){census.fullCallbackReturned(this,exactTask,cause);}
        public void fullCallbackStarted(Object exactTask){census.fullCallbackStarted(this,exactTask);}
        public void workerStarted(Object exactTask){census.workerStarted(this,exactTask);}
        public void workerReturned(Object exactTask,Throwable cause){census.workerReturned(this,exactTask,cause);}
        public void taskMapRemoved(Object exactTask){census.taskMapRemoved(this,exactTask);}
        public void cancellationRequested(Object exactTask){census.cancellationRequested(this,exactTask);}
        public void cancellationAcknowledged(Object exactTask,boolean removedBeforeRun){census.cancellationAcknowledged(this,exactTask,removedBeforeRun);}
        public void producerBound(Object exactProducer){census.bindActualOwner(this,exactProducer,Kind.PRODUCER);}
        public void queueEntryBound(Object actualNbt,Object exactChunkPos,Object exactLedgerSlot){census.queueEntryBound(this,actualNbt,exactChunkPos,exactLedgerSlot);}
        public void attemptBound(Object exactAttempt){census.attemptBound(this,exactAttempt);}
        public void flightStarted(Object exactAttempt,Object exactQueueEntry){census.flightStarted(this,exactAttempt,exactQueueEntry);}
        public void flightStarted(Object exactAttempt,Object exactNbt,Object exactChunkPos,Object exactLedgerSlot){census.flightStarted(this,exactAttempt,exactNbt,exactChunkPos,exactLedgerSlot);}
        public void outputOpened(Object exactRegionFile,Object exactStream,Object exactChunkBuffer){census.outputOpened(this,exactRegionFile,exactStream,exactChunkBuffer);}
        public void outputClosed(Object exactStream,Throwable cause){census.outputClosed(this,exactStream,cause);}
        public void cacheLeaseOpened(Object exactLease,Object exactRegionFile){census.cacheLeaseOpened(this,exactLease,exactRegionFile);}
        public void cacheLeaseClosed(Object exactLease,Throwable cause){census.cacheLeaseClosed(this,exactLease,cause);}
        public void flightEnded(Object exactAttempt,Throwable cause){census.flightEnded(this,exactAttempt,cause);}
        public void fail(Throwable cause){census.fail(this,cause);}
        public void acknowledgeTerminal(Object exactOwner){census.acknowledgeTerminal(this,exactOwner);}
        public void requestSchedulerHold(Object exactTask){census.requestSchedulerHold(this,exactTask);}
        public void requeueSourceLoader(AnvilChunkLoader exactLoader){census.requeueSourceLoader(exactLoader,this);}
        public void requeueSourceWork(IThreadedFileIO exactWork){census.requeueSourceWork(exactWork,this);}
        public ExecutionScope enterExecution(){return census.enterTaskExecution(this);}
        public Token prepareSchedulerAdmission(Object exactTask,Object queueMonitor,boolean alreadyMember){
            return census.prepareSchedulerAdmission(exactTask,queueMonitor,alreadyMember);
        }
        public void schedulerAdmissionReturned(Object exactTask,boolean wasMember,boolean isMember,Throwable cause){
            census.schedulerAdmissionReturned(this,exactTask,wasMember,isMember,cause);
        }
    }

    /** Optional marker implemented only by an actual source-owned IThreadedFileIO work item. */
    public interface ScheduledWork {
        Token floatingpod$sourceWorkToken();
    }

    /** Implemented by the transformed original ThreadedFileIOBase; requeue always targets its exact queue. */
    public interface SchedulerAccess {
        void floatingpod$requeueSourceWork(IThreadedFileIO exactWork,Token exactToken);
    }

    /** Access bridge for the actual package-private Forge ChunkIOProvider instance. */
    public interface SourceChunkIoProviderAccess {
        Token floatingpod$sourceTaskToken();
        void floatingpod$bindSourceTaskToken(Token exactToken);
        boolean floatingpod$hasCallbacks();
    }

    public enum ForgeCallKind { QUEUE, SYNC, DROP }

    /** Temporary call context only; persistent task/callback obligations live in the charged census. */
    public static final class ForgeCall implements AutoCloseable {
        private final ManagedSourcePathCensus census;
        private final ForgeCall previous;
        private final PathBinding binding;
        private final Object world;
        private final AnvilChunkLoader loader;
        private final Object chunkProvider;
        private final int dimension,x,z;
        private final ForgeCallKind kind;
        private Token task;
        private boolean returned;
        private boolean taskMapRemovalSeen;
        private boolean createdSynchronousTask;
        private boolean charged=true;

        private ForgeCall(ManagedSourcePathCensus census,ForgeCall previous,Object world,
                AnvilChunkLoader loader,Object chunkProvider,int dimension,int x,int z,ForgeCallKind kind) {
            this.census=census;this.previous=previous;this.world=world;this.loader=loader;
            this.chunkProvider=chunkProvider;this.dimension=dimension;this.x=x;this.z=z;this.kind=kind;
            this.binding=census.resolveWorldBinding(world,loader,dimension,"FORGE_"+kind.name());
        }
        public Token providerConstructed(Object exactProvider) {
            synchronized(census) {
                requireCurrent();
                if(kind==ForgeCallKind.DROP)throw census.retainFailure(new IllegalStateException("Drop call cannot create a Forge provider"));
                if(task!=null)throw census.retainFailure(new IllegalStateException("One Forge call created multiple source tasks"));
                task=census.createToken(Kind.FORGE_TASK,binding,chunkProvider,exactProvider,world,x,z,State.CREATED);
                createdSynchronousTask=kind==ForgeCallKind.SYNC;
                task.notPublishedSynchronous=createdSynchronousTask;
                return task;
            }
        }
        public Token requireProvider(Object exactProvider,Token installed) {
            synchronized(census) {
                requireCurrent();
                if(installed==null) {
                    if(kind==ForgeCallKind.DROP) return null;
                    installed=findForgeTask(exactProvider);
                    if(installed==null)throw census.retainFailure(new IllegalStateException("Exact existing Forge provider has no census task token"));
                }
                census.requireTask(installed,exactProvider,"FORGE_PROVIDER");
                if(installed.binding!=binding||installed.actualWorld!=world||installed.x!=x||installed.z!=z)
                    throw census.retainFailure(new IllegalStateException("Forge provider differs from exact loader/world/chunk call"));
                task=installed;
                return installed;
            }
        }
        public void taskMapRemoved(Object exactProvider) {
            synchronized(census) {
                requireCurrent();Token exact=requireProvider(exactProvider,
                    exactProvider instanceof SourceChunkIoProviderAccess access?access.floatingpod$sourceTaskToken():null);
                if(exact==null)throw census.retainFailure(new IllegalStateException("Exact Forge task token required for map removal"));
                exact.taskMapRemoved(exactProvider);taskMapRemovalSeen=true;
            }
        }
        public void returned(Throwable cause) {
            synchronized(census) {
                requireCurrent();if(returned)throw census.retainFailure(new IllegalStateException("Forge call return acknowledged twice"));
                returned=true;
                if(task==null)return;
                if(cause!=null){census.rememberFailure(task,cause);return;}
                if(kind==ForgeCallKind.QUEUE)task.queuePublished(task.actualTask);
                else if(kind==ForgeCallKind.SYNC&&!taskMapRemovalSeen&&!createdSynchronousTask)
                    throw census.retainFailure(new IllegalStateException("Existing synchronous Forge task lacks actual task-map removal acknowledgement"));
            }
        }
        private void requireCurrent() {
            if(CURRENT_FORGE_CALL.get()!=this)throw census.retainFailure(new IllegalStateException("Exact Forge call context is not current"));
        }
        @Override public void close() {
            synchronized(census) {
                requireCurrent();
                Throwable failure=null;
                if(!returned)failure=census.retainFailure(new IllegalStateException("Forge call closed without original return acknowledgement"));
                if(previous==null)CURRENT_FORGE_CALL.remove();else CURRENT_FORGE_CALL.set(previous);
                if(charged){charged=false;try{census.release(FORGE_CALL_BYTES,"closed actual Forge call context");}
                    catch(Throwable cleanup){if(failure==null)failure=cleanup;else if(failure!=cleanup)ManagedSourceSaveLedger.suppressSecondary(failure,cleanup);}}
                if(failure!=null){if(failure instanceof RuntimeException runtime)throw runtime;if(failure instanceof Error error)throw error;throw new IllegalStateException("Forge call context cleanup failed",failure);}
            }
        }
        private Token findForgeTask(Object exactProvider) {
            Token found=null;
            for(Token candidate:census.liveTokens.keySet())if(candidate.kind==Kind.FORGE_TASK&&
                    candidate.actualTask==exactProvider&&candidate.binding==binding&&candidate.actualWorld==world&&
                    candidate.x==x&&candidate.z==z) {
                if(found!=null)throw census.retainFailure(new IllegalStateException("Ambiguous exact Forge provider census task"));
                found=candidate;
            }
            return found;
        }
    }

    public final class ExecutionScope implements AutoCloseable {
        private final Token previous;
        private final Token exact;
        private boolean closed;
        private boolean charged=true;
        private ExecutionScope(Token exact) {this.exact=exact;this.previous=CURRENT_TASK.get();CURRENT_TASK.set(exact);}
        @Override public void close() {
            synchronized(ManagedSourcePathCensus.this) {
                if(closed||CURRENT_TASK.get()!=exact)throw retainFailure(new IllegalStateException("Source task execution scope mismatch"));
                closed=true;if(previous==null)CURRENT_TASK.remove();else CURRENT_TASK.set(previous);
                if(charged){charged=false;release(EXECUTION_SCOPE_BYTES,"closed actual source execution context");}
            }
        }
    }

    private final ManagedBootSaveScope scope;
    private final NormalRecallRecoverySourceCustody custody;
    private SourceForestAllocationBudget.Account allocation;
    private IdentityHashMap<Token,Boolean> liveTokens;
    private Admission admission=Admission.OPEN;
    private Throwable primary;
    private long epoch=1L;
    private long closingEpoch=Long.MAX_VALUE;
    private long observerGeneration=1L;
    private long tokenTableCharge;
    private ColdSourceObserver coldObserver;
    private static final ThreadLocal<ForgeCall> CURRENT_FORGE_CALL=new ThreadLocal<>();
    private static final ThreadLocal<Token> CURRENT_TASK=new ThreadLocal<>();
    private int activeTokens;
    private int pendingQueues,heldTasks,inFlightTasks,heldOutputs,activeReads,rawNbt,mainInstalls,fullCallbacks;
    private boolean custodyAccountBound;

    /** Only the existing ManagedBootSaveScope may create the one census for its exact original custody. */
    ManagedSourcePathCensus(ManagedBootSaveScope scope,NormalRecallRecoverySourceCustody custody) {
        this.scope=Objects.requireNonNull(scope,"original managed boot scope");
        this.custody=Objects.requireNonNull(custody,"original source custody");
    }

    public synchronized void requireExactCustody(NormalRecallRecoverySourceCustody actual) {
        if(actual!=custody || Thread.currentThread()!=scope.exactMain() || scope.sourceCensus()!=this)
            throw new IllegalStateException("Exact original boot scope/custody binding required");
        actual.requireAllocationResponsibility();
    }

    /** Called only by the exact scope after Custody created a child of its original charged account. */
    synchronized void attachAccount(SourceForestAllocationBudget.Account exactAccount) throws IOException {
        if(custodyAccountBound || allocation!=null)throw new IllegalStateException("Original census account is one-shot");
        Objects.requireNonNull(exactAccount,"same-root census account");
        coldObserver=custody.requireColdSourceObserver(this);
        allocation=exactAccount;
        custodyAccountBound=true;
        charge(CENSUS_BASE_BYTES,CENSUS_BASE_CONTEXT);
        liveTokens=new IdentityHashMap<>();
        advanceEpoch("CENSUS_REGISTERED");
    }

    public synchronized void requireAllocationOwner() {
        if(!Thread.holdsLock(this) || !custodyAccountBound || allocation==null)
            throw new IllegalStateException("Only the exact synchronized source census may use its custody account");
    }

    public synchronized ForgeCall enterForgeCall(Object actualWorld,AnvilChunkLoader exactLoader,
            Object exactChunkProvider,int dimension,int x,int z,ForgeCallKind kind) {
        requireOperational("FORGE_CALL");
        charge(FORGE_CALL_BYTES,"actual Forge call context BEFORE allocation");
        try{
            ForgeCall call=new ForgeCall(this,CURRENT_FORGE_CALL.get(),actualWorld,exactLoader,
                exactChunkProvider,dimension,x,z,Objects.requireNonNull(kind));
            CURRENT_FORGE_CALL.set(call);
            return call;
        }catch(Throwable cause){try{release(FORGE_CALL_BYTES,"failed actual Forge call context allocation");}
            catch(Throwable cleanup){if(cleanup!=cause)ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}
            if(cause instanceof RuntimeException runtime)throw runtime;
            if(cause instanceof Error error)throw error;
            throw new IllegalStateException("Actual Forge call context allocation failed",cause);}
    }

    public static ForgeCall currentForgeCall(){return CURRENT_FORGE_CALL.get();}

    public synchronized ExecutionScope enterTaskExecution(Token exactTask) {
        requireTask(exactTask,exactTask.actualTask,"TASK_EXECUTION");
        if(exactTask.kind!=Kind.FORGE_TASK)throw retainFailure(new IllegalStateException("Exact Forge source task required for execution context"));
        charge(EXECUTION_SCOPE_BYTES,"actual source execution context BEFORE allocation");
        try{return new ExecutionScope(exactTask);}
        catch(Throwable cause){try{release(EXECUTION_SCOPE_BYTES,"failed source execution context allocation");}
            catch(Throwable cleanup){if(cleanup!=cause)ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}
            if(cause instanceof RuntimeException runtime)throw runtime;
            if(cause instanceof Error error)throw error;
            throw new IllegalStateException("Actual source execution context allocation failed",cause);}
    }

    public static synchronized Token currentTaskFor(ManagedSourcePathCensus exactCensus,
            AnvilChunkLoader exactLoader,Object actualWorld,int x,int z) {
        Objects.requireNonNull(exactCensus);Token task=CURRENT_TASK.get();
        if(task==null)return null;
        synchronized(exactCensus) {
            exactCensus.requireToken(task,"CURRENT_TASK");
            return task.binding!=null&&task.binding.census==exactCensus&&task.binding.actualLoader==exactLoader&&
                    task.actualWorld==actualWorld&&task.x==x&&task.z==z?task:null;
        }
    }

    public synchronized Token taskForProvider(ForgeCall call,Object exactProvider,Token installed) {
        Objects.requireNonNull(call);
        if(call.census!=this)throw retainFailure(new IllegalStateException("Foreign Forge call census"));
        return call.requireProvider(exactProvider,installed);
    }

    public synchronized Admission admission(){return admission;}
    public synchronized Throwable retainedFailure(){return primary;}
    public synchronized long epoch(){return epoch;}
    public synchronized long observerGeneration(){return observerGeneration;}
    public synchronized int activeTokenCount(){return activeTokens;}
    public synchronized boolean hasOutstandingResponsibilities(){
        return activeTokens!=0||pendingQueues!=0||heldTasks!=0||inFlightTasks!=0||heldOutputs!=0||
            activeReads!=0||rawNbt!=0||mainInstalls!=0||fullCallbacks!=0;
    }
    public synchronized Optional<PathBinding> pathBindingForLoader(AnvilChunkLoader exactLoader) {
        Objects.requireNonNull(exactLoader,"actual AnvilChunkLoader");
        return Optional.ofNullable(existingBinding(exactLoader));
    }
    public synchronized boolean complete(){return admission==Admission.EXCLUSIVE&&primary==null&&activeTokens==0&&
        pendingQueues==0&&heldTasks==0&&inFlightTasks==0&&heldOutputs==0&&activeReads==0&&rawNbt==0&&mainInstalls==0&&fullCallbacks==0;}

    synchronized PathBinding registerLoader(Object actualLoader,Path exactDirectory) {
        requireOperational("LOADER_REGISTER");requireMain("LOADER_REGISTER");
        Objects.requireNonNull(actualLoader,"actual loader");Objects.requireNonNull(exactDirectory,"actual loader directory");
        Path normalized=exactDirectory.toAbsolutePath().normalize();
        charge(LOADER_BINDING_BYTES,LOADER_BINDING_CONTEXT);
        PathBinding binding=new PathBinding(this,actualLoader,normalized);
        advanceEpoch("LOADER_REGISTERED");
        return binding;
    }

    public synchronized void noteDimensionMutation() {
        requireMain("DIMENSION_MUTATION");
        if(admission==Admission.EXCLUSIVE)throw retainFailure(new IllegalStateException("Dimension registry changed after census exclusivity"));
        if(admission==Admission.FAILED)throw retained("DIMENSION_MUTATION");
        try{observerGeneration=Math.addExact(observerGeneration,1L);}
        catch(ArithmeticException cause){throw retainFailure(new IllegalStateException("Source observer generation exhausted",cause));}
        for(Object value:scope.registeredLoadersSnapshotForCensus())if(value instanceof PathBinding binding)binding.observerGeneration=observerGeneration;
        advanceEpoch("DIMENSION_MUTATION");
    }

    synchronized PathBinding existingBinding(Object actualLoader) {
        Object value=scope.registeredLoader(actualLoader);
        return value instanceof PathBinding binding?binding:null;
    }

    synchronized void bindActualWorld(PathBinding binding,Object actualWorld,int actualDimension,Path actualWorldRoot) {
        requireBinding(binding,"WORLD_BIND");requireOperational("WORLD_BIND");requireMain("WORLD_BIND");
        if(!(actualWorld instanceof WorldServer serverWorld) || serverWorld.getMinecraftServer()!=scope.actualServer() ||
                serverWorld.provider==null || serverWorld.provider.getDimension()!=actualDimension)
            throw retainFailure(new IllegalStateException("Exact source WorldServer/dimension required"));
        Path normalized=Objects.requireNonNull(actualWorldRoot,"actual source world root").toAbsolutePath().normalize();
        if(!normalized.equals(scope.worldRoot()) || !serverWorld.getSaveHandler().getWorldDirectory().toPath().toAbsolutePath().normalize().equals(normalized))
            throw retainFailure(new IllegalStateException("Actual source world root differs from original managed scope"));
        if(binding.actualWorld!=null && (binding.actualWorld!=actualWorld || binding.dimension!=actualDimension ||
                !normalized.equals(binding.verifiedWorldRoot)))
            throw retainFailure(new IllegalStateException("Actual source loader world/dimension binding changed"));
        binding.actualWorld=actualWorld;binding.dimension=actualDimension;binding.verifiedWorldRoot=normalized;
        advanceEpoch("WORLD_BOUND");
    }

    synchronized void bindLoadedManifest(PathBinding binding,Object actualWorld,SourceLoadManifest manifest) {
        requireBinding(binding,"LOADED_BIND");requireOperational("LOADED_BIND");requireMain("LOADED_BIND");
        Objects.requireNonNull(manifest,"actual loaded SourceLoadManifest");
        if(binding.actualWorld!=actualWorld)throw retainFailure(new IllegalStateException("Loaded manifest world identity differs from registered loader"));
        try { manifest.assertInstallation(binding.actualLoader);manifest.requireCurrentDomain(); }
        catch(Throwable cause){throw retainFailure(cause);}
        if(binding.exactManifest!=null && binding.exactManifest!=manifest)
            throw retainFailure(new IllegalStateException("Actual source loader manifest was replaced"));
        if(binding.exactManifest==manifest)return;
        binding.exactManifest=manifest;binding.domainLifetime=manifest;
        observerGeneration=Math.addExact(observerGeneration,1L);binding.observerGeneration=observerGeneration;
        advanceEpoch("LOADED_DOMAIN_BOUND");
    }

    public synchronized void bindLoadedManifestForLoader(Object exactLoader,SourceLoadManifest exactManifest) {
        requireMain("LOADED_BIND");
        PathBinding binding=existingBinding(exactLoader);
        if(binding==null||binding.actualWorld==null||binding.dimension==Integer.MIN_VALUE||binding.verifiedWorldRoot==null)
            throw retainFailure(new IllegalStateException("Actual registered source loader/world must be bound before its manifest"));
        bindLoadedManifest(binding,binding.actualWorld,exactManifest);
    }

    public synchronized Token beginForgeQueue(Object actualWorld,AnvilChunkLoader actualLoader,
            Object provider,int actualDimension,int x,int z,Object actualCallback) {
        PathBinding binding=resolveWorldBinding(actualWorld,actualLoader,actualDimension,"FORGE_QUEUE");
        Token token=createToken(Kind.FORGE_TASK,binding,provider,null,actualWorld,x,z,State.CREATED);
        token.actualPayload=Objects.requireNonNull(actualCallback,"actual Forge callback");
        return token;
    }

    public synchronized Token beginForgeSync(Object actualWorld,AnvilChunkLoader actualLoader,
            Object provider,int actualDimension,int x,int z) {
        PathBinding binding=resolveWorldBinding(actualWorld,actualLoader,actualDimension,"FORGE_SYNC");
        return createToken(Kind.FORGE_TASK,binding,provider,null,actualWorld,x,z,State.CREATED);
    }

    public synchronized Token beginRead(AnvilChunkLoader exactLoader,Object exactWorld,int actualDimension,int x,int z) {
        PathBinding binding=existingBinding(exactLoader);
        if(binding==null||binding.actualWorld!=exactWorld||binding.dimension!=actualDimension||binding.verifiedWorldRoot==null)
            throw retainFailure(new IllegalStateException("Actual registered source loader/world/dimension binding required for read"));
        Token parent=CURRENT_TASK.get();
        if(parent!=null&&parent.census==this&&parent.kind==Kind.FORGE_TASK&&parent.binding==binding&&
                parent.actualWorld==exactWorld&&parent.x==x&&parent.z==z) {
            parent.readStarted(exactLoader,exactWorld,x,z);
            Token child=findChildByKind(parent,Kind.SOURCE_READ);
            if(child==null)throw retainFailure(new IllegalStateException("Registered Forge source read child is missing"));
            return child;
        }
        requireOperational("DIRECT_SOURCE_READ");
        Token read=createToken(Kind.SOURCE_READ,binding,exactLoader,null,exactWorld,x,z,State.READ_RUNNING);
        return read;
    }

    public synchronized Token activeReadFor(AnvilChunkLoader exactLoader,Object exactWorld,int x,int z) {
        PathBinding binding=existingBinding(exactLoader);Token found=null;
        if(binding==null)return null;
        for(Token token:liveTokens.keySet())if(token.kind==Kind.SOURCE_READ&&token.binding==binding&&
                token.actualWorld==exactWorld&&token.x==x&&token.z==z) {
            if(found!=null)throw retainFailure(new IllegalStateException("Ambiguous exact live source read identity"));
            found=token;
        }
        return found;
    }

    /** Called only by the exact ThreadedFileIOBase mixin while its real queue monitor is held. */
    public synchronized Token prepareSchedulerAdmission(Object actualWork,Object exactQueueMonitor,boolean alreadyMember) {
        if(!custodyAccountBound||allocation==null)throw new IllegalStateException("Original custody census account is not installed at SCHEDULER_ADMISSION");
        if(primary!=null||admission==Admission.FAILED)throw retained("SCHEDULER_ADMISSION");
        if(actualWork==null||exactQueueMonitor==null||!Thread.holdsLock(exactQueueMonitor))
            throw retainFailure(new IllegalStateException("Exact actual scheduler queue monitor required"));
        PathBinding binding=existingBinding(actualWork);
        if(binding==null) {
            if(actualWork instanceof ScheduledWork scheduled) {
                Token capture=scheduled.floatingpod$sourceWorkToken();
                requireToken(capture,"SCHEDULED_CAPTURE_WORK");
                if(capture.actualTask!=actualWork||capture.kind!=Kind.CAPTURE_JOB)
                    throw retainFailure(new IllegalStateException("Actual scheduled capture token/task identity mismatch"));
                if(admission!=Admission.OPEN&&!admittedBeforeClosing(capture))
                    throw retainFailure(new IllegalStateException("Closed census cannot publish unrelated source capture work"));
                if(alreadyMember&&capture.state!=State.QUEUED&&capture.state!=State.IN_FLIGHT)
                    throw retainFailure(new IllegalStateException("Capture task queue membership differs from token state"));
                if(!alreadyMember&&capture.state!=State.CREATED&&capture.state!=State.HELD||
                        !alreadyMember&&capture.state==State.CREATED&&capture.schedulerRemoved)
                    throw retainFailure(new IllegalStateException("Capture scheduler admission is not a first admission or exact held requeue"));
                return capture;
            }
            return null;
        }
        Token token=binding.activeSchedulerToken;
        if(alreadyMember) {
            if(token==null||token.actualTask!=actualWork||token.state!=State.QUEUED&&token.state!=State.IN_FLIGHT)
                throw retainFailure(new IllegalStateException("Actual loader queue membership lacks its exact census token"));
            return token;
        }
        if(token!=null) {
            requireToken(token,"SCHEDULER_REQUEUE");
            boolean parked=token.state==State.HELD;
            if(token.actualTask!=actualWork||!parked)
                throw retainFailure(new IllegalStateException("Absent loader membership has a non-held live census token"));
            return token;
        }
        if(admission!=Admission.OPEN)throw retainFailure(new IllegalStateException("Closed census cannot enroll a new source scheduler task"));
        token=createToken(Kind.FILE_IO_WORK,binding,actualWork,actualWork,null,0,0,State.CREATED);
        binding.activeSchedulerToken=token;
        return token;
    }

    /** Confirms queueIO's actual membership transition; original failures remain sticky on this token. */
    public synchronized void schedulerAdmissionReturned(Token token,Object exactWork,
            boolean wasMember,boolean isMember,Throwable cause) {
        if(token==null)return;
        requireToken(token,"SCHEDULER_ADMISSION_RETURN");
        if(token.actualTask!=exactWork)throw retainFailure(new IllegalStateException("Scheduler queueIO task identity mismatch"));
        if(cause!=null){rememberFailure(token,cause);return;}
        if(wasMember) {
            if(!isMember||token.state!=State.QUEUED&&token.state!=State.IN_FLIGHT)
                throw retainFailure(new IllegalStateException("Existing scheduler membership changed unexpectedly"));
            return;
        }
        if(!isMember)throw retainFailure(new IllegalStateException("Original queueIO returned without publishing actual membership"));
        if(token.state==State.HELD){decrementState(token.state);token.state=State.QUEUED;pendingQueues=Math.addExact(pendingQueues,1);token.schedulerHoldRequested=false;token.awaitingSchedulerRemoval=false;token.schedulerRemoved=false;}
        else if(token.state==State.CREATED){token.state=State.QUEUED;pendingQueues=Math.addExact(pendingQueues,1);token.schedulerRemoved=false;}
        else throw retainFailure(new IllegalStateException("Scheduler membership published from invalid census state"));
        advanceEpoch("SCHEDULER_QUEUE_PUBLISHED");
    }

    /** Empty means no active source scheduler token; it never means the loader is complete. */
    public synchronized Optional<Token> activeSchedulerWorkFor(Object exactLoader) {
        PathBinding binding=existingBinding(exactLoader);
        if(binding==null)return Optional.empty();
        Token token=binding.activeSchedulerToken;
        if(token==null)return Optional.empty();
        requireToken(token,"SCHEDULER_TOKEN_LOOKUP");
        if(token.actualTask!=exactLoader)throw retainFailure(new IllegalStateException("Loader scheduler token identity mismatch"));
        return Optional.of(token);
    }

    public synchronized void requeueSourceLoader(AnvilChunkLoader exactLoader,Token exactToken) {
        if(exactLoader==null)throw retainFailure(new IllegalArgumentException("Exact source Anvil loader required"));
        requeueSourceWork(exactLoader,exactToken);
    }

    public synchronized void requeueSourceWork(IThreadedFileIO exactWork,Token exactToken) {
        requireToken(exactToken,"LOADER_REQUEUE");
        if(exactWork==null||exactToken.actualTask!=exactWork||exactToken.state!=State.HELD||
                exactToken.kind!=Kind.FILE_IO_WORK&&exactToken.kind!=Kind.CAPTURE_JOB)
            throw retainFailure(new IllegalStateException("Exact held source scheduler work token required for requeue"));
        if(exactToken.kind==Kind.FILE_IO_WORK&&(!(exactWork instanceof AnvilChunkLoader)||exactToken.binding==null||
                exactToken.binding.activeSchedulerToken!=exactToken))
            throw retainFailure(new IllegalStateException("Exact active source loader scheduler token required for requeue"));
        if(exactToken.kind==Kind.CAPTURE_JOB&&(!(exactWork instanceof ScheduledWork scheduled)||
                scheduled.floatingpod$sourceWorkToken()!=exactToken))
            throw retainFailure(new IllegalStateException("Exact source CaptureJob work token required for requeue"));
        Object scheduler=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(scheduler instanceof SchedulerAccess access))
            throw retainFailure(new IllegalStateException("Actual transformed ThreadedFileIOBase scheduler access required"));
        access.floatingpod$requeueSourceWork(exactWork,exactToken);
        if(exactToken.state!=State.QUEUED)
            throw retainFailure(new IllegalStateException("Original scheduler did not acknowledge exact source work requeue"));
    }

    public synchronized Token beginProducer(PathBinding binding,Object actualProducer) {
        requireBinding(binding,"PRODUCER");requireOperational("PRODUCER");
        if(Thread.currentThread()!=scope.exactMain())throw retainFailure(new IllegalStateException("Exact source producer Main required"));
        return createToken(Kind.PRODUCER,binding,actualProducer,actualProducer,binding.actualWorld,0,0,State.CREATED);
    }

    private synchronized Token beginCapture(Token loaderToken,Object exactJob,Object exactAttempt,
            Object actualNbt,Object exactChunkPos,Object exactLedgerSlot) {
        requireToken(loaderToken,"CAPTURE_BEGIN");requireAdmittedChild(loaderToken,"CAPTURE_BEGIN");
        if(loaderToken.kind!=Kind.FILE_IO_WORK||loaderToken.binding==null||
                loaderToken.binding.activeSchedulerToken!=loaderToken||
                loaderToken.state!=State.QUEUED&&loaderToken.state!=State.HELD&&loaderToken.state!=State.IN_FLIGHT)
            throw retainFailure(new IllegalStateException("Exact active original loader scheduler token required for capture"));
        Objects.requireNonNull(exactJob,"actual source CaptureJob");Objects.requireNonNull(exactAttempt,"actual source Attempt");
        Objects.requireNonNull(actualNbt,"actual queued NBT root");Objects.requireNonNull(exactChunkPos,"actual ChunkPos identity");
        Objects.requireNonNull(exactLedgerSlot,"actual source ledger Slot");
        Token capture=createChildToken(Kind.CAPTURE_JOB,loaderToken.binding,loaderToken,exactJob,
            loaderToken.actualWorld,loaderToken.x,loaderToken.z,State.CREATED,loaderToken);
        capture.actualAttempt=exactAttempt;capture.actualPayload=actualNbt;
        capture.actualQueueEntry=exactChunkPos;capture.actualLedgerSlot=exactLedgerSlot;
        advanceEpoch("CAPTURE_JOB_REGISTERED");
        return capture;
    }

    public synchronized Token child(Token parent,Kind kind,Object actualOwner,Object actualPayload,State state) {
        requireToken(parent,"TOKEN_CHILD");requireOperational("TOKEN_CHILD");
        if(parent.primary!=null)throw retainFailure(new IllegalStateException("Failed source parent cannot issue child token",parent.primary));
        Token child=createChildToken(kind,parent.binding,actualOwner,parent.actualTask,parent.actualWorld,parent.x,parent.z,state,parent);
        child.actualPayload=actualPayload;
        advanceEpoch("TOKEN_CHILD_CREATED");return child;
    }

    public synchronized void beginClosing() {
        if(admission==Admission.FAILED)throw retained("CENSUS_CLOSING");
        if(admission==Admission.EXCLUSIVE)return;
        if(admission==Admission.CLOSING)return;
        requireMain("CENSUS_CLOSING");closingEpoch=epoch;admission=Admission.CLOSING;advanceEpoch("ADMISSION_CLOSING");
    }

    /** Nonblocking settle: callers may retry after actual terminal owner ACKs; no lock is held while waiting. */
    public synchronized long tryExclusive() {
        requireMain("CENSUS_EXCLUSIVE");
        if(admission==Admission.FAILED)throw retained("CENSUS_EXCLUSIVE");
        if(admission==Admission.OPEN)throw new IllegalStateException("Census must close admission before settling");
        if(admission==Admission.EXCLUSIVE)return epoch;
        if(activeTokens!=0||pendingQueues!=0||heldTasks!=0||inFlightTasks!=0||heldOutputs!=0||activeReads!=0||rawNbt!=0||mainInstalls!=0||fullCallbacks!=0)
            return -1L;
        for(Object value:scope.registeredLoadersSnapshotForCensus()) {
            if(value instanceof PathBinding binding && (binding.actualWorld==null || binding.verifiedWorldRoot==null || binding.dimension==Integer.MIN_VALUE))
                return -1L;
        }
        advanceEpoch("EXCLUSIVE_EPOCH");admission=Admission.EXCLUSIVE;return epoch;
    }

    private PathBinding resolveWorldBinding(Object actualWorld,AnvilChunkLoader loader,int dimension,String stage) {
        requireOperational(stage);Objects.requireNonNull(loader,"actual source Anvil loader");
        if(!(actualWorld instanceof WorldServer world) || world.getMinecraftServer()!=scope.actualServer() ||
                world.provider==null || world.provider.getDimension()!=dimension)
            throw retainFailure(new IllegalStateException("Exact managed source world/dimension required at "+stage));
        PathBinding binding=existingBinding(loader);
        if(binding==null || !binding.loaderDirectory.equals(loader.chunkSaveLocation.toPath().toAbsolutePath().normalize()))
            throw retainFailure(new IllegalStateException("Actual source loader was not registered beneath the original scope at "+stage));
        bindActualWorld(binding,actualWorld,dimension,world.getSaveHandler().getWorldDirectory().toPath());
        if(binding.exactManifest!=null) {
            try { binding.exactManifest.requireCurrentDomain(); }
            catch(Throwable cause){throw retainFailure(cause);}
        }
        return binding;
    }

    private Token createToken(Kind kind,PathBinding binding,Object actualOwner,Object actualTask,Object actualWorld,
                              int x,int z,State state) {
        requireOperational("TOKEN_CREATE");
        return allocateToken(kind,binding,null,actualOwner,actualTask,actualWorld,x,z,state);
    }
    private Token createChildToken(Kind kind,PathBinding binding,Object actualOwner,Object actualTask,Object actualWorld,
                              int x,int z,State state,Token parent) {
        requireToken(parent,"TOKEN_CHILD_CREATE");
        if(!custodyAccountBound||allocation==null)throw retainFailure(new IllegalStateException("Original charged census account is absent"));
        if(admission==Admission.EXCLUSIVE)throw retainFailure(new IllegalStateException("Exclusive census cannot acquire child work"));
        if(parent.primary!=null)throw retainFailure(new IllegalStateException("Failed source parent cannot acquire child work",parent.primary));
        return allocateToken(kind,binding,parent,actualOwner,actualTask,actualWorld,x,z,state);
    }
    private Token allocateToken(Kind kind,PathBinding binding,Token parent,Object actualOwner,Object actualTask,Object actualWorld,
                              int x,int z,State state) {
        long nextCapacity=Math.max(tokenTableCharge,(long)(activeTokens+1)*TOKEN_BYTES);
        if(nextCapacity>tokenTableCharge)charge(nextCapacity-tokenTableCharge,"source census live-token identity table BEFORE growth");
        charge(TOKEN_BYTES,TOKEN_CONTEXT);
        Token token=new Token(this,Objects.requireNonNull(kind),binding,parent,actualOwner,actualTask,actualWorld,x,z,state);
        liveTokens.put(token,Boolean.TRUE);tokenTableCharge=nextCapacity;
        activeTokens=Math.addExact(activeTokens,1);if(parent!=null)parent.children=Math.addExact(parent.children,1);advanceEpoch("TOKEN_CREATED");
        switch(state){
            case QUEUED -> pendingQueues=Math.addExact(pendingQueues,1);
            case HELD -> heldTasks=Math.addExact(heldTasks,1);
            case IN_FLIGHT -> inFlightTasks=Math.addExact(inFlightTasks,1);
            case OUTPUT_HELD, RAW_NBT_HELD -> heldOutputs=Math.addExact(heldOutputs,1);
            case READ_QUEUED, READ_RUNNING -> activeReads=Math.addExact(activeReads,1);
            case MAIN_INSTALL -> mainInstalls=Math.addExact(mainInstalls,1);
            case FULL_CALLBACK -> fullCallbacks=Math.addExact(fullCallbacks,1);
            default -> { }
        }
        return token;
    }

    private void requireAdmittedChild(Token parent,String stage) {
        requireToken(parent,stage);
        if(admission==Admission.FAILED)throw retained(stage);
        if(admission==Admission.EXCLUSIVE||admission==Admission.CLOSING&&!wasAdmittedBeforeClosing(parent))
            throw retainFailure(new IllegalStateException("Source child is outside its original open admission at "+stage));
    }

    private boolean admittedBeforeClosing(Token child) {
        Token parent=child.parent;
        return parent!=null&&wasAdmittedBeforeClosing(parent)&&parent.binding==child.binding&&
            (parent.kind==Kind.FILE_IO_WORK||parent.kind==Kind.FORGE_TASK);
    }
    private boolean wasAdmittedBeforeClosing(Token token){
        return token!=null&&token.census==this&&!token.closed&&liveTokens.containsKey(token)&&token.registrationEpoch<closingEpoch;
    }

    private void bindTask(Token token,Object exactTask) {
        synchronized(this){requireToken(token,"TASK_BIND");Objects.requireNonNull(exactTask);
            if(token.actualTask!=null && token.actualTask!=exactTask)throw retainFailure(new IllegalStateException("Forge task identity changed"));
            token.actualTask=exactTask;token.state=State.QUEUED;pendingQueues=Math.addExact(pendingQueues,1);advanceEpoch("TASK_BOUND");}
    }
    private void queuePublished(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"TASK_PUBLISH");if(token.state==State.CREATED){token.state=State.QUEUED;pendingQueues=Math.addExact(pendingQueues,1);}else if(token.state==State.HELD){decrementState(token.state);token.state=State.QUEUED;pendingQueues=Math.addExact(pendingQueues,1);}else if(token.state!=State.QUEUED)throw retainFailure(new IllegalStateException("Task published outside created/held queue state"));advanceEpoch("TASK_PUBLISHED");}}
    private void schedulerStarted(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"TASK_START");if(token.state!=State.QUEUED)throw retainFailure(new IllegalStateException("Task start outside actual queue state"));decrementState(token.state);token.state=State.IN_FLIGHT;inFlightTasks=Math.addExact(inFlightTasks,1);token.schedulerRemoved=false;token.awaitingSchedulerRemoval=false;advanceEpoch("TASK_IN_FLIGHT");}}
    private void schedulerReturned(Token token,Object exactTask,boolean moreWork){synchronized(this){requireTask(token,exactTask,"TASK_RETURN");if(token.state!=State.IN_FLIGHT)throw retainFailure(new IllegalStateException("Task return without actual in-flight owner"));if(moreWork&&!token.schedulerHoldRequested){decrementState(token.state);token.state=State.QUEUED;pendingQueues=Math.addExact(pendingQueues,1);token.awaitingSchedulerRemoval=false;}else token.awaitingSchedulerRemoval=true;advanceEpoch(moreWork&&!token.schedulerHoldRequested?"TASK_REMAINS_QUEUED":"TASK_AWAITS_QUEUE_REMOVE");}}
    private void schedulerHeld(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"TASK_HELD");if(token.state!=State.IN_FLIGHT||!token.schedulerHoldRequested||!token.awaitingSchedulerRemoval)throw retainFailure(new IllegalStateException("Actual held task requires requested hold and List.remove acknowledgement"));decrementState(token.state);token.state=State.HELD;heldTasks=Math.addExact(heldTasks,1);token.awaitingSchedulerRemoval=false;token.schedulerRemoved=true;advanceEpoch("TASK_HELD");}}
    private void schedulerRequeued(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"TASK_REQUEUE");if(token.state!=State.HELD)throw retainFailure(new IllegalStateException("Only exact held task can be requeued"));decrementState(token.state);token.state=State.QUEUED;pendingQueues=Math.addExact(pendingQueues,1);token.schedulerHoldRequested=false;token.schedulerRemoved=false;advanceEpoch("TASK_REQUEUED");}}
    private void schedulerRemoved(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"TASK_REMOVE");if(!token.awaitingSchedulerRemoval||token.state!=State.IN_FLIGHT)throw retainFailure(new IllegalStateException("Actual task removal lacks its matching writeNextIO return"));if(token.schedulerHoldRequested){schedulerHeld(token,exactTask);return;}decrementState(token.state);token.state=State.CREATED;token.awaitingSchedulerRemoval=false;token.schedulerRemoved=true;advanceEpoch("TASK_QUEUE_REMOVED");trySettleToken(token);}}
    private void syncAcquired(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"SYNC_HOLD");if(token.syncHeld)throw retainFailure(new IllegalStateException("Sync hold repeated"));token.syncHeld=true;advanceEpoch("SYNC_HOLD_ACQUIRED");}}
    private void syncReturned(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"SYNC_RETURN");if(!token.syncHeld)throw retainFailure(new IllegalStateException("Sync hold was not acquired"));token.syncHeld=false;advanceEpoch("SYNC_HOLD_RETURNED");trySettleToken(token);}}
    private void callbackAdded(Token token,Object exactTask,Object exactRunnable){synchronized(this){requireTask(token,exactTask,"CALLBACK_ADD");if(token.actualQueueEntry==exactRunnable)throw retainFailure(new IllegalStateException("Callback identity reused as task queue entry"));Token callback=createChildToken(Kind.FORGE_CALLBACK,token.binding,exactRunnable,exactTask,token.actualWorld,token.x,token.z,State.QUEUED,token);callback.actualQueueEntry=exactRunnable;advanceEpoch("CALLBACK_REGISTERED");}}
    private void callbackRemoved(Token token,Object exactTask,Object exactRunnable,boolean removed){synchronized(this){requireTask(token,exactTask,"CALLBACK_REMOVE");if(!removed)return;Token found=findChild(token,Kind.FORGE_CALLBACK,exactRunnable);if(found==null){throw retainFailure(new IllegalStateException("Actual callback removal has no matching source token"));}if(found.state==State.IN_FLIGHT){found.stateBeforeCancellation=found.state;found.state=State.CANCEL_PENDING;advanceEpoch("CALLBACK_CANCEL_PENDING");return;}closeToken(found,exactRunnable);advanceEpoch("CALLBACK_REMOVED");}}
    private void callbackStarted(Token token,Object exactTask,Object exactRunnable){synchronized(this){requireTask(token,exactTask,"CALLBACK_RUN");Token found=findChild(token,Kind.FORGE_CALLBACK,exactRunnable);if(found==null)throw retainFailure(new IllegalStateException("Actual callback run has no matching source token"));if(found.state!=State.QUEUED)throw retainFailure(new IllegalStateException("Callback run outside queued state"));decrementState(found.state);found.state=State.IN_FLIGHT;inFlightTasks=Math.addExact(inFlightTasks,1);advanceEpoch("CALLBACK_IN_FLIGHT");}}
    private void callbackEnded(Token token,Object exactTask,Object exactRunnable,Throwable cause){synchronized(this){requireTask(token,exactTask,"CALLBACK_END");Token found=findChild(token,Kind.FORGE_CALLBACK,exactRunnable);if(found==null)throw retainFailure(new IllegalStateException("Actual callback end has no matching source token"));if(cause!=null){rememberFailure(found,cause);rememberFailure(token,cause);}if(found.state==State.CANCEL_PENDING){decrementState(found.stateBeforeCancellation);found.stateBeforeCancellation=null;found.state=State.CREATED;}else if(found.state==State.IN_FLIGHT){decrementState(found.state);found.state=State.CREATED;}closeToken(found,exactRunnable);advanceEpoch(cause==null?"CALLBACK_RETURNED":"CALLBACK_FAILED");}}
    private void readStarted(Token token,Object exactLoader,Object exactWorld,int x,int z){synchronized(this){requireToken(token,"READ_START");if(token.binding==null||token.binding.actualLoader!=exactLoader||token.actualWorld!=exactWorld||token.x!=x||token.z!=z)throw retainFailure(new IllegalStateException("Source read differs from exact task world/loader/chunk"));if(token.kind!=Kind.FORGE_TASK&&token.kind!=Kind.SOURCE_READ)throw retainFailure(new IllegalStateException("Source read attached to a non-read task"));Token read=token.kind==Kind.SOURCE_READ?token:createChildToken(Kind.SOURCE_READ,token.binding,exactLoader,token.actualTask,exactWorld,x,z,State.READ_RUNNING,token);if(read==token){if(token.state!=State.READ_RUNNING)throw retainFailure(new IllegalStateException("Direct source read is not active"));}advanceEpoch("READ_RUNNING");}}
    private void rawOutput(Token token,Object exactLoader,Object exactResult,Object exactRoot){synchronized(this){requireToken(token,"RAW_OUTPUT");if(token.kind==Kind.FORGE_TASK){Token read=findChildByKind(token,Kind.SOURCE_READ);if(read==null)throw retainFailure(new IllegalStateException("Forge source task returned without a registered read"));token=read;}if(token.kind!=Kind.SOURCE_READ||token.binding.actualLoader!=exactLoader||token.state!=State.READ_RUNNING)throw retainFailure(new IllegalStateException("Raw source output lacks exact active read"));token.readReturned=true;if(exactResult==null){if(exactRoot!=null)throw retainFailure(new IllegalStateException("Empty source read retained a non-null NBT root"));token.actualPayload=null;decrementState(token.state);token.state=State.CREATED;advanceEpoch("EMPTY_SOURCE_READ_RETURNED");if(token.parent==null)closeToken(token,token.actualOwner);return;}if(!(exactResult instanceof Object[] returned)||returned.length<2||returned[1]!=exactRoot||!(exactRoot instanceof NBTTagCompound))throw retainFailure(new IllegalStateException("Actual source read result/root identity mismatch"));token.actualPayload=exactResult;token.actualStream=exactRoot;decrementState(token.state);token.state=State.RAW_NBT_HELD;rawNbt=Math.addExact(rawNbt,1);heldOutputs=Math.addExact(heldOutputs,1);advanceEpoch("RAW_NBT_HELD");}}
    private void mainInstallStarted(Token token,Object exactWorld,Object exactRoot,Object exactLevel){synchronized(this){requireToken(token,"MAIN_INSTALL");if(token.kind==Kind.FORGE_TASK){Token read=findChildByKind(token,Kind.SOURCE_READ);if(read==null){if(token.actualPayload==null)return;throw retainFailure(new IllegalStateException("Main install has no raw read token"));}token=read;}if(token.kind!=Kind.SOURCE_READ||token.actualWorld!=exactWorld||token.actualStream!=exactRoot||!(exactRoot instanceof NBTTagCompound root)||!root.hasKey("Level",10)||root.getCompoundTag("Level")!=exactLevel)throw retainFailure(new IllegalStateException("Main install lacks exact raw NBT/world/Level identity"));if(token.state!=State.RAW_NBT_HELD)throw retainFailure(new IllegalStateException("Main install before raw read completion"));token.exactMainLevel=Objects.requireNonNull(exactLevel,"actual Level NBT");token.state=State.MAIN_INSTALL;decrementState(State.RAW_NBT_HELD);mainInstalls=Math.addExact(mainInstalls,1);advanceEpoch("MAIN_INSTALL_STARTED");}}
    private void mainInstallReturned(Token token,Object exactWorld,Object exactRoot,Throwable cause){synchronized(this){requireToken(token,"MAIN_INSTALL_END");if(token.kind==Kind.FORGE_TASK){Token read=findChildByKind(token,Kind.SOURCE_READ);if(read==null)return;token=read;}if(token.kind!=Kind.SOURCE_READ||token.actualWorld!=exactWorld||token.actualStream!=exactRoot||token.exactMainLevel==null||token.state!=State.MAIN_INSTALL)throw retainFailure(new IllegalStateException("Main install end differs from actual raw read"));if(cause!=null)rememberFailure(token,cause);decrementState(token.state);token.state=State.RAW_NBT_HELD;token.exactMainLevel=null;rawNbt=Math.addExact(rawNbt,1);heldOutputs=Math.addExact(heldOutputs,1);advanceEpoch(cause==null?"MAIN_INSTALL_RETURNED":"MAIN_INSTALL_FAILED");if(cause==null&&token.parent==null)closeToken(token,token.actualOwner);}}
    private void workerStarted(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"FORGE_WORKER_START");if(token.kind!=Kind.FORGE_TASK||token.workerActive||token.workerRan||token.taskMapRemoved&&!token.state.equals(State.CANCEL_PENDING))throw retainFailure(new IllegalStateException("Exact Forge task worker start required"));token.workerActive=true;inFlightTasks=Math.addExact(inFlightTasks,1);advanceEpoch("FORGE_WORKER_STARTED");}}
    private void workerReturned(Token token,Object exactTask,Throwable cause){synchronized(this){requireTask(token,exactTask,"FORGE_WORKER_RETURN");if(token.kind!=Kind.FORGE_TASK||!token.workerActive)throw retainFailure(new IllegalStateException("Exact active Forge worker return required"));if(cause!=null)rememberFailure(token,cause);token.workerActive=false;token.workerRan=true;inFlightTasks--;if(inFlightTasks<0)throw retainFailure(new IllegalStateException("Forge worker obligation underflow"));advanceEpoch(cause==null?"FORGE_WORKER_RETURNED":"FORGE_WORKER_FAILED");trySettleCancelledTask(token);}}
    private void fullCallbackStarted(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"FULL_CALLBACK_START");if(token.kind!=Kind.FORGE_TASK||(token.state!=State.CREATED&&token.state!=State.QUEUED))throw retainFailure(new IllegalStateException("Actual Forge task must await its full Main callback"));if(token.state==State.QUEUED)decrementState(State.QUEUED);token.state=State.FULL_CALLBACK;fullCallbacks=Math.addExact(fullCallbacks,1);advanceEpoch("FULL_CALLBACK_STARTED");}}
    private void fullCallbackReturned(Token token,Object exactTask,Throwable cause){synchronized(this){requireTask(token,exactTask,"FULL_CALLBACK_END");if(token.kind!=Kind.FORGE_TASK||token.state!=State.FULL_CALLBACK)throw retainFailure(new IllegalStateException("Exact Forge full callback end required"));if(cause!=null)rememberFailure(token,cause);token.callbackReturned=cause==null;if(cause==null){Token read=findChildByKind(token,Kind.SOURCE_READ);if(read!=null){if(read.state==State.MAIN_INSTALL)throw retainFailure(new IllegalStateException("Full callback returned during Main install"));if(read.state==State.RAW_NBT_HELD||read.state==State.READ_RUNNING||read.readReturned&&read.state==State.CREATED)closeToken(read,exactTask);}decrementState(token.state);token.state=State.CREATED;}advanceEpoch(cause==null?"FULL_CALLBACK_RETURNED":"FULL_CALLBACK_FAILED");trySettleToken(token);}}
    private void taskMapRemoved(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"TASK_MAP_REMOVE");if(token.kind!=Kind.FORGE_TASK)throw retainFailure(new IllegalStateException("Task-map removal applies only to exact Forge task"));if(token.taskMapRemoved)return;if(token.state==State.QUEUED){decrementState(token.state);token.state=State.CREATED;}token.taskMapRemoved=true;advanceEpoch("TASK_MAP_REMOVED");trySettleToken(token);trySettleCancelledTask(token);}}
    private void cancellationRequested(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"CANCEL_REQUEST");if(token.state!=State.IN_FLIGHT&&token.state!=State.QUEUED)throw retainFailure(new IllegalStateException("Cancellation requires actual queued or running task"));token.stateBeforeCancellation=token.state;token.state=State.CANCEL_PENDING;advanceEpoch("CANCEL_REQUESTED");}}
    private void cancellationAcknowledged(Token token,Object exactTask,boolean removedBeforeRun){synchronized(this){requireTask(token,exactTask,"CANCEL_ACK");if(token.kind!=Kind.FORGE_TASK||token.state!=State.CANCEL_PENDING||token.cancellationAcknowledged)throw retainFailure(new IllegalStateException("One exact Forge cancellation acknowledgement required"));State prior=token.stateBeforeCancellation;if(prior==null)throw retainFailure(new IllegalStateException("Cancellation prior state missing"));token.cancellationAcknowledged=true;token.cancelledBeforeRun=removedBeforeRun;if(removedBeforeRun){decrementState(prior);token.state=State.CREATED;}else token.state=prior;token.stateBeforeCancellation=null;advanceEpoch(removedBeforeRun?"CANCELLED_BEFORE_RUN_ACK":"CANCELLED_AFTER_RUN_ACK");trySettleCancelledTask(token);}}
    private void bindActualOwner(Token token,Object exactOwner,Kind expected){synchronized(this){requireToken(token,"OWNER_BIND");if(token.kind!=expected||token.actualOwner!=null&&token.actualOwner!=exactOwner)throw retainFailure(new IllegalStateException("Actual source owner binding mismatch"));token.actualOwner=Objects.requireNonNull(exactOwner);advanceEpoch("SOURCE_OWNER_BOUND");}}
    private void queueEntryBound(Token token,Object actualNbt,Object exactChunkPos,Object exactLedgerSlot){synchronized(this){requireToken(token,"QUEUE_ENTRY_BIND");if(token.kind!=Kind.PRODUCER&&token.kind!=Kind.QUEUE_ENTRY)throw retainFailure(new IllegalStateException("Queue entry requires exact producer token"));if(token.actualQueueEntry!=null)throw retainFailure(new IllegalStateException("Producer queue entry is one-shot"));token.actualPayload=Objects.requireNonNull(actualNbt);token.actualQueueEntry=Objects.requireNonNull(exactChunkPos);token.actualLedgerSlot=Objects.requireNonNull(exactLedgerSlot);advanceEpoch("QUEUE_ENTRY_BOUND");}}
    private void attemptBound(Token token,Object exactAttempt){synchronized(this){requireToken(token,"ATTEMPT_BIND");if(token.kind!=Kind.PRODUCER&&token.kind!=Kind.ATTEMPT&&token.kind!=Kind.CAPTURE_JOB)throw retainFailure(new IllegalStateException("Attempt requires original producer/capture"));if(token.actualAttempt!=null&&token.actualAttempt!=exactAttempt)throw retainFailure(new IllegalStateException("Actual source Attempt changed"));token.actualAttempt=Objects.requireNonNull(exactAttempt);advanceEpoch("ATTEMPT_BOUND");}}
    private void flightStarted(Token token,Object exactAttempt,Object exactQueueEntry){synchronized(this){requireToken(token,"FLIGHT_START");if(token.kind!=Kind.PRODUCER&&token.kind!=Kind.FLIGHT)throw retainFailure(new IllegalStateException("Flight requires exact producer"));if(token.actualAttempt!=exactAttempt||token.actualQueueEntry!=exactQueueEntry)throw retainFailure(new IllegalStateException("Flight identity differs from exact Attempt/queue entry"));if(token.flightActive)throw retainFailure(new IllegalStateException("Source flight already active"));token.flightActive=true;inFlightTasks=Math.addExact(inFlightTasks,1);advanceEpoch("SOURCE_FLIGHT_STARTED");}}
    private void flightStarted(Token token,Object exactAttempt,Object exactNbt,Object exactChunkPos,Object exactLedgerSlot){synchronized(this){requireToken(token,"FLIGHT_START");if(token.kind!=Kind.CAPTURE_JOB&&token.kind!=Kind.PRODUCER||token.actualAttempt!=exactAttempt||token.actualPayload!=exactNbt||token.actualQueueEntry!=exactChunkPos||token.actualLedgerSlot!=exactLedgerSlot)throw retainFailure(new IllegalStateException("Flight identity differs from exact captured Attempt/NBT/ChunkPos/Slot"));if(token.flightActive)throw retainFailure(new IllegalStateException("Exact source flight already active"));token.flightActive=true;inFlightTasks=Math.addExact(inFlightTasks,1);advanceEpoch(token.kind==Kind.CAPTURE_JOB?"SOURCE_CAPTURE_FLIGHT_STARTED":"SOURCE_FLIGHT_STARTED");}}
    private void outputOpened(Token token,Object exactRegionFile,Object exactStream,Object exactChunkBuffer){synchronized(this){requireToken(token,"OUTPUT_OPEN");if(token.kind!=Kind.PRODUCER&&token.kind!=Kind.FLIGHT&&token.kind!=Kind.CAPTURE_JOB)throw retainFailure(new IllegalStateException("Output requires exact producer/flight/capture"));if(!token.flightActive||token.actualAttempt==null||token.actualStream!=null)throw retainFailure(new IllegalStateException("Output needs an exact active flight and is one-shot"));token.actualRegionFile=Objects.requireNonNull(exactRegionFile);token.actualStream=Objects.requireNonNull(exactStream);token.actualChunkBuffer=Objects.requireNonNull(exactChunkBuffer);if(token.kind!=Kind.CAPTURE_JOB)token.state=State.OUTPUT_HELD;heldOutputs=Math.addExact(heldOutputs,1);advanceEpoch("REGION_OUTPUT_HELD");}}
    private void outputClosed(Token token,Object exactStream,Throwable cause){synchronized(this){requireToken(token,"OUTPUT_CLOSE");if(token.actualStream!=exactStream)throw retainFailure(new IllegalStateException("Actual Region output close acknowledgement required"));if(cause!=null)rememberFailure(token,cause);heldOutputs--;if(heldOutputs<0)throw retainFailure(new IllegalStateException("Source output obligation underflow"));token.actualStream=null;token.actualChunkBuffer=null;if(token.kind!=Kind.CAPTURE_JOB)token.state=State.CREATED;advanceEpoch(cause==null?"REGION_OUTPUT_CLOSED":"REGION_OUTPUT_CLOSE_FAILED");}}
    private void cacheLeaseOpened(Token token,Object exactLease,Object exactRegionFile){synchronized(this){requireToken(token,"CACHE_OPEN");if(token.kind!=Kind.PRODUCER&&token.kind!=Kind.FLIGHT&&token.kind!=Kind.CAPTURE_JOB)throw retainFailure(new IllegalStateException("Cache lease requires exact producer/flight/capture"));if(token.actualCacheLease!=null)throw retainFailure(new IllegalStateException("Cache lease is one-shot"));if(token.actualRegionFile!=exactRegionFile)throw retainFailure(new IllegalStateException("Cache lease RegionFile identity mismatch"));token.actualCacheLease=Objects.requireNonNull(exactLease);advanceEpoch("CACHE_LEASE_HELD");}}
    private void cacheLeaseClosed(Token token,Object exactLease,Throwable cause){synchronized(this){requireToken(token,"CACHE_CLOSE");if(token.actualCacheLease!=exactLease)throw retainFailure(new IllegalStateException("Exact original cache lease close acknowledgement required"));if(cause!=null)rememberFailure(token,cause);token.actualCacheLease=null;advanceEpoch(cause==null?"CACHE_LEASE_CLOSED":"CACHE_LEASE_CLOSE_FAILED");}}
    private void flightEnded(Token token,Object exactAttempt,Throwable cause){synchronized(this){requireToken(token,"FLIGHT_END");if(token.actualAttempt!=exactAttempt||!token.flightActive)throw retainFailure(new IllegalStateException("Exact active Attempt flight end required"));if(token.actualStream!=null||token.actualCacheLease!=null)throw retainFailure(new IllegalStateException("Source flight cannot end before actual output/cache lease close acknowledgement"));if(cause!=null)rememberFailure(token,cause);token.flightActive=false;inFlightTasks--;if(inFlightTasks<0)throw retainFailure(new IllegalStateException("Source flight obligation underflow"));advanceEpoch(cause==null?"SOURCE_FLIGHT_ENDED":"SOURCE_FLIGHT_FAILED");trySettleToken(token);}}
    private void fail(Token token,Throwable cause){synchronized(this){requireToken(token,"TOKEN_FAILURE");Objects.requireNonNull(cause,"original source failure");rememberFailure(token,cause);advanceEpoch("SOURCE_LIFECYCLE_FAILED");}}
    private void acknowledgeTerminal(Token token,Object exactOwner){synchronized(this){requireToken(token,"TOKEN_TERMINAL");if(token.actualOwner!=exactOwner&&token.actualTask!=exactOwner)throw retainFailure(new IllegalStateException("Exact source terminal owner acknowledgement required"));if(token.children!=0||token.syncHeld||token.actualCacheLease!=null||token.actualStream!=null||token.flightActive||token.awaitingSchedulerRemoval||token.state==State.QUEUED||token.state==State.HELD||token.state==State.IN_FLIGHT||token.state==State.OUTPUT_HELD||token.state==State.RAW_NBT_HELD||token.state==State.MAIN_INSTALL||token.state==State.FULL_CALLBACK||token.state==State.CANCEL_PENDING)throw retainFailure(new IllegalStateException("Source lifecycle still has queue/held/inflight/output/read/callback ownership"));if((token.kind==Kind.FILE_IO_WORK||token.kind==Kind.CAPTURE_JOB)&&!token.schedulerRemoved)throw retainFailure(new IllegalStateException("Exact source scheduler remove acknowledgement required"));if(token.kind==Kind.FORGE_TASK&&(!token.taskMapRemoved||!token.callbackReturned))throw retainFailure(new IllegalStateException("Forge task map removal and full callback acknowledgement required"));if(token.primary!=null)throw retainFailure(new IllegalStateException("Failed source token cannot acknowledge success",token.primary));token.state=State.SETTLED;closeToken(token,exactOwner);advanceEpoch("SOURCE_TERMINAL_ACKNOWLEDGED");}}
    private void requestSchedulerHold(Token token,Object exactTask){synchronized(this){requireTask(token,exactTask,"SCHEDULER_HOLD");if(token.state!=State.IN_FLIGHT&&token.state!=State.QUEUED)throw retainFailure(new IllegalStateException("Only actual queued/in-flight work may request a scheduler hold"));if(token.schedulerHoldRequested)throw retainFailure(new IllegalStateException("Source scheduler hold request is one-shot per membership"));token.schedulerHoldRequested=true;advanceEpoch("SCHEDULER_HOLD_REQUESTED");}}

    private Token findChild(Token parent,Kind kind,Object exactIdentity){
        for(Token token:liveTokens.keySet())if(token.kind==kind&&token.actualOwner==exactIdentity&&token.actualTask==parent.actualTask)return token;
        return null;
    }
    private Token findChildByKind(Token parent,Kind kind){
        Token found=null;
        for(Token token:liveTokens.keySet())if(token.kind==kind&&token.actualTask==parent.actualTask){if(found!=null)throw retainFailure(new IllegalStateException("Ambiguous live source child token"));found=token;}
        return found;
    }
    private void trySettleToken(Token token){
        if(token.kind==Kind.FORGE_TASK&&token.children==0&&token.taskMapRemoved&&token.callbackReturned&&token.primary==null&&
                token.actualStream==null&&token.actualCacheLease==null&&!token.flightActive&&!token.workerActive&&token.state==State.CREATED){
            token.state=State.SETTLED;closeToken(token,token.actualTask==null?token.actualOwner:token.actualTask);
        }
    }
    private void trySettleCancelledTask(Token token){
        if(token.kind!=Kind.FORGE_TASK||!token.taskMapRemoved||!token.cancellationAcknowledged||token.primary!=null||token.workerActive||
                !token.cancelledBeforeRun&&!token.workerRan)return;
        Token read=findChildByKind(token,Kind.SOURCE_READ);
        if(read!=null){
            if(read.state==State.MAIN_INSTALL||read.actualStream!=null&&read.state!=State.RAW_NBT_HELD)return;
            closeToken(read,token.actualTask);
        }
        if(token.children!=0)return;
        if(token.state==State.QUEUED){decrementState(token.state);token.state=State.CREATED;}
        if(token.state==State.CANCEL_PENDING){
            State prior=token.stateBeforeCancellation;
            if(prior!=null)decrementState(prior);
            token.stateBeforeCancellation=null;token.state=State.CREATED;
        }
        token.callbackReturned=true;trySettleToken(token);
    }
    private void closeToken(Token token,Object exactOwner){
        if(token.closed)return;
        if(token.children!=0)throw retainFailure(new IllegalStateException("Source token children remain active"));
        Token parent=token.parent;
        if(parent!=null&&parent.children<=0)throw retainFailure(new IllegalStateException("Source token parent child count underflow"));
        token.closed=true;liveTokens.remove(token);activeTokens--;
        switch(token.state){
            case QUEUED -> pendingQueues--;
            case HELD -> heldTasks--;
            case IN_FLIGHT -> inFlightTasks--;
            case OUTPUT_HELD -> heldOutputs--;
            case RAW_NBT_HELD -> {heldOutputs--;rawNbt--;}
            case READ_QUEUED, READ_RUNNING -> activeReads--;
            case MAIN_INSTALL -> mainInstalls--;
            case FULL_CALLBACK -> fullCallbacks--;
            default -> { }
        }
        if(token.binding!=null&&token.binding.activeSchedulerToken==token)token.binding.activeSchedulerToken=null;
        token.actualOwner=null;token.actualTask=null;token.actualQueueEntry=null;token.actualAttempt=null;
        token.actualPayload=null;token.actualStream=null;token.actualCacheLease=null;token.actualChunkBuffer=null;
        token.actualRegionFile=null;token.actualLedgerSlot=null;token.actualWorld=null;
        release(TOKEN_BYTES,"settled source lifecycle token");
        if(parent!=null){parent.children--;trySettleToken(parent);}
    }
    private void decrementState(State state){
        switch(state){
            case QUEUED -> pendingQueues--;
            case HELD -> heldTasks--;
            case IN_FLIGHT -> inFlightTasks--;
            case OUTPUT_HELD -> heldOutputs--;
            case RAW_NBT_HELD -> {heldOutputs--;rawNbt--;}
            case READ_QUEUED, READ_RUNNING -> activeReads--;
            case MAIN_INSTALL -> mainInstalls--;
            case FULL_CALLBACK -> fullCallbacks--;
            default -> { }
        }
        if(pendingQueues<0||heldTasks<0||inFlightTasks<0||heldOutputs<0||activeReads<0||rawNbt<0||mainInstalls<0||fullCallbacks<0)
            throw retainFailure(new IllegalStateException("Source census obligation counter underflow"));
    }
    private void rememberFailure(Token token,Throwable cause){
        if(token.primary==null)token.primary=cause;else if(token.primary!=cause)ManagedSourceSaveLedger.suppressSecondary(token.primary,cause);
        if(primary==null)primary=cause;else if(primary!=cause)ManagedSourceSaveLedger.suppressSecondary(primary,cause);
        admission=Admission.FAILED;
    }
    private RuntimeException retainFailure(Throwable cause){
        if(cause==null)cause=new IllegalStateException("Missing original census failure");
        if(primary==null)primary=cause;else if(primary!=cause)ManagedSourceSaveLedger.suppressSecondary(primary,cause);
        admission=Admission.FAILED;
        if(cause instanceof RuntimeException runtime)return runtime;
        if(cause instanceof Error error)throw error;
        return new IllegalStateException("Original source census operation failed",cause);
    }
    private IllegalStateException retained(String stage){return new IllegalStateException("Original source census failed stage="+stage,primary);}
    private void requireBinding(PathBinding binding,String stage){if(binding==null||binding.census!=this||scope.registeredLoader(binding.actualLoader)!=binding)throw retainFailure(new IllegalStateException("Foreign/unregistered source path binding at "+stage));}
    private void requireToken(Token token,String stage){if(token==null||token.census!=this||token.closed||!liveTokens.containsKey(token))throw retainFailure(new IllegalStateException("Foreign/closed source token at "+stage));}
    private void requireTask(Token token,Object exactTask,String stage){requireToken(token,stage);if(token.actualTask!=exactTask)throw retainFailure(new IllegalStateException("Actual source task identity mismatch at "+stage));}
    private void requireOperational(String stage){
        if(!custodyAccountBound||allocation==null)throw new IllegalStateException("Original custody census account is not installed at "+stage);
        if(primary!=null||admission==Admission.FAILED)throw retained(stage);
        if(admission!=Admission.OPEN)throw retainFailure(new IllegalStateException("Source census admission is closed at "+stage));
    }
    private void requireMain(String stage){if(Thread.currentThread()!=scope.exactMain())throw retainFailure(new IllegalStateException("Exact source Main required at "+stage));}
    private void charge(long bytes,String context){
        requireAllocationOwner();
        if(bytes<0)throw new IllegalArgumentException("Negative source census charge");
        try{allocation.reserveTemporary(bytes,context);}catch(IOException|RuntimeException|Error cause){throw retainFailure(cause);}
    }
    private void release(long bytes,String context){
        requireAllocationOwner();
        try{allocation.releaseTemporary(bytes,context);}catch(IOException|RuntimeException|Error cause){throw retainFailure(cause);}
    }
    private void advanceEpoch(String stage){
        try{epoch=Math.addExact(epoch,1L);}catch(ArithmeticException cause){throw retainFailure(new IllegalStateException("Census epoch exhausted at "+stage,cause));}
    }
}

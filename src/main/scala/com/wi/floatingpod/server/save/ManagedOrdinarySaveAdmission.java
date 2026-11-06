package com.wi.floatingpod.server.save;

import java.util.Objects;
import com.wi.floatingpod.bootstrap.ManagedBootSaveScope;
import com.wi.floatingpod.server.MainBootProjection;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.gen.ChunkProviderServer;


/** 短锁仅保护资格和责任计数；外部 current、队列操作和 IO 均由调用方在锁外执行。 */
public final class ManagedOrdinarySaveAdmission {
    public enum Phase { CANDIDATE, PUTTING, QUEUED, FLIGHT, COMPLETE, UNKNOWN }
    public enum ProviderOperation { SAVE_EXTRA_DATA, SAVE, UNLOAD }
    public enum ProviderDecision { ALLOW, HELD, DEFER }
    /** 原安装 callback 提供当前保护判断及原 Session 临时账本，不发行 source lease。 */
    public interface OrdinaryProviderPolicy {
        ProviderDecision decide(ChunkProviderServer provider,WorldServer world,Chunk chunk,
                ProviderOperation operation) throws Throwable;
        AutoCloseable retainTemporary(long bytes,String purpose) throws Exception;
        default void requireConfirmedSuccessor() { throw new IllegalStateException("No original confirmed successor issuer"); }
    }
    private final OrdinaryProviderPolicy ordinaryProviderPolicy;
    private static final int PROVIDER_SAVE_EXTRA_DATA=0,PROVIDER_SAVE=1,PROVIDER_UNLOAD=2;
    private static final ThreadLocal<ProviderScope> PROVIDER_SCOPE=new ThreadLocal<>();
    public final class Token {
        private final ManagedOrdinarySaveAdmission owner=ManagedOrdinarySaveAdmission.this;
        private Phase phase=Phase.CANDIDATE;
        private Thread flightActor;
        private Object loader;private int x,z;
        private Object attempt,payload;
        private long generation;
        private long finalKeySetCharge;
        private SourceGeneration sourceGeneration;
        private final boolean sourceContinuationToken;
        private Token(SourceGeneration sourceGeneration) {
            this.sourceGeneration=sourceGeneration;this.sourceContinuationToken=sourceGeneration instanceof SourceScopedBarrier;
        }
        public Phase phase(){synchronized(ManagedOrdinarySaveAdmission.this){return phase;}}
    }
    public final class Mutation {
        private boolean resolved,accepted;
        private Mutation(){ }
        /** 工作线程可以先于 submit 返回到达；先等真实结果，再等原 save 责任。 */
        public void awaitBeforeRepository() throws InterruptedException {
            if(Thread.currentThread()==main)throw new IllegalStateException("Main must not wait for save IO");
            synchronized(ManagedOrdinarySaveAdmission.this){
                while(!resolved && failure==null)ManagedOrdinarySaveAdmission.this.wait();
                requireHealthy();
                if(!accepted)throw new IllegalStateException("Unaccepted mutation cannot execute");
                while((outstanding!=0 || (ordinaryProviderPolicy!=null && (activeProviderScopes!=0 || activeFlights!=0))) && failure==null)ManagedOrdinarySaveAdmission.this.wait();
                requireHealthy();
            }
        }
    }
    /**
     * A short, synchronous Main token for one actual ChunkProviderServer call. It retains only the
     * currently visited chunk and three typed decisions; chunk dirty/unload state remains the
     * durable responsibility after this lexical scope closes.
     */
    public final class ProviderScope implements AutoCloseable {
        private final Thread actor=Thread.currentThread();
        private ProviderScope previous;
        private SourceScopedBarrier sourceGeneration;
        private AutoCloseable charge;
        private Object provider,world,chunk;
        private ProviderDecision saveExtraDataDecision,saveDecision,unloadDecision;
        private boolean heldAny,deferredAny,closed;
        private ProviderScope(Object exactProvider,Object exactWorld,SourceScopedBarrier exactGeneration,
                AutoCloseable exactCharge){
            provider=Objects.requireNonNull(exactProvider);world=Objects.requireNonNull(exactWorld);
            previous=PROVIDER_SCOPE.get();sourceGeneration=exactGeneration;
            charge=Objects.requireNonNull(exactCharge);
            if(previous!=null&&previous.actor!=Thread.currentThread())throw new IllegalStateException("Exact nested provider scope actor required");
            synchronized(ManagedOrdinarySaveAdmission.this){
                requireMain();requireHealthy();
                if(sourceContinuationGeneration!=sourceGeneration||
                        (sourceGeneration!=null?sourceGeneration.closed:ordinaryProviderPolicy==null))
                    throw new IllegalStateException("Exact active source continuation generation required for provider scope");
                activeProviderScopes=Math.incrementExact(activeProviderScopes);
            }
            PROVIDER_SCOPE.set(this);
        }
        private void requireCurrent(Object actualProvider,Object actualWorld){
            if(closed||Thread.currentThread()!=actor||PROVIDER_SCOPE.get()!=this||
                    provider!=actualProvider||world!=actualWorld)
                throw new IllegalStateException("Exact active Main provider scope required");
        }
        private boolean ownerIs(ManagedOrdinarySaveAdmission exactOwner){return ManagedOrdinarySaveAdmission.this==exactOwner;}
        private void selectChunk(Object actualChunk){
            if(chunk==actualChunk)return;
            chunk=Objects.requireNonNull(actualChunk);
            saveExtraDataDecision=null;saveDecision=null;unloadDecision=null;
        }
        private ProviderDecision stored(ProviderOperation operation){
            return switch(operation){
                case SAVE_EXTRA_DATA -> saveExtraDataDecision;
                case SAVE -> saveDecision;
                case UNLOAD -> unloadDecision;
            };
        }
        private void store(ProviderOperation operation,ProviderDecision decision){
            switch(operation){
                case SAVE_EXTRA_DATA -> saveExtraDataDecision=decision;
                case SAVE -> saveDecision=decision;
                case UNLOAD -> unloadDecision=decision;
            }
            if(decision!=ProviderDecision.ALLOW){heldAny=true;if(decision==ProviderDecision.DEFER)deferredAny=true;}
        }
        /** Returns the exact sticky decision for this chunk/effect, defaulting to DEFER when unobserved. */
        public ProviderDecision decision(Object actualChunk,ProviderOperation operation)throws Throwable{
            requireCurrent(provider,world);Objects.requireNonNull(operation);selectChunk(actualChunk);
            if(!(actualChunk instanceof Chunk exactChunk)||exactChunk.getWorld()!=world)
                throw new IllegalStateException("Exact provider-scope chunk/world required");
            ProviderDecision existing=stored(operation);
            ProviderDecision blocked=chunkBlocked();if(blocked!=ProviderDecision.ALLOW){store(operation,blocked);return blocked;}
            ProviderDecision selected;
            try{selected=ownerDecision(provider,world,actualChunk,operation,sourceGeneration);}
            catch(Throwable cause){providerDecisionFailed(cause);throw cause;}
            return storeObserved(actualChunk,operation,existing==null?selected:restrictive(existing,selected));
        }
        /**
         * Carries a typed late decision from the exact Anvil save wrapper back to this provider token.
         * A later ALLOW never clears an earlier HELD/DEFER result for the same chunk or scope.
         */
        public ProviderDecision observeDecision(Object actualChunk,ProviderOperation operation,ProviderDecision decision)throws Throwable{
            requireCurrent(provider,world);Objects.requireNonNull(operation);Objects.requireNonNull(decision);selectChunk(actualChunk);
            if(!(actualChunk instanceof Chunk exactChunk)||exactChunk.getWorld()!=world)
                throw new IllegalStateException("Exact provider-scope chunk/world required");
            ProviderDecision prior=stored(operation);
            ProviderDecision selected=decision;
            selected=restrictProviderDecision(selected,sourceGeneration);
            ProviderDecision combined=selected;
            if(prior!=null)combined=restrictive(prior,combined);
            store(operation,combined);return combined;
        }
        private ProviderDecision storeObserved(Object actualChunk,ProviderOperation operation,ProviderDecision decision){
            selectChunk(actualChunk);store(operation,Objects.requireNonNull(decision));return decision;
        }
        private ProviderDecision chunkBlocked(){
            if(saveExtraDataDecision!=null&&saveExtraDataDecision!=ProviderDecision.ALLOW)return saveExtraDataDecision;
            if(saveDecision!=null&&saveDecision!=ProviderDecision.ALLOW)return saveDecision;
            if(unloadDecision!=null&&unloadDecision!=ProviderDecision.ALLOW)return unloadDecision;
            return ProviderDecision.ALLOW;
        }
        public boolean held(Object actualChunk){
            requireCurrent(provider,world);return chunk==actualChunk&&chunkBlocked()!=ProviderDecision.ALLOW;
        }
        public boolean hasHeld(){requireCurrent(provider,world);return heldAny;}
        public boolean hasDeferred(){requireCurrent(provider,world);return deferredAny;}
        public void failed(Throwable cause){
            requireCurrent(provider,world);providerDecisionFailed(Objects.requireNonNull(cause));
        }
        /** Reserves an additional short-lived Main allocation through the same source lease/account. */
        public AutoCloseable retainTemporaryCharge(long bytes,String purpose){
            requireCurrent(provider,world);
            if(bytes<=0||purpose==null||purpose.isEmpty())throw new IllegalArgumentException("Positive exact provider temporary charge required");
            try{return sourceGeneration!=null?sourceGeneration.retainTemporaryCharge(bytes,purpose):
                    ordinaryProviderPolicy.retainTemporary(bytes,purpose);}
            catch(RuntimeException cause){throw cause;}
            catch(Exception cause){throw new IllegalStateException("Original ordinary provider temporary reservation failed",cause);}
        }
        public boolean matchesChunk(Object actualChunk,int x,int z){
            requireCurrent(provider,world);
            return chunk==actualChunk&&actualChunk instanceof Chunk exact&&exact.x==x&&exact.z==z&&exact.getWorld()==world;
        }
        private boolean chunkCoordinatesMatch(int x,int z){
            return chunk instanceof Chunk exact&&exact.x==x&&exact.z==z&&exact.getWorld()==world;
        }
        private ProviderDecision operationDecision(ProviderOperation operation){return stored(operation);}
        private boolean isCurrentSourceSave(Object exactLoader,int x,int z,SourceScopedBarrier exactGeneration){
            requireCurrent(provider,world);
            return sourceGeneration==exactGeneration&&provider instanceof ChunkProviderServer exactProvider&&
                    exactProvider.chunkLoader==exactLoader&&chunkCoordinatesMatch(x,z)&&
                    stored(ProviderOperation.SAVE)==ProviderDecision.ALLOW;
        }
        @Override public void close() throws Exception{
            requireCurrent(provider,world);
            ProviderScope oldPrevious=previous;SourceScopedBarrier retainedGeneration=sourceGeneration;
            AutoCloseable release=charge;Throwable primary=null;
            // Drop game-object references before retiring this lexical responsibility count.
            chunk=null;provider=null;world=null;sourceGeneration=null;previous=null;
            saveExtraDataDecision=null;saveDecision=null;unloadDecision=null;charge=null;closed=true;
            try{
                PROVIDER_SCOPE.set(oldPrevious);
                synchronized(ManagedOrdinarySaveAdmission.this){
                    if(heldAny&&retainedGeneration!=null)retainedGeneration.retainIncomplete();
                    if(heldAny&&oldPrevious!=null&&!oldPrevious.closed&&oldPrevious.ownerIs(ManagedOrdinarySaveAdmission.this)&&
                            oldPrevious.sourceGeneration==retainedGeneration){
                        oldPrevious.heldAny=true;
                        if(deferredAny)oldPrevious.deferredAny=true;
                    }
                    activeProviderScopes--;ManagedOrdinarySaveAdmission.this.notifyAll();
                }
            }catch(Throwable cause){primary=cause;providerDecisionFailed(cause);}
            try{release.close();}
            catch(Throwable cleanup){
                providerDecisionFailed(cleanup);
                if(primary==null)primary=cleanup;else suppress(primary,cleanup);
            }
            if(primary!=null)throwCloseFailure(primary);
        }
    }
    @FunctionalInterface public interface SaveStep { boolean run() throws Throwable; }
    @FunctionalInterface public interface Diagnostic { void failed(Throwable cause) throws Throwable; }
    private final java.util.IdentityHashMap<Object,Throwable> quarantined=new java.util.IdentityHashMap<>();
    private Throwable schedulerSecondaryFailure,schedulerRecordingFailure,schedulerDiagnosticFailure;
    private Throwable quarantineMapFailure;
    private boolean quarantineRecordingIncomplete;
    /** 调度接口的 false 仅移除失败 loader；failure/tokens 继续保留，flush 必须观察失败。 */
    public boolean executeLoader(Object loader,SaveStep original,Diagnostic diagnostic)throws Throwable{
        Throwable retained;
        synchronized(this){retained=quarantineRecordingIncomplete?failure:quarantined.get(loader);}
        if(retained!=null){if(isMain())throw retained;return false;}
        try{
            Flight registered;
            synchronized(this){
                // escrow准入与旧Flight登记同锁：先到者保留责任，后到空调度真实返回false退休。
                if((finalGeneration==null&&(oldGenerationFenced||finalEscrow!=null))||(finalGeneration!=null&&finalGeneration.consumed))return false;
                registered=flight(loader);
            }
            try(var flight=registered){
                try{return original.run();}
                catch(Throwable cause){flight.failed(cause);throw cause;}
            }
        }catch(Throwable cause){
            schedulerFailed(loader,cause);
            associateSchedulerFailure(cause);
            try{diagnostic.failed(cause);}catch(Throwable secondary){
                synchronized(this){if(schedulerDiagnosticFailure==null)schedulerDiagnosticFailure=secondary;}
                try{suppress(cause,secondary);}
                catch(Throwable association){synchronized(this){if(schedulerRecordingFailure==null)schedulerRecordingFailure=association;}}
            }
            if(isMain())throw cause;
            return false;
        }
    }
    private static void suppress(Throwable primary,Throwable secondary){
        if(primary!=secondary&&!reaches(primary,secondary)&&!reaches(secondary,primary))primary.addSuppressed(secondary);
    }
    @FunctionalInterface public interface Recheck { void run() throws Throwable; }
    /** 原 AnvilSaveHandler 仅捕获 InterruptedException；用有 cause 的 unchecked failure 越过该吞错点。 */
    public static final class FlushWaitInterrupted extends IllegalStateException {
        private FlushWaitInterrupted(InterruptedException cause){super("Managed Anvil flush wait interrupted; save outcome UNKNOWN",cause);}
    }
    public void waitForFlush(Recheck originalWait)throws Throwable{
        requireMain();Objects.requireNonNull(originalWait);
        try{originalWait.run();}
        catch(InterruptedException original){
            Thread.currentThread().interrupt();
            unknownAtBoundary(original); // 不释放 token；其后真实 IO 完成也不能擦掉本次失败。
            throw new FlushWaitInterrupted(original);
        }
    }
    /** 原 flush 异常为主；后置身份/quarantine 诊断失败只作 suppressed。 */
    public void flush(Recheck originalFlush,Recheck actualOutcome)throws Throwable{
        requireMain();Objects.requireNonNull(originalFlush);Objects.requireNonNull(actualOutcome);
        Throwable primary=null;
        try{originalFlush.run();}
        catch(Throwable cause){primary=cause;throw cause;}
        finally{
            try{actualOutcome.run();}
            catch(Throwable diagnostic){if(primary!=null)suppress(primary,diagnostic);else throw diagnostic;}
        }
    }
    private record Slot(int x,int z) { }
    private final java.util.Set<Token> unresolved=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final java.util.Set<Token> liveTokens=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final java.util.IdentityHashMap<Object,java.util.Map<Slot,Token>> queued=new java.util.IdentityHashMap<>();
    private static final ThreadLocal<Producer> PRODUCER=new ThreadLocal<>();
    private static final ThreadLocal<Flight> FLIGHT=new ThreadLocal<>();
    private static final ThreadLocal<MainFlush> MAIN_FLUSH=new ThreadLocal<>();
    public final class MainFlush implements AutoCloseable {
        private Object loader;private MainFlush previous;private boolean closed;
        private MainFlush(Object loader){this.loader=loader;previous=MAIN_FLUSH.get();MAIN_FLUSH.set(this);}
        private ManagedOrdinarySaveAdmission owner(){return ManagedOrdinarySaveAdmission.this;}
        @Override public void close(){
            requireMain();if(closed||MAIN_FLUSH.get()!=this)throw new IllegalStateException("Exact Main flush scope close required");
            closed=true;
            try{
                MAIN_FLUSH.set(previous);
                synchronized(ManagedOrdinarySaveAdmission.this){
                    if(finalGeneration!=null&&failure==null){loader=null;previous=null;finalRetainedCharge-=48L;}
                }
            }catch(Throwable cause){
                if(finalGeneration!=null){schedulerFailed(loader,cause);associateSchedulerFailure(cause);}
                throw cause;
            }
        }
    }
    /** 仅由实际 loader.flush 的词法 wrapper 使用；不是一般 Main writer 资格。 */
    public synchronized MainFlush mainFlush(Object loader){
        requireMain();requireHealthy();Objects.requireNonNull(loader);
        if(ordinarySavesSealed)throw new IllegalStateException("Ordinary save admission has ended");
        if(stopped&&finalSealing)throw new IllegalStateException("Final Main flush producer ended");
        if(finalGeneration==null){
            if(finalEscrow!=null)throw new IllegalStateException("Old Main flush sealed by final source escrow");
            return new MainFlush(loader);
        }
        reserveFinalLive(48L);
        try{return new MainFlush(loader);}catch(Throwable cause){schedulerFailed(loader,cause);throw cause;}
    }
    public boolean isMain(){return Thread.currentThread()==main;}
    private boolean mainFlushAllows(Object loader){MainFlush scope=MAIN_FLUSH.get();return scope!=null&&!scope.closed&&scope.owner()==this&&scope.loader==loader;}
    public final class Producer implements AutoCloseable {
        private Token token;private Recheck recheck;private Producer previous;private boolean closed;
        private Producer(Token token,Recheck recheck){this.token=token;this.recheck=recheck;previous=PRODUCER.get();PRODUCER.set(this);}
        public void beforePut(Object loader,int x,int z,Object payload)throws Throwable{
            if(PRODUCER.get()!=this||closed||token.loader!=loader||token.x!=x||token.z!=z)throw new IllegalStateException("Exact ordinary producer/loader/slot required");
            if(token.sourceGeneration instanceof FinalSaveGeneration generation){
                recheck.run(); // 原Scope物理检查及final source分支，不重新借Boot投影。
                synchronized(ManagedOrdinarySaveAdmission.this){
                    requireMain();requireHealthy();
                    if(generation!=finalGeneration||token.phase!=Phase.CANDIDATE||token.payload!=null)
                        throw new IllegalStateException("Exact final candidate root binds once");
                    token.payload=Objects.requireNonNull(payload); // 先保partialroot，失败仍由原Token承担。
                }
                int count=generation.source.expectedArtifacts(token.attempt,loader,x,z,payload);
                if(count<0)throw new IllegalStateException("Actual nonnegative final physical Pod count required");
                synchronized(ManagedOrdinarySaveAdmission.this){
                    long charge=Math.multiplyExact(24L,count);reserveFinalLive(Math.addExact(charge,568L));token.finalKeySetCharge=charge;
                }
                boolean valid=false;
                try{generation.source.requireMainPayload(token.attempt,loader,x,z,payload);valid=true;}
                finally{if(valid)synchronized(ManagedOrdinarySaveAdmission.this){finalRetainedCharge-=568L;}}
            }else if(token.sourceGeneration instanceof SourceScopedBarrier generation){
                recheck.run(); // exact source/current callbacks stay outside Admission monitor.
                ProviderScope scope=currentProviderScopeForSave(token.loader,token.x,token.z);
                if(scope==null||scope.sourceGeneration!=generation||scope.decision(scope.chunk,ProviderOperation.SAVE)!=ProviderDecision.ALLOW)
                    throw new IllegalStateException("Exact ALLOW provider source scope required before source save Map.put");
                synchronized(ManagedOrdinarySaveAdmission.this){
                    requireMain();requireHealthy();
                    if(generation!=sourceContinuationGeneration||generation.closed||token.phase!=Phase.CANDIDATE||token.payload!=null||
                            !scope.isCurrentSourceSave(token.loader,token.x,token.z,generation))
                        throw new IllegalStateException("Exact source continuation generation/chunk decision changed before Map.put");
                    token.payload=Objects.requireNonNull(payload);
                }
            }else recheck.run(); // 游戏验证始终在queue/Admission短锁外。
            ManagedOrdinarySaveAdmission.this.beforePut(token,token,payload);
        }
        public void putReturned(Object actualPrevious){acceptActualPut(token,actualPrevious);}
        public void failed(Throwable cause){
            synchronized(ManagedOrdinarySaveAdmission.this){
                if((token.sourceGeneration instanceof FinalSaveGeneration||token.sourceContinuationToken)&&token.phase==Phase.COMPLETE)unknown(cause);
                else ManagedOrdinarySaveAdmission.this.failed(token,cause);
            }
        }
        /** 原 save 在实际 put 后仍可能 queueIO/Error 失败；不能只靠正常 close 留悬空 token。 */
        public void failedAfterAdmission(Throwable cause){
            synchronized(ManagedOrdinarySaveAdmission.this){
                if(token.phase==Phase.CANDIDATE)return;
                if(token.phase==Phase.COMPLETE&&(token.sourceGeneration instanceof FinalSaveGeneration||token.sourceContinuationToken))unknown(cause);
                else ManagedOrdinarySaveAdmission.this.failed(token,cause);
            }
        }
        @Override public void close(){
            if(closed||PRODUCER.get()!=this)throw new IllegalStateException("Exact ordinary producer close required");
            closed=true;PRODUCER.set(previous);
            synchronized(ManagedOrdinarySaveAdmission.this){
                if(token.phase==Phase.CANDIDATE)noPut(token);
                else if(token.phase==Phase.PUTTING)ManagedOrdinarySaveAdmission.this.failed(token,new IllegalStateException("Map.put outcome unresolved at producer return"));
                if(token.sourceGeneration instanceof FinalSaveGeneration&&token.phase!=Phase.UNKNOWN){
                    token=null;recheck=null;previous=null;
                    // 字段撤引用后最后一步结算；之后没有外部调用，词法栈随本close返回。
                    finalRetainedCharge-=56L;
                }else if(token.sourceContinuationToken&&token.phase!=Phase.UNKNOWN){
                    token=null;recheck=null;previous=null;
                }
            }
        }
    }
    public final class Flight implements AutoCloseable {
        private Flight previous;private Object expectedLoader;
        private SourceGeneration sourceGeneration;private Token token;private boolean closed;
        private Flight(Object loader,SourceGeneration sourceGeneration){expectedLoader=loader;this.sourceGeneration=sourceGeneration;previous=FLIGHT.get();FLIGHT.set(this);}
        private ManagedOrdinarySaveAdmission owner(){return ManagedOrdinarySaveAdmission.this;}
        public void removed(Object loader,int x,int z,Object actualCompound){
            if(closed||FLIGHT.get()!=this||token!=null||(expectedLoader!=null&&expectedLoader!=loader))throw new IllegalStateException("Exact first ordinary remove required");
            token=removeActual(loader,x,z,actualCompound,sourceGeneration);
        }
        public void beforeWrite(Object loader,int x,int z,Object compound){
            synchronized(ManagedOrdinarySaveAdmission.this){
                if(token==null||token.loader!=loader||token.x!=x||token.z!=z||token.payload!=compound||token.flightActor!=Thread.currentThread()||token.phase!=Phase.FLIGHT)
                    throw new IllegalStateException("Exact ordinary flight payload/loader/slot required before write");
            }
        }
        public void failed(Throwable cause){if(token==null)unknownAtBoundary(cause);else ManagedOrdinarySaveAdmission.this.failed(token,cause);}
        @Override public void close(){
            synchronized(ManagedOrdinarySaveAdmission.this){
                if(closed||FLIGHT.get()!=this)throw new IllegalStateException("Exact ordinary flight close required");
                closed=true;
                try{
                    if(token!=null){
                        // 原业务失败已保留；仅撤词法 TLS，不宣称未结算 registration 完成。
                        if(token.phase==Phase.UNKNOWN){FLIGHT.set(previous);return;}
                        flightReturned(token);
                    }
                    FLIGHT.set(previous);
                    boolean finalFlight=sourceGeneration instanceof FinalSaveGeneration;
                    boolean sourceFlight=sourceGeneration instanceof SourceScopedBarrier;
                    if(finalFlight||sourceFlight){token=null;previous=null;expectedLoader=null;sourceGeneration=null;}
                    activeFlights--;ManagedOrdinarySaveAdmission.this.notifyAll();
                    if(finalFlight)finalRetainedCharge-=64L;
                }catch(Throwable cause){
                    try{if(FLIGHT.get()==this)FLIGHT.set(previous);}catch(Throwable cleanup){suppress(cause,cleanup);}
                    unknown(cause);throw cause;
                }
            }
        }
    }
    public Producer producer(Object loader,int x,int z,Recheck recheck)throws Throwable{
        Objects.requireNonNull(loader);Objects.requireNonNull(recheck);
        SourceScopedBarrier generation;
        synchronized(this){requireHealthy();generation=sourceContinuationGeneration;}
        if(generation!=null){
            ProviderScope scope=currentProviderScopeForSave(loader,x,z);
            if(scope==null||scope.sourceGeneration!=generation)
                throw new IllegalStateException("Exact source-scoped provider token required before producer allocation");
            ProviderDecision decision;
            try{decision=scope.decision(scope.chunk,ProviderOperation.SAVE);}
            catch(Throwable cause){providerDecisionFailed(cause);throw cause;}
            if(decision!=ProviderDecision.ALLOW)
                throw new IllegalStateException("Source provider save is held/deferred before producer allocation: "+decision);
            synchronized(this){
                requireMain();requireHealthy();
                if(generation!=sourceContinuationGeneration||generation.closed||stopped||pending!=null||finalSealing||
                        !scope.isCurrentSourceSave(loader,x,z,generation))
                    throw new IllegalStateException("Exact active source provider decision required for ordinary producer");
                if(outstanding>=maximumTokens)throw new IllegalStateException("Save token capacity exhausted");
                Token token=new Token(generation);token.loader=loader;token.x=x;token.z=z;
                liveTokens.add(token);outstanding++;return new Producer(token,recheck);
            }
        }
        Token token=candidate();token.loader=loader;token.x=x;token.z=z;return new Producer(token,recheck);
    }
    private ProviderScope currentProviderScopeForSave(Object exactLoader,int x,int z){
        if(Thread.currentThread()!=main)return null;
        for(ProviderScope scope=PROVIDER_SCOPE.get();scope!=null;scope=scope.previous){
            if(scope.closed||scope.actor!=Thread.currentThread()||!scope.ownerIs(this)||
                    !(scope.provider instanceof ChunkProviderServer provider)||
                    !(scope.world instanceof WorldServer world)||provider.world!=world||
                    world.getChunkProvider()!=provider||
                    provider.chunkLoader!=exactLoader||!scope.chunkCoordinatesMatch(x,z))continue;
            return scope;
        }
        return null;
    }
    public Flight flight(){return flight(null);}
    public synchronized Flight flight(Object actualLoader){
        if(ordinarySavesSealed)throw new IllegalStateException("Ordinary save admission has ended");
        if(isMain()&&!mainFlushAllows(actualLoader))throw new IllegalStateException("Main Flight requires original exact loader.flush scope");
        // 先登记再分配/TLS 发布；失败保留责任且明确 UNKNOWN，不能伪装健康等待。
        if(finalGeneration==null&&(oldGenerationFenced||finalEscrow!=null))throw new IllegalStateException("Old save generation sealed before final activation");
        try{
            if(finalGeneration!=null)reserveFinalLive(64L);
            activeFlights=Math.incrementExact(activeFlights);
            SourceGeneration generation=finalGeneration!=null?finalGeneration:
                    sourceContinuationGeneration!=null?sourceContinuationGeneration:freshSourceBoundary;
            return new Flight(actualLoader,generation);
        }
        catch(Throwable cause){unknown(cause);throw cause;}
    }
    public Producer currentProducer(){Producer value=PRODUCER.get();return value!=null&&value.token.owner==this?value:null;}
    public Flight currentFlight(){Flight value=FLIGHT.get();return value!=null&&value.owner()==this?value:null;}
    private synchronized void acceptActualPut(Token token,Object actualPrevious){
        owned(token,Phase.PUTTING);
        var map=queued.computeIfAbsent(token.loader,k->new java.util.HashMap<>());var slot=new Slot(token.x,token.z);
        Token old=map.get(slot);
        if((old==null&&actualPrevious!=null)||(old!=null&&old.payload!=actualPrevious)){
            failed(token,new IllegalStateException("Actual Map.put previous differs from live ordinary generation"));
            throw new IllegalStateException("Ordinary queue lineage UNKNOWN",failure);
        }
        long generation=Math.incrementExact(nextGeneration);
        putReturned(token,generation,old);nextGeneration=generation;map.put(slot,token);
    }
    private long nextGeneration;
    private synchronized Token removeActual(Object loader,int x,int z,Object actualCompound,SourceGeneration sourceGeneration){
        var map=queued.get(loader);var slot=new Slot(x,z);Token token=map==null?null:map.get(slot);
        if(actualCompound==null&&token==null)return null;
        if(token==null||token.payload!=actualCompound){unknownAtBoundary(new IllegalStateException("Actual Map.remove differs from live ordinary generation"));throw new IllegalStateException("Ordinary queue removal UNKNOWN",failure);}
        if(token.sourceGeneration!=sourceGeneration){
            var cause=new IllegalStateException("Exact save source generation required before remove");unknown(cause);throw cause;
        }
        removed(token,token,token.generation,actualCompound);map.remove(slot);if(map.isEmpty())queued.remove(loader);return token;
    }
    public synchronized void unknownAtBoundary(Throwable cause){unknown(cause);}
    /** 调度退休只接管真实 QUEUED；UNKNOWN 不得重新调度。调用方持真实 payload guard。 */
    public synchronized int schedulerQueuedCount(Object loader){
        if(quarantineRecordingIncomplete||quarantined.containsKey(loader))return -1;
        var entries=queued.get(loader);return entries==null?0:entries.size();
    }
    public synchronized void requireSchedulerQueuedPayload(Object loader,int x,int z,Object payload){
        if(quarantineRecordingIncomplete)throw new IllegalStateException("Scheduler quarantine tracking incomplete",failure);
        if(quarantined.containsKey(loader))throw new IllegalStateException("Quarantined scheduler loader",quarantined.get(loader));
        var entries=queued.get(loader);Token found=null;
        if(entries!=null)for(Token token:entries.values())if(token.x==x&&token.z==z){found=token;break;}
        if(found==null||found.owner!=this||found.loader!=loader||found.phase!=Phase.QUEUED||found.payload!=payload)
            throw new IllegalStateException("Scheduler pending payload differs from original QUEUED token");
    }
    public synchronized void schedulerFailed(Object loader,Throwable cause){
        Objects.requireNonNull(loader);Objects.requireNonNull(cause);
        // 先锁存真实责任；Map分配和可覆盖的Throwable访问失败不能撤销退休或逃出全局IO。
        if(failure==null)failure=cause;
        else if(failure!=cause&&schedulerSecondaryFailure==null)schedulerSecondaryFailure=cause;
        retired=true;notifyAll();
        if(quarantineRecordingIncomplete)return;
        try{quarantined.putIfAbsent(loader,cause);}
        catch(Throwable recording){quarantineMapFailure=recording;quarantineRecordingIncomplete=true;return;}
    }
    /** 只由真实锁外诊断出口调用；可覆盖Throwable访问不得进入任何调度/queue/Admission短锁。 */
    public void associateSchedulerFailure(Throwable cause){
        Throwable primary;synchronized(this){primary=failure;}
        if(primary==null||primary==cause)return;
        try{suppress(primary,cause);}
        catch(Throwable association){synchronized(this){if(schedulerRecordingFailure==null)schedulerRecordingFailure=association;}}
    }
    public synchronized Throwable schedulerRecordingFailure(){return schedulerRecordingFailure;}
    public synchronized Throwable schedulerQuarantineMapFailure(){return quarantineMapFailure;}
    private final Thread main;
    private final int maximumTokens;
    private int outstanding;
    private int activeFlights;
    private int activeProviderScopes;
    private boolean providerFlushIncomplete;
    private long nextSourceContinuationGeneration;
    private SourceScopedBarrier sourceContinuationGeneration;
    private Mutation pending;
    private boolean retired,stopped,ordinarySavesSealed;
    private Throwable failure;
    private FreshSourceBoundary freshSourceBoundary;
    private boolean finalSealing,oldGenerationFenced;
    private long finalRetainedCharge;
    private long finalMutationCharge;
    private SourceEscrow finalEscrow;
    private FinalSaveGeneration finalGeneration;
    /** 此桥只由原 Main factory 登记；File IO 不调用任何游戏字段验证。 */
    public interface FinalSaveValidator {
        void requireComplete();
        void endMainConsumers();
        void requireMainSave(Object world,int x,int z,Object chunk) throws Throwable;
        int expectedArtifacts(Object actualChunk,Object loader,int x,int z,Object root) throws Throwable;
        void requireMainPayload(Object actualChunk,Object loader,int x,int z,Object root) throws Throwable;
    }
    /** 固定 static 图；没有隐含外部 Admission/Scala scope 捕获。 */
    private static final class FinalSaveGeneration implements SourceGeneration {
        private final ManagedOrdinarySaveAdmission owner;
        private FinalSaveValidator source;
        private final long generation;
        private boolean consumed;
        private FinalSaveGeneration(ManagedOrdinarySaveAdmission owner,FinalSaveValidator source,long generation){
            this.owner=owner;this.source=source;this.generation=generation;
        }
    }
    public static final class SourceEscrow {
        private final ManagedOrdinarySaveAdmission owner;
        private Object sourceOwner;
        private long amount;
        private boolean consumed;
        private SourceEscrow(ManagedOrdinarySaveAdmission owner,Object sourceOwner,long amount){
            this.owner=owner;this.sourceOwner=sourceOwner;this.amount=amount;
        }
    }
    /** Exact registered source lease plus its live per-chunk decision closure; never a chunk list. */
    public final class SourceScopedBarrier implements SourceGeneration,AutoCloseable {
        private MainBootProjection.SourceContinuationLease lease;
        private MainBootProjection.SourceContinuationObservation observation;
        private AutoCloseable charge;
        private final long generation;
        private boolean incomplete,transferred,closed;
        private SourceScopedBarrier(MainBootProjection.SourceContinuationLease lease,
                MainBootProjection.SourceContinuationObservation observation,AutoCloseable charge,long generation){
            this.lease=Objects.requireNonNull(lease);this.observation=Objects.requireNonNull(observation);
            this.charge=Objects.requireNonNull(charge);this.generation=generation;
        }
        private ProviderDecision decide(Object actualProvider,Object actualWorld,Object actualChunk,
                ProviderOperation operation)throws Throwable{
            if(closed||lease==null)throw new IllegalStateException("Exact active source continuation barrier required");
            if(!(actualProvider instanceof ChunkProviderServer provider)||!(actualWorld instanceof WorldServer world)||
                    !(actualChunk instanceof Chunk chunk)||world.getChunkProvider()!=provider||chunk.getWorld()!=world)
                throw new IllegalStateException("Exact source provider/world/chunk effect required");
            synchronized(ManagedOrdinarySaveAdmission.this){
                requireHealthy();
                if(sourceContinuationGeneration!=this||stopped||pending!=null)return ProviderDecision.DEFER;
            }
            return Objects.requireNonNull(lease.decideProviderChunk(provider,world,chunk,operation),
                    "registered source lease decision");
        }
        private void retainIncomplete(){incomplete=true;}
        private AutoCloseable retainProviderScopeCharge(){
            MainBootProjection.SourceContinuationLease exactLease=lease;
            MainBootProjection.SourceContinuationObservation exactObservation=observation;
            if(closed||exactLease==null||exactObservation==null)
                throw new IllegalStateException("Exact live source continuation charge owner required");
            return exactLease.retainAdmissionCharge(exactObservation,256L,
                    "provider scope+charge holder BEFORE allocation");
        }
        private AutoCloseable retainTemporaryCharge(long bytes,String purpose){
            MainBootProjection.SourceContinuationLease exactLease=lease;
            MainBootProjection.SourceContinuationObservation exactObservation=observation;
            if(closed||exactLease==null||exactObservation==null)
                throw new IllegalStateException("Exact live source continuation charge owner required");
            if(bytes<=0||purpose==null||purpose.isEmpty())
                throw new IllegalArgumentException("Positive exact provider temporary charge required");
            return exactLease.retainAdmissionCharge(exactObservation,bytes,purpose);
        }
        /** The exact lease closure takes ownership of this same generation; this call never settles it. */
        public void requireTransferToLease(MainBootProjection.SourceContinuationLease exactLease,
                MainBootProjection.SourceContinuationObservation exactObservation){
            requireMain();Objects.requireNonNull(exactLease);Objects.requireNonNull(exactObservation);
            synchronized(ManagedOrdinarySaveAdmission.this){
                requireHealthy();
                if(closed||sourceContinuationGeneration!=this||lease!=exactLease||observation!=exactObservation)
                    throw new IllegalStateException("Exact active source barrier/lease/observation required for transfer");
                if(transferred)return; // Per-chunk closure validation rechecks the original handoff in active provider scopes.
                if(activeProviderScopes!=0||outstanding!=0||activeFlights!=0||pending!=null||!queued.isEmpty()||
                        !liveTokens.isEmpty()||!unresolved.isEmpty())
                    throw new IllegalStateException("Source generation transfer requires settled provider/save consumers");
                transferred=true;
            }
        }
        /**
         * Main 撤销此计划视图前的只读检查，用于阻止该代次仍有活动词法消费者时撤销视图。
         * 它不将保留的区块或 Provider 责任视为已结算，也不改变代次、费用或普通准入状态。
         */
        public void requirePlanViewRevocationCurrent(MainBootProjection.SourceContinuationLease exactLease,
                MainBootProjection.SourceContinuationObservation exactObservation){
            requireMain();Objects.requireNonNull(exactLease);Objects.requireNonNull(exactObservation);
            synchronized(ManagedOrdinarySaveAdmission.this){
                requireHealthy();
                if(closed||!transferred||sourceContinuationGeneration!=this||lease!=exactLease||
                        observation!=exactObservation)
                    throw new IllegalStateException("Exact transferred source generation/lease/observation required before plan-view revocation");
                if(activeProviderScopes!=0||PRODUCER.get()!=null||MAIN_FLUSH.get()!=null)
                    throw new IllegalStateException("Plan-view revocation overlaps an active provider, producer, or Main flush consumer");
            }
        }
        /** 仅真实 Provider 生命周期 successor/terminal owner 核验其精确 owner-issued 证据后可结算；只读 plan drop ACK 不结算，UNKNOWN 保留本 token。 */
        @Override public void close()throws Exception{
            requireMain();AutoCloseable release;
            synchronized(ManagedOrdinarySaveAdmission.this){
                if(closed||sourceContinuationGeneration!=this)throw new IllegalStateException("Exact live source barrier required for settlement");
                if(transferred)throw new IllegalStateException("Transferred source generation requires owner-issued provider lifecycle successor/terminal evidence; plan-drop ACK cannot release it");
                if(activeProviderScopes!=0||outstanding!=0||activeFlights!=0||pending!=null||!queued.isEmpty()||
                        !liveTokens.isEmpty()||!unresolved.isEmpty()||PRODUCER.get()!=null||MAIN_FLUSH.get()!=null)
                    throw new IllegalStateException("Source barrier cannot settle while ordinary save responsibility remains active");
                if(incomplete)throw new IllegalStateException("Source barrier retains held/deferred provider responsibility");
                sourceContinuationGeneration=null;
                lease=null;observation=null;release=charge;charge=null;closed=true;
                notifyAll();
            }
            try{release.close();}
            catch(Throwable cause){providerDecisionFailed(cause);throwCloseFailure(cause);}
        }
    }
    public ProviderScope beginProviderScope(Object exactProvider,Object exactWorld)throws Exception{
        requireMain();requireHealthy();Objects.requireNonNull(exactProvider);Objects.requireNonNull(exactWorld);
        if(!(exactProvider instanceof ChunkProviderServer provider)||!(exactWorld instanceof WorldServer world)||
                provider.world!=world||world.getChunkProvider()!=provider)
            throw new IllegalStateException("Exact installed ChunkProviderServer/WorldServer required");
        SourceScopedBarrier generation;
        synchronized(this){
            requireHealthy();generation=sourceContinuationGeneration;
            if(ordinarySavesSealed)throw new IllegalStateException("Ordinary save admission has ended");
            if(generation==null?ordinaryProviderPolicy==null:generation.closed)
                throw new IllegalStateException("Original ordinary policy or active source continuation required for provider scope");
        }
        AutoCloseable charge=generation!=null?generation.retainProviderScopeCharge():
                ordinaryProviderPolicy.retainTemporary(256L,"ordinary provider scope and charge holder BEFORE allocation");
        Throwable primary=null;ProviderScope scope=null;
        try{scope=new ProviderScope(provider,world,generation,charge);}
        catch(Throwable cause){primary=cause;}
        if(primary!=null){
            try{charge.close();}catch(Throwable cleanup){suppress(primary,cleanup);}
            providerDecisionFailed(primary);throwCloseFailure(primary);
        }
        return scope;
    }
    /** Finds only a lexical token for this exact admission/provider/world on the current Main stack. */
    public static ProviderScope currentProviderScope(ManagedOrdinarySaveAdmission exactAdmission,
            Object exactProvider,Object exactWorld){
        Objects.requireNonNull(exactAdmission);Objects.requireNonNull(exactProvider);Objects.requireNonNull(exactWorld);
        if(Thread.currentThread()!=exactAdmission.main)return null;
        for(ProviderScope scope=PROVIDER_SCOPE.get();scope!=null;scope=scope.previous)
            if(!scope.closed&&scope.actor==Thread.currentThread()&&scope.ownerIs(exactAdmission)&&
                    scope.provider==exactProvider&&scope.world==exactWorld)return scope;
        return null;
    }
    private boolean providerScopeRequired(){
        synchronized(this){return ordinaryProviderPolicy!=null||retired||stopped||pending!=null||sourceContinuationGeneration!=null;}
    }
    /** Managed Anvil calls without this exact provider token may not cross retirement or a source barrier. */
    public boolean requiresProviderScope(){requireMain();requireHealthy();return providerScopeRequired();}
    /**
     * Installs one private continuation generation over the exact registered source lease. An
     * existing Main save/provider scope makes this a nonblocking defer (null); callers may retry
     * after that lexical scope returns. Generic retirement/stopped/failure is never reset here.
     */
    public SourceScopedBarrier tryBeginSourceScopedBarrier(
            MainBootProjection.SourceContinuationLease exactRegisteredLease,MinecraftServer exactServer,
            MainBootProjection.SourceContinuationObservation exactSourceObservation)throws Throwable{
        requireMain();Objects.requireNonNull(exactRegisteredLease);Objects.requireNonNull(exactServer);
        Objects.requireNonNull(exactSourceObservation);
        SourceScopedBarrier existing=null;
        synchronized(this){
            requireHealthy();
            if(stopped)throw new IllegalStateException("Stopped save admission cannot begin source continuation");
            if(activeProviderScopes!=0||outstanding!=0||activeFlights!=0||pending!=null||!queued.isEmpty()||
                    !liveTokens.isEmpty()||!unresolved.isEmpty()||mainFlushOwnedByThisAdmission()||producerOwnedByThisAdmission())return null;
            if(sourceContinuationGeneration!=null){
                if(sourceContinuationGeneration.lease==exactRegisteredLease&&
                        sourceContinuationGeneration.observation==exactSourceObservation)existing=sourceContinuationGeneration;
                else throw new IllegalStateException("Original source continuation generation remains unresolved");
            }
            if(existing==null&&(finalGeneration!=null||finalSealing||freshSourceBoundary!=null))
                throw new IllegalStateException("Final or completed fresh source generation cannot be replaced by continuation");
        }
        exactLeaseActionCheck(exactRegisteredLease,exactServer,exactSourceObservation); // Callback/freshness checks run outside the Admission monitor.
        if(existing!=null){
            synchronized(this){
                requireHealthy();
                if(sourceContinuationGeneration!=existing||existing.closed||existing.lease!=exactRegisteredLease||
                        existing.observation!=exactSourceObservation)
                    throw new IllegalStateException("Original source continuation generation changed during action recheck");
            }
            return existing;
        }
        AutoCloseable charge=exactRegisteredLease.retainAdmissionCharge(exactSourceObservation,256L,
                "source continuation barrier+charge holder BEFORE allocation");
        SourceScopedBarrier installed=null;boolean retainCharge=false;boolean retryLater=false;Throwable primary=null;
        try{
            synchronized(this){
                requireHealthy();
                if(stopped||activeProviderScopes!=0||outstanding!=0||activeFlights!=0||pending!=null||!queued.isEmpty()||
                        !liveTokens.isEmpty()||!unresolved.isEmpty()||mainFlushOwnedByThisAdmission()||producerOwnedByThisAdmission()){
                    retryLater=true;
                }else if(sourceContinuationGeneration!=null){
                    if(sourceContinuationGeneration.lease==exactRegisteredLease&&
                            sourceContinuationGeneration.observation==exactSourceObservation)installed=sourceContinuationGeneration;
                    else throw new IllegalStateException("Original source continuation generation raced");
                }else if(finalGeneration!=null||finalSealing||freshSourceBoundary!=null){
                    throw new IllegalStateException("Final or completed fresh source generation raced with continuation");
                }else{
                    long next=Math.incrementExact(nextSourceContinuationGeneration);
                    installed=new SourceScopedBarrier(exactRegisteredLease,exactSourceObservation,charge,next);
                    sourceContinuationGeneration=installed;nextSourceContinuationGeneration=next;retainCharge=true;
                }
            }
        }catch(Throwable cause){primary=cause;}
        if(!retainCharge){
            try{charge.close();}catch(Throwable cleanup){
                if(primary==null)primary=cleanup;else suppress(primary,cleanup);
            }
        }
        if(primary!=null){providerDecisionFailed(primary);throw primary;}
        return retryLater?null:installed;
    }
    private static void exactLeaseActionCheck(MainBootProjection.SourceContinuationLease exactLease,
            MinecraftServer exactServer,MainBootProjection.SourceContinuationObservation exactObservation){
        exactLease.requireActionCurrent(exactObservation);
        // requireActionCurrent verifies the registered callbacks route and exact source observation.
        if(exactServer.getServerThread()!=Thread.currentThread())throw new IllegalStateException("Exact source continuation Main required");
    }
    private boolean mainFlushOwnedByThisAdmission(){MainFlush scope=MAIN_FLUSH.get();return scope!=null&&scope.owner()==this;}
    private boolean producerOwnedByThisAdmission(){Producer producer=PRODUCER.get();return producer!=null&&producer.token.owner==this;}
    private ProviderDecision ownerDecision(Object provider,Object world,Object chunk,ProviderOperation operation,
            SourceScopedBarrier generation)throws Throwable{
        if(restrictProviderDecision(ProviderDecision.ALLOW,generation)!=ProviderDecision.ALLOW)
            return ProviderDecision.DEFER;
        if(generation!=null)return generation.decide(provider,world,chunk,operation);
        if(ordinaryProviderPolicy==null)throw new IllegalStateException("Original ordinary provider policy required");
        return Objects.requireNonNull(ordinaryProviderPolicy.decide((ChunkProviderServer)provider,
                (WorldServer)world,(Chunk)chunk,operation),"original ordinary provider decision");
    }
    /** 首次判断及晚期覆核共用同一owner门；DEFER保留chunk，不能等producer分配才拒绝。 */
    synchronized ProviderDecision restrictProviderDecision(ProviderDecision selected,SourceScopedBarrier generation){
        requireMain();requireHealthy();Objects.requireNonNull(selected);
        if(sourceContinuationGeneration!=generation)return ProviderDecision.DEFER;
        if(selected==ProviderDecision.ALLOW&&(stopped||pending!=null||
                (finalSealing&&finalGeneration==null)||(retired&&generation==null)))return ProviderDecision.DEFER;
        return selected;
    }
    private void providerDecisionFailed(Throwable cause){
        Objects.requireNonNull(cause);synchronized(this){unknown(cause);}
    }
    private static ProviderDecision restrictive(ProviderDecision left,ProviderDecision right){
        if(left==ProviderDecision.DEFER||right==ProviderDecision.DEFER)return ProviderDecision.DEFER;
        if(left==ProviderDecision.HELD||right==ProviderDecision.HELD)return ProviderDecision.HELD;
        return ProviderDecision.ALLOW;
    }
    public synchronized boolean providerResponsibilityIncomplete(){requireMain();return providerFlushIncomplete||
            activeProviderScopes!=0||sourceContinuationGeneration!=null;}
    /** 先封新普通 producer；旧 queue/Flight 继续按原路径排空。 */
    public synchronized void beginFinalSealing(){
        requireMain();requireHealthy();
        if(stopped||finalSealing||freshSourceBoundary!=null)throw new IllegalStateException("Original Ordinary final sealing is one-shot");
        finalSealing=true;
    }
    public synchronized boolean isFinalActive(){return finalGeneration!=null;}
    /** 原final source独立于已撤Main DATA/SQL root；实际词法费用仍受同一有限B约束。 */
    public synchronized AutoCloseable retainFinalProviderTemporary(long bytes,String purpose){
        requireMain();requireHealthy();
        if(stopped||finalGeneration==null||finalGeneration.consumed||bytes<=0||purpose==null||purpose.isEmpty())
            throw new IllegalStateException("Exact active final source temporary charge required");
        long amount=Math.addExact(bytes,48L); // charge holder先计费，随后分配。
        FinalSaveGeneration generation=finalGeneration;
        reserveFinalLive(amount);
        try{return new AutoCloseable(){
            private boolean closed;
            @Override public void close(){
                synchronized(ManagedOrdinarySaveAdmission.this){
                    requireMain();
                    if(closed||finalGeneration!=generation||generation.consumed||finalRetainedCharge<amount)
                        throw new IllegalStateException("Original final source temporary charge settlement required");
                    finalRetainedCharge-=amount;closed=true;
                }
            }
        };}catch(Throwable cause){finalRetainedCharge-=amount;throw cause;}
    }
    public void requireFinalMainSave(Object world,int x,int z,Object chunk)throws Throwable{
        FinalSaveGeneration generation;
        synchronized(this){requireMain();requireHealthy();generation=finalGeneration;if(generation==null)throw new IllegalStateException("Final source not active");}
        generation.source.requireMainSave(world,x,z,chunk);
    }
    /** 最大B只沿原logical bookkeeping；不足整个B是明确容量失败，非无限pending。 */
    public synchronized SourceEscrow tryReserveFinalSource(Object sourceOwner,long amount){
        requireMain();requireHealthy();Objects.requireNonNull(sourceOwner);
        if(!finalSealing||stopped||finalEscrow!=null||amount<=0)throw new IllegalStateException("Exact one-shot final source escrow required");
        long budget=Math.multiplyExact((long)maximumTokens,512L);
        if(amount>budget)throw new IllegalStateException("Final source minimum exceeds original save bookkeeping budget");
        // 不与旧词法图重叠收费：token、空Flight、MainFlush和Producer全部真实退栈后才准入。
        // 同锁后的executeLoader不再建立旧Flight；旧scheduler entry仍由原List退休协议移除。
        if(outstanding!=0||activeFlights!=0||pending!=null||PRODUCER.get()!=null||MAIN_FLUSH.get()!=null)return null;
        // escrow本体及后续固定图已包含在调用方逐项amount，先记费再new。
        finalRetainedCharge=amount;
        try{finalEscrow=new SourceEscrow(this,sourceOwner,amount);return finalEscrow;}
        catch(Throwable cause){unknown(cause);throw cause;}
    }
    public synchronized boolean tryGrowFinalSourceEscrow(SourceEscrow escrow,Object sourceOwner,long amount,long minimumLiveWorkspace){
        requireMain();requireHealthy();if(stopped)throw new IllegalStateException("Final source preparation stopped");requireEscrow(escrow,sourceOwner);
        if(amount<escrow.amount||minimumLiveWorkspace<0)throw new IllegalArgumentException("Monotonic source escrow required");
        long budget=Math.multiplyExact((long)maximumTokens,512L);
        long retained=Math.addExact(amount,finalMutationCharge);
        if(Math.addExact(retained,minimumLiveWorkspace)>budget)
            throw new IllegalStateException("Final source minimum exceeds entire original save bookkeeping budget");
        if(outstanding!=0||activeFlights!=0||pending!=null||PRODUCER.get()!=null||MAIN_FLUSH.get()!=null)
            throw new IllegalStateException("Old lexical responsibility entered after source escrow");
        finalRetainedCharge=retained;escrow.amount=amount;return true;
    }
    /** 调用方已按实际scheduler List/queue guard验证所有loader；此处仅原子封旧Flight入口。 */
    public synchronized boolean tryFenceFinalOldGeneration(SourceEscrow escrow,Object sourceOwner){
        requireMain();requireHealthy();if(stopped)throw new IllegalStateException("Final source preparation stopped");requireEscrow(escrow,sourceOwner);
        if(oldGenerationFenced)return true;
        // 只有安装了原successor policy的owner持有此待交接责任；旧owner没有该注册机制。
        if((ordinaryProviderPolicy!=null&&retired)||activeFlights!=0||!liveTokens.isEmpty()||pending!=null||PRODUCER.get()!=null||MAIN_FLUSH.get()!=null)return false;
        if(outstanding!=0||!queued.isEmpty()||!unresolved.isEmpty()||!quarantined.isEmpty()||quarantineRecordingIncomplete)
            throw new IllegalStateException("Old save generation contains unresolved responsibilities");
        oldGenerationFenced=true;return true;
    }
    public void activateFinalSource(ManagedBootSaveScope actualScope,SourceEscrow escrow,Object sourceOwner,FinalSaveValidator source){
        requireMain();Objects.requireNonNull(actualScope).requireFinalSourceActivation(this,escrow,sourceOwner,source);
        synchronized(this){
            requireHealthy();if(stopped)throw new IllegalStateException("Final source preparation stopped");requireEscrow(escrow,sourceOwner);Objects.requireNonNull(source);
            if(!oldGenerationFenced||finalGeneration!=null||activeFlights!=0||outstanding!=0)
                throw new IllegalStateException("Original fenced save generation required before activation");
            finalGeneration=new FinalSaveGeneration(this,source,Math.incrementExact(nextGeneration));
            escrow.consumed=true;escrow.sourceOwner=actualScope; // 原publication退出，由同槽保管实际Scope到map真实退役。
        }
    }
    private void requireEscrow(SourceEscrow escrow,Object sourceOwner){
        if(escrow==null||escrow!=finalEscrow||escrow.owner!=this||escrow.sourceOwner!=sourceOwner||escrow.consumed)
            throw new IllegalStateException("Original unconsumed source escrow required");
    }
    public Producer finalProducer(Object loader,int x,int z,Object actualWorld,Object actualChunk,Recheck physicalRecheck){
        Token token;
        synchronized(this){
            requireMain();requireHealthy();
            if(finalGeneration==null||stopped)throw new IllegalStateException("Exact active final source required");
            if(!(actualChunk instanceof Chunk chunk)||chunk.getWorld()!=actualWorld||chunk.x!=x||chunk.z!=z)
                throw new IllegalStateException("Original actual final chunk/world/coordinates required");
            if(outstanding>=maximumTokens)throw new IllegalStateException("Final save token capacity exhausted");
            reserveFinalLive(512L+56L);
            try{token=new Token(finalGeneration);token.loader=Objects.requireNonNull(loader);token.x=x;token.z=z;token.attempt=chunk;
                liveTokens.add(token);outstanding++;return new Producer(token,Objects.requireNonNull(physicalRecheck));}
            catch(Throwable cause){unknown(cause);throw cause;}
        }
    }
    /** 只封新Main producer；原排队FileIO仍由finalGeneration完成真实退尾。 */
    public void beginFinalGenerationEnd(ManagedBootSaveScope scope){
        requireMain();Objects.requireNonNull(scope).requireFinalGenerationEnd(this);
        synchronized(this){
            requireHealthy();
            if(stopped||!finalSealing||finalGeneration==null||finalGeneration.consumed)
                throw new IllegalStateException("Original active final generation ends once");
            stopped=true;
        }
    }
    /** 实际scheduler List锁内、各loader queue核空后提交；不在锁内调用Scope/Callbacks。 */
    public synchronized void endFinalGenerationSource(ManagedFileIoOwnerAccess scheduler){
        requireMain();requireHealthy();
        if(scheduler!=(Object)net.minecraft.world.storage.ThreadedFileIOBase.getThreadedIOInstance())
            throw new IllegalStateException("Actual original scheduler required");
        scheduler.floatingpod$requireFinalEndLock();
        if(!stopped||!finalSealing||!oldGenerationFenced||finalGeneration==null||finalGeneration.consumed||
                finalEscrow==null||!finalEscrow.consumed||activeFlights!=0||outstanding!=0||pending!=null||
                !liveTokens.isEmpty()||!queued.isEmpty()||!unresolved.isEmpty()||!quarantined.isEmpty()||
                quarantineRecordingIncomplete||PRODUCER.get()!=null||FLIGHT.get()!=null||MAIN_FLUSH.get()!=null)
            throw new IllegalStateException("Actual final save generation retains responsibility");
        long settled=Math.addExact(finalEscrow.amount,finalMutationCharge);
        if(finalRetainedCharge!=settled)throw new IllegalStateException("Final lexical/source bookkeeping does not balance");
        // 所有可能失败的算术及资格先完成；source只撤私有数组，不访问游戏/外部consumer。
        finalGeneration.source.endMainConsumers();
        finalGeneration.source=null;finalGeneration.consumed=true;
        // observer原字段在本锁外撤销前保整笔账，失败不退款、不重放。
    }
    public void completeFinalGenerationEnd(ManagedBootSaveScope scope){
        requireMain();Objects.requireNonNull(scope).requireFinalGenerationEnd(this);
        synchronized(this){
            requireFinalGenerationCommitIdentity(scope);
            scope.retireFinalEnrolledLoaders(this);
            // map已真实撤销；此后仅原槽plain提交，不再调用外部资格或算术。
            finalEscrow.sourceOwner=null;
            finalGeneration=null;finalEscrow=null;finalMutationCharge=0L;finalRetainedCharge=0L;
        }
    }
    /** 仅固定Scope提交边可用；不取得Scope/HOLDERS/Runtime或游戏锁。 */
    public void requireFinalGenerationCommitIdentity(ManagedBootSaveScope scope){
        if(!Thread.holdsLock(this))throw new IllegalStateException("Original Admission monitor required");
        requireMain();requireHealthy();
        if(scope==null||!stopped||!finalSealing||!oldGenerationFenced||finalGeneration==null||
                finalGeneration.owner!=this||!finalGeneration.consumed||finalGeneration.source!=null||
                finalEscrow==null||finalEscrow.owner!=this||!finalEscrow.consumed||finalEscrow.sourceOwner!=scope||
                activeFlights!=0||outstanding!=0||pending!=null||!liveTokens.isEmpty()||!queued.isEmpty()||
                !unresolved.isEmpty()||!quarantined.isEmpty()||quarantineRecordingIncomplete||
                PRODUCER.get()!=null||FLIGHT.get()!=null||MAIN_FLUSH.get()!=null)
            throw new IllegalStateException("Actual final source consumer end required");
        long settled=Math.addExact(finalEscrow.amount,finalMutationCharge);
        if(finalRetainedCharge!=settled)throw new IllegalStateException("Final lexical/source bookkeeping does not balance");
    }
    private void reserveFinalLive(long amount){
        long next=Math.addExact(finalRetainedCharge,amount);
        if(next>Math.multiplyExact((long)maximumTokens,512L))throw new IllegalStateException("Final save bookkeeping capacity exhausted");
        finalRetainedCharge=next;
    }
    /** 原 boot 保存代的真实 token 已结算；不证明后续 vanilla 保存成功或 native close。 */
    private sealed interface SourceGeneration permits FreshSourceBoundary, FinalSaveGeneration, SourceScopedBarrier { }
    public final class FreshSourceBoundary implements SourceGeneration {
        private FreshSourceBoundary() { }
        public void requireOriginal(ManagedOrdinarySaveAdmission original) {
            synchronized (ManagedOrdinarySaveAdmission.this) {
                requireMain();requireHealthy();
                if (original != ManagedOrdinarySaveAdmission.this || freshSourceBoundary != this)
                    throw new IllegalArgumentException("Original Fresh source-save boundary required");
            }
        }
    }
    /** Main 非阻塞消费原 token ledger；尚有实际 producer/flight 时等待下一真实调度。 */
    public synchronized FreshSourceBoundary tryFinishFreshSourceGeneration() {
        requireMain();requireOpen();
        if (freshSourceBoundary != null) throw new IllegalStateException("Fresh source-save boundary is one-shot");
        Producer producer=PRODUCER.get();
        if (producer != null || MAIN_FLUSH.get() != null || activeFlights != 0 || !liveTokens.isEmpty()) return null;
        if (outstanding != 0 || !queued.isEmpty() || !unresolved.isEmpty())
            throw new IllegalStateException("Original save token ledger differs from its physical queue responsibility");
        freshSourceBoundary=new FreshSourceBoundary();
        return freshSourceBoundary;
    }
    public ManagedOrdinarySaveAdmission(Thread exactMain,int maximumTokens){
        this(exactMain,maximumTokens,null);
    }
    public ManagedOrdinarySaveAdmission(Thread exactMain,int maximumTokens,OrdinaryProviderPolicy policy){
        ordinaryProviderPolicy=policy;
        main=Objects.requireNonNull(exactMain);
        if(maximumTokens<1)throw new IllegalArgumentException("Positive token capacity required");
        this.maximumTokens=maximumTokens;
    }
    /** 仅在真正的 typed current/Unprotected 验证之后调用；每次实际 put 前还须重核。 */
    public synchronized Token candidate(){
        requireMain();requireOpen();
        if(finalSealing)throw new IllegalStateException("Ordinary producer admission sealed for final source");
        if(outstanding==maximumTokens)throw new IllegalStateException("Save token capacity exhausted");
        Token token=new Token(freshSourceBoundary);liveTokens.add(token);outstanding++;return token;
    }
    /** 单一ordinary代次尚未交接时，由真实submit入口按既有容量拒绝返回，不能再推进epoch。 */
    public synchronized boolean awaitingOrdinarySuccessor(){requireMain();requireHealthy();return ordinaryProviderPolicy!=null&&retired;}
    /** 可预期的代际暂拒；不得把它写入callback/backing的永久失败槽。 */
    public static final class MainReadPending extends IllegalStateException {
        private MainReadPending(){super("Original Main read generation awaits confirmed successor");}
    }
    /** 在借用DATA之前检查，避免用旧epoch的读取失败污染原读责任。 */
    public synchronized void requireMainReadAvailable(){
        requireMain();requireHealthy();
        if(ordinaryProviderPolicy==null||stopped)throw new IllegalStateException("Original live ordinary read admission required");
        if(retired||pending!=null)throw new MainReadPending();
    }
    /** 在调用 io.submit 前关闭新资格；本方法绝不调用 submit 或等待。 */
    public synchronized Mutation reserveMutation(){
        requireMain();requireHealthy();
        if(ordinarySavesSealed)throw new IllegalStateException("Ordinary save admission has ended");
        if(ordinaryProviderPolicy!=null&&retired)throw new IllegalStateException("Original ordinary successor remains pending");
        if(freshSourceBoundary!=null||oldGenerationFenced)throw new IllegalStateException("Source generation is closed to repository mutations");
        if(pending!=null)throw new IllegalStateException("Mutation submission already reserved");
        if(finalEscrow!=null){
            // Mutation32与进入Work的Some24在new前付款；submitted/await返回均不能证明Work撤引用。
            long next=Math.addExact(finalMutationCharge,56L);
            reserveFinalLive(56L);finalMutationCharge=next;
            try{Mutation result=new Mutation();pending=result;return result;}
            catch(Throwable cause){unknown(cause);throw cause;}
        }
        Mutation result=new Mutation();pending=result;return result;
    }
    /** 必须在 router/issuer 后处理前报告真实 io.submit Accepted。 */
    public synchronized void submitted(Mutation mutation,boolean accepted){
        requireMain();requirePending(mutation);
        mutation.accepted=accepted;mutation.resolved=true;pending=null;
        if(accepted)retired=true;
        notifyAll();
    }
    /** 仅原policy的真实confirmed/current successor可恢复；任何旧保存责任仍在则明确拒绝。
     * final sealing只封普通producer，停服原mutation仍须交接；最终fence之后不可再恢复。 */
    public void resumeConfirmedSuccessor(OrdinaryProviderPolicy policy) {
        requireMain();
        synchronized(this){requireHealthy();requireSuccessorDrain(policy);}
        policy.requireConfirmedSuccessor();
        synchronized(this){requireHealthy();requireSuccessorDrain(policy);retired=false;notifyAll();}
    }
    private void requireSuccessorDrain(OrdinaryProviderPolicy policy) {
        if(policy==null||policy!=ordinaryProviderPolicy||!retired||stopped||pending!=null||
                outstanding!=0||activeFlights!=0||activeProviderScopes!=0||!queued.isEmpty()||
                !liveTokens.isEmpty()||!unresolved.isEmpty()||producerOwnedByThisAdmission()||mainFlushOwnedByThisAdmission()||
                sourceContinuationGeneration!=null||freshSourceBoundary!=null||oldGenerationFenced||finalGeneration!=null)
            throw new IllegalStateException("Original retired ordinary generation must be drained before confirmed successor");
    }
    /** submit 抛异常时无法证明未接管；保留首因并显式 UNKNOWN，不能重新开放。 */
    public synchronized void submissionUnknown(Mutation mutation,Throwable cause){
        requireMain();requirePending(mutation);mutation.resolved=true;pending=null;unknown(cause);
    }
    /** 原 map.put 前调用；在 queue guard 内重核成功后绑定，实际 put 失败必须 unknown。 */
    public synchronized void beforePut(Token token,Object attempt,Object payload){
        requireMain();owned(token,Phase.CANDIDATE);
        if(token.sourceGeneration instanceof FinalSaveGeneration generation){
            requireHealthy();if(generation!=finalGeneration||token.payload!=payload)throw new IllegalStateException("Original final payload binding required");
        }else if(token.sourceGeneration instanceof SourceScopedBarrier generation){
            requireHealthy();
            ProviderScope scope=currentProviderScopeForSave(token.loader,token.x,token.z);
            if(generation!=sourceContinuationGeneration||generation.closed||stopped||pending!=null||finalSealing||
                    token.payload!=payload||scope==null||scope.sourceGeneration!=generation||
                    !scope.isCurrentSourceSave(token.loader,token.x,token.z,generation))
                throw new IllegalStateException("Exact source-scoped provider decision required before queue Map.put");
        }else requireOpen();
        token.attempt=Objects.requireNonNull(attempt);token.payload=Objects.requireNonNull(payload);
        token.phase=Phase.PUTTING;

    }
    /** 原 map.put 返回后，以 ledger 已核实的 previous token 收束真实 replacement。 */
    public synchronized void putReturned(Token token,long actualGeneration,Token previous){
        requireMain();owned(token,Phase.PUTTING);
        if(token.attempt==null)throw new IllegalStateException("Missing exact put binding");
        if(previous!=null){owned(previous,Phase.QUEUED);complete(previous);}
        if(actualGeneration<=0)throw new IllegalArgumentException("Actual positive generation required");
        token.generation=actualGeneration;token.phase=Phase.QUEUED;
    }
    public synchronized void noPut(Token token){requireMain();owned(token,Phase.CANDIDATE);complete(token);}
    public synchronized void removed(Token token,Object attempt,long generation,Object payload){
        owned(token,Phase.QUEUED);
        if(isMain()&&!mainFlushAllows(token.loader))throw new IllegalStateException("Main IO requires original exact loader.flush scope");
        if(token.attempt!=attempt||token.generation!=generation||token.payload!=payload)
            throw new IllegalStateException("Exact accepted attempt/generation/payload required");
        token.flightActor=Thread.currentThread();token.phase=Phase.FLIGHT;
    }
    /** 退休不撤销已接管物理写；仅真实 flight 的完整返回才能解除 SQL barrier。 */
    public synchronized void flightReturned(Token token){
        if(isMain()&&!mainFlushAllows(token.loader))throw new IllegalStateException("Main IO requires original exact loader.flush scope");
        owned(token,Phase.FLIGHT);
        if(token.flightActor!=Thread.currentThread())throw new IllegalStateException("Exact flight IO owner required");
        complete(token);
    }
    public synchronized void failed(Token token,Throwable cause){
        if(token==null||token.owner!=this)throw new IllegalArgumentException("Foreign save token");
        if(token.phase==Phase.COMPLETE)throw new IllegalStateException("Completed save cannot acquire failure");
        token.phase=Phase.UNKNOWN;unresolved.add(token);unknown(cause);
    }
    public synchronized void stop(Throwable cause){requireMain();stopped=true;if(cause!=null)unknown(cause);else notifyAll();}
    /** 调用者先在真实scheduler List锁内核所有loader；本方法只原子封本admission的空责任图。 */
    public synchronized boolean trySealOrdinaryAfterDrain(){
        requireMain();requireHealthy();
        if(ordinarySavesSealed)return true;
        if(ordinaryProviderPolicy==null||stopped||finalSealing||finalGeneration!=null||finalEscrow!=null||
                oldGenerationFenced||freshSourceBoundary!=null||sourceContinuationGeneration!=null)
            throw new IllegalStateException("Original ordinary save generation required for terminal drain");
        if(retired||pending!=null||activeFlights!=0||activeProviderScopes!=0||outstanding!=0||
                !liveTokens.isEmpty()||!queued.isEmpty()||producerOwnedByThisAdmission()||mainFlushOwnedByThisAdmission())return false;
        if(!unresolved.isEmpty()||!quarantined.isEmpty()||quarantineRecordingIncomplete||providerFlushIncomplete)
            throw new IllegalStateException("Ordinary save generation retains unresolved responsibility");
        ordinarySavesSealed=true;stopped=true;notifyAll();return true;
    }
    /** 原 SaveHandler.flush 完成后的失败观察；不颁发新的保存资格或清空责任。 */
    public synchronized void requireHealthyAfterFlush(){
        requireMain();requireHealthy();
        if(providerFlushIncomplete||activeProviderScopes!=0||sourceContinuationGeneration!=null)
            throw new IllegalStateException("Managed source provider save/unload responsibility remains incomplete");
    }
    public synchronized int outstanding(){return outstanding;}
    private void complete(Token token){
        if(!liveTokens.remove(token))throw new IllegalStateException("Original live save token required for completion");
        if(token.sourceGeneration instanceof FinalSaveGeneration){
            long completedCharge=Math.addExact(512L,token.finalKeySetCharge);
            token.finalKeySetCharge=0L;token.payload=null;token.loader=null;token.attempt=null;token.flightActor=null;
            finalRetainedCharge=Math.subtractExact(finalRetainedCharge,completedCharge);
        }else if(token.sourceContinuationToken){
            token.sourceGeneration=null;token.payload=null;token.loader=null;token.attempt=null;token.flightActor=null;
        }
        token.phase=Phase.COMPLETE;outstanding--;notifyAll();
    }
    private void unknown(Throwable cause){
        Objects.requireNonNull(cause);
        if(failure==null)failure=cause;
        else if(failure!=cause && !reaches(failure,cause) && !reaches(cause,failure))failure.addSuppressed(cause);
        retired=true;notifyAll();
    }
    private static boolean reaches(Throwable root,Throwable target){
        var seen=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable,Boolean>());
        var queue=new java.util.ArrayDeque<Throwable>();queue.add(root);
        while(!queue.isEmpty()){
            Throwable next=queue.remove();if(next==target)return true;if(!seen.add(next))continue;
            if(next.getCause()!=null)queue.add(next.getCause());for(Throwable child:next.getSuppressed())queue.add(child);
        }
        return false;
    }
    private static void throwCloseFailure(Throwable cause)throws Exception{
        if(cause instanceof Exception exception)throw exception;
        if(cause instanceof Error error)throw error;
        throw new IllegalStateException("Provider responsibility cleanup failed",cause);
    }
    private void requireHealthy(){if(failure!=null)throw new IllegalStateException("Save responsibility UNKNOWN; repository mutation denied",failure);}
    private void requireOpen(){requireHealthy();if(retired||stopped||pending!=null)throw new IllegalStateException("Save admission retired, stopped or reserved");}
    private void requireMain(){if(Thread.currentThread()!=main)throw new IllegalStateException("Exact Main required");}
    private void requirePending(Mutation mutation){if(mutation==null||mutation!=pending||mutation.resolved)throw new IllegalArgumentException("Exact pending mutation required");}
    private void owned(Token token,Phase phase){if(token==null||token.owner!=this||token.phase!=phase)throw new IllegalStateException("Exact active save phase required");}
}

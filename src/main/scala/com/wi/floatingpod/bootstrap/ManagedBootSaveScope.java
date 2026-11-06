package com.wi.floatingpod.bootstrap;

import java.nio.file.Path;
import java.util.IdentityHashMap;
import java.util.Objects;
import com.wi.floatingpod.server.NormalRecallRecoverySourceCustody;
import java.util.UUID;
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission;
import com.wi.floatingpod.server.save.ManagedFileIoOwnerAccess;
import net.minecraft.world.storage.ThreadedFileIOBase;

/**
 * 保留 pre-Anvil 启动期保存责任；没有全局 Open，仅接受逐次 typed admission。
 * 构造器仅包内可见；生产签发点是精确 Main 成功 claim 后的
 * ProductionBootstrapInstallation。
 */
public final class ManagedBootSaveScope {
    @FunctionalInterface
    interface SaveCall extends ManagedOrdinarySaveAdmission.Recheck { void call() throws Throwable; default void run() throws Throwable {call();} }

    static final class AdmissionException extends IllegalStateException {
        private final UUID bootId;
        private final long bootGeneration;
        private final long connectionGeneration;
        private final UUID operationId;
        private final String stage;

        private AdmissionException(UUID bootId,long bootGeneration,long connectionGeneration,
                                   UUID operationId,String stage,String reason,Throwable cause) {
            super("Managed boot save denied stage="+stage+" reason="+reason+
                " boot="+bootId+" bootGeneration="+bootGeneration+
                " connectionGeneration="+connectionGeneration+" operation="+operationId,cause);
            this.bootId=bootId;
            this.bootGeneration=bootGeneration;
            this.connectionGeneration=connectionGeneration;
            this.operationId=operationId;
            this.stage=stage;
        }
        UUID bootId(){return bootId;}
        long bootGeneration(){return bootGeneration;}
        long connectionGeneration(){return connectionGeneration;}
        UUID operationId(){return operationId;}
        String stage(){return stage;}
    }

    private final Object actualServer;
    private final Object exactControl;
    private final Object exactCallbacks;
    private final Thread exactMain;
    private final Path worldRoot;
    private final UUID bootId;
    private final long bootGeneration;
    private final long connectionGeneration;
    private final UUID operationId;
    private IdentityHashMap<Object,LoaderRegistration> enrolledLoaders=new IdentityHashMap<>();
    private ManagedSourcePathCensus sourceCensus;
    private Throwable sourceCensusFailure;

    private static final class LoaderRegistration {
        final Path directory;
        final ManagedSourcePathCensus.PathBinding sourceBinding;
        boolean providerSaveIncomplete;
        long incompleteRevision;
        int providerSaveDepth;
        Throwable providerSaveFailure;
        LoaderRegistration(Path directory,ManagedSourcePathCensus.PathBinding sourceBinding) {
            this.directory=directory;this.sourceBinding=sourceBinding;
        }
    }

    @FunctionalInterface public interface ProviderSaveCall { boolean call() throws Throwable; }
    /** 原loader登记内保留真实pass结果；完整后续pass只能解除正常暂拒，不能抹掉异常。 */
    boolean invokeProviderSave(Object loader,boolean all,ProviderSaveCall original)throws Throwable {
        requireExactMain("PROVIDER_SAVE_RESULT");
        Objects.requireNonNull(original,"actual provider save call");
        LoaderRegistration registration;
        synchronized(this){
            requireEnrolledLoadersLive("PROVIDER_SAVE_RESULT");
            registration=enrolledLoaders.get(loader);
            if(registration==null)throw failure("PROVIDER_SAVE_RESULT_LOADER");
        }
        long revision=registration.incompleteRevision;
        registration.providerSaveDepth=Math.incrementExact(registration.providerSaveDepth);
        registration.providerSaveIncomplete=true;
        try {
            boolean complete=original.call();
            if(!complete)registration.incompleteRevision=Math.incrementExact(registration.incompleteRevision);
            if(all&&complete&&registration.providerSaveDepth==1&&revision==registration.incompleteRevision&&registration.providerSaveFailure==null)
                registration.providerSaveIncomplete=false;
            return complete;
        }catch(Throwable cause){
            if(registration.providerSaveFailure==null)registration.providerSaveFailure=cause;
            else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(registration.providerSaveFailure,cause);
            throw cause;
        }finally{registration.providerSaveDepth--;}
    }
    /** 只证明原provider保存pass没有遗留；scheduler/token/flight的终端drain仍须独立核对。 */
    synchronized void requireProviderSavesComplete(){
        requireExactMain("PROVIDER_SAVE_RESULT");requireEnrolledLoadersLive("PROVIDER_SAVE_RESULT");
        for(LoaderRegistration registration:enrolledLoaders.values()){
            if(registration.providerSaveFailure!=null)throw failure("PROVIDER_SAVE_FAILED",registration.providerSaveFailure);
            if(registration.providerSaveIncomplete||registration.providerSaveDepth!=0)
                throw failure("PROVIDER_SAVE_INCOMPLETE","Actual provider save requires a complete subsequent full pass");
        }
    }
    /** 只结束原ordinary保存入口；不签发stopServer/Stopped已返回或SQL已关闭证明。 */
    boolean trySealDbFirstOrdinarySaves(ProductionDbFirstBootstrapCallbacks callbacks){
        requireExactMain("ORDINARY_SAVE_DRAIN");requireEnrolledLoadersLive("ORDINARY_SAVE_DRAIN");
        if(callbacks==null||callbacks!=exactCallbacks)throw failure("ORDINARY_SAVE_DRAIN_OWNER");
        requireProviderSavesComplete();
        // Main独占登记/退休；不能跨scheduler List持scope锁，IO在List内会查询原scope登记。
        Object scheduler=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(scheduler instanceof ManagedFileIoOwnerAccess owner))throw failure("ORDINARY_SAVE_DRAIN_SCHEDULER");
        return owner.floatingpod$trySealOrdinarySaves(callbacks.ordinarySaveAdmission(),enrolledLoaders.keySet());
    }

    ManagedBootSaveScope(Object actualServer,Object exactControl,Object exactCallbacks,Thread exactMain,
                         Path worldRoot,UUID bootId,long bootGeneration,long connectionGeneration,UUID operationId) {
        this.actualServer=Objects.requireNonNull(actualServer,"actual dedicated server");
        this.exactControl=Objects.requireNonNull(exactControl,"exact bootstrap control");
        this.exactCallbacks=Objects.requireNonNull(exactCallbacks,"exact callback holder");
        this.exactMain=Objects.requireNonNull(exactMain,"exact original Main");
        this.worldRoot=normalize(Objects.requireNonNull(worldRoot,"pinned world root"));
        this.bootId=Objects.requireNonNull(bootId,"boot id");
        if(bootGeneration<=0 || connectionGeneration<=0)
            throw new IllegalArgumentException("Positive boot and connection generations required");
        this.bootGeneration=bootGeneration;
        this.connectionGeneration=connectionGeneration;
        this.operationId=Objects.requireNonNull(operationId,"original boot operation id");
    }

    Object actualServer(){return actualServer;}
    Object exactControl(){return exactControl;}
    Object exactCallbacks(){return exactCallbacks;}
    Thread exactMain(){return exactMain;}
    Path worldRoot(){return worldRoot;}

    synchronized ManagedSourcePathCensus sourceCensus(){
        if(sourceCensusFailure!=null)throw failure("SOURCE_CENSUS",sourceCensusFailure);
        return sourceCensus;
    }

    synchronized void attachColdSourceCensus(NormalRecallRecoverySourceCustody exactCustody) throws java.io.IOException {
        requireExactMain("SOURCE_CENSUS_BIND");
        if(sourceCensus!=null||sourceCensusFailure!=null)
            throw failure("SOURCE_CENSUS_BIND","Original cold source census binding is one-shot");
        ManagedSourcePathCensus candidate=new ManagedSourcePathCensus(this,exactCustody);
        sourceCensus=candidate;
        try {
            var exactAccount=exactCustody.bindColdSourceCensus(candidate);
            candidate.attachAccount(exactAccount);
        } catch(java.io.IOException|RuntimeException|Error cause) {
            sourceCensusFailure=cause;
            throw cause;
        }
    }

    synchronized ManagedSourcePathCensus.PathBinding registeredLoader(Object loader) {
        requireEnrolledLoadersLive("LOADER_ENROLLMENT");
        LoaderRegistration registration=enrolledLoaders.get(loader);
        return registration==null?null:registration.sourceBinding;
    }

    synchronized Path registeredLoaderDirectory(Object loader) {
        requireEnrolledLoadersLive("LOADER_ENROLLMENT");
        LoaderRegistration registration=enrolledLoaders.get(loader);
        return registration==null?null:registration.directory;
    }

    synchronized Object[] registeredLoadersSnapshotForCensus() {
        requireEnrolledLoadersLive("SOURCE_CENSUS_SNAPSHOT");
        return enrolledLoaders.values().stream().map(value->value.sourceBinding).toArray();
    }

    boolean contains(Path value) {
        Path normalized=checkedPath(value,"PATH_CLASSIFICATION");
        return normalized.equals(worldRoot) || normalized.startsWith(worldRoot);
    }

    synchronized void registerLoader(Object loader,Path directory) {
        requireEnrolledLoadersLive("LOADER_ENROLLMENT");
        Objects.requireNonNull(loader,"actual Anvil loader");
        Path normalized=checkedPath(directory,"LOADER_ENROLLMENT");
        if(!isWithin(normalized))return;
        requireExactMain("LOADER_ENROLLMENT");
        LoaderRegistration existing=enrolledLoaders.get(loader);
        if(existing!=null) {
            if(!existing.directory.equals(normalized))throw failure("LOADER_ENROLLMENT","Exact loader path changed");
            throw failure("LOADER_ENROLLMENT","Exact Anvil loader enrolled more than once");
        }
        ManagedSourcePathCensus.PathBinding binding=sourceCensus==null?null:sourceCensus.registerLoader(loader,normalized);
        enrolledLoaders.put(loader,new LoaderRegistration(normalized,binding));
    }

    /** 精确校验受控服务器路径后，在原 saveChunk 副作用前拒绝；其它路径仅在与所有保留 scope 不相交时委托原实现。 */
    void invoke(Object actualWorldServer,Object actualLoader,Object providerCurrentLoader,
                Path actualWorldRoot,String providerFolder,Path actualLoaderDirectory,
                SaveCall original) throws Throwable {
        invokeOrdinary(actualWorldServer,actualLoader,providerCurrentLoader,actualWorldRoot,providerFolder,
            actualLoaderDirectory,original,null,0,0,null);
    }
    void invokeOrdinary(Object actualWorldServer,Object actualLoader,Object providerCurrentLoader,
                Path actualWorldRoot,String providerFolder,Path actualLoaderDirectory,
                SaveCall original,com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission,
                int chunkX,int chunkZ,SaveCall recheck) throws Throwable {
        invokeOrdinary(actualWorldServer,actualLoader,providerCurrentLoader,actualWorldRoot,providerFolder,actualLoaderDirectory,original,admission,chunkX,chunkZ,recheck,null,null);
    }
    void invokeOrdinary(Object actualWorldServer,Object actualLoader,Object providerCurrentLoader,
                Path actualWorldRoot,String providerFolder,Path actualLoaderDirectory,
                SaveCall original,ManagedOrdinarySaveAdmission admission,int chunkX,int chunkZ,SaveCall recheck,Object finalWorld,Object finalChunk) throws Throwable {
        requireEnrolledLoadersLive("SAVE_CHUNK_ENTRY");
        Objects.requireNonNull(original,"original save body");
        Path normalizedWorldRoot=checkedPath(actualWorldRoot,"WORLD_SAVE_ROOT");
        Path normalizedLoader=checkedPath(actualLoaderDirectory,"LOADER_PATH");
        Path actualProviderRoot=providerPath(normalizedWorldRoot,providerFolder,"PROVIDER_FOLDER");
        if(actualWorldServer!=actualServer) {
            requireUnmanagedDisjoint(actualLoader,normalizedLoader,normalizedWorldRoot,providerFolder);
            original.call();
            return;
        }
        requireExactMain("SAVE_CHUNK_ENTRY");
        if(!normalizedWorldRoot.equals(worldRoot))
            throw failure("WORLD_SAVE_ROOT","Controlled server world root differs from pinned root");
        Path expectedProviderRoot=providerPath(worldRoot,providerFolder,"PROVIDER_FOLDER");
        if(!expectedProviderRoot.equals(actualProviderRoot))
            throw failure("PROVIDER_FOLDER","World provider folder did not resolve consistently");
        if(providerCurrentLoader!=actualLoader)
            throw failure("CURRENT_LOADER","World chunk provider does not retain this exact Anvil loader");
        if(!normalizedLoader.equals(expectedProviderRoot))
            throw failure("LOADER_PATH","Anvil loader path differs from actual world/provider path");
        Path enrolled;
        synchronized(this){requireEnrolledLoadersLive("LOADER_ENROLLMENT");LoaderRegistration registration=enrolledLoaders.get(actualLoader);enrolled=registration==null?null:registration.directory;}
        if(enrolled==null || !enrolled.equals(expectedProviderRoot))
            throw failure("LOADER_ENROLLMENT","Exact Anvil loader was not enrolled beneath the pinned root");
        if(admission==null)throw failure("CLOSED_BOOT_SCOPE","This boot has no physical-save permit");
        boolean finalSave=admission.isFinalActive();
        try{
            if(finalSave){
                Objects.requireNonNull(recheck,"actual final physical/source check").call();
                admission.requireFinalMainSave(finalWorld,chunkX,chunkZ,finalChunk);
            }else Objects.requireNonNull(recheck,"actual current save check").call();
            try(var producer=finalSave?admission.finalProducer(actualLoader,chunkX,chunkZ,
                    finalWorld,finalChunk,recheck):admission.producer(actualLoader,chunkX,chunkZ,recheck::call)) {
                try{original.call();}catch(Throwable cause){producer.failedAfterAdmission(cause);throw cause;}
            }
        }catch(Throwable cause){
            // final工厂、保存体和词法清理同属原loader责任；失败不重开代或假退款。
            if(finalSave){admission.schedulerFailed(actualLoader,cause);admission.associateSchedulerFailure(cause);}
            throw cause;
        }
    }

    /** 原enrolledLoaders由实际Main注册；Main枚举期间无其它注册线程，IO不取得scope锁。 */
    synchronized boolean tryFenceFinalSource(Object callbacks,ManagedOrdinarySaveAdmission admission,
            ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner){
        requireEnrolledLoadersLive("FINAL_SOURCE_FENCE");
        requireExactMain("FINAL_SOURCE_FENCE");
        if(callbacks!=exactCallbacks||admission==null)throw failure("FINAL_SOURCE_OWNER");
        Object actualIo=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(actualIo instanceof ManagedFileIoOwnerAccess owner))throw failure("FINAL_SOURCE_SCHEDULER");
        return owner.floatingpod$tryFenceFinalSource(admission,escrow,sourceOwner,enrolledLoaders.keySet());
    }
    void installFinalSource(Object callbacks,ManagedOrdinarySaveAdmission admission,
            ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner,
            ManagedOrdinarySaveAdmission.FinalSaveValidator source){
        requireExactMain("FINAL_SOURCE_INSTALL");
        if(callbacks!=exactCallbacks||admission==null||source==null)throw failure("FINAL_SOURCE_OWNER");
        source.requireComplete();
        admission.activateFinalSource(this,escrow,sourceOwner,source);
    }

    public void requireFinalGenerationEnd(ManagedOrdinarySaveAdmission admission){
        requireExactMain("FINAL_GENERATION_END");
        ProductionBootstrapInstallation.requireOriginalFinalGenerationEnd(this,admission);
    }
    void endOrdinaryFinalGeneration(Object callbacks,com.wi.floatingpod.network.DedicatedProjectionRuntime runtime){
        requireEnrolledLoadersLive("FINAL_GENERATION_END");
        requireExactMain("FINAL_GENERATION_END");
        if(callbacks!=exactCallbacks||!(callbacks instanceof ProductionDedicatedBootstrapCallbacks actual))
            throw failure("FINAL_GENERATION_OWNER");
        ManagedOrdinarySaveAdmission admission=actual.ordinarySaveAdmission();
        admission.beginFinalGenerationEnd(this);
        Object scheduler=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(scheduler instanceof ManagedFileIoOwnerAccess owner))throw failure("FINAL_GENERATION_SCHEDULER");
        owner.floatingpod$endFinalSource(admission,enrolledLoaders.keySet());
        // 不持List/payload/Admission锁撤原Main observer字段；此时账仍完整保留。
        actual.detachOrdinaryExitObservation(runtime);
        runtime.detachOrdinaryExitObservation(actual);
        admission.completeFinalGenerationEnd(this);
    }

    void endDbFirstFinalGeneration(ProductionDbFirstBootstrapCallbacks callbacks,
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime){
        requireEnrolledLoadersLive("FINAL_GENERATION_END");requireExactMain("FINAL_GENERATION_END");
        if(callbacks!=exactCallbacks)throw failure("FINAL_GENERATION_OWNER");
        runtime.requireFinalSaveEndOwner(callbacks);
        requireProviderSavesComplete();
        ManagedOrdinarySaveAdmission admission=callbacks.ordinarySaveAdmission();
        admission.beginFinalGenerationEnd(this);
        Object scheduler=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(scheduler instanceof ManagedFileIoOwnerAccess owner))throw failure("FINAL_GENERATION_SCHEDULER");
        owner.floatingpod$endFinalSource(admission,enrolledLoaders.keySet());
        // 原scheduler List/loader queue均已实际退尾，锁外撤observer；失败保账，不重建finalizer。
        callbacks.detachDbFirstExitObservation(runtime);
        runtime.detachExitObservation(callbacks);
        admission.completeFinalGenerationEnd(this);
    }

    /** constructor仍仅原bootstrap包可见；真实binding/私有factory核对，不发行可伪造boolean。 */
    public void requireFinalSourceActivation(ManagedOrdinarySaveAdmission admission,
            ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner,
            ManagedOrdinarySaveAdmission.FinalSaveValidator source){
        requireExactMain("FINAL_SOURCE_ACTIVATION");
        ProductionBootstrapInstallation.requireOriginalFinalSourceActivation(this,admission,escrow,sourceOwner,source);
    }
    void requireUnmanagedDisjoint(Object loader,Path loaderDirectory,Path actualWorldRoot,String providerFolder) {
        requireEnrolledLoadersLive("UNMANAGED_CLASSIFICATION");
        Path normalizedLoader=checkedPath(loaderDirectory,"UNMANAGED_LOADER_PATH");
        if(ownsLoader(loader) || isWithin(normalizedLoader))
            throw failure("UNMANAGED_CLASSIFICATION","Loader identity or path intersects a retained managed root");
        if(actualWorldRoot!=null) {
            Path normalizedRoot=checkedPath(actualWorldRoot,"UNMANAGED_WORLD_ROOT");
            if(isWithin(normalizedRoot))
                throw failure("UNMANAGED_CLASSIFICATION","World save root intersects a retained managed root");
            Path provider=providerPath(normalizedRoot,providerFolder,"UNMANAGED_PROVIDER_FOLDER");
            if(isWithin(provider))
                throw failure("UNMANAGED_CLASSIFICATION","Provider save path intersects a retained managed root");
        }
    }

    synchronized boolean ownsLoader(Object loader) {
        requireEnrolledLoadersLive("LOADER_CLASSIFICATION");
        return loader!=null && enrolledLoaders.containsKey(loader);
    }

    /** 固定Admission提交边：所有检查先完成，随后只撤原map槽及其缓存视图。 */
    public void retireFinalEnrolledLoaders(ManagedOrdinarySaveAdmission admission) {
        if(admission==null||!Thread.holdsLock(admission))
            throw failure("FINAL_GENERATION_COMMIT","Original Admission monitor required");
        admission.requireFinalGenerationCommitIdentity(this);
        requireExactMain("FINAL_GENERATION_COMMIT");
        requireEnrolledLoadersLive("FINAL_GENERATION_COMMIT");
        enrolledLoaders=null;
    }

    private void requireEnrolledLoadersLive(String stage) {
        if(enrolledLoaders==null)throw failure(stage,"Original managed loader registry retired");
    }

    AdmissionException failure(String stage) {
        return failure(stage,"exact claimed boot save scope remains closed");
    }
    AdmissionException failure(String stage,String reason) {
        return new AdmissionException(bootId,bootGeneration,connectionGeneration,operationId,stage,reason,null);
    }
    AdmissionException failure(String stage,Throwable cause) {
        Objects.requireNonNull(cause,"original admission failure");
        return new AdmissionException(bootId,bootGeneration,connectionGeneration,operationId,stage,
            "save admission facts could not be verified",cause);
    }

    private void requireExactMain(String stage) {
        if(Thread.currentThread()!=exactMain)
            throw failure(stage,"exact original dedicated Main required");
    }

    private Path providerPath(Path root,String folder,String stage) {
        if(folder==null || folder.isEmpty())return root;
        try {
            Path relative=Path.of(folder);
            if(relative.isAbsolute())throw failure(stage,"Provider folder must be relative");
            Path resolved=root.resolve(relative).normalize();
            if(!resolved.startsWith(root))
                throw failure(stage,"Provider folder escapes its world root");
            return resolved;
        } catch(AdmissionException cause) {
            throw cause;
        } catch(RuntimeException cause) {
            throw failure(stage,cause);
        }
    }

    private Path checkedPath(Path value,String stage) {
        if(value==null)throw failure(stage,"Required actual path is absent");
        try { return normalize(value); }
        catch(AdmissionException cause) { throw cause; }
        catch(Throwable cause) { throw failure(stage,cause); }
    }

    private boolean isWithin(Path normalized) {
        return normalized.equals(worldRoot) || normalized.startsWith(worldRoot);
    }

    private static Path normalize(Path path) { return path.toAbsolutePath().normalize(); }
}

package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;
import com.wi.floatingpod.server.save.ManagedSaveAccess;
import com.wi.floatingpod.server.save.ManagedFileIoOwnerAccess;
import com.wi.floatingpod.server.save.ManagedSaveQueueObservation;
import com.wi.floatingpod.server.save.ManagedSaveContext;
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import java.io.DataOutputStream;
import java.io.DataOutput;
import com.wi.floatingpod.server.save.ManagedCountingDataOutput;
import java.io.File;
import java.util.Map;
import java.util.Set;
import net.minecraft.entity.Entity;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.storage.ThreadedFileIOBase;
import net.minecraftforge.fml.common.FMLCommonHandler;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.World;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraft.world.gen.ChunkProviderServer;
import net.minecraftforge.event.world.ChunkDataEvent;
import net.minecraftforge.fml.common.eventhandler.Event;
import net.minecraftforge.fml.common.eventhandler.EventBus;
import org.apache.logging.log4j.Logger;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.Dynamic;
import org.spongepowered.asm.mixin.injection.At;

/** 普通保存使用 live token；既有 forensic observer 独立保留，不授 prepared write permit。 */
@Mixin(AnvilChunkLoader.class)
public abstract class ManagedAnvilSaveMixin implements ManagedSaveAccess {
    @Shadow @Final private static Logger LOGGER;
    @Shadow @Final private Map<ChunkPos,NBTTagCompound> chunksToSave;
    @Shadow @Final private Set<ChunkPos> chunksBeingSaved;
    @Unique private volatile ManagedSourceSaveLedger floatingpod$saveLedger;
    @Unique private volatile boolean floatingpod$sourceSaveDetached;
    @Unique private volatile com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission floatingpod$ordinaryAdmission;
    @Unique private Object floatingpod$ordinaryScheduler;
    @Unique private int floatingpod$schedulerMembership;
    @Unique private volatile Throwable floatingpod$schedulerDiagnostic;
    @Unique private Throwable floatingpod$schedulerDiagnosticFailure;
    @Unique private Throwable floatingpod$schedulerDiagnosticAssociationFailure;
    @Unique private boolean floatingpod$schedulerDiagnosticClaimed;
    @Override public final void floatingpod$installOrdinaryAdmission(com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission){
        MinecraftServer server=FMLCommonHandler.instance().getMinecraftServerInstance();
        if(server==null||Thread.currentThread()!=server.getServerThread())throw new IllegalStateException("Exact server Main installs ordinary admission");
        Object actualIo=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(actualIo instanceof ManagedFileIoOwnerAccess owner))throw new IllegalStateException("Actual File IO thread observation unavailable");
        if(admission==null||floatingpod$sourceSaveDetached||floatingpod$ordinaryAdmission!=null||floatingpod$saveLedger!=null||!chunksToSave.isEmpty()||!chunksBeingSaved.isEmpty())
            throw new IllegalStateException("Ordinary admission requires fresh exact loader once");
        floatingpod$queueObservation=new ManagedSaveQueueObservation(server.getServerThread(),owner.floatingpod$actualStartedIoThread());
        owner.floatingpod$installOrdinarySchedulerMembership(this,admission);
    }
    @Override public final boolean floatingpod$hasOrdinaryAdmission(){return floatingpod$ordinaryAdmission!=null;}
    @Override public final void floatingpod$initializeOrdinaryScheduler(Object scheduler,com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission,boolean member){
        if(floatingpod$ordinaryAdmission!=null||floatingpod$schedulerMembership!=0||floatingpod$queueObservation==null||
                admission==null||!admission.isMain()||scheduler!=ThreadedFileIOBase.getThreadedIOInstance()||
                !chunksToSave.isEmpty()||!chunksBeingSaved.isEmpty())throw new IllegalStateException("Exact fresh scheduler installation required");
        floatingpod$ordinaryScheduler=scheduler;floatingpod$schedulerMembership=member?1:2;
        floatingpod$ordinaryAdmission=admission;
    }
    @Unique private void floatingpod$requireScheduler(Object scheduler){
        if(floatingpod$ordinaryAdmission==null||floatingpod$schedulerMembership==0||scheduler!=floatingpod$ordinaryScheduler)
            throw new IllegalStateException("Original initialized scheduler required");
    }
    @Override public final boolean floatingpod$finalSourceQueueEmpty(Object scheduler,com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission){
        floatingpod$requireScheduler(scheduler);
        if(admission!=floatingpod$ordinaryAdmission)throw new IllegalStateException("Original final source Admission required");
        try(var guard=floatingpod$queueObservation.enter()){
            if(floatingpod$schedulerMembership!=2||!chunksToSave.isEmpty()||!chunksBeingSaved.isEmpty())return false;
            if(admission.schedulerQueuedCount(this)!=0)throw new IllegalStateException("Empty physical queue differs from original save ledger");
            return true;
        }
    }
    @Override public final void floatingpod$ordinarySchedulerAdded(Object scheduler){
        floatingpod$requireScheduler(scheduler);floatingpod$schedulerMembership=1;
    }
    @Override public final boolean floatingpod$ordinarySchedulerRemoved(Object scheduler){
        floatingpod$requireScheduler(scheduler);floatingpod$schedulerMembership=2;
        if(floatingpod$schedulerDiagnostic!=null)return false;
        var ordinary=floatingpod$ordinaryAdmission;
        try(var guard=floatingpod$queueObservation.enter()){
            int expected=ordinary.schedulerQueuedCount(this);
            if(expected<0)return false; // 原 UNKNOWN 已有责任及诊断，真实移除后不再制造二级失败。
            if(expected!=chunksToSave.size())throw new IllegalStateException("Scheduler actual pending queue count differs from QUEUED tokens");
            for(var entry:chunksToSave.entrySet())ordinary.requireSchedulerQueuedPayload(this,entry.getKey().x,entry.getKey().z,entry.getValue());
            return expected!=0;
        }
    }
    @Override public final void floatingpod$ordinarySchedulerFailed(Object scheduler,Throwable cause){
        synchronized(this){if(floatingpod$schedulerDiagnostic==null)floatingpod$schedulerDiagnostic=cause;}
        try{
            floatingpod$requireScheduler(scheduler);floatingpod$ordinaryAdmission.schedulerFailed(this,cause);
            Throwable recording=floatingpod$ordinaryAdmission.schedulerRecordingFailure();
            synchronized(this){if(floatingpod$schedulerDiagnosticAssociationFailure==null)floatingpod$schedulerDiagnosticAssociationFailure=recording;}
        }catch(Throwable recording){synchronized(this){if(floatingpod$schedulerDiagnosticAssociationFailure==null)floatingpod$schedulerDiagnosticAssociationFailure=recording;}}
    }
    @Override public final void floatingpod$reportOrdinarySchedulerFailure(){
        Throwable cause;
        synchronized(this){
            if(floatingpod$schedulerDiagnosticClaimed||floatingpod$schedulerDiagnostic==null)return;
            floatingpod$schedulerDiagnosticClaimed=true;cause=floatingpod$schedulerDiagnostic;
        }
        floatingpod$ordinaryAdmission.associateSchedulerFailure(cause);
        Throwable recording=floatingpod$ordinaryAdmission.schedulerRecordingFailure();
        synchronized(this){if(floatingpod$schedulerDiagnosticAssociationFailure==null)floatingpod$schedulerDiagnosticAssociationFailure=recording;}
        Throwable mapFailure=floatingpod$ordinaryAdmission.schedulerQuarantineMapFailure();
        try{
            LOGGER.error("Managed ordinary scheduler handoff quarantined; save responsibility UNKNOWN",cause);
            if(mapFailure!=null)LOGGER.error("Managed ordinary scheduler quarantine Map recording failed; all admission scheduling remains blocked",mapFailure);
        }
        catch(Throwable diagnostic){
            synchronized(this){floatingpod$schedulerDiagnosticFailure=diagnostic;}
            if(diagnostic!=cause)try{ManagedSourceSaveLedger.suppressSecondary(cause,diagnostic);}
            catch(Throwable association){synchronized(this){if(floatingpod$schedulerDiagnosticAssociationFailure==null)floatingpod$schedulerDiagnosticAssociationFailure=association;}}
        }
    }
    @Unique private ManagedSaveQueueObservation floatingpod$queueObservation;
    @Override public final void floatingpod$installSaveLedger(ManagedSourceSaveLedger ledger) {
        MinecraftServer actualServer=FMLCommonHandler.instance().getMinecraftServerInstance();
        if(actualServer==null || Thread.currentThread()!=actualServer.getServerThread())
            throw new IllegalStateException("Actual MinecraftServer Main thread required for save observation installation");
        Object actualFileIo=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(actualFileIo instanceof ManagedFileIoOwnerAccess owner))
            throw new IllegalStateException("Actual constructor File IO owner observation unavailable; no managed claim");
        ledger.assertExactIoThread(owner.floatingpod$actualStartedIoThread());
        ledger.assertInstallation(this);
        if(floatingpod$sourceSaveDetached || floatingpod$saveLedger!=null || floatingpod$ordinaryAdmission!=null || !chunksToSave.isEmpty() || !chunksBeingSaved.isEmpty())
            throw new IllegalStateException("Save ledger installation requires fresh exact loader, once");
        floatingpod$queueObservation=new ManagedSaveQueueObservation(ledger);floatingpod$saveLedger=ledger;
    }
    @Unique private ManagedSourceSaveLedger floatingpod$sourceLedgerForEntry(){
        ManagedSourceSaveLedger original=floatingpod$saveLedger;
        if(floatingpod$sourceSaveDetached)throw new IllegalStateException("Original source save consumer was detached");
        return original;
    }
    @Override public final boolean floatingpod$tryDetachSaveLedger(ManagedSourceSaveLedger original){
        if(original==null||floatingpod$saveLedger!=original||floatingpod$sourceSaveDetached)
            throw new IllegalStateException("Exact installed source save ledger required for one-shot detach");
        original.assertInstallation(this);
        ManagedSaveQueueObservation queue=floatingpod$queueObservation;
        if(queue==null)throw new IllegalStateException("Original source queue owner missing during detach");
        try(ManagedSaveQueueObservation.Guard guard=queue.enter()){
            if(!original.tryDetachStoppedConsumer(this,chunksToSave.size(),chunksBeingSaved.size()))return false;
            floatingpod$sourceSaveDetached=true;
            floatingpod$saveLedger=null;
            floatingpod$queueObservation=null;
        }
        return true;
    }
    @Override public final ManagedSourceSaveLedger.GenerationCapture floatingpod$captureSourceGeneration(){
        ManagedSourceSaveLedger ledger=floatingpod$sourceLedgerForEntry();
        if(ledger==null)throw new IllegalStateException("Actual installed source save ledger required");
        try(ManagedSaveQueueObservation.Guard guard=floatingpod$queueObservation.enter()){
            return ledger.captureGeneration(this,chunksToSave.size(),chunksBeingSaved.size());
        }
    }
    @Override public final void floatingpod$requireSourceGeneration(ManagedSourceSaveLedger.GenerationCapture original){
        ManagedSourceSaveLedger ledger=floatingpod$sourceLedgerForEntry();
        if(ledger==null)throw new IllegalStateException("Actual installed source save ledger required");
        try(ManagedSaveQueueObservation.Guard guard=floatingpod$queueObservation.enter()){
            ledger.requireGeneration(this,original,chunksToSave.size(),chunksBeingSaved.size());
        }
    }
    @Override public final ManagedSaveQueueObservation.Guard floatingpod$beginSourceQueuedRead(){
        ManagedSourceSaveLedger published=floatingpod$sourceLedgerForEntry();if(published==null)return null;
        ManagedSaveQueueObservation queue=floatingpod$queueObservation;if(queue==null)throw new IllegalStateException("Published save observer lacks exact queue lock");return queue.enterSourceReadObservation();
    }
    @Override public final ManagedSourceSaveLedger.QueuedReadObservation floatingpod$observeSourceQueuedGet(int x,int z,Object actualCompound){
        ManagedSourceSaveLedger ledger=floatingpod$sourceLedgerForEntry();if(ledger==null)return null;
        ManagedSaveQueueObservation queue=floatingpod$queueObservation;if(queue==null)throw new IllegalStateException("Published save observer lacks exact queue lock");queue.assertHeldForSourceGet();
        return ledger.observeSourceQueuedGet(this,new ManagedSourceSaveLedger.Slot(x,z),actualCompound);
    }
    @WrapMethod(method="saveChunk(Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/Chunk;)V",require=1,allow=1)
    private void floatingpod$save(World world,Chunk chunk,Operation<Void> original)throws Throwable {
        floatingpod$sourceLedgerForEntry();
        if(world!=null&&chunk!=null&&world.getChunkProvider() instanceof ChunkProviderServer exactProvider){
            var exactAdmission=ProductionBootstrapInstallation.ordinarySaveAdmissionForProvider(exactProvider);
            if(exactAdmission.isPresent()){
                ManagedOrdinarySaveAdmission admission=exactAdmission.get();
                ManagedOrdinarySaveAdmission.ProviderScope scope=
                    ManagedOrdinarySaveAdmission.currentProviderScope(admission,exactProvider,world);
                if(scope==null){
                    if(admission.requiresProviderScope())
                        throw new IllegalStateException("Exact managed provider scope required after save admission retirement");
                }else{
                    var exactDecision=ProductionBootstrapInstallation.decideSourceProviderOperation(exactProvider,chunk,
                        ManagedOrdinarySaveAdmission.ProviderOperation.SAVE);
                    if(exactDecision.isEmpty())
                        throw new IllegalStateException("Exact managed provider save scope lost its source admission decision");
                    ManagedOrdinarySaveAdmission.ProviderDecision decision=exactDecision.get();
                    scope.observeDecision(chunk,ManagedOrdinarySaveAdmission.ProviderOperation.SAVE,decision);
                    if(scope.decision(chunk,ManagedOrdinarySaveAdmission.ProviderOperation.SAVE)
                            !=ManagedOrdinarySaveAdmission.ProviderDecision.ALLOW)return;
                }
            }
        }
        ManagedSourceSaveLedger ledger=floatingpod$sourceLedgerForEntry();
        if(ledger==null){original.call(world,chunk);return;}
        try(var allocation=ledger.enterProducerAllocation(this)){
            ManagedSourceSaveLedger.Attempt a=ledger.begin(this,new ManagedSourceSaveLedger.Slot(chunk.x,chunk.z));
            Throwable primary=null;
            try(ManagedSaveContext scope=ManagedSaveContext.producer(ledger,a)) {
                try{original.call(world,chunk);}
                catch(Throwable cause){primary=cause;scope.recordFailure(cause);throw cause;}
                finally{try{ledger.endProducer(a);}catch(Throwable cleanup){if(primary!=null){ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);}else throw cleanup;}}
        }
        }
    }
    @WrapOperation(method="saveChunk(Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/Chunk;)V",at=@At(value="INVOKE",target="Lnet/minecraftforge/fml/common/eventhandler/EventBus;post(Lnet/minecraftforge/fml/common/eventhandler/Event;)Z",ordinal=0),require=1,allow=1)
    private boolean floatingpod$afterSaveEvent(EventBus bus,Event event,Operation<Boolean> original)throws Throwable {
        boolean returned=original.call(bus,event);
        ManagedSaveContext c=ManagedSaveContext.current();
        if(c!=null && c.ledger==floatingpod$saveLedger) {
            if(!(event instanceof ChunkDataEvent.Save save))throw new IllegalStateException("Actual SaveEvent required");
            NBTTagCompound finalRoot=save.getData();
            c.ledger.finalReference(c.attempt,finalRoot);
            c.ledger.finalNbt(c.attempt,finalRoot,ManagedSaveContext.encode(finalRoot,c.ledger.limits()));
        }
        return returned;
    }
    @WrapOperation(method="addChunkToPending(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Ljava/util/Set;contains(Ljava/lang/Object;)Z",ordinal=0),require=1,allow=1)
    private boolean floatingpod$contains(Set<?> set,Object pos,Operation<Boolean> original,ChunkPos actualPos,NBTTagCompound compound) {
        boolean busy=original.call(set,pos);ManagedSaveContext c=ManagedSaveContext.current();
        if(floatingpod$sourceLedgerForEntry()!=null){floatingpod$requireScope(c);c.ledger.assertProducerSlot(c.attempt,new ManagedSourceSaveLedger.Slot(actualPos.x,actualPos.z));if(busy)c.ledger.noPut(c.attempt,compound);}
        return busy;
    }
    @WrapOperation(method="addChunkToPending(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Ljava/util/Map;put(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",ordinal=0),require=1,allow=1)
    private Object floatingpod$put(Map<?,?> map,Object pos,Object compound,Operation<Object> original)throws Throwable {
        floatingpod$sourceLedgerForEntry();
        var ordinary=floatingpod$ordinaryAdmission;
        if(ordinary!=null){
            var producer=ordinary.currentProducer();
            if(producer==null)throw new IllegalStateException("Original ordinary save producer required");
            ChunkPos actual=(ChunkPos)pos;
            producer.beforePut(this,actual.x,actual.z,compound); // 外部 current 在 queue guard 外。
            try(ManagedSaveQueueObservation.Guard guard=floatingpod$queueObservation.enter()){
                if(map!=chunksToSave)throw new IllegalStateException("Actual chunksToSave map required");
                Object previous=original.call(map,pos,compound);producer.putReturned(previous);return previous;
            }catch(Throwable cause){producer.failed(cause);throw cause;}
        }
        if(floatingpod$sourceLedgerForEntry()==null)return original.call(map,pos,compound);
        ManagedSaveContext c=ManagedSaveContext.current();floatingpod$requireScope(c);
        try(ManagedSaveQueueObservation.Guard guard=floatingpod$queueObservation.enter()) {
            if(map!=chunksToSave)throw new IllegalStateException("Actual chunksToSave map required");
            try{ChunkPos actualPos=(ChunkPos)pos;c.ledger.assertProducerSlot(c.attempt,new ManagedSourceSaveLedger.Slot(actualPos.x,actualPos.z));c.ledger.queuePutBeginning(c.attempt,compound);Object previous=original.call(map,pos,compound);c.ledger.putReturned(c.attempt,compound,previous);return previous;}
            catch(Throwable cause){c.recordFailure(cause);throw cause;}
        }
    }
    /** UT 的 Set.contains redirect 在私有 helper 内 put；必须观察这次真实替换，不能为写入补造 token。 */
    @Dynamic("UniversalTweaks 1.21.0 UTChunkSavingMixin merged helper")
    @WrapMethod(method="queueChunkToSave(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NBTTagCompound;)V",remap=false,require=0,allow=1)
    private void floatingpod$utPut(ChunkPos pos,NBTTagCompound compound,Operation<Void> original)throws Throwable {
        floatingpod$sourceLedgerForEntry();
        if(floatingpod$ordinaryAdmission==null&&floatingpod$saveLedger==null){original.call(pos,compound);return;}
        // helper 同步方法只修改内存 map。沿用原 put 关口，实际 previous 在同一 queue guard 内读取。
        floatingpod$put(chunksToSave,pos,compound,args->{
            Object previous=chunksToSave.get(pos);
            original.call(pos,compound);
            if(chunksToSave.get(pos)!=compound)throw new IllegalStateException("Actual UT queue put did not retain exact payload");
            return previous;
        });
    }
    /** UT 以 Iterator.remove 转移到 chunksInWrite；原 writeNextIO 的 Map.remove 不会执行。 */
    @Dynamic("UniversalTweaks 1.21.0 UTChunkSavingMixin merged helper")
    @WrapMethod(method="fetchChunkToWrite()Ljava/util/Map$Entry;",remap=false,require=0,allow=1)
    private Map.Entry<ChunkPos,NBTTagCompound> floatingpod$utRemove(Operation<Map.Entry<ChunkPos,NBTTagCompound>> original)throws Throwable {
        ManagedSourceSaveLedger ledger=floatingpod$sourceLedgerForEntry();
        var ordinary=floatingpod$ordinaryAdmission;
        if(ordinary==null&&ledger==null)return original.call();
        ManagedSaveContext c=ordinary==null?ManagedSaveContext.current():null;
        if(ordinary==null)floatingpod$requireScope(c);
        try(var guard=floatingpod$queueObservation.enter()){
            Map.Entry<ChunkPos,NBTTagCompound> before=chunksToSave.isEmpty()?null:chunksToSave.entrySet().iterator().next();
            ChunkPos position=before==null?null:before.getKey();
            NBTTagCompound payload=before==null?null:before.getValue();
            if(ledger!=null&&before!=null)ledger.queueRemoveBeginning(this,
                new ManagedSourceSaveLedger.Slot(position.x,position.z),payload);
            Map.Entry<ChunkPos,NBTTagCompound> removed=original.call();
            if(before==null?removed!=null:removed==null||!position.equals(removed.getKey())||payload!=removed.getValue()||chunksToSave.containsKey(position))
                throw new IllegalStateException("Actual UT iterator transfer differs from original queue entry");
            if(removed!=null){
                if(ordinary!=null){
                    var flight=ordinary.currentFlight();
                    if(flight==null)throw new IllegalStateException("Original ordinary UT File IO flight required");
                    flight.removed(this,position.x,position.z,payload);
                }else{
                    c.attempt=ledger.removed(this,new ManagedSourceSaveLedger.Slot(position.x,position.z),payload);
                    if(c.hasQueueGuard())c.releaseQueueGuard();
                }
            }
            return removed;
        }catch(Throwable cause){
            if(ordinary!=null){var flight=ordinary.currentFlight();if(flight!=null)flight.failed(cause);else ordinary.unknownAtBoundary(cause);}
            else c.recordFailure(cause);
            throw cause;
        }
    }
    @WrapMethod(method="flush()V",require=1,allow=1)
    private void floatingpod$ordinaryMainFlush(Operation<Void> original)throws Throwable{
        floatingpod$sourceLedgerForEntry();
        var ordinary=floatingpod$ordinaryAdmission;
        if(ordinary==null||!ordinary.isMain()){original.call();return;}
        try(var scope=ordinary.mainFlush(this)){original.call();}
    }
    @WrapMethod(method="writeNextIO()Z",require=1,allow=1)
    private boolean floatingpod$next(Operation<Boolean> original)throws Throwable {
        floatingpod$sourceLedgerForEntry();
        var ordinary=floatingpod$ordinaryAdmission;
        Throwable schedulerFailure=floatingpod$schedulerDiagnostic;
        if(ordinary!=null&&schedulerFailure!=null){if(ordinary.isMain())throw schedulerFailure;return false;}
        if(ordinary!=null)return ordinary.executeLoader(this,()->original.call(),
            cause->LOGGER.error("Managed ordinary Anvil loader quarantined; save responsibility UNKNOWN",cause));
        ManagedSourceSaveLedger ledger=floatingpod$sourceLedgerForEntry();if(ledger==null)return original.call();
        try(var allocation=ledger.enterConsumerAllocation(this)){
            ledger.requireIo(this);Throwable primary=null;
            try(ManagedSaveContext c=ManagedSaveContext.writer(ledger)) {
                // 只在实际 Map.remove/UT iterator transfer 内持 queue guard。
                // UT fetch helper 先持 loader monitor；外层先持 queue 会与 Main put 形成反序。
                try{
                    return original.call();
                }
                catch(Throwable cause){primary=cause;c.recordFailure(cause);throw cause;}
                finally{
                    Throwable cleanup=null;
                    if(c.hasQueueGuard())try{c.releaseQueueGuard();}catch(Throwable cause){cleanup=cause;}
                    if(c.attempt!=null)try{ledger.endFlight(c.attempt);}catch(Throwable cause){if(cleanup==null)cleanup=cause;else ManagedSourceSaveLedger.suppressSecondary(cleanup,cause);}
                    if(cleanup!=null){if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else{try{ledger.closeFailure(cleanup);}catch(Throwable recording){ManagedSourceSaveLedger.suppressSecondary(cleanup,recording);}throw cleanup;}}
                }
        }
        }
    }
    @WrapOperation(method="writeNextIO()Z",at=@At(value="INVOKE",target="Ljava/util/Map;remove(Ljava/lang/Object;)Ljava/lang/Object;",ordinal=0),require=1,allow=1)
    private Object floatingpod$remove(Map<?,?> map,Object pos,Operation<Object> original)throws Throwable {
        floatingpod$sourceLedgerForEntry();
        var ordinary=floatingpod$ordinaryAdmission;
        if(ordinary!=null){
            var flight=ordinary.currentFlight();
            if(flight==null)throw new IllegalStateException("Original ordinary File IO flight required");
            try(ManagedSaveQueueObservation.Guard guard=floatingpod$queueObservation.enter()){
                if(map!=chunksToSave)throw new IllegalStateException("Actual chunksToSave map required");
                ChunkPos actual=(ChunkPos)pos;Object removed=original.call(map,pos);
                flight.removed(this,actual.x,actual.z,removed);return removed;
            }catch(Throwable cause){flight.failed(cause);throw cause;}
        }
        if(floatingpod$sourceLedgerForEntry()==null)return original.call(map,pos);
        ManagedSaveContext c=ManagedSaveContext.current();floatingpod$requireScope(c);
        if(c.hasQueueGuard()) {
            if(map!=chunksToSave)throw new IllegalStateException("Actual chunksToSave map required");
            try {
                ChunkPos p=(ChunkPos)pos;ManagedSourceSaveLedger.Slot slot=new ManagedSourceSaveLedger.Slot(p.x,p.z);
                Object queuedBefore=chunksToSave.get(p);c.ledger.queueRemoveBeginning(this,slot,queuedBefore);
                Object compound=original.call(map,pos);
                c.attempt=c.ledger.removed(this,slot,compound);
                c.releaseQueueGuard();
                return compound;
            } catch(Throwable cause) { c.recordFailure(cause);throw cause; }
        }
        try(ManagedSaveQueueObservation.Guard guard=floatingpod$queueObservation.enter()) {
            if(map!=chunksToSave)throw new IllegalStateException("Actual chunksToSave map required");
            try{ChunkPos p=(ChunkPos)pos;ManagedSourceSaveLedger.Slot slot=new ManagedSourceSaveLedger.Slot(p.x,p.z);Object queuedBefore=chunksToSave.get(p);c.ledger.queueRemoveBeginning(this,slot,queuedBefore);Object compound=original.call(map,pos);c.attempt=c.ledger.removed(this,slot,compound);return compound;}
            catch(Throwable cause){c.recordFailure(cause);throw cause;}
        }
    }
    @WrapMethod(method="writeChunkData(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NBTTagCompound;)V",require=1,allow=1)
    private void floatingpod$data(ChunkPos pos,NBTTagCompound compound,Operation<Void> original)throws Throwable {
        floatingpod$sourceLedgerForEntry();
        var ordinary=floatingpod$ordinaryAdmission;
        if(ordinary!=null){
            var flight=ordinary.currentFlight();
            if(flight==null)throw new IllegalStateException("Original ordinary File IO flight required before write");
            try{flight.beforeWrite(this,pos.x,pos.z,compound);original.call(pos,compound);return;}
            catch(Throwable cause){flight.failed(cause);throw cause;}
        }
        if(floatingpod$sourceLedgerForEntry()==null){original.call(pos,compound);return;}
        ManagedSaveContext c=ManagedSaveContext.current();floatingpod$requireScope(c);
        c.ledger.beforeWrite(c.attempt,compound,ManagedSaveContext.encode(compound,c.ledger.limits()));
        try{original.call(pos,compound);}catch(Throwable cause){c.failed(cause);throw cause;}
    }
    @WrapOperation(method="writeChunkData(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFileCache;getChunkOutputStream(Ljava/io/File;II)Ljava/io/DataOutputStream;",ordinal=0),require=1,allow=1)
    private DataOutputStream floatingpod$stream(File directory,int x,int z,Operation<DataOutputStream> original)throws Throwable {
        ManagedSaveContext c=ManagedSaveContext.current();if(c==null)return original.call(directory,x,z);
        c.ledger.reserveOutput(c.attempt);
        try{DataOutputStream stream=original.call(directory,x,z);if(stream==null)throw new IllegalStateException("Actual region stream unavailable");c.opened(stream);return stream;}
        catch(Throwable cause){
            try{c.ledger.outputOpeningFailed(c.attempt);}catch(Throwable cleanup){ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}
            c.failed(cause);throw cause;
        }
    }
    @WrapOperation(method="writeChunkData(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Ljava/io/DataOutputStream;close()V",ordinal=0),require=1,allow=1)
    private void floatingpod$streamClose(DataOutputStream stream,Operation<Void> original)throws Throwable {
        ManagedSaveContext c=ManagedSaveContext.current();if(c==null){original.call(stream);return;}
        c.closing(stream);
        try{original.call(stream);c.closeReturned();}catch(Throwable cause){c.failed(cause);throw cause;}
    }
    @WrapOperation(method="writeChunkToNBT(Lnet/minecraft/world/chunk/Chunk;Lnet/minecraft/world/World;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Lnet/minecraft/entity/Entity;writeToNBTOptional(Lnet/minecraft/nbt/NBTTagCompound;)Z",ordinal=0),require=1,allow=1)
    private boolean floatingpod$entityEncoder(Entity entity,NBTTagCompound root,Operation<Boolean> original) {
        boolean encoded=original.call(entity,root);ManagedSaveContext c=ManagedSaveContext.current();if(!encoded && c!=null)c.ledger.encoderFalse(c.attempt);return encoded;
    }
    @WrapOperation(method="saveChunk(Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/Chunk;)V",at=@At(value="INVOKE",target="Lorg/apache/logging/log4j/Logger;error(Ljava/lang/String;Ljava/lang/Throwable;)V",ordinal=0),require=1,allow=1)
    private void floatingpod$saveCatch(Logger logger,String message,Throwable cause,Operation<Void> original)throws Throwable {
        if(floatingpod$ordinaryAdmission!=null){
            var producer=floatingpod$ordinaryAdmission.currentProducer();
            if(producer!=null)producer.failed(cause);else floatingpod$ordinaryAdmission.unknownAtBoundary(cause);
            try{original.call(logger,message,cause);}catch(Throwable diagnostic){ManagedSourceSaveLedger.suppressSecondary(cause,diagnostic);}
            throw cause;
        }floatingpod$failure(cause);try{original.call(logger,message,cause);}catch(Throwable secondary){ManagedSourceSaveLedger.suppressSecondary(cause,secondary);throw cause;}}
    @WrapOperation(method="writeNextIO()Z",at=@At(value="INVOKE",target="Lorg/apache/logging/log4j/Logger;error(Ljava/lang/String;Ljava/lang/Throwable;)V",ordinal=0),require=1,allow=1)
    private void floatingpod$writeCatch(Logger logger,String message,Throwable cause,Operation<Void> original)throws Throwable {floatingpod$failure(cause);try{original.call(logger,message,cause);}catch(Throwable secondary){ManagedSourceSaveLedger.suppressSecondary(cause,secondary);throw cause;}}
    @WrapOperation(method="writeChunkData(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Lnet/minecraft/nbt/CompressedStreamTools;write(Lnet/minecraft/nbt/NBTTagCompound;Ljava/io/DataOutput;)V",ordinal=0),require=1,allow=1)
    private void floatingpod$encode(NBTTagCompound compound,DataOutput output,Operation<Void> original)throws Throwable {
        ManagedSaveContext c=ManagedSaveContext.current();if(c==null){original.call(compound,output);return;}
        ManagedCountingDataOutput bounded=new ManagedCountingDataOutput(output,c.ledger,c.attempt);
        original.call(compound,bounded);
        bounded.finishCompareAndForward();
    }
    @WrapOperation(method="writeChunkToNBT(Lnet/minecraft/world/chunk/Chunk;Lnet/minecraft/world/World;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Lorg/apache/logging/log4j/Logger;error(Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;)V",ordinal=0),require=1,allow=1)
    private void floatingpod$entityCatch(Logger logger,String message,Object name,Object actualCause,Operation<Void> original)throws Throwable {floatingpod$encoderCatch(actualCause);try{original.call(logger,message,name,actualCause);}catch(Throwable secondary){if(actualCause instanceof Throwable cause){ManagedSourceSaveLedger.suppressSecondary(cause,secondary);throw cause;}throw secondary;}}
    @WrapOperation(method="writeChunkToNBT(Lnet/minecraft/world/chunk/Chunk;Lnet/minecraft/world/World;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Lorg/apache/logging/log4j/Logger;error(Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;)V",ordinal=1),require=1,allow=1)
    private void floatingpod$tileCatch(Logger logger,String message,Object name,Object actualCause,Operation<Void> original)throws Throwable {floatingpod$encoderCatch(actualCause);try{original.call(logger,message,name,actualCause);}catch(Throwable secondary){if(actualCause instanceof Throwable cause){ManagedSourceSaveLedger.suppressSecondary(cause,secondary);throw cause;}throw secondary;}}
    @WrapOperation(method="writeChunkToNBT(Lnet/minecraft/world/chunk/Chunk;Lnet/minecraft/world/World;Lnet/minecraft/nbt/NBTTagCompound;)V",at=@At(value="INVOKE",target="Lorg/apache/logging/log4j/Logger;error(Ljava/lang/String;Ljava/lang/Throwable;)V",ordinal=0),require=1,allow=1)
    private void floatingpod$capCatch(Logger logger,String message,Throwable cause,Operation<Void> original)throws Throwable {floatingpod$failure(cause);try{original.call(logger,message,cause);}catch(Throwable secondary){ManagedSourceSaveLedger.suppressSecondary(cause,secondary);throw cause;}}
    @Unique private void floatingpod$encoderCatch(Object actualCause)throws Throwable {if(actualCause instanceof Throwable cause)floatingpod$failure(cause);else if(floatingpod$saveLedger!=null || floatingpod$ordinaryAdmission!=null)throw new IllegalStateException("Actual swallowed encoder cause unavailable");}
    @Unique private void floatingpod$failure(Throwable cause)throws Throwable {
        var ordinary=floatingpod$ordinaryAdmission;
        if(ordinary!=null){
            var producer=ordinary.currentProducer();var flight=ordinary.currentFlight();
            if(producer!=null)producer.failed(cause);else if(flight!=null)flight.failed(cause);else ordinary.unknownAtBoundary(cause);
            throw cause;
        }
        ManagedSaveContext c=ManagedSaveContext.current();if(c!=null && c.ledger==floatingpod$saveLedger)c.recordFailure(cause);}
    @Unique private void floatingpod$requireScope(ManagedSaveContext c){if(c==null || c.ledger!=floatingpod$saveLedger)throw new IllegalStateException("Actual installed loader scope required");}
}

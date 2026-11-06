package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.server.save.ManagedFileIoOwnerAccess;
import com.wi.floatingpod.server.save.ManagedSaveAccess;
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission;
import com.wi.floatingpod.bootstrap.ManagedSourcePathCensus;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;
import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import net.minecraft.world.storage.ThreadedFileIOBase;
import net.minecraft.world.storage.IThreadedFileIO;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import java.util.List;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;

/** 原constructor创建原thread，本hook只捕获actual.start receiver，不另创建线程/registry。 */
@Mixin(ThreadedFileIOBase.class)
public abstract class ManagedThreadedFileIoOwnerMixin implements ManagedFileIoOwnerAccess,ManagedSourcePathCensus.SchedulerAccess {
    @Shadow @Final private List<IThreadedFileIO> threadedIOQueue;
    @Unique private volatile StartObservation floatingpod$actualStart;
    @WrapOperation(method="<init>()V",at=@At(value="INVOKE",target="Ljava/lang/Thread;start()V",ordinal=0),require=1,allow=1)
    private void floatingpod$start(Thread actualReceiver,Operation<Void> original)throws Throwable {
        if(floatingpod$actualStart!=null)throw new IllegalStateException("Original File IO Thread.start observed more than once");
        StartObservation observed=new StartObservation(actualReceiver);floatingpod$actualStart=observed;
        try{original.call(actualReceiver);}
        catch(Throwable actualCause){observed.startFailed(actualCause);throw actualCause;}
        observed.startReturned();
    }
    @Override public final Thread floatingpod$actualStartedIoThread() {
        StartObservation observed=floatingpod$actualStart;
        if(observed==null)throw new IllegalStateException("Original File IO Thread.start not observed");
        return observed.actualStartedThread();
    }
    @Override public final void floatingpod$installOrdinarySchedulerMembership(ManagedSaveAccess loader,ManagedOrdinarySaveAdmission admission){
        if(admission==null||!admission.isMain())throw new IllegalStateException("Exact ordinary Main installs scheduler membership");
        floatingpod$actualStartedIoThread();
        synchronized(threadedIOQueue){loader.floatingpod$initializeOrdinaryScheduler(this,admission,threadedIOQueue.contains(loader));}
    }
    @Override public final boolean floatingpod$tryFenceFinalSource(ManagedOrdinarySaveAdmission admission,
            ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner,Iterable<?> actualLoaders){
        if(admission==null||!admission.isMain())throw new IllegalStateException("Original final source Main required");
        synchronized(threadedIOQueue){
            for(Object loader:actualLoaders){
                if(!(loader instanceof ManagedSaveAccess access))throw new IllegalStateException("Actual managed loader required");
                if(threadedIOQueue.contains(loader)||!access.floatingpod$finalSourceQueueEmpty(this,admission))return false;
            }
            return admission.tryFenceFinalOldGeneration(escrow,sourceOwner);
        }
    }
    @Override public final void floatingpod$requireFinalEndLock(){
        if((Object)this!=net.minecraft.world.storage.ThreadedFileIOBase.getThreadedIOInstance()||!Thread.holdsLock(threadedIOQueue))
            throw new IllegalStateException("Actual scheduler List final-end lock required");
    }
    @Override public final boolean floatingpod$trySealOrdinarySaves(ManagedOrdinarySaveAdmission admission,Iterable<?> actualLoaders){
        if(admission==null||!admission.isMain()||(Object)this!=ThreadedFileIOBase.getThreadedIOInstance())
            throw new IllegalStateException("Actual scheduler and original ordinary Main required");
        synchronized(threadedIOQueue){
            for(Object loader:actualLoaders){
                if(!(loader instanceof ManagedSaveAccess access))throw new IllegalStateException("Actual enrolled ordinary loader required");
                if(threadedIOQueue.contains(loader)||!access.floatingpod$finalSourceQueueEmpty(this,admission))return false;
            }
            return admission.trySealOrdinaryAfterDrain();
        }
    }
    @Override public final void floatingpod$endFinalSource(ManagedOrdinarySaveAdmission admission,Iterable<?> actualLoaders){
        if(admission==null||!admission.isMain())throw new IllegalStateException("Original final-source Main required");
        synchronized(threadedIOQueue){
            for(Object loader:actualLoaders){
                if(!(loader instanceof ManagedSaveAccess access))throw new IllegalStateException("Actual enrolled managed loader required");
                if(threadedIOQueue.contains(loader)||!access.floatingpod$finalSourceQueueEmpty(this,admission))
                    throw new IllegalStateException("Actual final scheduler/loader queue remains owned");
            }
            admission.endFinalGenerationSource(this);
        }
    }
    /** 原 contains、counter、add 同锁；不把真实 IO 或 sleep 放进此锁。 */
    @WrapMethod(method="queueIO(Lnet/minecraft/world/storage/IThreadedFileIO;)V",require=1,allow=1)
    private void floatingpod$queue(IThreadedFileIO loader,Operation<Void> original)throws Throwable{
        boolean nested=Thread.holdsLock(threadedIOQueue);
        ManagedSaveAccess managed=loader instanceof ManagedSaveAccess access&&access.floatingpod$hasOrdinaryAdmission()?access:null;
        ManagedSourcePathCensus.Token sourceToken=null;
        boolean wasMember,isMember=false;
        Throwable failure=null;
        synchronized(threadedIOQueue){
            wasMember=threadedIOQueue.contains(loader);
            sourceToken=floatingpod$prepareSourceQueue(loader,wasMember);
            try{
                original.call(loader);
                isMember=threadedIOQueue.contains(loader);
                if(managed!=null&&isMember&&!wasMember)managed.floatingpod$ordinarySchedulerAdded(this);
                if(sourceToken!=null)sourceToken.schedulerAdmissionReturned(loader,wasMember,isMember,null);
            }catch(Throwable cause){
                failure=cause;
                if(sourceToken!=null)try{sourceToken.schedulerAdmissionReturned(loader,wasMember,threadedIOQueue.contains(loader),cause);}
                    catch(Throwable censusFailure){if(cause!=censusFailure)cause.addSuppressed(censusFailure);}
                if(managed!=null)managed.floatingpod$ordinarySchedulerFailed(this,cause);
            }
        }
        if(!nested&&managed!=null)managed.floatingpod$reportOrdinarySchedulerFailure();
        if(failure!=null&&(sourceToken!=null||managed==null||Thread.currentThread()!=floatingpod$actualStartedIoThread()))throw failure;
    }

    @Unique private ManagedSourcePathCensus.Token floatingpod$prepareSourceQueue(IThreadedFileIO work,boolean alreadyMember){
        if(work instanceof ManagedSourcePathCensus.ScheduledWork scheduled){
            ManagedSourcePathCensus.Token token=scheduled.floatingpod$sourceWorkToken();
            if(token==null)throw new IllegalStateException("Actual source CaptureJob has no precharged census token");
            return token.prepareSchedulerAdmission(work,threadedIOQueue,alreadyMember);
        }
        if(work instanceof AnvilChunkLoader loader){
            var selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(loader);
            if(selected.isPresent())return selected.get().prepareSchedulerAdmission(work,threadedIOQueue,alreadyMember);
        }
        return null;
    }

    @Unique private ManagedSourcePathCensus.Token floatingpod$sourceTokenFor(IThreadedFileIO work){
        if(work instanceof ManagedSourcePathCensus.ScheduledWork scheduled){
            ManagedSourcePathCensus.Token token=scheduled.floatingpod$sourceWorkToken();
            return token==null?null:token;
        }
        if(work instanceof AnvilChunkLoader loader){
            var selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(loader);
            if(selected.isPresent())return selected.get().activeSchedulerWorkFor(loader).orElse(null);
        }
        return null;
    }

    @Override public final void floatingpod$requeueSourceWork(IThreadedFileIO exactWork,
            ManagedSourcePathCensus.Token exactToken){
        if((Object)this!=ThreadedFileIOBase.getThreadedIOInstance()||exactWork==null||exactToken==null||
                exactToken.actualTask()!=exactWork||exactToken.state()!=ManagedSourcePathCensus.State.HELD)
            throw new IllegalStateException("Exact original scheduler/work/held token required for requeue");
        synchronized(threadedIOQueue){
            if(threadedIOQueue.contains(exactWork))throw new IllegalStateException("Exact source work already has scheduler membership");
            if(exactWork instanceof AnvilChunkLoader loader){
                var selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(loader);
                if(selected.isEmpty()||selected.get().activeSchedulerWorkFor(loader).orElse(null)!=exactToken)
                    throw new IllegalStateException("Exact current Census loader token required for scheduler requeue");
            }else if(!(exactWork instanceof ManagedSourcePathCensus.ScheduledWork scheduled)||
                    scheduled.floatingpod$sourceWorkToken()!=exactToken||exactToken.kind()!=ManagedSourcePathCensus.Kind.CAPTURE_JOB){
                throw new IllegalStateException("Exact source CaptureJob token required for scheduler requeue");
            }
            try{
                ((ThreadedFileIOBase)(Object)this).queueIO(exactWork);
            }catch(Throwable cause){exactToken.fail(cause);throw cause;}
            if(!threadedIOQueue.contains(exactWork)||exactToken.state()!=ManagedSourcePathCensus.State.QUEUED)
                throw new IllegalStateException("Original scheduler did not publish exact source work requeue");
        }
    }

    /** Track the actual loader/CaptureJob invocation; a requested hold only turns its true result into false. */
    @WrapOperation(method="processQueue()V",at=@At(value="INVOKE",target="Lnet/minecraft/world/storage/IThreadedFileIO;writeNextIO()Z",ordinal=0),require=1,allow=1)
    private boolean floatingpod$writeNextIO(IThreadedFileIO actualWork,Operation<Boolean> original)throws Throwable{
        ManagedSourcePathCensus.Token token=floatingpod$sourceTokenFor(actualWork);
        if(token!=null)token.schedulerStarted(actualWork);
        boolean moreWork;
        try{moreWork=original.call(actualWork);}
        catch(Throwable cause){if(token!=null)token.fail(cause);throw cause;}
        if(token==null)return moreWork;
        token.schedulerReturned(actualWork,moreWork);
        return moreWork&&!token.schedulerHoldRequested(actualWork);
    }
    /** 每次真实 false/remove 都交接新到达的 QUEUED；不是额外 payload 写。 */
    @WrapOperation(method="processQueue()V",at=@At(value="INVOKE",target="Ljava/util/List;remove(I)Ljava/lang/Object;",ordinal=0),require=1,allow=1)
    private Object floatingpod$retire(List<?> actual,int index,Operation<Object> original)throws Throwable{
        if(actual!=threadedIOQueue)throw new IllegalStateException("Actual scheduler List required");
        Object removed;ManagedSaveAccess managed=null;
        synchronized(threadedIOQueue){
            removed=original.call(actual,index);
            if(removed instanceof IThreadedFileIO sourceWork){
                ManagedSourcePathCensus.Token sourceToken=floatingpod$sourceTokenFor(sourceWork);
                if(sourceToken!=null)sourceToken.schedulerRemoved(sourceWork);
            }
            if(removed instanceof ManagedSaveAccess access&&access.floatingpod$hasOrdinaryAdmission()){
                managed=access;
                try{
                    if(access.floatingpod$ordinarySchedulerRemoved(this))
                        ((ThreadedFileIOBase)(Object)this).queueIO((IThreadedFileIO)removed);
                }catch(Throwable cause){access.floatingpod$ordinarySchedulerFailed(this,cause);}
            }
        }
        if(managed!=null)managed.floatingpod$reportOrdinarySchedulerFailure();
        return removed;
    }
}

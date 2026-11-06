package com.wi.floatingpod.mixin;

import com.wi.floatingpod.server.source.SourceAnvilReadAccess;
import com.wi.floatingpod.server.source.SourceLoadManifest;
import com.wi.floatingpod.server.source.SourceReadOperation;
import com.wi.floatingpod.bootstrap.ManagedSourcePathCensus;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.server.save.ManagedSaveAccess;
import com.wi.floatingpod.server.save.ManagedSaveQueueObservation;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import net.minecraft.world.World;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraft.entity.Entity;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.util.datafix.DataFixer;
import net.minecraft.util.datafix.IFixType;
import net.minecraft.util.math.ChunkPos;
import java.io.File;
import java.io.DataInputStream;
import java.util.Map;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;

/** 实际 loader-local async结果身份跨 Main，不按UUID检索。 */
@Mixin(AnvilChunkLoader.class)
abstract class SourceAnvilReadMixin implements SourceAnvilReadAccess {
    @Shadow @Final private Map<ChunkPos,NBTTagCompound> chunksToSave;
    @Unique private volatile SourceLoadManifest floatingpod$sourceReads;
    @Unique private volatile boolean floatingpod$sourceReadsDetached;
    @Override public final void floatingpod$installSourceLoadManifest(SourceLoadManifest holder){
        holder.assertInstallation(this);if(floatingpod$sourceReads!=null||floatingpod$sourceReadsDetached)throw new IllegalStateException("Source loader installed twice or detached");
        var census=ProductionBootstrapInstallation.sourcePathCensusForLoader((AnvilChunkLoader)(Object)this);
        if(census.isPresent())census.get().bindLoadedManifestForLoader(this,holder);
        floatingpod$sourceReads=holder;
    }
    @Override public final void floatingpod$detachSourceLoadManifest(SourceLoadManifest holder){
        if(holder==null||floatingpod$sourceReads!=holder||floatingpod$sourceReadsDetached)
            throw new IllegalStateException("Exact installed source manifest required for one-shot detach");
        holder.requireStoppedReads(this);
        // 必须先发布拒绝状态，再清引用；晚到read不能把null理解为从未安装。
        floatingpod$sourceReadsDetached=true;
        floatingpod$sourceReads=null;
    }
    @WrapMethod(method="loadChunk__Async(Lnet/minecraft/world/World;II)[Ljava/lang/Object;",require=1,allow=1)
    private Object[] floatingpod$read(World world,int x,int z,Operation<Object[]> original)throws Throwable {
        SourceLoadManifest holder=floatingpod$sourceReads;
        if(floatingpod$sourceReadsDetached)throw new IllegalStateException("Original source loader read consumer was detached");
        AnvilChunkLoader actualLoader=(AnvilChunkLoader)(Object)this;
        var selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(actualLoader);
        ManagedSourcePathCensus.Token censusRead=selected.isPresent()?
            selected.get().beginRead(actualLoader,world,world.provider.getDimension(),x,z):null;
        if(holder==null){
            try{
                Object[] result=original.call(world,x,z);
                if(censusRead!=null)censusRead.rawOutput(actualLoader,result,result!=null&&result.length>1?result[1]:null);
                return result;
            }catch(Throwable cause){floatingpod$retainCensusFailure(censusRead,cause);throw cause;}
        }
        try {
        try(SourceLoadManifest.ReadEntry entry=holder.enterReadEntry(this,world)){
            SourceReadOperation operation=holder.beginRead(this,world,x,z);Throwable primary=null;
            try(SourceReadOperation.PhaseScope scope=operation.enterRead()){
                try{
                    Object[] result=original.call(world,x,z);
                    Object rawRoot=result!=null&&result.length>1?result[1]:null;
                    if(censusRead!=null)censusRead.rawOutput(actualLoader,result,rawRoot);
                    operation.returnedAsync(result);return result;
                }
                catch(Throwable cause){primary=cause;try{operation.recordFailure(cause);}catch(Throwable cleanup){ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}floatingpod$retainCensusFailure(censusRead,cause);throw cause;}
                finally{
                    try{operation.readEnded(primary);}catch(Throwable cleanup){if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else{try{operation.recordFailure(cleanup);}catch(Throwable nested){ManagedSourceSaveLedger.suppressSecondary(cleanup,nested);}floatingpod$retainCensusFailure(censusRead,cleanup);holder.rawFinished(operation,cleanup);throw cleanup;}}
                    try{holder.rawFinished(operation,primary);}catch(Throwable cleanup){if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else{floatingpod$retainCensusFailure(censusRead,cleanup);throw cleanup;}}
                }
        }
        }
        }catch(Throwable cause){
            if(censusRead!=null&&!censusRead.hasCause())floatingpod$retainCensusFailure(censusRead,cause);
            throw cause;
        }
    }
    @WrapOperation(method="loadChunk__Async(Lnet/minecraft/world/World;II)[Ljava/lang/Object;",at=@At(value="INVOKE",target="Ljava/util/Map;get(Ljava/lang/Object;)Ljava/lang/Object;",ordinal=0),require=1,allow=1)
    private Object floatingpod$queueRead(Map<?,?> map,Object key,Operation<Object> original)throws Throwable {
        SourceReadOperation operation=SourceReadOperation.currentRead();if(operation==null)return original.call(map,key);
        if(map!=chunksToSave||!(key instanceof ChunkPos position))throw new IllegalStateException("Exact source pending map/key required");
        ManagedSaveAccess access=(Object)this instanceof ManagedSaveAccess a?a:null;
        ManagedSourceSaveLedger.QueuedReadObservation observation=null;Object actual;
        // UT 的原 get redirect 会进入 synchronized reload helper；与 producer 统一为 loader -> queue。
        // monitor 与 queue 均只包围实际 get/记录，不跨后面的 queuedRoot copy/encoder。
        synchronized(this){
            try(ManagedSaveQueueObservation.Guard guard=access==null?null:access.floatingpod$beginSourceQueuedRead()){
                actual=original.call(map,key);if(access!=null)observation=access.floatingpod$observeSourceQueuedGet(position.x,position.z,actual);
            }
        }
        try(ManagedSourceSaveLedger.QueuedReadObservation lease=observation){
            if(actual==null)return null;if(!(actual instanceof NBTTagCompound compound))throw new IllegalStateException("Actual queued root is not compound");
            if(lease!=null){if(!lease.matchesActual(actual))throw new IllegalStateException("Source queued lease reference mismatch");if(lease.originalFailure()!=null)operation.recordFailure(lease.originalFailure());else if(lease.observedProgress()==ManagedSourceSaveLedger.Progress.FAULTED)operation.acceptedQueuedFaultWithoutCause();}
            return operation.queuedRoot(actual,compound,lease==null?0:lease.acceptedGeneration());
        }
    }
    @WrapOperation(method="loadChunk__Async(Lnet/minecraft/world/World;II)[Ljava/lang/Object;",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFileCache;getChunkInputStream(Ljava/io/File;II)Ljava/io/DataInputStream;",ordinal=0),require=1,allow=1)
    private DataInputStream floatingpod$input(File directory,int x,int z,Operation<DataInputStream> original)throws Throwable {
        DataInputStream stream=original.call(directory,x,z);SourceReadOperation operation=SourceReadOperation.currentRead();if(operation!=null&&stream!=null)operation.openedDiskStream(stream);return stream;
    }
    @WrapOperation(method="loadChunk__Async(Lnet/minecraft/world/World;II)[Ljava/lang/Object;",at=@At(value="INVOKE",target="Lnet/minecraft/nbt/CompressedStreamTools;read(Ljava/io/DataInputStream;)Lnet/minecraft/nbt/NBTTagCompound;",ordinal=0),require=1,allow=1)
    private NBTTagCompound floatingpod$raw(DataInputStream actual,Operation<NBTTagCompound> original)throws Throwable {
        SourceReadOperation operation=SourceReadOperation.currentRead();if(operation==null)return original.call(actual);
        DataInputStream owned=operation.ownedDiskInput(actual);NBTTagCompound root=original.call(owned);operation.diskRootReturned(root);return root;
    }
    @WrapOperation(method="loadChunk__Async(Lnet/minecraft/world/World;II)[Ljava/lang/Object;",at=@At(value="INVOKE",target="Lnet/minecraft/util/datafix/DataFixer;process(Lnet/minecraft/util/datafix/IFixType;Lnet/minecraft/nbt/NBTTagCompound;)Lnet/minecraft/nbt/NBTTagCompound;",ordinal=0),require=1,allow=1)
    private NBTTagCompound floatingpod$fix(DataFixer fixer,IFixType type,NBTTagCompound root,Operation<NBTTagCompound> original)throws Throwable {
        SourceReadOperation operation=SourceReadOperation.currentRead();if(operation==null)return original.call(fixer,type,root);
        operation.fixerBeginning(root);NBTTagCompound fixed=original.call(fixer,type,root);operation.fixerReturned(root,fixed);return fixed;
    }
    @WrapOperation(method="loadChunk__Async(Lnet/minecraft/world/World;II)[Ljava/lang/Object;",at=@At(value="INVOKE",target="Ljava/io/DataInputStream;close()V",ordinal=0),require=1,allow=1)
    private void floatingpod$inputClose(DataInputStream actual,Operation<Void> original)throws Throwable {
        SourceReadOperation operation=SourceReadOperation.currentRead();if(operation!=null)operation.actualStreamClosing(actual);original.call(actual);
    }
    @WrapMethod(method="loadEntities(Lnet/minecraft/world/World;Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/chunk/Chunk;)V",require=1,allow=1)
    private void floatingpod$entities(World world,NBTTagCompound level,Chunk chunk,Operation<Void> original)throws Throwable {
        SourceLoadManifest holder=floatingpod$sourceReads;
        if(floatingpod$sourceReadsDetached)throw new IllegalStateException("Original source loader decode consumer was detached");
        AnvilChunkLoader actualLoader=(AnvilChunkLoader)(Object)this;
        var selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(actualLoader);
        ManagedSourcePathCensus.Token censusRead=selected.isPresent()?
            selected.get().activeReadFor(actualLoader,world,chunk.x,chunk.z):null;
        if(holder==null){
            boolean started=false;Throwable primary=null;
            try{if(censusRead!=null){censusRead.mainInstallStarted(world,censusRead.actualStream(),level);started=true;}original.call(world,level,chunk);}
            catch(Throwable cause){primary=cause;floatingpod$retainCensusFailure(censusRead,cause);throw cause;}
            finally{if(started)try{censusRead.mainInstallReturned(world,censusRead.actualStream(),primary);}catch(Throwable cleanup){if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else throw cleanup;}}
            return;
        }
        SourceReadOperation operation=holder.beginDecode(this,world,level,chunk.x,chunk.z);
        if(operation==null){
            boolean started=false;Throwable primary=null;
            try{if(censusRead!=null){censusRead.mainInstallStarted(world,censusRead.actualStream(),level);started=true;}original.call(world,level,chunk);}
            catch(Throwable cause){primary=cause;floatingpod$retainCensusFailure(censusRead,cause);throw cause;}
            finally{if(started)try{censusRead.mainInstallReturned(world,censusRead.actualStream(),primary);}catch(Throwable cleanup){if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else throw cleanup;}}
            return;
        }
        boolean censusStarted=false;
        Throwable primary=null;
        try(SourceReadOperation.PhaseScope scope=operation.enterMain(world,level)){
            if(censusRead!=null){censusRead.mainInstallStarted(world,censusRead.actualStream(),level);censusStarted=true;}
            original.call(world,level,chunk);
        }
        catch(Throwable cause){primary=cause;try{operation.recordFailure(cause);}catch(Throwable cleanup){ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}floatingpod$retainCensusFailure(censusRead,cause);throw cause;}
        finally{
            try{operation.mainEnded(primary);holder.decoded(operation,primary);}
            catch(Throwable cleanup){if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else{primary=cleanup;}}
            try{if(censusStarted)censusRead.mainInstallReturned(world,censusRead.actualStream(),primary);}
            catch(Throwable cleanup){if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else throw cleanup;}
            if(primary!=null)throw primary;
        }
    }
    @WrapOperation(method="readChunkEntity(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/Chunk;)Lnet/minecraft/entity/Entity;",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/AnvilChunkLoader;createEntityFromNBT(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",ordinal=0),require=1,allow=1)
    private static Entity floatingpod$helper(NBTTagCompound node,World world,Operation<Entity> original)throws Throwable {
        SourceReadOperation operation=SourceReadOperation.currentMain();if(operation==null)return original.call(node,world);
        try(SourceReadOperation.DecoderScope scope=operation.enterDecoder(node,world)){
            try{Entity result=original.call(node,world);scope.helperReturned(result);return result;}
            catch(Throwable cause){scope.helperFailed(cause);throw cause;}
        }
    }
    @WrapOperation(method="createEntityFromNBT(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",at=@At(value="INVOKE",target="Lnet/minecraft/entity/EntityList;createEntityFromNBT(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",ordinal=0),require=1,allow=1)
    private static Entity floatingpod$inner(NBTTagCompound node,World world,Operation<Entity> original)throws Throwable {
        SourceReadOperation.DecoderScope scope=SourceReadOperation.decoder();
        try{return original.call(node,world);}catch(Throwable cause){if(scope!=null&&scope.exact(node,world))scope.helperFailed(cause);throw cause;}
    }

    @Unique private static void floatingpod$retainCensusFailure(ManagedSourcePathCensus.Token token,Throwable primary){
        if(token==null||token.hasCause())return;
        try{token.fail(primary);}
        catch(Throwable cleanup){if(cleanup!=primary)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);}
    }
}

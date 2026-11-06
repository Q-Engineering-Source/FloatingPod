package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.bootstrap.ManagedSourcePathCensus;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;
import net.minecraft.world.World;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraft.world.gen.ChunkProviderServer;
import net.minecraftforge.common.chunkio.ChunkIOExecutor;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;

/** Actual Cleanroom/Forge ChunkIOExecutor paths, with temporary contexts bound before task publication. */
@Mixin(targets="net.minecraftforge.common.chunkio.ChunkIOExecutor",remap=false)
abstract class SourceChunkIoExecutorMixin {
    @Shadow @Final private static Map<?,?> tasks;
    @Unique private static final ThreadLocal<Object> floatingpod$currentTickTask=new ThreadLocal<>();
    @WrapMethod(method="queueChunkLoad(Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/storage/AnvilChunkLoader;Lnet/minecraft/world/gen/ChunkProviderServer;IILjava/lang/Runnable;)V",require=1,allow=1)
    private static void floatingpod$queue(World world,AnvilChunkLoader loader,ChunkProviderServer provider,
            int x,int z,Runnable callback,Operation<Void> original)throws Throwable {
        Optional<ManagedSourcePathCensus> selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(loader);
        if(selected.isEmpty()){original.call(world,loader,provider,x,z,callback);return;}
        ManagedSourcePathCensus.ForgeCall call=selected.get().enterForgeCall(world,loader,provider,
            world.provider.getDimension(),x,z,ManagedSourcePathCensus.ForgeCallKind.QUEUE);
        floatingpod$invoke(call,()->original.call(world,loader,provider,x,z,callback));
    }

    @WrapMethod(method="syncChunkLoad(Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/storage/AnvilChunkLoader;Lnet/minecraft/world/gen/ChunkProviderServer;II)Lnet/minecraft/world/chunk/Chunk;",require=1,allow=1)
    private static Chunk floatingpod$sync(World world,AnvilChunkLoader loader,ChunkProviderServer provider,
            int x,int z,Operation<Chunk> original)throws Throwable {
        Optional<ManagedSourcePathCensus> selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(loader);
        if(selected.isEmpty())return original.call(world,loader,provider,x,z);
        ManagedSourcePathCensus.ForgeCall call=selected.get().enterForgeCall(world,loader,provider,
            world.provider.getDimension(),x,z,ManagedSourcePathCensus.ForgeCallKind.SYNC);
        final Chunk[] result=new Chunk[1];
        floatingpod$invoke(call,()->result[0]=original.call(world,loader,provider,x,z));
        return result[0];
    }

    @WrapMethod(method="dropQueuedChunkLoad(Lnet/minecraft/world/World;IILjava/lang/Runnable;)V",require=1,allow=1)
    private static void floatingpod$drop(World world,int x,int z,Runnable callback,Operation<Void> original)throws Throwable {
        if(!(world instanceof WorldServer serverWorld)||!(serverWorld.getChunkProvider().chunkLoader instanceof AnvilChunkLoader loader)){
            original.call(world,x,z,callback);return;
        }
        Optional<ManagedSourcePathCensus> selected=ProductionBootstrapInstallation.sourcePathCensusForLoader(loader);
        if(selected.isEmpty()){original.call(world,x,z,callback);return;}
        ManagedSourcePathCensus.ForgeCall call=selected.get().enterForgeCall(world,loader,
            world.getChunkProvider(),world.provider.getDimension(),x,z,ManagedSourcePathCensus.ForgeCallKind.DROP);
        floatingpod$invoke(call,()->original.call(world,x,z,callback));
    }

    @WrapOperation(method="syncChunkLoad(Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/storage/AnvilChunkLoader;Lnet/minecraft/world/gen/ChunkProviderServer;II)Lnet/minecraft/world/chunk/Chunk;",at=@At(value="INVOKE",target="Ljava/util/Map;remove(Ljava/lang/Object;)Ljava/lang/Object;",ordinal=0),require=1,allow=1)
    private static Object floatingpod$syncTaskRemoved(Map<?,?> map,Object key,Operation<Object> original)throws Throwable{
        if(map!=tasks)throw new IllegalStateException("Actual Forge ChunkIOExecutor task map required");
        Object removed=original.call(map,key);
        floatingpod$taskMapRemoved(removed);
        return removed;
    }

    @WrapOperation(method="dropQueuedChunkLoad(Lnet/minecraft/world/World;IILjava/lang/Runnable;)V",at=@At(value="INVOKE",target="Ljava/util/Map;remove(Ljava/lang/Object;)Ljava/lang/Object;",ordinal=0),require=1,allow=1)
    private static Object floatingpod$dropTaskRemoved(Map<?,?> map,Object key,Operation<Object> original)throws Throwable{
        if(map!=tasks)throw new IllegalStateException("Actual Forge ChunkIOExecutor task map required");
        Object candidate=map.get(key);
        ManagedSourcePathCensus.Token token=floatingpod$providerToken(candidate);
        if(token!=null&&candidate instanceof ManagedSourcePathCensus.SourceChunkIoProviderAccess access&&
                !access.floatingpod$hasCallbacks())token.cancellationRequested(candidate);
        Object removed=original.call(map,key);
        if(removed!=candidate)throw new IllegalStateException("Exact Forge source task changed during cancellation map removal");
        floatingpod$taskMapRemoved(removed);
        return removed;
    }

    @WrapOperation(method="dropQueuedChunkLoad(Lnet/minecraft/world/World;IILjava/lang/Runnable;)V",at=@At(value="INVOKE",target="Ljava/util/concurrent/ThreadPoolExecutor;remove(Ljava/lang/Runnable;)Z",ordinal=0),require=1,allow=1)
    private static boolean floatingpod$dropPoolRemoved(java.util.concurrent.ThreadPoolExecutor pool,Runnable runnable,
            Operation<Boolean> original)throws Throwable{
        boolean removed=original.call(pool,runnable);
        ManagedSourcePathCensus.Token token=floatingpod$providerToken(runnable);
        if(token!=null)token.cancellationAcknowledged(runnable,removed);
        return removed;
    }

    @WrapOperation(method="tick()V",at=@At(value="INVOKE",target="Ljava/util/Iterator;next()Ljava/lang/Object;",ordinal=0),require=1,allow=1)
    private static Object floatingpod$tickNext(Iterator<?> iterator,Operation<Object> original)throws Throwable{
        Object next=original.call(iterator);
        floatingpod$currentTickTask.set(next);
        return next;
    }

    @WrapOperation(method="tick()V",at=@At(value="INVOKE",target="Ljava/util/Iterator;remove()V",ordinal=0),require=1,allow=1)
    private static void floatingpod$tickTaskRemoved(Iterator<?> iterator,Operation<Void> original)throws Throwable{
        Object exactTask=floatingpod$currentTickTask.get();
        original.call(iterator);
        floatingpod$taskMapRemoved(exactTask);
        floatingpod$currentTickTask.remove();
    }

    @WrapMethod(method="tick()V",require=1,allow=1)
    private static void floatingpod$tick(Operation<Void> original)throws Throwable{
        try{original.call();}
        finally{floatingpod$currentTickTask.remove();}
    }

    @Unique private static ManagedSourcePathCensus.Token floatingpod$providerToken(Object candidate){
        return candidate instanceof ManagedSourcePathCensus.SourceChunkIoProviderAccess access?
            access.floatingpod$sourceTaskToken():null;
    }

    @Unique private static void floatingpod$taskMapRemoved(Object candidate){
        ManagedSourcePathCensus.Token token=floatingpod$providerToken(candidate);
        ManagedSourcePathCensus.ForgeCall call=ManagedSourcePathCensus.currentForgeCall();
        if(call!=null&&token!=null)call.taskMapRemoved(candidate);
        else if(token!=null)token.taskMapRemoved(candidate);
    }

    @FunctionalInterface private interface ThrowingCall { void run() throws Throwable; }
    private static void floatingpod$invoke(ManagedSourcePathCensus.ForgeCall call,ThrowingCall action)throws Throwable {
        Throwable primary=null;
        try{action.run();}catch(Throwable cause){primary=cause;throw cause;}
        finally{
            Throwable cleanupFailure=null;
            try{call.returned(primary);}catch(Throwable cleanup){cleanupFailure=cleanup;}
            try{call.close();}catch(Throwable cleanup){if(cleanupFailure==null)cleanupFailure=cleanup;else if(cleanupFailure!=cleanup)cleanupFailure.addSuppressed(cleanup);}
            if(cleanupFailure!=null){if(primary!=null){if(cleanupFailure!=primary)primary.addSuppressed(cleanupFailure);}else throw cleanupFailure;}
        }
    }
}

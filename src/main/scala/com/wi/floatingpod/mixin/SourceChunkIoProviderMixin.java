package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.bootstrap.ManagedSourcePathCensus;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraft.world.gen.ChunkProviderServer;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Coerce;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** Actual package-private Forge task owner: worker read, full Main callback, and callback identities. */
@Mixin(targets="net.minecraftforge.common.chunkio.ChunkIOProvider",remap=false)
abstract class SourceChunkIoProviderMixin implements ManagedSourcePathCensus.SourceChunkIoProviderAccess {
    @Unique private volatile ManagedSourcePathCensus.Token floatingpod$sourceTask;
    @Shadow public abstract boolean hasCallback();

    @Inject(method="<init>(Lnet/minecraftforge/common/chunkio/QueuedChunk;Lnet/minecraft/world/chunk/storage/AnvilChunkLoader;Lnet/minecraft/world/gen/ChunkProviderServer;)V",at=@At("RETURN"),require=1,allow=1)
    private void floatingpod$constructed(@Coerce Object queued,AnvilChunkLoader loader,ChunkProviderServer provider,CallbackInfo ci){
        ManagedSourcePathCensus.ForgeCall call=ManagedSourcePathCensus.currentForgeCall();
        if(call==null)return;
        ManagedSourcePathCensus.Token token=call.providerConstructed(this);
        floatingpod$bindSourceTaskToken(token);
    }

    @Override public final ManagedSourcePathCensus.Token floatingpod$sourceTaskToken(){return floatingpod$sourceTask;}
    @Override public final void floatingpod$bindSourceTaskToken(ManagedSourcePathCensus.Token exactToken){
        if(exactToken==null)throw new IllegalArgumentException("Exact Forge source task token required");
        if(floatingpod$sourceTask!=null&&floatingpod$sourceTask!=exactToken)
            throw new IllegalStateException("Forge provider census task token changed");
        floatingpod$sourceTask=exactToken;
    }
    @Override public final boolean floatingpod$hasCallbacks(){return hasCallback();}

    @WrapMethod(method="addCallback(Ljava/lang/Runnable;)V",require=1,allow=1)
    private void floatingpod$addCallback(Runnable callback,Operation<Void> original)throws Throwable{
        ManagedSourcePathCensus.Token token=floatingpod$sourceTask;
        ManagedSourcePathCensus.ForgeCall call=ManagedSourcePathCensus.currentForgeCall();
        if(token==null&&call==null){original.call(callback);return;}
        if(call!=null){
            if(token==null)throw new IllegalStateException("Managed Forge provider lacks its construction census token");
            token=call.requireProvider(this,token);
        }
        token.callbackAdded(this,callback);
        try{original.call(callback);}
        catch(Throwable cause){try{token.fail(cause);}catch(Throwable cleanup){if(cause!=cleanup)cause.addSuppressed(cleanup);}throw cause;}
    }

    @WrapOperation(method="removeCallback(Ljava/lang/Runnable;)V",at=@At(value="INVOKE",target="Ljava/util/concurrent/ConcurrentLinkedQueue;remove(Ljava/lang/Object;)Z",ordinal=0),require=1,allow=1)
    private boolean floatingpod$removeCallback(ConcurrentLinkedQueue<?> callbacks,Object callback,Operation<Boolean> original)throws Throwable{
        boolean removed=original.call(callbacks,callback);
        ManagedSourcePathCensus.Token token=floatingpod$sourceTask;
        if(token!=null)token.callbackRemoved(this,callback,removed);
        return removed;
    }

    @WrapMethod(method="run()V",require=1,allow=1)
    private void floatingpod$run( Operation<Void> original)throws Throwable{
        ManagedSourcePathCensus.Token token=floatingpod$sourceTask;
        if(token==null){original.call();return;}
        token.workerStarted(this);Throwable primary=null;
        try(ManagedSourcePathCensus.ExecutionScope execution=token.enterExecution()){
            original.call();
        }catch(Throwable cause){primary=cause;throw cause;}
        finally{
            try{token.workerReturned(this,primary);}
            catch(Throwable cleanup){if(primary!=null){if(cleanup!=primary)primary.addSuppressed(cleanup);}else throw cleanup;}
        }
    }

    @WrapMethod(method="syncCallback()V",require=1,allow=1)
    private void floatingpod$syncCallback(Operation<Void> original)throws Throwable{
        ManagedSourcePathCensus.Token token=floatingpod$sourceTask;
        if(token==null){original.call();return;}
        token.fullCallbackStarted(this);Throwable primary=null;
        try(ManagedSourcePathCensus.ExecutionScope execution=token.enterExecution()){
            original.call();
        }catch(Throwable cause){primary=cause;throw cause;}
        finally{
            try{token.fullCallbackReturned(this,primary);}
            catch(Throwable cleanup){if(primary!=null){if(cleanup!=primary)primary.addSuppressed(cleanup);}else throw cleanup;}
        }
    }

    @WrapOperation(method="runCallbacks()V",at=@At(value="INVOKE",target="Ljava/lang/Runnable;run()V",ordinal=0),require=1,allow=1)
    private void floatingpod$callback(Runnable callback,Operation<Void> original)throws Throwable{
        ManagedSourcePathCensus.Token task=floatingpod$sourceTask;
        if(task==null){original.call(callback);return;}
        task.callbackStarted(this,callback);Throwable primary=null;
        try{original.call(callback);}
        catch(Throwable cause){primary=cause;throw cause;}
        finally{
            try{task.callbackEnded(this,callback,primary);}
            catch(Throwable cleanup){if(primary!=null){if(cleanup!=primary)primary.addSuppressed(cleanup);}else throw cleanup;}
        }
    }
}

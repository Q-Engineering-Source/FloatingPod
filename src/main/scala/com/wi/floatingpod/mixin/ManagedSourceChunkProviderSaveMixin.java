package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision;
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderOperation;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.storage.IChunkLoader;
import net.minecraft.world.gen.ChunkProviderServer;
import net.minecraftforge.common.ForgeChunkManager;
import net.minecraftforge.common.DimensionManager;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;

/** Exact source-scoped gate before vanilla provider save/unload effects. */
@Mixin(ChunkProviderServer.class)
public abstract class ManagedSourceChunkProviderSaveMixin {
    @Shadow @Final private Set<Long> droppedChunks;
    @Shadow @Final public Long2ObjectMap<Chunk> loadedChunks;
    @Shadow @Final public WorldServer world;
    @Shadow @Final public IChunkLoader chunkLoader;
    @Shadow private void saveChunkExtraData(Chunk chunk) { throw new AssertionError(); }
    @Shadow private void saveChunkData(Chunk chunk) { throw new AssertionError(); }

    @WrapMethod(method="saveChunks(Z)Z",require=1,allow=1)
    private boolean floatingpod$saveChunks(boolean all,Operation<Boolean> original)throws Throwable {
        ChunkProviderServer provider=(ChunkProviderServer)(Object)this;
        Optional<ManagedOrdinarySaveAdmission> selected=
                ProductionBootstrapInstallation.ordinarySaveAdmissionForProvider(provider);
        if(selected.isEmpty())return original.call(all);
        ManagedOrdinarySaveAdmission admission=selected.get();
        if(!admission.requiresProviderScope())return original.call(all);
        return ProductionBootstrapInstallation.invokeProviderSave(provider,all,()->{
            try(ManagedOrdinarySaveAdmission.ProviderScope scope=admission.beginProviderScope(provider,world)){
                return floatingpod$saveChunksUnderScope(admission,provider,all,scope);
            }
        });
    }

    private boolean floatingpod$saveChunksUnderScope(ManagedOrdinarySaveAdmission admission,
            ChunkProviderServer provider,boolean all,
            ManagedOrdinarySaveAdmission.ProviderScope scope)throws Throwable {
        int saved=0;boolean held=false;
        int loadedCount=loadedChunks.size();
        long snapshotCharge=Math.addExact(512L,Math.multiplyExact((long)loadedCount,8L));
        try(AutoCloseable snapshotChargeHolder=scope.retainTemporaryCharge(snapshotCharge,
                "ChunkProviderServer.saveChunks snapshot list+values view+array+reference slots BEFORE allocation")){
            List<Chunk> snapshot=new ArrayList<>(loadedCount);
            int copied=0;
            for(Chunk chunk:loadedChunks.values()){
                if(copied>=loadedCount)
                    throw new IllegalStateException("Loaded chunk snapshot grew beyond its exact account reservation");
                snapshot.add(chunk);copied++;
            }
            if(copied!=loadedCount)throw new IllegalStateException("Loaded chunk snapshot size changed after exact account reservation");
            for(int index=0;index<snapshot.size();index++){
                Chunk chunk=snapshot.get(index);
                boolean saveData=chunk.needsSaving(all);
                if(!all&&!saveData)continue;
                try(ManagedOrdinarySaveAdmission.ProviderScope chunkScope=admission.beginProviderScope(provider,world)){
                    if(all&&floatingpod$decision(chunkScope,provider,chunk,ProviderOperation.SAVE_EXTRA_DATA)!=ProviderDecision.ALLOW){
                        held=true;continue;
                    }
                    if(saveData&&floatingpod$decision(chunkScope,provider,chunk,ProviderOperation.SAVE)!=ProviderDecision.ALLOW){
                        held=true;continue;
                    }
                    if(all)saveChunkExtraData(chunk);
                    if(saveData){
                        ManagedSourceChunkSaveStateAccess state=(ManagedSourceChunkSaveStateAccess)(Object)chunk;
                        long priorLastSaveTime=state.floatingpod$getLastSaveTime();
                        saveChunkData(chunk);
                        if(chunkScope.decision(chunk,ProviderOperation.SAVE)!=ProviderDecision.ALLOW){
                            state.floatingpod$setLastSaveTime(priorLastSaveTime);
                            held=true;continue;
                        }
                        chunk.setModified(false);
                        if(++saved==24&&!all)return false;
                    }
                }
            }
        }
        return !held&&!scope.hasHeld();
    }

    @WrapMethod(method="tick()Z",require=1,allow=1)
    private boolean floatingpod$tick(Operation<Boolean> original)throws Throwable {
        if(world.disableLevelSaving)return original.call();
        ChunkProviderServer provider=(ChunkProviderServer)(Object)this;
        Optional<ManagedOrdinarySaveAdmission> selected=
                ProductionBootstrapInstallation.ordinarySaveAdmissionForProvider(provider);
        if(selected.isEmpty())return original.call();
        ManagedOrdinarySaveAdmission admission=selected.get();
        if(!admission.requiresProviderScope())return original.call();
        try(ManagedOrdinarySaveAdmission.ProviderScope scope=admission.beginProviderScope(provider,world)){
            return floatingpod$tickUnderScope(admission,provider);
        }
    }

    private boolean floatingpod$tickUnderScope(ManagedOrdinarySaveAdmission admission,
            ChunkProviderServer provider)throws Throwable {
        if(!droppedChunks.isEmpty()){
            for(ChunkPos forced:world.getPersistentChunks().keySet())
                droppedChunks.remove(ChunkPos.asLong(forced.x,forced.z));
            Iterator<Long> iterator=droppedChunks.iterator();
            int unloaded=0,attempted=0;
            while(unloaded<100&&attempted<100&&iterator.hasNext()){
                attempted++;
                Long key=iterator.next();Chunk chunk=loadedChunks.get(key.longValue());boolean held=false;
                if(chunk!=null&&chunk.unloadQueued){
                    try(ManagedOrdinarySaveAdmission.ProviderScope chunkScope=admission.beginProviderScope(provider,world)){
                        ProviderDecision unload=floatingpod$decision(chunkScope,provider,chunk,ProviderOperation.UNLOAD);
                        ProviderDecision save=floatingpod$decision(chunkScope,provider,chunk,ProviderOperation.SAVE);
                        ProviderDecision extra=floatingpod$decision(chunkScope,provider,chunk,ProviderOperation.SAVE_EXTRA_DATA);
                        if(unload!=ProviderDecision.ALLOW||save!=ProviderDecision.ALLOW||extra!=ProviderDecision.ALLOW){
                            held=true;
                        }else{
                            ManagedSourceChunkSaveStateAccess state=(ManagedSourceChunkSaveStateAccess)(Object)chunk;
                            long priorLastSaveTime=state.floatingpod$getLastSaveTime();
                            chunk.onUnload();
                            ForgeChunkManager.putDormantChunk(ChunkPos.asLong(chunk.x,chunk.z),chunk);
                            saveChunkData(chunk);
                            if(chunkScope.decision(chunk,ProviderOperation.SAVE)!=ProviderDecision.ALLOW){
                                state.floatingpod$setLastSaveTime(priorLastSaveTime);
                                throw new IllegalStateException("Source provider SAVE decision changed after UNLOAD effects began");
                            }
                            saveChunkExtraData(chunk);
                            if(chunkScope.decision(chunk,ProviderOperation.SAVE_EXTRA_DATA)!=ProviderDecision.ALLOW)
                                throw new IllegalStateException("Source provider extra-data decision changed after UNLOAD effects began");
                            loadedChunks.remove(key.longValue());
                            unloaded++;
                        }
                    }
                }
                if(!held)iterator.remove();
            }
        }
        if(loadedChunks.isEmpty())DimensionManager.unloadWorld(world.provider.getDimension());
        chunkLoader.chunkTick();
        return false;
    }

    private static ProviderDecision floatingpod$decision(ManagedOrdinarySaveAdmission.ProviderScope scope,
            ChunkProviderServer provider,Chunk chunk,ProviderOperation operation)throws Throwable {
        try{
            ProviderDecision admissionDecision=scope.decision(chunk,operation);
            if(admissionDecision!=ProviderDecision.ALLOW)return admissionDecision;
            Optional<ProviderDecision> selected=ProductionBootstrapInstallation.decideSourceProviderOperation(
                    provider,chunk,operation);
            if(selected.isEmpty())throw new IllegalStateException("Exact managed source provider binding disappeared during save scope");
            return scope.observeDecision(chunk,operation,selected.get());
        }catch(Throwable cause){
            try{scope.failed(cause);}catch(Throwable cleanup){ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}
            throw cause;
        }
    }
}

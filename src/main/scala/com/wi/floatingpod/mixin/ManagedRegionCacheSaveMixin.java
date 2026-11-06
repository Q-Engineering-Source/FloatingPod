package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.server.save.ManagedRegionAccess;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import java.io.IOException;
import net.minecraft.world.chunk.storage.RegionFile;
import net.minecraft.world.chunk.storage.RegionFileCache;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;

/** actualRegion保留close责任；Map.clear不是ACK。仅短catch context，不是owner registry。 */
@Mixin(RegionFileCache.class)
public abstract class ManagedRegionCacheSaveMixin {
    @Unique private static final ThreadLocal<RegionFile> floatingpod$closeRegion=new ThreadLocal<>();
    @Unique private static final ThreadLocal<Throwable> floatingpod$closeCause=new ThreadLocal<>();

    @WrapMethod(method="clearRegionFileReferences()V",require=1,allow=1)
    private static void floatingpod$scope(Operation<Void> original)throws Throwable {
        synchronized(RegionFileCache.class){
            try{original.call();}finally{floatingpod$closeRegion.remove();floatingpod$closeCause.remove();}
        }
    }
    @WrapOperation(method="clearRegionFileReferences()V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFile;close()V",ordinal=0),require=1,allow=1)
    private static void floatingpod$close(RegionFile actual,Operation<Void> original)throws Throwable {
        floatingpod$closeRegion.remove();floatingpod$closeCause.remove();
        try{original.call(actual);}catch(Throwable cause){if(cause instanceof IOException){floatingpod$closeRegion.set(actual);floatingpod$closeCause.set(cause);}throw cause;}
    }
    @WrapOperation(method="clearRegionFileReferences()V",at=@At(value="INVOKE",target="Ljava/io/IOException;printStackTrace()V",ordinal=0),require=1,allow=1)
    private static void floatingpod$catch(IOException actualCause,Operation<Void> original)throws Throwable {
        RegionFile region=floatingpod$closeRegion.get();Throwable cause=floatingpod$closeCause.get();floatingpod$closeRegion.remove();floatingpod$closeCause.remove();
        try {
            if(region!=null && (Object)region instanceof ManagedRegionAccess managed)
                managed.floatingpod$cacheSwallowed(cause,actualCause);
        } catch(Throwable observation) {
            ManagedSourceSaveLedger.suppressSecondary(actualCause,observation);throw actualCause;
        }
        try{original.call(actualCause);}catch(Throwable secondary){ManagedSourceSaveLedger.suppressSecondary(actualCause,secondary);throw actualCause;}
    }
}

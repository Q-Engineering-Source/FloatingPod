package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;
import java.io.File;
import net.minecraft.util.datafix.DataFixer;
import net.minecraft.world.World;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** 首个 pre-Anvil 关口：保留精确启动 scope，仅为当前 Unprotected chunk 逐次签发 live token。 */
@Mixin(AnvilChunkLoader.class)
public abstract class ManagedBootSaveAdmissionMixin {
    @Inject(
        method="<init>(Ljava/io/File;Lnet/minecraft/util/datafix/DataFixer;)V",
        at=@At("RETURN"),
        require=1,
        allow=1
    )
    private void floatingpod$enrollClosedBootLoader(File directory,DataFixer fixer,CallbackInfo ci) {
        ProductionBootstrapInstallation.observeAnvilChunkLoader((AnvilChunkLoader)(Object)this);
    }

    @WrapMethod(
        method="saveChunk(Lnet/minecraft/world/World;Lnet/minecraft/world/chunk/Chunk;)V",
        require=1,
        allow=1
    )
    private void floatingpod$closedBootSaveAdmission(World world,Chunk chunk,Operation<Void> original) throws Throwable {
        ProductionBootstrapInstallation.invokeAnvilSave(
            (AnvilChunkLoader)(Object)this,
            world,chunk,
            () -> { original.call(world,chunk); }
        );
    }
}

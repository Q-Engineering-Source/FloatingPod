package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;
import net.minecraft.world.chunk.storage.AnvilSaveHandler;
import net.minecraft.world.storage.ThreadedFileIOBase;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;

/** 原 wait/cache 清理保留；managed wait 中断必须越过 vanilla 的 InterruptedException 吞错点。 */
@Mixin(AnvilSaveHandler.class)
public abstract class ManagedAnvilSaveHandlerMixin {
    @WrapOperation(method="flush()V",at=@At(value="INVOKE",target="Lnet/minecraft/world/storage/ThreadedFileIOBase;waitForFinish()V",ordinal=0),require=1,allow=1)
    private void floatingpod$waitOutcome(ThreadedFileIOBase actualIo,Operation<Void> original)throws Throwable{
        ProductionBootstrapInstallation.invokeAnvilFlushWait((AnvilSaveHandler)(Object)this,()->original.call(actualIo));
    }
    @WrapMethod(method="flush()V",require=1,allow=1)
    private void floatingpod$requireSaveOutcome(Operation<Void> original)throws Throwable{
        ProductionBootstrapInstallation.invokeAnvilHandlerFlush((AnvilSaveHandler)(Object)this,()->original.call());
    }
}
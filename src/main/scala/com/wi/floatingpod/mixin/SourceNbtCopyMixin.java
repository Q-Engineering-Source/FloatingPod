package com.wi.floatingpod.mixin;

import com.wi.floatingpod.server.source.SourceReadOperation;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import net.minecraft.nbt.NBTTagCompound;
import org.spongepowered.asm.mixin.Mixin;

/** 只记录受控 queued 根 copy 调用中实际 compound 输入/返回引用，不排除外部mutable mutator。 */
@Mixin(NBTTagCompound.class)
abstract class SourceNbtCopyMixin {
    @WrapMethod(method="copy()Lnet/minecraft/nbt/NBTTagCompound;",require=1,allow=1)
    private NBTTagCompound floatingpod$sourceCopy(Operation<NBTTagCompound> original){
        NBTTagCompound result=original.call();
        SourceReadOperation.actualCompoundCopyReturned((NBTTagCompound)(Object)this,result);
        return result;
    }
}

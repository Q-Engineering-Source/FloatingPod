package com.wi.floatingpod.mixin;

import com.wi.floatingpod.server.source.SourceDomainCapture;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.DimensionType;
import net.minecraft.world.WorldServer;
import net.minecraftforge.common.DimensionManager;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/** 仅标实际表 mutation 世代，不发来源域资格。 */
@Mixin(DimensionManager.class)
abstract class SourceDimensionManagerMixin {
    @Inject(method="getStaticDimensionIDs()[Ljava/lang/Integer;",at=@At("RETURN"),require=1,allow=1)
    private static void floatingpod$observed(CallbackInfoReturnable<Integer[]> result){SourceDomainCapture.observeDimensionRegistryRead();}
    @WrapMethod(method="registerDimension(ILnet/minecraft/world/DimensionType;)V",require=1,allow=1)
    private static void floatingpod$register(int id,DimensionType type,Operation<Void> original)throws Throwable {
        SourceDomainCapture.beginDimensionRegistryMutation();Throwable primary=null;
        try{com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.noteSourceDimensionMutation();original.call(id,type);}catch(Throwable cause){primary=cause;throw cause;}
        finally{finish(primary);}
    }
    @WrapMethod(method="unregisterDimension(I)V",require=1,allow=1)
    private static void floatingpod$unregister(int id,Operation<Void> original)throws Throwable {
        SourceDomainCapture.beginDimensionRegistryMutation();Throwable primary=null;
        try{com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.noteSourceDimensionMutation();original.call(id);}catch(Throwable cause){primary=cause;throw cause;}
        finally{finish(primary);}
    }
    @WrapMethod(method="setWorld(ILnet/minecraft/world/WorldServer;Lnet/minecraft/server/MinecraftServer;)V",require=1,allow=1)
    private static void floatingpod$setWorld(int id,WorldServer world,MinecraftServer server,Operation<Void> original)throws Throwable {
        SourceDomainCapture.beginDimensionRegistryMutation();Throwable primary=null;
        try{
            com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.noteSourceDimensionMutation();
            if(world!=null)com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.requireSourceWorldMutationTarget(server,world,id);
            original.call(id,world,server);
            if(world!=null&&server!=null)com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.observeSourceWorld(server,world,id);
        }catch(Throwable cause){primary=cause;throw cause;}
        finally{finish(primary);}
    }
    @Unique private static void finish(Throwable primary)throws Throwable {
        try{SourceDomainCapture.endDimensionRegistryMutation(primary);}
        catch(Throwable cleanup){if(primary!=null){com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);}else throw cleanup;}
    }
}

package com.wi.floatingpod.mixin;

import com.wi.floatingpod.server.source.SourceReadOperation;
import com.wi.floatingpod.server.source.SourceForestCodec;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.entity.FloatingPodEntity;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import net.minecraft.entity.Entity;
import net.minecraft.entity.EntityList;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.util.ResourceLocation;
import net.minecraft.world.World;
import net.minecraftforge.fml.common.registry.EntityEntry;
import net.minecraftforge.registries.IForgeRegistry;
import net.minecraftforge.registries.IForgeRegistryEntry;
import org.apache.logging.log4j.Logger;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;

/** 在 vanilla catch/log/null 之前保留实际inner cause；非选定NBT调用被mask隔离。 */
@Mixin(EntityList.class)
abstract class SourceEntityDecodeMixin {
    @WrapMethod(method="createEntityFromNBT(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",require=1,allow=1)
    private static Entity floatingpod$entry(NBTTagCompound node,World world,Operation<Entity> original)throws Throwable {
        try(SourceReadOperation.Mask mask=SourceReadOperation.enterEntityList(node,world)){return original.call(node,world);}
    }
    @WrapMethod(method="createEntityByIDFromName(Lnet/minecraft/util/ResourceLocation;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",require=1,allow=1)
    private static Entity floatingpod$byId(ResourceLocation key,World world,Operation<Entity> original)throws Throwable {
        try(SourceReadOperation.Mask mask=SourceReadOperation.enterLookup(key.toString(),world)){return original.call(key,world);}
    }
    @WrapOperation(method="createEntityByIDFromName(Lnet/minecraft/util/ResourceLocation;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",at=@At(value="INVOKE",target="Lnet/minecraftforge/registries/IForgeRegistry;getValue(Lnet/minecraft/util/ResourceLocation;)Lnet/minecraftforge/registries/IForgeRegistryEntry;",ordinal=0),require=1,allow=1)
    private static IForgeRegistryEntry<?> floatingpod$registry(IForgeRegistry<?> registry,ResourceLocation key,Operation<IForgeRegistryEntry<?>> original)throws Throwable {
        IForgeRegistryEntry<?> returned=original.call(registry,key);SourceReadOperation.DecoderScope scope=SourceReadOperation.lookup();
        if(scope!=null){scope.registryLookupReturned(registry,key.toString(),returned);if(returned!=null&&!(returned instanceof EntityEntry))throw new IllegalStateException("Actual entity registry returned foreign entry");
            EntityEntry entry=(EntityEntry)returned;Class<?> entityClass=entry==null?null:entry.getEntityClass();
            SourceForestCodec.Classification classification=entry==null||entityClass==null?SourceForestCodec.Classification.UNKNOWN:entityClass==FloatingPodEntity.class?SourceForestCodec.Classification.FLOATING_POD:SourceForestCodec.Classification.OTHER;
            scope.registryClassified(entry==null||entry.getRegistryName()==null?"":entry.getRegistryName().toString(),entityClass==null?"":entityClass.getName(),classification);
        }return returned;
    }
    @WrapOperation(method="createEntityByIDFromName(Lnet/minecraft/util/ResourceLocation;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",at=@At(value="INVOKE",target="Lnet/minecraftforge/fml/common/registry/EntityEntry;newInstance(Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",ordinal=0),require=1,allow=1)
    private static Entity floatingpod$factory(EntityEntry entry,World world,Operation<Entity> original)throws Throwable {
        SourceReadOperation.DecoderScope scope=SourceReadOperation.lookup();if(scope==null)return original.call(entry,world);
        scope.factoryBeginning(entry,world);try{Entity body=original.call(entry,world);scope.factoryReturned(body);return body;}catch(Throwable cause){scope.factoryFailed(cause);throw cause;}
    }
    @WrapOperation(method="createEntityFromNBT(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",at=@At(value="INVOKE",target="Lnet/minecraft/entity/Entity;readFromNBT(Lnet/minecraft/nbt/NBTTagCompound;)V",ordinal=0),require=1,allow=1)
    private static void floatingpod$read(Entity body,NBTTagCompound node,Operation<Void> original)throws Throwable {
        SourceReadOperation.DecoderScope scope=SourceReadOperation.inner();if(scope==null){original.call(body,node);return;}
        scope.readBeginning(body,node);try{original.call(body,node);scope.readReturned();}catch(Throwable cause){scope.readFailed(cause);throw cause;}
        NBTTagCompound target=new NBTTagCompound();
        try{scope.serializationBeginning(body,target);NBTTagCompound actual=body.writeToNBT(target);scope.serializationReturned(body,actual);}
        catch(Throwable cause){scope.serializationFailed(cause);throw cause;}
    }
    @WrapOperation(method="createEntityFromNBT(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",at=@At(value="INVOKE",target="Lnet/minecraft/entity/Entity;getName()Ljava/lang/String;",ordinal=0),require=1,allow=1)
    private static String floatingpod$diagnosticName(Entity body,Operation<String> original)throws Throwable {
        try{return original.call(body);}catch(Throwable secondary){SourceReadOperation.DecoderScope scope=SourceReadOperation.inner();Throwable primary=scope==null?null:scope.primary();if(primary!=null){ManagedSourceSaveLedger.suppressSecondary(primary,secondary);throw primary;}throw secondary;}
    }
    @WrapOperation(method="createEntityFromNBT(Lnet/minecraft/nbt/NBTTagCompound;Lnet/minecraft/world/World;)Lnet/minecraft/entity/Entity;",at=@At(value="INVOKE",target="Lorg/apache/logging/log4j/Logger;error(Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;)V",ordinal=0),require=1,allow=1)
    private static void floatingpod$diagnosticLog(Logger logger,String message,Object id,Object name,Object cause,Operation<Void> original)throws Throwable {
        try{original.call(logger,message,id,name,cause);}catch(Throwable secondary){SourceReadOperation.DecoderScope scope=SourceReadOperation.inner();Throwable primary=scope==null?null:scope.primary();if(primary!=null){ManagedSourceSaveLedger.suppressSecondary(primary,secondary);throw primary;}throw secondary;}
    }
}

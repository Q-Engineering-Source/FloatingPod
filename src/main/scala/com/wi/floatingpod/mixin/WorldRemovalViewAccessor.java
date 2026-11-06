package com.wi.floatingpod.mixin;

import net.minecraft.entity.Entity;
import net.minecraft.world.World;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

import java.util.List;

/** 仅向 Main removal observer 暴露卸载队列观察；不提供 setter 或修改方法。 */
@Mixin(World.class)
public interface WorldRemovalViewAccessor {
    @Accessor("unloadedEntityList")
    List<Entity> floatingpod$getUnloadedEntityList();
}

package com.wi.floatingpod.mixin;

import net.minecraft.entity.EntityTracker;
import net.minecraft.entity.EntityTrackerEntry;
import net.minecraft.util.IntHashMap;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

import java.util.Set;

/** 捕获 ID slot 与全 entry set 的只读观察入口；observer 不 mutate/untrack。 */
@Mixin(EntityTracker.class)
public interface EntityTrackerRemovalViewAccessor {
    @Accessor("entries")
    Set<EntityTrackerEntry> floatingpod$getEntries();

    @Accessor("trackedEntityHashTable")
    IntHashMap<EntityTrackerEntry> floatingpod$getTrackedEntityHashTable();
}

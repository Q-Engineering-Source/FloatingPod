package com.wi.floatingpod.mixin;

import net.minecraft.world.chunk.Chunk;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

/** Public cross-package access bridge for preserving the actual vanilla save timestamp on HOLD. */
@Mixin(Chunk.class)
public interface ManagedSourceChunkSaveStateAccess {
    @Accessor("lastSaveTime") long floatingpod$getLastSaveTime();
    @Accessor("lastSaveTime") void floatingpod$setLastSaveTime(long value);
}

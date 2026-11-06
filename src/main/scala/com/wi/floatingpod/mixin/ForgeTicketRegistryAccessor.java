package com.wi.floatingpod.mixin;

import com.google.common.collect.Multimap;
import net.minecraft.world.World;
import net.minecraftforge.common.ForgeChunkManager;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

import java.util.Map;

/** Forge normal ticket 的只读登记观察；不得修改返回的框架集合。 */
@Mixin(value = ForgeChunkManager.class, remap = false)
public interface ForgeTicketRegistryAccessor {
    @Accessor(value = "tickets", remap = false)
    // 重映射可能去掉显式 target；合法 getter 名仍须精确推导 tickets。
    static Map<World, Multimap<String, ForgeChunkManager.Ticket>> getTickets() {
        throw new AssertionError("Forge ticket registry accessor was not transformed");
    }
}

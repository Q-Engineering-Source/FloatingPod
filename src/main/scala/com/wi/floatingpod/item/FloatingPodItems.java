package com.wi.floatingpod.item;

import com.wi.floatingpod.FloatingPod;
import net.minecraft.item.Item;
import net.minecraftforge.event.RegistryEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;

@Mod.EventBusSubscriber(modid = FloatingPod.MOD_ID)
public final class FloatingPodItems {
    public static final FloatingPodDebugItem POD_DEBUG_TOOL = new FloatingPodDebugItem();
    private FloatingPodItems() { }
    @SubscribeEvent
    public static void onRegisterItems(RegistryEvent.Register<Item> event) {
        event.getRegistry().register(POD_DEBUG_TOOL);
    }
}

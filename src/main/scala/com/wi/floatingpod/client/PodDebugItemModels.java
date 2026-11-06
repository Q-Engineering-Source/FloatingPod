package com.wi.floatingpod.client;

import com.wi.floatingpod.FloatingPod;
import com.wi.floatingpod.item.FloatingPodItems;
import net.minecraft.client.renderer.block.model.ModelResourceLocation;
import net.minecraftforge.client.event.ModelRegistryEvent;
import net.minecraftforge.client.model.ModelLoader;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;
import net.minecraftforge.fml.relauncher.Side;
import net.minecraftforge.fml.relauncher.SideOnly;

@SideOnly(Side.CLIENT)
@Mod.EventBusSubscriber(modid = FloatingPod.MOD_ID, value = Side.CLIENT)
public final class PodDebugItemModels {
    private PodDebugItemModels() { }
    @SubscribeEvent
    public static void onModelRegistry(ModelRegistryEvent event) {
        ModelLoader.setCustomModelResourceLocation(FloatingPodItems.POD_DEBUG_TOOL, 0,
                new ModelResourceLocation("floatingpod:pod_debug_tool", "inventory"));
    }
}

package com.wi.floatingpod.item;

import com.wi.floatingpod.FloatingPod;
import com.wi.floatingpod.debug.DebugItemInteractions;
import net.minecraft.creativetab.CreativeTabs;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.item.Item;
import net.minecraft.util.EnumActionResult;
import net.minecraft.util.EnumFacing;
import net.minecraft.util.EnumHand;
import net.minecraft.util.ResourceLocation;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;

public final class FloatingPodDebugItem extends Item {
    public FloatingPodDebugItem() {
        setRegistryName(new ResourceLocation(FloatingPod.MOD_ID, "pod_debug_tool"));
        setTranslationKey("floatingpod.pod_debug_tool");
        setCreativeTab(CreativeTabs.TOOLS);
        setMaxStackSize(1);
    }
    @Override
    public EnumActionResult onItemUseFirst(EntityPlayer player, World world, BlockPos pos,
            EnumFacing face, float hitX, float hitY, float hitZ, EnumHand hand) {
        return DebugItemInteractions.handleBlockUse(player, world, pos, face, hand);
    }
}

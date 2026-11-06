package com.wi.floatingpod.debug;

import com.wi.floatingpod.entity.FloatingPodEntity;
import net.minecraft.world.WorldServer;

/** 两个真实输入入口共用的 Main 保管接口；游戏引用不进入 worker。 */
public interface MainPodRequestContext {
    WorldServer world();
    void assertCurrent();
    void retainCreatedBody(FloatingPodEntity body);
    void feedbackCommitted(DebugItemEndpoint.View view);
    void feedbackFailed();
}

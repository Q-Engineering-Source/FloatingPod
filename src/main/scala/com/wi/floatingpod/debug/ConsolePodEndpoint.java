package com.wi.floatingpod.debug;

import net.minecraft.command.ICommandSender;
import net.minecraft.server.MinecraftServer;
import java.util.UUID;

/** 本机控制台入口；PENDING 只表示接纳，COMMITTED 由原异步流程最终反馈。 */
public interface ConsolePodEndpoint {
    boolean isReady();
    DebugItemEndpoint.Reply submitConsole(MinecraftServer server, ICommandSender sender, UUID operation,
                                         int dimension, DebugItemEndpoint.Position position);
    DebugItemEndpoint.Reply resumeConsole(MinecraftServer server, ICommandSender sender, UUID originalCreate);
    State inspectConsole(MinecraftServer server, ICommandSender sender, UUID pod);

    record State(UUID pod, UUID owner, String mode, boolean active, String phase, long version,
                 int dimension, double x, double y, double z) { }
}

package com.wi.floatingpod.debug;

import net.minecraft.command.CommandBase;
import net.minecraft.command.CommandException;
import net.minecraft.command.ICommandSender;
import net.minecraft.command.WrongUsageException;
import net.minecraft.server.MinecraftServer;
import net.minecraft.util.text.TextComponentString;
import org.apache.logging.log4j.LogManager;
import java.util.UUID;

/** 仅本机控制台。业务提交由已发布的原 Ready endpoint 执行。 */
public final class FloatingPodCommand extends CommandBase {
    public String getName() { return "floatingpod"; }
    public String getUsage(ICommandSender sender) {
        return "floatingpod create <dimension> [<x> <y> <z> [yaw]] | floatingpod resume <originalCreateUUID> | floatingpod status <podUUID>";
    }
    public boolean checkPermission(MinecraftServer server,ICommandSender sender) { return sender == server; }
    public void execute(MinecraftServer server,ICommandSender sender,String[] args) throws CommandException {
        PodDebugRuntimeRegistry.requireMain(server);
        if (sender != server) throw new CommandException("FP_POD_COMMAND code=LOCAL_CONSOLE_REQUIRED");
        ConsolePodCommandArguments parsed;
        try { parsed=ConsolePodCommandArguments.parse(args); }
        catch (IllegalArgumentException invalid) { throw new WrongUsageException(getUsage(sender)); }
        var lease=PodDebugRuntimeRegistry.current(server);
        if (lease == null || !(lease.endpoint() instanceof ConsolePodEndpoint endpoint) || !endpoint.isReady())
            throw new CommandException("FP_POD_COMMAND code=UNAVAILABLE");
        switch (parsed) {
            case ConsolePodCommandArguments.Create create -> {
                UUID operation=UUID.randomUUID();
                try {
                    var reply=endpoint.submitConsole(server,sender,operation,create.dimension(),create.position());
                    sender.sendMessage(new TextComponentString("FP_POD_CREATE code="+reply.status()+" operation="+operation));
                } catch (MainConsoleCreateContext.InvalidPlacement invalid) {
                    // 参数/位置拒绝发生在资源保留前；内部完整原因留受控日志。
                    LogManager.getLogger(FloatingPodCommand.class).warn("Console Pod create refused: operation="+operation,invalid);
                    throw new CommandException("FP_POD_CREATE code=REJECTED operation="+operation);
                }
            }
            case ConsolePodCommandArguments.Status status -> {
                var state=endpoint.inspectConsole(server,sender,status.pod());
                sender.sendMessage(new TextComponentString(state==null?"FP_POD_STATUS code=NOT_FOUND pod="+status.pod():
                        "FP_POD_STATUS code=FOUND pod="+state.pod()+" owner="+(state.owner()==null?"unbound":state.owner())
                        +" mode="+state.mode()+" active="+state.active()+" phase="+state.phase()+" version="+state.version()
                        +" dimension="+state.dimension()+" x="+state.x()+" y="+state.y()+" z="+state.z()));
            }
            case ConsolePodCommandArguments.Resume resume -> {
                try {
                    var reply=endpoint.resumeConsole(server,sender,resume.originalCreate());
                    sender.sendMessage(new TextComponentString("FP_POD_RESUME code="+reply.status()
                            +" operation="+resume.originalCreate()));
                } catch (MainConsoleCreateContext.InvalidPlacement invalid) {
                    LogManager.getLogger(FloatingPodCommand.class).warn(
                            "Console original Pod resume refused: operation="+resume.originalCreate(),invalid);
                    throw new CommandException("FP_POD_RESUME code=REJECTED operation="+resume.originalCreate());
                }
            }
        }
    }
}

package com.wi.floatingpod.debug;

import java.util.UUID;

/** 纯参数解析；不伪造玩家、世界或成功的业务结果。 */
public sealed interface ConsolePodCommandArguments {
    record Create(int dimension, DebugItemEndpoint.Position position) implements ConsolePodCommandArguments { }
    record Status(UUID pod) implements ConsolePodCommandArguments { }
    record Resume(UUID originalCreate) implements ConsolePodCommandArguments { }
    static ConsolePodCommandArguments parse(String[] args) {
        if (args == null || args.length == 0) throw new IllegalArgumentException("Missing subcommand");
        if (("status".equals(args[0]) || "resume".equals(args[0])) && args.length == 2) {
            UUID pod=UUID.fromString(args[1]);
            if (!pod.toString().equalsIgnoreCase(args[1])) throw new IllegalArgumentException("Canonical UUID required");
            return "status".equals(args[0]) ? new Status(pod) : new Resume(pod);
        }
        if ("create".equals(args[0]) && (args.length == 2 || args.length == 5 || args.length == 6)) {
            int dimension=Integer.parseInt(args[1]);
            if (args.length == 2) return new Create(dimension,null);
            return new Create(dimension,new DebugItemEndpoint.Position(Double.parseDouble(args[2]),
                    Double.parseDouble(args[3]),Double.parseDouble(args[4]),args.length==6?Float.parseFloat(args[5]):0F));
        }
        throw new IllegalArgumentException("Invalid floatingpod arguments");
    }
}

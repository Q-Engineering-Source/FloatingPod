package com.wi.floatingpod.debug;

import java.util.Objects;
import java.util.UUID;

/** Main 操作来源；本地控制台不借用玩家 identity、session 或 gesture。不得交给 IO。 */
public sealed interface MainPodOperationRequest {
    enum OriginKind { PLAYER_SESSION, LOCAL_CONSOLE_BOOT }
    record Origin(OriginKind kind, UUID identity) {
        public Origin { Objects.requireNonNull(kind); Objects.requireNonNull(identity); }
    }
    UUID boot();
    UUID operation();
    Origin origin();
    DebugItemEndpoint.Action action();
    String worldName();
    int dimension();
    DebugItemEndpoint.Position placement();
    default DebugItemEndpoint.Request playerRequest() {
        throw new IllegalStateException("This operation has no player request");
    }

    record Player(DebugItemEndpoint.Request playerRequest) implements MainPodOperationRequest {
        public Player { Objects.requireNonNull(playerRequest); }
        public UUID boot() { return playerRequest.identity().boot(); }
        public UUID operation() { return playerRequest.operation(); }
        public Origin origin() { return new Origin(OriginKind.PLAYER_SESSION, playerRequest.identity().session()); }
        public DebugItemEndpoint.Action action() { return playerRequest.action(); }
        public String worldName() { return playerRequest.identity().worldName(); }
        public int dimension() { return playerRequest.identity().dimension(); }
        public DebugItemEndpoint.Position placement() { return playerRequest.placement(); }
    }

    record ConsoleCreate(UUID boot, UUID operation, String worldName, int dimension,
                         DebugItemEndpoint.Position placement) implements MainPodOperationRequest {
        public ConsoleCreate {
            Objects.requireNonNull(boot); Objects.requireNonNull(operation); Objects.requireNonNull(placement);
            if (worldName == null || worldName.isBlank()) throw new IllegalArgumentException("Actual world required");
        }
        public Origin origin() { return new Origin(OriginKind.LOCAL_CONSOLE_BOOT, boot); }
        public DebugItemEndpoint.Action action() { return DebugItemEndpoint.Action.PLACE; }
    }
}

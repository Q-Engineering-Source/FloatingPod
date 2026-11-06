package com.wi.floatingpod.entity;

import java.util.Objects;
import java.util.UUID;

/** 保存于实体物理artifact NBT子树中的不可变身份。 */
public record PodArtifactIdentity(UUID bindingUuid, UUID attemptUuid, Origin origin) {
    public enum Origin {
        OWNED_RELEASE,
        UNBOUND_ADOPTION,
        DEBUG_ITEM_CREATE
    }

    public PodArtifactIdentity {
        Objects.requireNonNull(bindingUuid, "bindingUuid");
        Objects.requireNonNull(attemptUuid, "attemptUuid");
        Objects.requireNonNull(origin, "origin");
    }
}

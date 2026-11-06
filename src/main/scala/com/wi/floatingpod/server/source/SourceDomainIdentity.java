package com.wi.floatingpod.server.source;

import java.util.Objects;
import java.util.UUID;

/** 同一实际 Main session 发出的身份投影；没有完整域或文件身份资格。 */
public final class SourceDomainIdentity {
    private final UUID sessionId, bootId;
    private final long bootGeneration;
    private SourceDomainIdentity(UUID sessionId, UUID bootId, long bootGeneration) {
        this.sessionId=Objects.requireNonNull(sessionId);this.bootId=Objects.requireNonNull(bootId);
        if(bootGeneration<0)throw new IllegalArgumentException("Negative boot generation");
        this.bootGeneration=bootGeneration;
    }
    static SourceDomainIdentity issue(UUID bootId,long generation) {return new SourceDomainIdentity(UUID.randomUUID(),bootId,generation);}
    public UUID sessionId(){return sessionId;}
    public UUID bootId(){return bootId;}
    public long bootGeneration(){return bootGeneration;}
}

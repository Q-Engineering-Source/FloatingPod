package com.wi.floatingpod.bootstrap;

import java.util.Objects;
import java.util.UUID;

/**
 * 同一 server/boot 的停止请求邮箱。请求不改变运行阶段，也不证明资源已关闭。
 * 实际 Installation 装配及 Main 的真实调度负责后续排空；请求本身不签发停止许可。
 */
public final class DedicatedNormalStopBridge {
    public static final class Request {
        private final DedicatedNormalStopBridge owner;
        private Request(DedicatedNormalStopBridge owner) { this.owner = owner; }
    }

    private final Object server;
    private final Thread main;
    private final UUID bootId;
    private final long bootGeneration;
    private Request requested;
    private boolean consumed;
    private final class FreshCompletionExitPermit {
        private final Request request;
        private boolean inFlight=true;
        private boolean originalEntryConsumed;
        private boolean returned;
        private FreshCompletionExitPermit(Request request) { this.request=request; }
    }
    private FreshCompletionExitPermit freshExitPermit;

    DedicatedNormalStopBridge(Object server, Thread main, UUID bootId, long bootGeneration) {
        this.server = Objects.requireNonNull(server, "actual server");
        this.main = Objects.requireNonNull(main, "actual Main");
        this.bootId = Objects.requireNonNull(bootId, "actual boot identity");
        if (bootGeneration <= 0L) throw new IllegalArgumentException("Positive boot generation required");
        this.bootGeneration = bootGeneration;
    }

    /** 外线程只登记请求；短锁内不调用 Main、world、IO 或任何用户回调。 */
    synchronized void request(Object actualServer) {
        if (actualServer != server) throw new IllegalArgumentException("Stop request belongs to another server");
        if (requested == null) requested = new Request(this);
    }

    /** 只交出一次原请求；重复请求不能重建 census、operation 或资源 owner。 */
    synchronized Request pollOnMain() {
        requireMain();
        if (requested == null || consumed) return null;
        consumed = true;
        return requested;
    }

    synchronized void requireOriginalOnMain(Request request) {
        requireMain();
        if (request == null || request.owner != this || request != requested || !consumed)
            throw new IllegalArgumentException("Original consumed stop request required");
    }

    /** 仅 Installation 在原生产 completion 资格核定后调用；不接收任意 shutdown 回调。 */
    synchronized void beginFreshInvocation(Request request) {
        requireOriginalOnMain(request);
        if (freshExitPermit!=null) throw new IllegalStateException("Fresh original shutdown invocation is one-shot");
        freshExitPermit=new FreshCompletionExitPermit(request);
    }
    synchronized boolean consumeOriginalEntry(Object actualServer) {
        if (actualServer != server || Thread.currentThread() != main || freshExitPermit==null ||
                !freshExitPermit.inFlight || freshExitPermit.originalEntryConsumed)
            return false;
        freshExitPermit.originalEntryConsumed=true;
        return true;
    }
    synchronized void finishFreshInvocation(Request request, boolean returned) {
        requireOriginalOnMain(request);
        if(freshExitPermit==null || freshExitPermit.request!=request || !freshExitPermit.inFlight)
            throw new IllegalStateException("Original in-flight Fresh exit permit required");
        freshExitPermit.inFlight=false;
        freshExitPermit.returned=returned;
        if (!freshExitPermit.originalEntryConsumed) throw new IllegalStateException("Actual shutdown hook did not consume its original Fresh invocation");
    }
    synchronized void requireFreshInvocation(Request request) {
        requireOriginalOnMain(request);
        if (freshExitPermit==null || freshExitPermit.request!=request || !freshExitPermit.originalEntryConsumed ||
                !freshExitPermit.returned || freshExitPermit.inFlight)
            throw new IllegalStateException("Original Fresh shutdown invocation was not completed");
    }

    private void requireMain() {
        if (Thread.currentThread() != main)
            throw new IllegalStateException("Stop request consumption requires original Main");
    }
}

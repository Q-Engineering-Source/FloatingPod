package com.wi.floatingpod.bootstrap;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 早期 Java-only 控制面。Launcher 与 game Main 从不等待 preparation/JDBC/worker exit。
 * 这里不创建 server thread、不实现 SQL、不发布 Ready，也不生成 IO exit/clean 证明。
 */
public final class DedicatedBootstrapBridge {
    public enum Phase { NEW, PREPARING, PREPARED, TRANSFERRED, CLAIMING, CLAIMED, TERMINAL_FAILED }
    public enum CancellationDecision { CANCEL_REQUESTED, CLAIM_IN_PROGRESS, ALREADY_CLAIMED, ALREADY_FAILED }
    public enum ClaimDisposition { NOT_ISSUED, PENDING, IN_PROGRESS, CLAIMED, ABORTED, CLAIMED_AFTER_FAILURE, ABORTED_AFTER_FAILURE }
    /** 仅 owner 上的有界 signal wait；生产直接使用真实 CountDownLatch.await。 */
    @FunctionalInterface interface ClaimWait {
        boolean await(CountDownLatch signal, Duration limit) throws InterruptedException;
    }
    public record Limits(Duration preparation, Duration claim, Duration finalization) {
        public Limits {
            requirePositive(preparation); requirePositive(claim); requirePositive(finalization);
        }
        private static void requirePositive(Duration value) {
            if (value == null || value.isZero() || value.isNegative() || value.toNanos() <= 0)
                throw new IllegalArgumentException("Bootstrap limits must be finite positive durations");
        }
    }
    /** 在 boot-control owner 上装配；实际 discover/pins/SQL/resource adapter 尚须 Main 接线。 */
    public interface Factory { Callbacks open(Object actualServer, DedicatedBootstrapBridge control) throws Exception; }
    public interface Callbacks {
        /** 返回实际 preparation，adapter 负责 cancellation/迟到结果资源；不能返回默认成功。 */
        CompletionStage<Object> prepare() throws Exception;
        /** owner 调真实 OwnedSerialIo.issueOwnerTransfer，原始 opaque receipt 必须原样返回。 */
        Object issueTransfer(Object preparation, Thread exactNewTarget) throws Exception;
        /** exact target runHEAD 调真实 claimOwnerTransfer；禁止 IO wait/JDBC 或额外成功步骤。 */
        void claim(Object preparation, Object exactReceipt) throws Exception;
        /** exact preclaim target，或 owner 且 target NEW/TERMINATED 调实际 abortOwnerTransfer。 */
        void abortTransfer(Object preparation, Object exactReceipt, Throwable originalCause) throws Exception;
        /** owner 发起 drain/quiesce；adapter 的另 off-owner finalizer awaitExit，返回有界处理信号。 */
        CompletionStage<Void> finalizeFailure(Object preparation, Object exactReceipt, Throwable originalCause) throws Exception;
        /** 唯一失败责任：保存原 cause/scope/receipt；只可结束其自己的 dedicated instance。 */
        void failedStartupTermination(FailureReport report) throws Exception;
    }
    /** finalization signal 不是资源关闭/IO exit/durability证明；未消费 receipt 必须保留。 */
    public record FailureReport(Object actualServer, Phase failedAt, Throwable originalCause,
                                Object originalReceipt, boolean unresolvedTransfer,
                                boolean finalizationSignalCompleted, ClaimDisposition disposition,
                                Thread responsibleActor) { }
    public record Installation(Factory factory, Limits limits) {
        public Installation { Objects.requireNonNull(factory); Objects.requireNonNull(limits); }
    }
    public static final class BootstrapFailure extends RuntimeException {
        public BootstrapFailure(String stage, Throwable cause) { super("Dedicated bootstrap failed at " + stage, cause); }
    }
    private static volatile Installation installed;
    public static synchronized Installation install(Factory factory, Limits limits) {
        if (installed != null) throw new IllegalStateException("Bootstrap factory already installed");
        installed = new Installation(factory, limits);
        return installed;
    }
    public static synchronized void uninstall(Installation expected) {
        if (installed == expected) installed = null;
    }
    public static DedicatedBootstrapBridge create(Object actualServer, Runnable originalStart) {
        return create(actualServer, originalStart, (signal, limit) -> signal.await(limit.toNanos(), TimeUnit.NANOSECONDS));
    }
    static DedicatedBootstrapBridge create(Object actualServer, Runnable originalStart, ClaimWait wait) {
        Installation binding = installed;
        if (binding == null) throw new BootstrapFailure("Unavailable", new IllegalStateException("Bootstrap DI is not installed"));
        return new DedicatedBootstrapBridge(actualServer, originalStart, binding, wait);
    }

    private final Object server;
    private final Runnable originalStart;
    private final Installation binding;
    private final ClaimWait claimWait;
    private final Object lock = new Object();
    private final CountDownLatch claimOrAbort = new CountDownLatch(1);
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile Phase phase = Phase.NEW;
    private volatile Throwable failure;
    private volatile Throwable cancellation;
    private volatile Thread owner;
    private volatile Thread target;
    private volatile Object preparation;
    private volatile Object transferReceipt;
    private volatile Callbacks callbacks;
    private volatile ClaimDisposition disposition = ClaimDisposition.NOT_ISSUED;
    private volatile Phase failedPhase;
    private volatile boolean unresolvedClaimTermination;
    private boolean originalEntered;
    private boolean abortConsumed;

    private DedicatedBootstrapBridge(Object server, Runnable originalStart, Installation binding, ClaimWait wait) {
        this.server = Objects.requireNonNull(server);
        this.originalStart = Objects.requireNonNull(originalStart);
        this.binding = binding;
        this.claimWait = Objects.requireNonNull(wait);
    }
    /** 初次 HEAD 调用后立即返回；仅新建非 game 的控制线程。 */
    public void startDeferred() {
        synchronized (lock) {
            if (phase != Phase.NEW || owner != null) throw new IllegalStateException("Bootstrap already started");
            owner = Thread.ofPlatform().name("FloatingPod bootstrap control").unstarted(this::controlRun);
            phase = Phase.PREPARING;
        }
        try { owner.start(); }
        catch (Throwable cause) { failure = cause; phase = Phase.TERMINAL_FAILED; finished.countDown(); throw wrap("control-thread-start", cause); }
    }
    /** preparation 成功后仅 owner 获准重入一次实际 original startServerThread。 */
    public boolean allowOriginalStart() {
        synchronized (lock) {
            if (phase == Phase.TERMINAL_FAILED && failure != null) throw wrap("repeat-start-after-failure", failure);
            if (Thread.currentThread() != owner) return false;
            if (phase != Phase.PREPARED || originalEntered || cancellation != null)
                throw new IllegalStateException("Original server start cannot be reentered in " + phase);
            originalEntered = true;
            return true;
        }
    }
    /** Redirect 取得 original 创建/保存的 exact NEW thread，再发布 receipt 后原样 Thread.start。 */
    public void transferAndStart(Thread actualTarget, Runnable actualThreadStart) {
        requireOwner();
        synchronized (lock) {
            if (!originalEntered || phase != Phase.PREPARED || target != null || cancellation != null)
                throw new IllegalStateException("Bootstrap transfer is not permitted");
            if (actualTarget == owner || actualTarget.getState() != Thread.State.NEW)
                throw new IllegalArgumentException("Original server target must be exact NEW thread");
            target = actualTarget;
        }
        try {
            Object receipt = Objects.requireNonNull(callbacks.issueTransfer(preparation, actualTarget), "Missing actual IO transfer receipt");
            synchronized (lock) {
                transferReceipt = receipt;
                phase = Phase.TRANSFERRED;
                disposition = ClaimDisposition.PENDING;
            }
            if (cancellation != null) throw wrap("cancel-before-start", cancellation);
            actualThreadStart.run();
        } catch (Throwable cause) {
            throw wrap("transfer-or-thread-start", original(cause));
        }
    }
    /** runHEAD：无等待，只 exact target 消费原 receipt，成功后 original run 才能进入 init/FML。 */
    public void claimTarget() {
        if (Thread.currentThread() != target) throw new IllegalStateException("Only exact original server thread may claim bootstrap");
        Throwable cancelledBeforeClaim;
        synchronized (lock) {
            if (phase == Phase.TERMINAL_FAILED && failure != null)
                throw wrap("late-run-after-terminal-failure", failure);
            if (phase != Phase.TRANSFERRED || transferReceipt == null)
                throw new IllegalStateException("Bootstrap transfer is unavailable or already consumed");
            // 单一仲裁点：取消先赢则不调用 claim；claim 先赢则 owner 不能再取消/夺回责任。
            cancelledBeforeClaim = cancellation;
            if (cancelledBeforeClaim == null) { phase = Phase.CLAIMING; disposition = ClaimDisposition.IN_PROGRESS; }
        }
        try {
            if (cancelledBeforeClaim != null) throw wrap("cancel-before-claim", cancelledBeforeClaim);
            callbacks.claim(preparation, transferReceipt);
            synchronized (lock) {
                if (phase == Phase.TERMINAL_FAILED) {
                    disposition = ClaimDisposition.CLAIMED_AFTER_FAILURE;
                    throw wrap("late-claim-after-terminal-failure", failure);
                }
                disposition = ClaimDisposition.CLAIMED;
                phase = Phase.CLAIMED;
            }
            claimOrAbort.countDown();
        } catch (Throwable cause) {
            Throwable primary;
            synchronized (lock) {
                primary = failure == null ? original(cause) : failure;
                if (primary != original(cause)) suppress(primary, original(cause));
                if (failedPhase == null) failedPhase = phase;
                failure = primary;
                phase = Phase.TERMINAL_FAILED;
            }
            // owner 已有界结束时，不能把 late abort 恢复到死 owner 包装为闭合。
            if (!unresolvedClaimTermination && disposition != ClaimDisposition.CLAIMED_AFTER_FAILURE)
                abortOnCurrentActor(primary);
            claimOrAbort.countDown();
            throw wrap("claim", primary);
        }
    }
    /** 非阻塞取消信号。不是 stop/exit hook，更不是正常 durable shutdown 完成证明。 */
    public CancellationDecision requestCancellation(Throwable originalCause) {
        Objects.requireNonNull(originalCause);
        synchronized (lock) {
            if (phase == Phase.CLAIMED) return CancellationDecision.ALREADY_CLAIMED;
            if (phase == Phase.CLAIMING) return CancellationDecision.CLAIM_IN_PROGRESS;
            if (phase == Phase.TERMINAL_FAILED) return CancellationDecision.ALREADY_FAILED;
            if (cancellation == null) cancellation = originalCause;
            if (phase == Phase.PREPARING && owner != null) owner.interrupt();
            return CancellationDecision.CANCEL_REQUESTED;
        }
    }
    public Phase phase() { return phase; }
    public Throwable failure() { return failure; }
    public Object originalTransferReceipt() { return transferReceipt; }
    public ClaimDisposition claimDisposition() { return disposition; }
    public Thread controlThread() { return owner; }
    /** 原成功 control 的尾栈尚活时只返回 pending；不等待，也不发行关闭许可。 */
    public boolean ordinaryApplicationControlEnded(Callbacks expected, Object receipt) {
        synchronized (lock) {
            requireOrdinaryApplicationOwner(expected, receipt);
            return finished.getCount()==0 && owner!=null && owner.getState()==Thread.State.TERMINATED && !owner.isAlive();
        }
    }
    private void requireOrdinaryApplicationOwner(Callbacks expected, Object receipt) {
        if(expected==null || callbacks!=expected || target!=Thread.currentThread() ||
                phase!=Phase.CLAIMED || disposition!=ClaimDisposition.CLAIMED ||
                receipt==null || transferReceipt!=receipt || unresolvedClaimTermination)
            throw new IllegalStateException("Exact successful claimed application owner required");
        if(failure!=null)throw wrap("application-retirement",failure);
        if(cancellation!=null)throw wrap("application-retirement-cancellation",cancellation);
    }
    public void requireOrdinaryApplicationPreparation(Callbacks expected, Object receipt, Object original) {
        synchronized(lock) {
            requireOrdinaryApplicationOwner(expected,receipt);
            if(finished.getCount()!=0 || owner==null || owner.getState()!=Thread.State.TERMINATED || owner.isAlive())
                throw new IllegalStateException("Original bootstrap control consumer is not terminated");
            if(original==null || preparation!=original)throw new IllegalStateException("Original bridge application value differs");
        }
    }
    public void retireOrdinaryApplicationPreparation(Callbacks expected, Object receipt, Object original) {
        synchronized(lock) {
            requireOrdinaryApplicationOwner(expected,receipt);
            if(finished.getCount()!=0 || owner==null || owner.getState()!=Thread.State.TERMINATED || owner.isAlive())
                throw new IllegalStateException("Original bootstrap control consumer is not terminated");
            if(original==null || preparation!=original)throw new IllegalStateException("Original bridge application value differs");
            preparation=null;
        }
    }
    public void requireOrdinaryApplicationPreparationRetired(Callbacks expected, Object receipt) {
        synchronized(lock) {
            requireOrdinaryApplicationOwner(expected,receipt);
            if(preparation!=null)throw new IllegalStateException("Bridge application value remains retained");
        }
    }

    public Thread originalTargetThread() { return target; }
    public boolean abortConsumed() { synchronized (lock) { return abortConsumed; } }
    /** 仅外部诊断/测试非 game caller 使用；生产 launcher/Main 不调用。 */
    public boolean awaitFinishedOffGame(Duration limit) throws InterruptedException {
        if (Thread.currentThread() == owner || Thread.currentThread() == target)
            throw new IllegalStateException("Game/owner thread cannot await bootstrap finish");
        return finished.await(limit.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void controlRun() {
        Phase failedAt = Phase.PREPARING;
        Throwable primary = null;
        boolean reported = false;
        try {
            callbacks = Objects.requireNonNull(binding.factory().open(server, this), "Missing actual bootstrap callbacks");
            if (cancellation != null) throw wrap("cancel-before-prepare", cancellation);
            preparation = Objects.requireNonNull(await(callbacks.prepare(), binding.limits().preparation()), "Missing actual preparation");
            if (cancellation != null) throw wrap("cancel-after-prepare", cancellation);
            phase = Phase.PREPARED;
            failedAt = Phase.PREPARED;
            originalStart.run();
            if (!originalEntered || transferReceipt == null) throw new IllegalStateException("Original start did not publish actual transfer");
            failedAt = Phase.TRANSFERRED;
            if (!claimWait.await(claimOrAbort, binding.limits().claim())) {
                TimeoutException timeout = new TimeoutException("Exact original server thread did not claim or abort bootstrap");
                CancellationDecision decision = requestCancellation(timeout);
                if (decision == CancellationDecision.ALREADY_CLAIMED) return;
                // claim 已赢的 callback 只在 target 上消费；oldowner 不可调用 abort/drain/finalize。
                boolean observed = claimWait.await(claimOrAbort, binding.limits().claim());
                synchronized (lock) {
                    if (phase == Phase.CLAIMED) return;
                    if (phase == Phase.CLAIMING) {
                        unresolvedClaimTermination = true;
                        failure = timeout;
                        failedPhase = Phase.CLAIMING;
                        phase = Phase.TERMINAL_FAILED;
                    }
                }
                if (!observed)
                    suppress(timeout, new IllegalStateException("Unresolved bootstrap transfer after cancellation deadline"));
                throw timeout;
            }
            if (failure != null) throw wrap("target-claim", failure);
            if (phase != Phase.CLAIMED) throw new IllegalStateException("Bootstrap ended without exact claim");
        } catch (Throwable cause) {
            boolean claimOwned;
            synchronized (lock) {
                // deadline 与成功 claim 的最终仲裁：成功者永远不由 oldowner 转失败。
                if (phase == Phase.CLAIMED) return;
                primary = failure != null ? failure : cancellation != null ? cancellation : original(cause);
                if (primary != original(cause)) suppress(primary, original(cause));
                claimOwned = disposition == ClaimDisposition.IN_PROGRESS
                        || disposition == ClaimDisposition.CLAIMED_AFTER_FAILURE;
                if (claimOwned) unresolvedClaimTermination = true;
                if (failedPhase == null) failedPhase = phase;
                failedAt = failedPhase;
                failure = primary;
                phase = Phase.TERMINAL_FAILED;
            }
            if (transferReceipt != null && !abortConsumed && !claimOwned) abortOnCurrentActor(primary);
            // 仅失败处理的信号；即便 finalizer 成功，也永远保持 TERMINAL_FAILED。
            Thread.interrupted();
            if (callbacks != null) {
                boolean finalizationSignalCompleted = false;
                try {
                    if (claimOwned || (transferReceipt != null && !abortConsumed))
                        throw new IllegalStateException("Unresolved transfer/claim responsibility: old owner must not finalize resources");
                    await(callbacks.finalizeFailure(preparation, transferReceipt, primary), binding.limits().finalization());
                    finalizationSignalCompleted = true;
                }
                catch (Throwable cleanup) { suppress(primary, original(cleanup)); }
                phase = Phase.TERMINAL_FAILED;
                try {
                    callbacks.failedStartupTermination(new FailureReport(server, failedAt, primary,
                            transferReceipt, unresolvedClaimTermination || (transferReceipt != null && !abortConsumed),
                            finalizationSignalCompleted, disposition, responsibilityActor()));
                    reported = true;
                }
                catch (Throwable logging) { suppress(primary, original(logging)); }
            }
            phase = Phase.TERMINAL_FAILED;
        } finally { finished.countDown(); }
        if (primary != null && !reported) throw wrap("boot-control", primary);
    }
    private void abortOnCurrentActor(Throwable primary) {
        if (transferReceipt == null) return;
        synchronized (lock) { if (abortConsumed) return; }
        Thread actor = Thread.currentThread();
        Thread.State state = target.getState();
        if (actor != target && !(actor == owner && (state == Thread.State.NEW || state == Thread.State.TERMINATED))) {
            suppress(primary, new IllegalStateException("Unresolved bootstrap transfer: old owner cannot abort running target"));
            return;
        }
        try {
            callbacks.abortTransfer(preparation, transferReceipt, primary);
            synchronized (lock) {
                abortConsumed = true;
                disposition = unresolvedClaimTermination ? ClaimDisposition.ABORTED_AFTER_FAILURE : ClaimDisposition.ABORTED;
            }
        } catch (Throwable cleanup) { suppress(primary, original(cleanup)); }
    }
    private void requireOwner() {
        if (Thread.currentThread() != owner) throw new IllegalStateException("Only exact bootstrap control owner may issue transfer");
    }
    private Thread responsibilityActor() {
        return switch (disposition) {
            case CLAIMED, CLAIMED_AFTER_FAILURE, IN_PROGRESS -> target;
            case PENDING, ABORTED_AFTER_FAILURE -> null;
            case NOT_ISSUED, ABORTED -> owner;
        };
    }
    private static <T> T await(CompletionStage<T> stage, Duration limit) throws Exception {
        return Objects.requireNonNull(stage, "Missing bootstrap completion signal").toCompletableFuture()
                .get(limit.toNanos(), TimeUnit.NANOSECONDS);
    }
    private static Throwable original(Throwable cause) {
        if ((cause instanceof ExecutionException || cause instanceof BootstrapFailure) && cause.getCause() != null)
            return original(cause.getCause());
        return cause;
    }
    private static BootstrapFailure wrap(String stage, Throwable cause) { return new BootstrapFailure(stage, cause); }
    private static void suppress(Throwable primary, Throwable secondary) { if (secondary != primary) primary.addSuppressed(secondary); }
}

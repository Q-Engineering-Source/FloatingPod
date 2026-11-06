package com.wi.floatingpod.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** 真实线程直接调用生产 controller；不冒充 Minecraft/Mixin/OwnedIO/SQL runtime验收。 */
@ResourceLock("FloatingPod-bootstrap-DI")
class DedicatedBootstrapBridgeTest {
    private static final Duration WAIT = Duration.ofSeconds(2);
    private static final DedicatedBootstrapBridge.Limits LIMITS = new DedicatedBootstrapBridge.Limits(WAIT, WAIT, WAIT);
    private static final class Fixture implements DedicatedBootstrapBridge.Callbacks, AutoCloseable {
        final Object server = new Object();
        final Object prepared = new Object();
        final Object receipt = new Object();
        final CompletableFuture<Object> preparation = new CompletableFuture<>();
        final CountDownLatch opened = new CountDownLatch(1);
        final CountDownLatch allowTarget;
        final CountDownLatch claimEntered = new CountDownLatch(1);
        final CountDownLatch targetRunEntered = new CountDownLatch(1);
        volatile CountDownLatch allowClaim = new CountDownLatch(0);
        final AtomicInteger originalStarts = new AtomicInteger();
        final AtomicInteger initCalls = new AtomicInteger();
        final AtomicInteger claims = new AtomicInteger();
        final AtomicInteger aborts = new AtomicInteger();
        volatile Runnable afterClaim = () -> { };
        volatile boolean retainInstallationHolder;
        volatile Thread owner;
        volatile Thread target;
        volatile Thread abortActor;
        volatile Thread finalizerActor;
        volatile Throwable claimFailure;
        volatile Throwable startFailure;
        volatile Throwable cleanupFailure;
        volatile Throwable targetFailure;
        volatile CompletionStage<Void> finalizerSignal = CompletableFuture.completedFuture(null);
        volatile DedicatedBootstrapBridge.FailureReport report;
        volatile DedicatedBootstrapBridge bridge;
        final DedicatedBootstrapBridge.Installation installation;
        Fixture() { this(LIMITS, false); }
        Fixture(DedicatedBootstrapBridge.Limits limits, boolean delayTarget) {
            this(limits, delayTarget, (signal, limit) -> signal.await(limit.toNanos(), TimeUnit.NANOSECONDS));
        }
        Fixture(DedicatedBootstrapBridge.Limits limits, boolean delayTarget, DedicatedBootstrapBridge.ClaimWait wait) {
            allowTarget = new CountDownLatch(delayTarget ? 1 : 0);
            installation = DedicatedBootstrapBridge.install((actual, control) -> {
                assertSame(server, actual);
                owner = Thread.currentThread();
                if (retainInstallationHolder) ProductionBootstrapInstallation.retain(actual, control, this);
                opened.countDown();
                return this;
            }, limits);
            bridge = DedicatedBootstrapBridge.create(server, this::originalStart, wait);
        }
        void originalStart() {
            assertSame(owner, Thread.currentThread());
            assertTrue(bridge.allowOriginalStart());
            originalStarts.incrementAndGet();
            target = new Thread(() -> {
                try {
                    targetRunEntered.countDown();
                    if (!allowTarget.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("test target gate timed out");
                    bridge.claimTarget();
                    afterClaim.run();
                    initCalls.incrementAndGet();
                } catch (Throwable failure) { targetFailure = failure; }
            }, "test-exact-original-server-thread");
            bridge.transferAndStart(target, () -> {
                if (startFailure != null) throw (RuntimeException) startFailure;
                target.start();
            });
        }
        @Override public CompletionStage<Object> prepare() { assertSame(owner, Thread.currentThread()); return preparation; }
        @Override public Object issueTransfer(Object actualPreparation, Thread actualTarget) {
            assertSame(owner, Thread.currentThread()); assertSame(prepared, actualPreparation);
            assertSame(target, actualTarget); assertEquals(Thread.State.NEW, actualTarget.getState());
            return receipt;
        }
        @Override public void claim(Object actualPreparation, Object actualReceipt) throws Exception {
            assertSame(target, Thread.currentThread()); assertSame(prepared, actualPreparation); assertSame(receipt, actualReceipt);
            claims.incrementAndGet();
            claimEntered.countDown();
            if (!allowClaim.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("test claim gate timed out");
            if (claimFailure != null) throw (RuntimeException) claimFailure;
        }
        @Override public void abortTransfer(Object actualPreparation, Object actualReceipt, Throwable cause) {
            assertSame(prepared, actualPreparation); assertSame(receipt, actualReceipt);
            abortActor = Thread.currentThread();
            assertTrue(abortActor == target || (abortActor == owner
                    && (target.getState() == Thread.State.NEW || target.getState() == Thread.State.TERMINATED)));
            aborts.incrementAndGet();
        }
        @Override public CompletionStage<Void> finalizeFailure(Object actualPreparation, Object exactReceipt, Throwable cause) {
            assertSame(owner, Thread.currentThread()); finalizerActor = Thread.currentThread();
            return cleanupFailure == null ? finalizerSignal
                    : CompletableFuture.failedFuture(cleanupFailure);
        }
        @Override public void failedStartupTermination(DedicatedBootstrapBridge.FailureReport actual) {
            assertSame(owner, Thread.currentThread()); assertSame(server, actual.actualServer()); report = actual;
        }
        void start() throws InterruptedException { bridge.startDeferred(); assertTrue(opened.await(2, TimeUnit.SECONDS)); }
        void finish() throws InterruptedException {
            assertTrue(bridge.awaitFinishedOffGame(WAIT));
            owner.join(2000); assertFalse(owner.isAlive());
            if (target != null && target.getState() != Thread.State.NEW) { target.join(2000); assertFalse(target.isAlive()); }
        }
        @Override public void close() throws Exception {
            try {
                allowTarget.countDown();
                allowClaim.countDown();
                if (owner != null && owner.isAlive() && !preparation.isDone()) {
                    bridge.requestCancellation(new IllegalStateException("test cleanup cancellation"));
                }
                if (owner != null) { owner.join(4000); assertFalse(owner.isAlive(), "control thread leaked"); }
                if (target != null && target.getState() != Thread.State.NEW) { target.join(4000); assertFalse(target.isAlive(), "target thread leaked"); }
            } finally { DedicatedBootstrapBridge.uninstall(installation); }
        }
    }

    @Test void launcherReturnsBeforePreparationAndOnlyActualOwnerReenters() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Thread launcher = Thread.currentThread(); fixture.start();
            assertNotSame(launcher, fixture.owner);
            assertEquals(DedicatedBootstrapBridge.Phase.PREPARING, fixture.bridge.phase());
            assertNull(fixture.target); assertEquals(0, fixture.initCalls.get());
            assertFalse(fixture.bridge.allowOriginalStart());
            fixture.preparation.complete(fixture.prepared); fixture.finish();
            assertEquals(DedicatedBootstrapBridge.Phase.CLAIMED, fixture.bridge.phase());
            assertEquals(1, fixture.originalStarts.get()); assertEquals(1, fixture.claims.get()); assertEquals(1, fixture.initCalls.get());
            assertSame(fixture.receipt, fixture.bridge.originalTransferReceipt());
            assertSame(fixture.target, fixture.bridge.originalTargetThread());
            assertFalse(fixture.bridge.abortConsumed()); assertNull(fixture.report);
        }
    }

    @Test void claimedInstallationAssemblyIsConsumedOnceByExactOriginalMain() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.retainInstallationHolder = true;
            fixture.afterClaim = () -> {
                assertSame(fixture, ProductionBootstrapInstallation.requireClaimedCallbacks(fixture.server));
                assertThrows(IllegalStateException.class,
                        () -> ProductionBootstrapInstallation.consumeClaimedCallbacks(new Object()));
                assertSame(fixture, ProductionBootstrapInstallation.consumeClaimedCallbacks(fixture.server));
                assertThrows(IllegalStateException.class,
                        () -> ProductionBootstrapInstallation.consumeClaimedCallbacks(fixture.server));
                assertSame(fixture, ProductionBootstrapInstallation.requireClaimedCallbacks(fixture.server),
                        "consumption must retain the original failure/exit holder");
            };
            fixture.preparation.complete(fixture.prepared);
            fixture.start(); fixture.finish();
            assertNull(fixture.targetFailure);
            assertEquals(1, fixture.claims.get());
            assertEquals(1, fixture.initCalls.get());
            assertThrows(IllegalStateException.class,
                    () -> ProductionBootstrapInstallation.consumeClaimedCallbacks(fixture.server),
                    "caller outside exact claimed target never acquires assembly");
        }
    }

    @Test void preparationFailureNeverCreatesTargetAndRetainsOriginalCause() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException original = new IllegalStateException("preparation cause");
            fixture.preparation.completeExceptionally(original); fixture.start(); fixture.finish();
            assertSame(original, fixture.bridge.failure()); assertSame(original, fixture.report.originalCause());
            assertEquals(DedicatedBootstrapBridge.Phase.TERMINAL_FAILED, fixture.bridge.phase());
            assertNull(fixture.target); assertEquals(0, fixture.originalStarts.get()); assertEquals(0, fixture.aborts.get());
            assertSame(fixture.owner, fixture.finalizerActor); assertTrue(fixture.report.finalizationSignalCompleted());
        }
    }

    @Test void originalThreadStartFailureAbortsExactReceiptOnOldOwner() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException original = new IllegalStateException("Thread.start cause"); fixture.startFailure = original;
            fixture.preparation.complete(fixture.prepared); fixture.start(); fixture.finish();
            assertSame(original, fixture.bridge.failure()); assertSame(original, fixture.report.originalCause());
            assertSame(fixture.owner, fixture.abortActor); assertEquals(1, fixture.aborts.get());
            assertTrue(fixture.bridge.abortConsumed()); assertSame(fixture.receipt, fixture.report.originalReceipt());
            assertFalse(fixture.report.unresolvedTransfer()); assertEquals(0, fixture.initCalls.get());
        }
    }

    @Test void claimFailureAbortsOnExactPreclaimTargetBeforeAnyInit() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException original = new IllegalArgumentException("claim cause"); fixture.claimFailure = original;
            fixture.preparation.complete(fixture.prepared); fixture.start(); fixture.finish();
            assertSame(original, fixture.bridge.failure()); assertSame(original, fixture.targetFailure.getCause());
            assertSame(fixture.target, fixture.abortActor); assertEquals(1, fixture.aborts.get());
            assertEquals(0, fixture.initCalls.get()); assertEquals(1, fixture.claims.get());
            assertSame(fixture.owner, fixture.finalizerActor); assertFalse(fixture.report.unresolvedTransfer());
        }
    }

    @Test void cancellationDuringPreparationAndFinalizationFailureKeepCauseAndSuppressed() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException cancellation = new IllegalStateException("shutdown cancellation");
            RuntimeException cleanup = new IllegalArgumentException("finalizer cause"); fixture.cleanupFailure = cleanup;
            fixture.start(); fixture.bridge.requestCancellation(cancellation); fixture.finish();
            assertSame(cancellation, fixture.bridge.failure()); assertSame(cancellation, fixture.report.originalCause());
            assertTrue(java.util.Arrays.asList(cancellation.getSuppressed()).contains(cleanup));
            assertFalse(fixture.report.finalizationSignalCompleted()); assertNull(fixture.target);
            assertEquals(DedicatedBootstrapBridge.Phase.TERMINAL_FAILED, fixture.bridge.phase());
        }
    }

    @Test void unavailableFailsExplicitlyWithoutStartingAnyThread() {
        DedicatedBootstrapBridge.BootstrapFailure failure = assertThrows(DedicatedBootstrapBridge.BootstrapFailure.class,
                () -> DedicatedBootstrapBridge.create(new Object(), () -> fail("Unavailable must not start")));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
    }

    @Test void replayAndWrongTargetClaimCannotReinitialize() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.preparation.complete(fixture.prepared); fixture.start(); fixture.finish();
            assertThrows(IllegalStateException.class, fixture.bridge::claimTarget);
            assertThrows(IllegalStateException.class, fixture.bridge::startDeferred);
            assertFalse(fixture.bridge.allowOriginalStart());
            assertEquals(1, fixture.initCalls.get()); assertEquals(1, fixture.originalStarts.get());
        }
    }

    @Test void claimDeadlineRetainsUnresolvedReceiptAndLateTargetNeverEntersInit() throws Exception {
        DedicatedBootstrapBridge.Limits limits = new DedicatedBootstrapBridge.Limits(WAIT, Duration.ofMillis(50), WAIT);
        try (Fixture fixture = new Fixture(limits, true)) {
            fixture.preparation.complete(fixture.prepared); fixture.start();
            assertTrue(fixture.bridge.awaitFinishedOffGame(WAIT));
            assertEquals(DedicatedBootstrapBridge.Phase.TERMINAL_FAILED, fixture.bridge.phase());
            assertInstanceOf(java.util.concurrent.TimeoutException.class, fixture.bridge.failure());
            assertSame(fixture.receipt, fixture.bridge.originalTransferReceipt()); assertSame(fixture.receipt, fixture.report.originalReceipt());
            assertTrue(fixture.report.unresolvedTransfer()); assertFalse(fixture.bridge.abortConsumed());
            assertEquals(0, fixture.aborts.get());
            fixture.allowTarget.countDown(); fixture.finish();
            assertSame(fixture.bridge.failure(), fixture.targetFailure.getCause()); assertEquals(0, fixture.initCalls.get());
            assertTrue(fixture.report.unresolvedTransfer());
        }
    }

    @Test void finalizationDeadlineIsSuppressedAndNeverBecomesCleanSuccess() throws Exception {
        DedicatedBootstrapBridge.Limits limits = new DedicatedBootstrapBridge.Limits(WAIT, WAIT, Duration.ofMillis(50));
        try (Fixture fixture = new Fixture(limits, false)) {
            RuntimeException original = new IllegalStateException("original start failure"); fixture.startFailure = original;
            fixture.finalizerSignal = new CompletableFuture<>();
            fixture.preparation.complete(fixture.prepared); fixture.start(); fixture.finish();
            assertSame(original, fixture.bridge.failure()); assertSame(original, fixture.report.originalCause());
            assertTrue(java.util.Arrays.stream(original.getSuppressed()).anyMatch(cause -> cause instanceof java.util.concurrent.TimeoutException));
            assertFalse(fixture.report.finalizationSignalCompleted());
            assertEquals(DedicatedBootstrapBridge.Phase.TERMINAL_FAILED, fixture.bridge.phase());
        }
    }

    @Test void claimAlreadyInProgressWinsTimeoutAndOldOwnerNeverFinalizesItsResources() throws Exception {
        CountDownLatch secondAwaitEntered = new CountDownLatch(1);
        AtomicInteger waits = new AtomicInteger();
        DedicatedBootstrapBridge.ClaimWait wait = (signal, limit) -> {
            if (waits.incrementAndGet() == 2) secondAwaitEntered.countDown();
            return signal.await(limit.toNanos(), TimeUnit.NANOSECONDS);
        };
        DedicatedBootstrapBridge.Limits limits = new DedicatedBootstrapBridge.Limits(WAIT, Duration.ofMillis(500), WAIT);
        try (Fixture fixture = new Fixture(limits, false, wait)) {
            fixture.allowClaim = new CountDownLatch(1);
            fixture.preparation.complete(fixture.prepared); fixture.start();
            assertTrue(fixture.claimEntered.await(2, TimeUnit.SECONDS));
            assertTrue(secondAwaitEntered.await(2, TimeUnit.SECONDS));
            assertEquals(DedicatedBootstrapBridge.Phase.CLAIMING, fixture.bridge.phase());
            fixture.allowClaim.countDown(); fixture.finish();
            assertEquals(DedicatedBootstrapBridge.Phase.CLAIMED, fixture.bridge.phase());
            assertEquals(DedicatedBootstrapBridge.ClaimDisposition.CLAIMED, fixture.bridge.claimDisposition());
            assertEquals(1, fixture.initCalls.get()); assertEquals(1, fixture.claims.get());
            assertEquals(0, fixture.aborts.get()); assertNull(fixture.finalizerActor); assertNull(fixture.report);
            assertNull(fixture.bridge.failure());
        }
    }

    @Test void targetClaimAfterFirstTrueDeadlineButBeforeCancellationWinsArbitration() throws Exception {
        CountDownLatch firstDeadlineObserved = new CountDownLatch(1);
        CountDownLatch allowOwnerDecision = new CountDownLatch(1);
        DedicatedBootstrapBridge.ClaimWait wait = (signal, limit) -> {
            boolean result = signal.await(limit.toNanos(), TimeUnit.NANOSECONDS);
            if (!result) {
                firstDeadlineObserved.countDown();
                if (!allowOwnerDecision.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("test owner decision gate timed out");
            }
            return result;
        };
        DedicatedBootstrapBridge.Limits limits = new DedicatedBootstrapBridge.Limits(WAIT, Duration.ofMillis(50), WAIT);
        try (Fixture fixture = new Fixture(limits, true, wait)) {
            try {
                fixture.preparation.complete(fixture.prepared); fixture.start();
                assertTrue(firstDeadlineObserved.await(2, TimeUnit.SECONDS));
                fixture.allowTarget.countDown();
                fixture.target.join(1000); assertFalse(fixture.target.isAlive());
                assertEquals(DedicatedBootstrapBridge.Phase.CLAIMED, fixture.bridge.phase());
                assertEquals(1, fixture.initCalls.get());
                allowOwnerDecision.countDown(); fixture.finish();
                assertNull(fixture.bridge.failure()); assertNull(fixture.report); assertNull(fixture.finalizerActor);
                assertEquals(0, fixture.aborts.get()); assertEquals(1, fixture.claims.get());
            } finally { allowOwnerDecision.countDown(); }
        }
    }

    @Test void cancellationBeforeClaimWinsAndExactTargetAbortsWithoutInit() throws Exception {
        try (Fixture fixture = new Fixture(LIMITS, true)) {
            fixture.preparation.complete(fixture.prepared); fixture.start();
            assertTrue(fixture.targetRunEntered.await(2, TimeUnit.SECONDS));
            assertEquals(DedicatedBootstrapBridge.Phase.TRANSFERRED, fixture.bridge.phase());
            RuntimeException cancellation = new IllegalStateException("preclaim cancellation");
            assertEquals(DedicatedBootstrapBridge.CancellationDecision.CANCEL_REQUESTED,
                    fixture.bridge.requestCancellation(cancellation));
            fixture.allowTarget.countDown(); fixture.finish();
            assertSame(cancellation, fixture.bridge.failure()); assertEquals(0, fixture.claims.get()); assertEquals(0, fixture.initCalls.get());
            assertEquals(1, fixture.aborts.get()); assertSame(fixture.target, fixture.abortActor);
            assertEquals(DedicatedBootstrapBridge.ClaimDisposition.ABORTED, fixture.bridge.claimDisposition());
        }
    }

    @Test void unresolvedClaimDeadlineNeverLetsOldOwnerFinalizeAndLateSuccessfulClaimCannotInit() throws Exception {
        DedicatedBootstrapBridge.Limits limits = new DedicatedBootstrapBridge.Limits(WAIT, Duration.ofMillis(50), WAIT);
        try (Fixture fixture = new Fixture(limits, false)) {
            fixture.allowClaim = new CountDownLatch(1);
            fixture.preparation.complete(fixture.prepared); fixture.start();
            assertTrue(fixture.claimEntered.await(2, TimeUnit.SECONDS));
            assertTrue(fixture.bridge.awaitFinishedOffGame(WAIT));
            assertEquals(DedicatedBootstrapBridge.Phase.TERMINAL_FAILED, fixture.bridge.phase());
            assertEquals(DedicatedBootstrapBridge.ClaimDisposition.IN_PROGRESS, fixture.report.disposition());
            assertSame(fixture.target, fixture.report.responsibleActor());
            assertSame(fixture.receipt, fixture.report.originalReceipt()); assertTrue(fixture.report.unresolvedTransfer());
            assertNull(fixture.finalizerActor); assertEquals(0, fixture.aborts.get());
            Throwable original = fixture.bridge.failure();
            fixture.allowClaim.countDown(); fixture.finish();
            assertEquals(DedicatedBootstrapBridge.ClaimDisposition.CLAIMED_AFTER_FAILURE, fixture.bridge.claimDisposition());
            assertSame(original, fixture.targetFailure.getCause()); assertEquals(0, fixture.initCalls.get());
            assertEquals(0, fixture.aborts.get()); assertNull(fixture.finalizerActor);
        }
    }

    @Test void ordinaryApplicationRetirementRequiresActualControlTailEndAndOriginalClaim() throws Exception {
        CountDownLatch allowControlExit = new CountDownLatch(1);
        try (Fixture fixture = new Fixture(LIMITS, false, (signal, limit) -> {
            boolean claimed = signal.await(limit.toNanos(), TimeUnit.NANOSECONDS);
            if (claimed && !allowControlExit.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("control tail gate timeout");
            return claimed;
        })) {
            fixture.afterClaim = () -> {
                try {
                    assertFalse(fixture.bridge.ordinaryApplicationControlEnded(fixture, fixture.receipt));
                    assertThrows(IllegalStateException.class, () ->
                        fixture.bridge.retireOrdinaryApplicationPreparation(fixture, fixture.receipt, fixture.prepared));
                    allowControlExit.countDown();
                    fixture.owner.join(2000);
                    assertFalse(fixture.owner.isAlive());
                    assertTrue(fixture.bridge.ordinaryApplicationControlEnded(fixture, fixture.receipt));
                    assertThrows(IllegalStateException.class, () ->
                        fixture.bridge.requireOrdinaryApplicationPreparation(fixture, new Object(), fixture.prepared));
                    fixture.bridge.requireOrdinaryApplicationPreparation(fixture, fixture.receipt, fixture.prepared);
                    fixture.bridge.retireOrdinaryApplicationPreparation(fixture, fixture.receipt, fixture.prepared);
                    fixture.bridge.requireOrdinaryApplicationPreparationRetired(fixture, fixture.receipt);
                    assertThrows(IllegalStateException.class, () ->
                        fixture.bridge.retireOrdinaryApplicationPreparation(fixture, fixture.receipt, fixture.prepared));
                } catch (InterruptedException failure) { throw new IllegalStateException(failure); }
                finally { allowControlExit.countDown(); }
            };
            fixture.start();
            fixture.preparation.complete(fixture.prepared);
            fixture.finish();
            assertNull(fixture.targetFailure);
        } finally { allowControlExit.countDown(); }
    }
}

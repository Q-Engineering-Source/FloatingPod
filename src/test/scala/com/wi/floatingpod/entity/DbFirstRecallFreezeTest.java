package com.wi.floatingpod.entity;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

final class DbFirstRecallFreezeTest {
    private final UUID pod = UUID.randomUUID(), owner = UUID.randomUUID(), operation = UUID.randomUUID();
    private final UUID attempt = UUID.randomUUID(), binding = UUID.randomUUID();

    private FloatingPodControlSnapshot state(UUID actualOwner, long version, UUID actualAttempt, UUID actualBinding) {
        return new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED, pod, actualOwner,
                "Normal", true, version, "world", 1, 64, 2, 3, 4, 0, actualBinding, actualAttempt, null, null);
    }

    @Test void exactOperationCanRereadButCannotReplaceOriginalControlOrOwnerVersion() {
        FloatingPodControlSnapshot original = state(owner, 7, attempt, binding);
        DbFirstRecallFreeze freeze = new DbFirstRecallFreeze(operation, owner, 7, original);
        assertFalse(freeze.isBootRecovery());
        freeze.requireCurrent(operation, 7, original);
        freeze.requireCurrent(operation, 7, original);
        assertSame(original, freeze.original());
        assertEquals(operation, freeze.operationUuid());
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(UUID.randomUUID(), 7, original));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 8, original));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 7, state(owner, 7, attempt, binding)));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 7, original.asFaulted(new Exception("original"))));
        assertThrows(IllegalStateException.class, () -> new DbFirstRecallFreeze(operation, UUID.randomUUID(), 7, original));
        assertThrows(IllegalStateException.class, () -> new DbFirstRecallFreeze(operation, owner, 8, original));
        assertThrows(IllegalStateException.class, () -> new DbFirstRecallFreeze(operation, owner, 7, state(owner, 7, null, binding)));
        assertThrows(IllegalStateException.class, () -> new DbFirstRecallFreeze(operation, owner, 7, state(owner, 7, attempt, null)));
    }

    @Test void bootFreezeCannotBeCreatedFromDurableMetadataWithoutActualOwner() {
        var original = state(owner, 7, attempt, binding);
        assertThrows(NullPointerException.class, () -> DbFirstRecallFreeze.forBoot(operation, 7, original, null, null));
        var player = new DbFirstRecallFreeze(operation, owner, 7, original);
        assertFalse(player.isBootRecovery());
        assertNull(player.normalStopRequest());
        player.requireCurrent(operation, 7, original);
    }

    @Test void shutdownRecallUsesOriginalStopFreezeWithoutPlayerAuthentication() {
        var original = state(owner, 7, attempt, binding);
        UUID request = UUID.randomUUID();
        var stop = new NormalStopEntityFreeze(request, original);
        var recalled = DbFirstRecallFreeze.forNormalStop(operation, request, 7, original, stop);
        assertFalse(recalled.isBootRecovery());
        assertSame(original, recalled.original());
        assertEquals(request, recalled.normalStopRequest());
        recalled.requireCurrent(operation, 7, original);
        recalled.requireNormalStopOwner(stop);
        assertThrows(IllegalStateException.class, () -> recalled.requireNormalStopOwner(null));
        assertThrows(IllegalStateException.class, () -> recalled.requireNormalStopOwner(new NormalStopEntityFreeze(request, original)));
        assertThrows(IllegalStateException.class, () -> recalled.requireCurrent(operation, 7, state(owner, 7, attempt, binding)));
        var player = new DbFirstRecallFreeze(operation, owner, 7, original);
        assertNull(player.normalStopRequest());
        player.requireNormalStopOwner(null);
        assertThrows(IllegalStateException.class, () -> player.requireNormalStopOwner(stop));
    }

    @Test void shutdownRecallRejectsForeignRequestWorldIdleOwnerlessFaultAndVersion() {
        var original = state(owner, 7, attempt, binding);
        UUID request = UUID.randomUUID();
        var stop = new NormalStopEntityFreeze(request, original);
        assertThrows(IllegalStateException.class, () -> DbFirstRecallFreeze.forNormalStop(operation, UUID.randomUUID(), 7, original, stop));
        assertThrows(IllegalStateException.class, () -> DbFirstRecallFreeze.forNormalStop(operation, request, 8, original, stop));
        assertThrows(IllegalStateException.class, () -> DbFirstRecallFreeze.forNormalStop(operation, request, 7, original.asFaulted(new Exception("original")), stop));
        assertThrows(IllegalArgumentException.class, () -> state(null, 7, attempt, binding));
        var ownerless = new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED, pod, null,
                "WorldIdle", true, 7, "world", 1, 64, 2, 3, 4, 0, binding, attempt, null, null);
        assertThrows(IllegalStateException.class, () -> DbFirstRecallFreeze.forNormalStop(operation, request, 7, ownerless, new NormalStopEntityFreeze(request, ownerless)));
        var idle = new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED, pod, owner,
                "WorldIdle", true, 7, "world", 1, 64, 2, 3, 4, 0, binding, attempt, null, null);
        assertThrows(IllegalStateException.class, () -> DbFirstRecallFreeze.forNormalStop(operation, request, 7, idle, new NormalStopEntityFreeze(request, idle)));
    }

    @Test void shutdownRecallCreationAndUseRemainOnOriginalMain() throws InterruptedException {
        var original = state(owner, 7, attempt, binding);
        UUID request = UUID.randomUUID();
        var stop = new NormalStopEntityFreeze(request, original);
        var recalled = DbFirstRecallFreeze.forNormalStop(operation, request, 7, original, stop);
        var failure = new AtomicReference<Throwable>();
        Thread worker = new Thread(() -> {
            try { DbFirstRecallFreeze.forNormalStop(operation, request, 7, original, stop); }
            catch (Throwable cause) { failure.set(cause); }
        });
        worker.start(); worker.join(3000);
        assertFalse(worker.isAlive()); assertInstanceOf(IllegalStateException.class, failure.get());
        failure.set(null);
        worker = new Thread(() -> {
            try { recalled.requireNormalStopOwner(stop); } catch (Throwable cause) { failure.set(cause); }
        });
        worker.start(); worker.join(3000);
        assertFalse(worker.isAlive()); assertInstanceOf(IllegalStateException.class, failure.get());
        recalled.requireCurrent(operation, 7, original);
    }

    @Test void originalFaultControlAndCauseStayRetainedWithoutNormalAdmission() {
        var original = state(owner, 7, attempt, binding);
        var freeze = new DbFirstRecallFreeze(operation, owner, 7, original);
        var cause = new IllegalStateException("original recall failure");
        var fault = freeze.retainFault(original, cause);
        assertEquals(FloatingPodControlSnapshot.Phase.FAULTED, fault.phase());
        assertSame(original, freeze.original());
        freeze.requireRetainedFailure(operation, 7, fault, cause);
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 7, fault));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 7, original));
        assertThrows(IllegalStateException.class, () -> freeze.requireRetainedFailure(operation, 7, original, cause));
        assertThrows(IllegalStateException.class, () -> freeze.requireRetainedFailure(UUID.randomUUID(), 7, fault, cause));
        assertThrows(IllegalStateException.class, () -> freeze.requireRetainedFailure(operation, 8, fault, cause));
        assertThrows(IllegalStateException.class, () -> freeze.requireRetainedFailure(operation, 7, fault,
                new IllegalStateException("original recall failure")));
        assertThrows(IllegalStateException.class, () -> freeze.requireRetainedFailure(operation, 7,
                original.asFaulted(cause), cause));
        assertSame(fault, freeze.retainFault(fault, new Exception("later diagnostic")));
        freeze.requireRetainedFailure(operation, 7, fault, cause);
        assertEquals(cause.getClass().getName(), fault.faultType());
        assertThrows(IllegalStateException.class, () -> freeze.retainFault(original, cause));
    }

    @Test void unknownFaultCannotMintRetainedCauseAndShutdownKeepsOriginalOwner() {
        var original = state(owner, 7, attempt, binding);
        var unknown = new DbFirstRecallFreeze(operation, owner, 7, original);
        var fault = unknown.retainFault(original, null);
        assertSame(fault, unknown.retainFault(fault, new Exception("later cause cannot replace unknown")));
        assertThrows(IllegalStateException.class, () -> unknown.requireRetainedFailure(operation, 7, fault, null));
        assertThrows(IllegalStateException.class, () -> unknown.requireRetainedFailure(operation, 7, fault, new Exception()));
        UUID request = UUID.randomUUID();
        var stop = new NormalStopEntityFreeze(request, original);
        var freeze = DbFirstRecallFreeze.forNormalStop(operation, request, 7, original, stop);
        var cause = new Exception("original shutdown recall failure");
        var stopFault = freeze.retainFault(original, cause);
        freeze.requireNormalStopOwner(stop);
        freeze.requireRetainedFailure(operation, 7, stopFault, cause);
        assertThrows(IllegalStateException.class, () -> freeze.requireNormalStopOwner(null));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 7, stopFault));
    }

    @Test void foreignThreadCannotRegisterOrUseOriginalRetainedFault() throws InterruptedException {
        var original = state(owner, 7, attempt, binding);
        var freeze = new DbFirstRecallFreeze(operation, owner, 7, original);
        var cause = new Exception("original failure");
        var failure = new AtomicReference<Throwable>();
        Thread worker = new Thread(() -> {
            try { freeze.retainFault(original, cause); } catch (Throwable caught) { failure.set(caught); }
        });
        worker.start(); worker.join(3000);
        assertFalse(worker.isAlive()); assertInstanceOf(IllegalStateException.class, failure.get());
        var fault = freeze.retainFault(original, cause);
        failure.set(null);
        worker = new Thread(() -> {
            try { freeze.requireRetainedFailure(operation, 7, fault, cause); }
            catch (Throwable caught) { failure.set(caught); }
        });
        worker.start(); worker.join(3000);
        assertFalse(worker.isAlive()); assertInstanceOf(IllegalStateException.class, failure.get());
        freeze.requireRetainedFailure(operation, 7, fault, cause);
    }

    @Test void onlyOriginalThreadCanUseRetainedFreeze() throws InterruptedException {
        FloatingPodControlSnapshot original = state(owner, 7, attempt, binding);
        DbFirstRecallFreeze freeze = new DbFirstRecallFreeze(operation, owner, 7, original);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try { freeze.requireCurrent(operation, 7, original); }
            catch (Throwable cause) { failure.set(cause); }
        });
        worker.start(); worker.join(3000);
        assertFalse(worker.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        freeze.requireCurrent(operation, 7, original);
    }
}

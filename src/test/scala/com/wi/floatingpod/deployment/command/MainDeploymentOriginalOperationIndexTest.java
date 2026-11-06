package com.wi.floatingpod.deployment.command;

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.DeployRequest;
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.ExpectedCommandVersions;
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.QueryOriginalRequest;
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.Request;
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.RetractRequest;
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex.AdmissionKind;
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex.Counts;
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex.Entry;
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex.Limits;
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex.LookupMatch;
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex.Stage;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 直接测试实际索引；lookup/start通知只是数据阶段，不模拟数据库或认证成功。 */
public class MainDeploymentOriginalOperationIndexTest {
    private static final UUID ACTOR = id(1);

    @Test void pendingDuplicateAndConflictKeepOneOriginalAndOneSubmissionOpportunity() {
        var index = index(2, 3, 1, 2);
        DeployRequest request = request(10);
        Entry entry = index.begin(ACTOR, request).entry();
        assertSame(request, entry.request());
        assertSame(entry, index.begin(ACTOR, roundTrip(request)).entry());
        assertEquals(AdmissionKind.CONFLICT, index.begin(id(2), request).kind());
        assertSame(entry, index.begin(id(2), request).entry());
        assertEquals(AdmissionKind.CONFLICT, index.begin(ACTOR, withVersion(request, 9)).kind());
        assertEquals(new Counts(1, 1, 0, 0, 0, 0), index.counts());
        index.absent(entry);
        assertTrue(index.reserveRegistration(entry));
        index.registerSubmitted(entry);
        assertThrows(IllegalStateException.class, () -> index.registerSubmitted(entry));
        for (int i = 0; i < 5; i++) {
            assertSame(entry, index.begin(ACTOR, request).entry());
        }
        assertEquals(new Counts(1, 0, 1, 1, 1, 0), index.counts());
        assertEquals(Stage.REGISTERING, entry.stage());
        assertFalse(entry.dispatched());
    }

    @Test void equalityChecksEveryActualRequestFieldAndCommandKind() {
        var index = index(1, 2, 1, 1);
        DeployRequest original = request(20);
        Entry entry = index.begin(ACTOR, original).entry();
        ExpectedCommandVersions e = original.expected();
        Request[] different = {
            new RetractRequest(original.operationId(), original.podUuid(), original.moduleInstanceId(), original.moduleGeneration(), e),
            new DeployRequest(original.operationId(), id(21), original.moduleInstanceId(), original.moduleGeneration(), e),
            new DeployRequest(original.operationId(), original.podUuid(), id(22), original.moduleGeneration(), e),
            new DeployRequest(original.operationId(), original.podUuid(), original.moduleInstanceId(), 2, e),
            expected(original, new ExpectedCommandVersions(id(30), e.bootGeneration(), e.worldBindingId(), e.entityGeneration(), e.assemblyVersion(), e.controlVersion(), e.deploymentSequence())),
            expected(original, new ExpectedCommandVersions(e.bootUuid(), 2, e.worldBindingId(), e.entityGeneration(), e.assemblyVersion(), e.controlVersion(), e.deploymentSequence())),
            expected(original, new ExpectedCommandVersions(e.bootUuid(), e.bootGeneration(), id(31), e.entityGeneration(), e.assemblyVersion(), e.controlVersion(), e.deploymentSequence())),
            expected(original, new ExpectedCommandVersions(e.bootUuid(), e.bootGeneration(), e.worldBindingId(), 2, e.assemblyVersion(), e.controlVersion(), e.deploymentSequence())),
            expected(original, new ExpectedCommandVersions(e.bootUuid(), e.bootGeneration(), e.worldBindingId(), e.entityGeneration(), 2, e.controlVersion(), e.deploymentSequence())),
            expected(original, new ExpectedCommandVersions(e.bootUuid(), e.bootGeneration(), e.worldBindingId(), e.entityGeneration(), e.assemblyVersion(), 2, e.deploymentSequence())),
            expected(original, new ExpectedCommandVersions(e.bootUuid(), e.bootGeneration(), e.worldBindingId(), e.entityGeneration(), e.assemblyVersion(), e.controlVersion(), 2))
        };
        for (Request changed : different) {
            var result = index.begin(ACTOR, changed);
            assertEquals(AdmissionKind.CONFLICT, result.kind());
            assertSame(entry, result.entry());
        }
        assertEquals(AdmissionKind.CONFLICT, index.begin(id(99), original).kind());
        assertSame(entry, index.begin(id(99), original).entry());
        assertSame(entry, index.begin(ACTOR, roundTrip(original)).entry());
        assertEquals(new Counts(1, 1, 0, 0, 0, 0), index.counts());
    }

    @Test void commitAckLostRemainsOriginalUncertainDespiteDuplicateFoundAndAbsent() {
        var index = index(2, 2, 1, 1);
        DeployRequest request = request(40);
        Entry entry = registering(index, request);
        Throwable primary = new IllegalStateException("register ACK unavailable");
        index.registerUncertain(entry, primary);
        assertSame(entry, index.begin(ACTOR, request).entry());
        assertThrows(IllegalStateException.class, () -> index.absent(entry));
        assertThrows(IllegalStateException.class, () -> index.found(entry, ACTOR, request));
        assertThrows(IllegalStateException.class, () -> index.registrationConfirmed(entry));
        assertThrows(IllegalStateException.class, () -> index.registrationNotAccepted(entry));
        assertThrows(IllegalStateException.class, () -> index.startConfirmed(entry));
        assertThrows(IllegalStateException.class, () -> index.claimDispatch(entry));
        assertSame(primary, entry.cause());
        assertFalse(entry.dispatched());
        index.beginReconciliation(entry);
        assertEquals(Stage.RECONCILING, entry.stage());
        assertThrows(IllegalStateException.class, () -> index.registrationConfirmed(entry));
        assertEquals(new Counts(1, 0, 1, 1, 1, 0), index.counts());
    }

    @Test void unloadedHistoricalDuplicateUsesLookupWhileLiveBudgetIsFull() {
        var index = index(1, 1, 1, 1);
        DeployRequest historical = request(50);
        Entry completed = registering(index, historical);
        index.registrationConfirmed(completed);
        index.startConfirmed(completed);
        index.claimDispatch(completed);
        index.releaseSettled(completed);
        assertEquals(Stage.SETTLED, completed.stage());
        Entry occupying = registering(index, request(51));
        Entry lookup = index.begin(ACTOR, historical).entry();
        assertNotSame(completed, lookup);
        assertEquals(LookupMatch.EQUAL, index.found(lookup, ACTOR, roundTrip(historical)));
        Entry changed = index.begin(ACTOR, withVersion(historical, 3)).entry();
        assertEquals(LookupMatch.CONFLICT, index.found(changed, ACTOR, historical));
        Entry absent = index.begin(ACTOR, request(52)).entry();
        index.absent(absent);
        assertFalse(index.reserveRegistration(absent));
        assertEquals(Stage.REFUSED, absent.stage());
        assertThrows(IllegalArgumentException.class, () -> index.registerSubmitted(absent));
        assertSame(occupying, index.begin(ACTOR, request(51)).entry());
        assertEquals(new Counts(1, 0, 1, 1, 1, 0), index.counts());
    }

    @Test void lookupAndPendingAndQueueAreSeparatelyBounded() {
        var index = index(1, 3, 1, 2);
        Entry first = index.begin(ACTOR, request(60)).entry();
        assertEquals(AdmissionKind.CAPACITY, index.begin(ACTOR, request(61)).kind());
        assertSame(first, index.begin(ACTOR, request(60)).entry());
        index.absent(first);
        assertTrue(index.reserveRegistration(first));
        Entry pendingFull = index.begin(ACTOR, request(61)).entry();
        index.absent(pendingFull);
        assertFalse(index.reserveRegistration(pendingFull));
        index.registerSubmitted(first);
        index.registrationConfirmed(first);
        Entry second = registering(index, request(62));
        index.registrationConfirmed(second);
        Entry queueFull = index.begin(ACTOR, request(63)).entry();
        index.absent(queueFull);
        assertFalse(index.reserveRegistration(queueFull));
        assertEquals(new Counts(2, 0, 2, 0, 2, 2), index.counts());
        index.startConfirmed(first);
        assertEquals(new Counts(2, 0, 2, 0, 1, 1), index.counts());
        assertNotNull(registering(index, request(64)));
        assertEquals(new Counts(3, 0, 3, 1, 2, 1), index.counts());
    }

    @Test void actualDispatchOpportunityAndSettlementAreOnceOnly() {
        var index = index(1, 1, 1, 1);
        Entry entry = registering(index, request(70));
        assertThrows(IllegalStateException.class, () -> index.claimDispatch(entry));
        index.registrationConfirmed(entry);
        index.startConfirmed(entry);
        assertThrows(IllegalStateException.class, () -> index.startConfirmed(entry));
        assertThrows(IllegalStateException.class, () -> index.releaseSettled(entry));
        index.claimDispatch(entry);
        assertTrue(entry.dispatched());
        assertThrows(IllegalStateException.class, () -> index.claimDispatch(entry));
        index.releaseSettled(entry);
        assertThrows(IllegalArgumentException.class, () -> index.releaseSettled(entry));
        assertEquals(new Counts(0, 0, 0, 0, 0, 0), index.counts());
    }

    @Test void retirePreservesAllOriginalSlotsCausesAndLateFailureResponsibility() {
        var index = index(2, 2, 1, 1);
        Entry registered = registering(index, request(80));
        Entry lookup = index.begin(ACTOR, request(81)).entry();
        Counts before = index.counts();
        Throwable primary = new IllegalStateException("original");
        Throwable secondary = new IllegalArgumentException("cleanup");
        index.retainFailure(registered, primary);
        index.retire();
        index.retire();
        index.retainFailure(registered, secondary);
        index.retainFailure(registered, primary);
        index.retainFailure(registered, secondary);
        assertSame(primary, registered.cause());
        assertArrayEquals(new Throwable[] {secondary}, primary.getSuppressed());
        assertSame(registered, index.begin(ACTOR, request(80)).entry());
        assertEquals(AdmissionKind.RETIRED, index.begin(ACTOR, request(82)).kind());
        assertThrows(IllegalStateException.class, () -> index.registrationConfirmed(registered));
        assertThrows(IllegalStateException.class, () -> index.found(lookup, ACTOR, request(81)));
        assertEquals(before, index.counts());
        assertTrue(index.retired());
    }

    @Test void foreignThreadAndForeignEntryCannotTouchOriginalState() throws Exception {
        var index = index(1, 1, 1, 1);
        Entry entry = index.begin(ACTOR, request(90)).entry();
        var other = index(1, 1, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> other.absent(entry));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread foreign = new Thread(() -> {
            try { index.retire(); } catch (Throwable cause) { failure.set(cause); }
        }, "original-index-owner-negative");
        foreign.start();
        foreign.join(5000);
        assertFalse(foreign.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        assertFalse(index.retired());
        assertEquals(Stage.LOOKING_UP, entry.stage());
    }

    @Test void invalidInputAndKnownNoClaimRejectionLeaveBudgetsCorrect() {
        assertThrows(IllegalArgumentException.class, () -> new Limits(0, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new Limits(1, 1, 2, 1));
        var index = index(1, 1, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> index.begin(ACTOR, new QueryOriginalRequest(id(100))));
        assertThrows(NullPointerException.class, () -> index.begin(null, request(100)));
        Entry entry = index.begin(ACTOR, request(100)).entry();
        assertThrows(IllegalArgumentException.class, () -> index.found(entry, ACTOR, request(101)));
        assertEquals(new Counts(1, 1, 0, 0, 0, 0), index.counts());
        index.absent(entry);
        assertThrows(IllegalStateException.class, () -> index.absent(entry));
        assertTrue(index.reserveRegistration(entry));
        index.registrationNotAccepted(entry);
        assertEquals(new Counts(0, 0, 0, 0, 0, 0), index.counts());
    }

    @Test void observePendingReturnsOriginalAndHidesOtherActorsWithoutBudgetChanges() {
        var index = index(1, 1, 1, 1);
        DeployRequest request = request(110);
        Entry entry = index.begin(ACTOR, request).entry();
        Counts before = index.counts();
        assertSame(entry, index.observePending(ACTOR, request.operationId()).orElseThrow());
        assertTrue(index.observePending(id(2), request.operationId()).isEmpty());
        assertTrue(index.observePending(ACTOR, id(111)).isEmpty());
        assertEquals(before, index.counts());
        assertSame(request, entry.request());
        assertEquals(Stage.LOOKING_UP, entry.stage());
        assertNull(entry.cause());
        assertFalse(entry.dispatched());
    }

    @Test void observePendingPreservesUncertainReconciliationAndRetiredResponsibility() {
        var index = index(2, 2, 1, 1);
        DeployRequest request = request(120);
        Entry entry = registering(index, request);
        Throwable primary = new IllegalStateException("register ACK unavailable");
        Throwable secondary = new IllegalArgumentException("cleanup");
        index.registerUncertain(entry, primary);
        Counts before = index.counts();
        assertSame(entry, index.observePending(ACTOR, request.operationId()).orElseThrow());
        assertEquals(Stage.REGISTER_UNCERTAIN, entry.stage());
        assertSame(primary, entry.cause());
        assertEquals(before, index.counts());
        index.beginReconciliation(entry);
        assertSame(entry, index.observePending(ACTOR, request.operationId()).orElseThrow());
        assertEquals(Stage.RECONCILING, entry.stage());
        assertEquals(before, index.counts());
        Entry lookup = index.begin(ACTOR, request(121)).entry();
        index.retainFailure(entry, secondary);
        index.retire();
        before = index.counts();
        assertSame(entry, index.observePending(ACTOR, request.operationId()).orElseThrow());
        assertSame(lookup, index.observePending(ACTOR, id(121)).orElseThrow());
        assertTrue(index.observePending(id(2), request.operationId()).isEmpty());
        assertTrue(index.observePending(ACTOR, id(122)).isEmpty());
        assertEquals(Stage.RECONCILING, entry.stage());
        assertEquals(Stage.LOOKING_UP, lookup.stage());
        assertSame(primary, entry.cause());
        assertArrayEquals(new Throwable[] {secondary}, primary.getSuppressed());
        assertFalse(entry.dispatched());
        assertTrue(index.retired());
        assertEquals(before, index.counts());
    }

    @Test void observePendingReturnsEmptyAfterOriginalRemoval() {
        var index = index(1, 1, 1, 1);
        DeployRequest request = request(130);
        Entry entry = registering(index, request);
        index.registrationConfirmed(entry);
        index.startConfirmed(entry);
        index.claimDispatch(entry);
        assertSame(entry, index.observePending(ACTOR, request.operationId()).orElseThrow());
        assertTrue(entry.dispatched());
        assertEquals(Stage.EXECUTING, entry.stage());
        index.releaseSettled(entry);
        Counts before = index.counts();
        assertTrue(index.observePending(ACTOR, request.operationId()).isEmpty());
        assertEquals(Stage.SETTLED, entry.stage());
        assertTrue(entry.dispatched());
        assertEquals(before, index.counts());
    }

    @Test void observePendingRequiresMainBeforeValidatingNonNullArguments() throws Exception {
        var index = index(1, 1, 1, 1);
        Entry entry = index.begin(ACTOR, request(140)).entry();
        Counts before = index.counts();
        assertThrows(NullPointerException.class, () -> index.observePending(null, id(140)));
        assertThrows(NullPointerException.class, () -> index.observePending(ACTOR, null));
        assertThrows(NullPointerException.class, () -> index.observePending(null, null));
        AtomicReference<Throwable> validFailure = new AtomicReference<>();
        AtomicReference<Throwable> nullFailure = new AtomicReference<>();
        Thread foreign = new Thread(() -> {
            try { index.observePending(ACTOR, id(140)); } catch (Throwable cause) { validFailure.set(cause); }
            try { index.observePending(null, null); } catch (Throwable cause) { nullFailure.set(cause); }
        }, "original-index-observation-owner-negative");
        foreign.start();
        foreign.join(5000);
        assertFalse(foreign.isAlive());
        assertInstanceOf(IllegalStateException.class, validFailure.get());
        assertInstanceOf(IllegalStateException.class, nullFailure.get());
        assertSame(entry, index.observePending(ACTOR, id(140)).orElseThrow());
        assertEquals(Stage.LOOKING_UP, entry.stage());
        assertNull(entry.cause());
        assertFalse(entry.dispatched());
        assertEquals(before, index.counts());
    }

    private static MainDeploymentOriginalOperationIndex index(int lookups, int live, int pending, int queue) {
        return new MainDeploymentOriginalOperationIndex(Thread.currentThread(), new Limits(lookups, live, pending, queue));
    }

    private static Entry registering(MainDeploymentOriginalOperationIndex index, DeployRequest request) {
        var result = index.begin(ACTOR, request);
        assertEquals(AdmissionKind.NEW_LOOKUP, result.kind());
        Entry entry = result.entry();
        index.absent(entry);
        assertTrue(index.reserveRegistration(entry));
        index.registerSubmitted(entry);
        return entry;
    }

    private static UUID id(long value) { return new UUID(0, value); }
    private static DeployRequest request(long operation) {
        return new DeployRequest(id(operation), id(200), id(201), 1,
                new ExpectedCommandVersions(id(202), 1, id(203), 1, 1, 1, 1));
    }
    private static DeployRequest expected(DeployRequest request, ExpectedCommandVersions expected) {
        return new DeployRequest(request.operationId(), request.podUuid(), request.moduleInstanceId(), request.moduleGeneration(), expected);
    }
    private static DeployRequest withVersion(DeployRequest request, long sequence) {
        var e = request.expected();
        return expected(request, new ExpectedCommandVersions(e.bootUuid(), e.bootGeneration(), e.worldBindingId(),
                e.entityGeneration(), e.assemblyVersion(), e.controlVersion(), sequence));
    }
    private static Request roundTrip(Request request) {
        var result = DeploymentCommandWireCodec.decodeRequest(DeploymentCommandWireCodec.encodeRequest(request));
        return ((DeploymentCommandWireCodec.Decoded<Request>) result).value();
    }

    /** 定向直接入口同时保留 JUnit @Test，后续标准发现使用相同用例，不使用反射。 */
    public static void main(String[] args) throws Exception {
        var tests = new MainDeploymentOriginalOperationIndexTest();
        if (args.length == 1 && args[0].equals("--observe-pending")) {
            tests.observePendingReturnsOriginalAndHidesOtherActorsWithoutBudgetChanges();
            tests.observePendingPreservesUncertainReconciliationAndRetiredResponsibility();
            tests.observePendingReturnsEmptyAfterOriginalRemoval();
            tests.observePendingRequiresMainBeforeValidatingNonNullArguments();
            System.out.println("MainDeploymentOriginalOperationIndexTest: PASS (4 observePending behavior cases)");
            return;
        }
        tests.pendingDuplicateAndConflictKeepOneOriginalAndOneSubmissionOpportunity();
        tests.equalityChecksEveryActualRequestFieldAndCommandKind();
        tests.commitAckLostRemainsOriginalUncertainDespiteDuplicateFoundAndAbsent();
        tests.unloadedHistoricalDuplicateUsesLookupWhileLiveBudgetIsFull();
        tests.lookupAndPendingAndQueueAreSeparatelyBounded();
        tests.actualDispatchOpportunityAndSettlementAreOnceOnly();
        tests.retirePreservesAllOriginalSlotsCausesAndLateFailureResponsibility();
        tests.foreignThreadAndForeignEntryCannotTouchOriginalState();
        tests.invalidInputAndKnownNoClaimRejectionLeaveBudgetsCorrect();
        System.out.println("MainDeploymentOriginalOperationIndexTest: PASS (9 directed behavior cases)");
    }
}

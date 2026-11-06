package com.wi.floatingpod.debug;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class DebugItemDispatchTest {
    private final UUID actor = UUID.randomUUID();
    private final DebugItemEndpoint.Identity identity = new DebugItemEndpoint.Identity(UUID.randomUUID(), actor,
            UUID.randomUUID(), UUID.randomUUID(), "test-world", 0, 7, UUID.randomUUID(), UUID.randomUUID());
    private final DebugItemEndpoint.Identity placementIdentity = new DebugItemEndpoint.Identity(identity.boot(), actor,
            identity.session(), identity.world(), identity.worldName(), identity.dimension(), -1, null, null);
    private final DebugItemDispatch dispatch = new DebugItemDispatch();

    private DebugItemEndpoint.Position position(DebugItemEndpoint.Action action) {
        return action == DebugItemEndpoint.Action.PLACE
                ? new DebugItemEndpoint.Position(1.5, 65, 1.5, 0) : null;
    }
    private DebugItemEndpoint.PlacementContext context(DebugItemEndpoint.Action action) {
        return action == DebugItemEndpoint.Action.PLACE
                ? new DebugItemEndpoint.PlacementContext(1, 64, 1, DebugItemEndpoint.PlacementFace.UP) : null;
    }

    private DebugItemDispatch.Result invoke(Endpoint endpoint, DebugItemEndpoint.Identity target,
            DebugItemDispatch.Hand callbackHand, DebugItemDispatch.Hand actualFactHand,
            DebugItemEndpoint.Action action, long tick, DebugItemEndpoint.Position position,
            DebugItemEndpoint.PlacementContext context, boolean held, boolean admin, boolean protection) {
        return dispatch.handleActual(endpoint,
                new DebugItemDispatch.Facts(true, true, actualFactHand, held, true, true, protection, admin),
                target, callbackHand, tick, action, position, context);
    }
    private DebugItemDispatch.Result invoke(Endpoint endpoint, DebugItemDispatch.Hand hand,
                                             DebugItemEndpoint.Action action, long tick) {
        DebugItemEndpoint.Identity target = action == DebugItemEndpoint.Action.PLACE ? placementIdentity : identity;
        return invoke(endpoint, target, hand, hand, action, tick, position(action), context(action),
                true, action == DebugItemEndpoint.Action.PLACE, true);
    }
    private static final class Endpoint implements DebugItemEndpoint {
        boolean ready = true;
        int submissions;
        int inspections;
        CachedPod pod;
        Reply result = Reply.of(Status.PENDING);
        RuntimeException failure;
        @Override public boolean isReady() { return ready; }
        @Override public CachedPod inspect(Request request) { inspections++; return pod; }
        @Override public Reply submit(Request request) {
            submissions++;
            if (failure != null) throw failure;
            return result;
        }
    }

    @Test void pendingRecallCanBeInspectedButCannotAdmitModeMutation() {
        Endpoint endpoint = new Endpoint();
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, actor, "Normal", true,
                "PendingRecall", DebugItemEndpoint.Reason.RECOVERY_PENDING);
        DebugItemDispatch.Result inspected = invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemEndpoint.Action.INSPECT, 1);
        assertEquals(DebugItemEndpoint.Status.INSPECTED, inspected.reply().status());
        assertEquals("PendingRecall", inspected.reply().view().phase());
        assertEquals(DebugItemEndpoint.Reason.RECOVERY_PENDING, inspected.reply().view().reason());
        DebugItemDispatch.Result toggle = invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemEndpoint.Action.TOGGLE, 2);
        assertEquals(DebugItemEndpoint.Status.PENDING, toggle.reply().status());
        assertEquals(0, endpoint.submissions);
    }

    @Test void eitherHandCanIndependentlyPlaceInspectAndOwnerToggle() {
        for (DebugItemDispatch.Hand hand : DebugItemDispatch.Hand.values()) {
            Endpoint endpoint = new Endpoint();
            DebugItemDispatch.Result placed = invoke(endpoint, hand, DebugItemEndpoint.Action.PLACE,
                    hand == DebugItemDispatch.Hand.MAIN_HAND ? 1 : 2);
            assertEquals(DebugItemEndpoint.Status.PENDING, placed.reply().status());
            assertEquals(hand, placed.request().hand());

            endpoint.pod = new DebugItemEndpoint.CachedPod(identity, actor, "WorldIdle", true,
                    "Committed", DebugItemEndpoint.Reason.NONE);
            DebugItemDispatch.Result inspected = invoke(endpoint, hand, DebugItemEndpoint.Action.INSPECT,
                    hand == DebugItemDispatch.Hand.MAIN_HAND ? 3 : 4);
            assertEquals(DebugItemEndpoint.Status.INSPECTED, inspected.reply().status());
            assertEquals(hand, inspected.request().hand());

            DebugItemDispatch.Result toggled = invoke(endpoint, hand, DebugItemEndpoint.Action.TOGGLE,
                    hand == DebugItemDispatch.Hand.MAIN_HAND ? 5 : 6);
            assertEquals(DebugItemEndpoint.Status.PENDING, toggled.reply().status());
            assertEquals(hand, toggled.request().hand());
            assertEquals(2, endpoint.submissions);
        }
    }

    @Test void actualHandMismatchAndMissingHeldToolRejectBeforeEndpoint() {
        Endpoint endpoint = new Endpoint();
        DebugItemDispatch.Result mismatch = invoke(endpoint, placementIdentity,
                DebugItemDispatch.Hand.MAIN_HAND, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemEndpoint.Action.PLACE, 1, position(DebugItemEndpoint.Action.PLACE),
                context(DebugItemEndpoint.Action.PLACE), true, true, true);
        assertEquals(DebugItemEndpoint.Status.REJECTED, mismatch.reply().status());
        assertNull(mismatch.request());

        DebugItemDispatch.Result noTool = invoke(endpoint, placementIdentity,
                DebugItemDispatch.Hand.OFF_HAND, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemEndpoint.Action.PLACE, 2, position(DebugItemEndpoint.Action.PLACE),
                context(DebugItemEndpoint.Action.PLACE), false, true, true);
        assertEquals(DebugItemEndpoint.Status.REJECTED, noTool.reply().status());
        assertEquals(0, endpoint.submissions);
    }

    @Test void dualWieldSameGestureReusesFirstHandRequestOperationAndReply() {
        Endpoint endpoint = new Endpoint();
        DebugItemDispatch.Result first = invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemEndpoint.Action.PLACE, 1);
        DebugItemDispatch.Result replay = invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemEndpoint.Action.PLACE, 1);
        assertEquals(DebugItemEndpoint.Status.PENDING, first.reply().status());
        assertTrue(replay.duplicate());
        assertSame(first.request(), replay.request());
        assertEquals(first.request().operation(), replay.request().operation());
        assertEquals(DebugItemDispatch.Hand.OFF_HAND, replay.request().hand());
        assertEquals(first.reply(), replay.reply());
        assertEquals(1, endpoint.submissions);
    }

    @Test void duplicateDoesNotReauthorizeDifferentPermissionFacts() {
        Endpoint endpoint = new Endpoint();
        DebugItemDispatch.Result first = invoke(endpoint, placementIdentity,
                DebugItemDispatch.Hand.OFF_HAND, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemEndpoint.Action.PLACE, 1, position(DebugItemEndpoint.Action.PLACE),
                context(DebugItemEndpoint.Action.PLACE), true, false, true);
        DebugItemDispatch.Result replay = invoke(endpoint, placementIdentity,
                DebugItemDispatch.Hand.MAIN_HAND, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemEndpoint.Action.PLACE, 1, position(DebugItemEndpoint.Action.PLACE),
                context(DebugItemEndpoint.Action.PLACE), true, true, true);
        assertEquals(DebugItemEndpoint.Status.REJECTED, first.reply().status());
        assertEquals(first.reply(), replay.reply());
        assertSame(first.request(), replay.request());
        assertTrue(replay.duplicate());
        assertEquals(0, endpoint.submissions);
    }

    @Test void sameTickDifferentClickedFacesKeepSeparateGuards() {
        Endpoint endpoint = new Endpoint();
        DebugItemEndpoint.Position sharedTarget = new DebugItemEndpoint.Position(1.5, 65, 1.5, 0);
        DebugItemEndpoint.PlacementContext up =
                new DebugItemEndpoint.PlacementContext(1, 64, 1, DebugItemEndpoint.PlacementFace.UP);
        DebugItemEndpoint.PlacementContext east =
                new DebugItemEndpoint.PlacementContext(0, 65, 1, DebugItemEndpoint.PlacementFace.EAST);
        DebugItemDispatch.Result first = invoke(endpoint, placementIdentity, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.PLACE, 9, sharedTarget, up,
                true, true, true);
        DebugItemDispatch.Result otherFace = invoke(endpoint, placementIdentity, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemDispatch.Hand.OFF_HAND, DebugItemEndpoint.Action.PLACE, 9, sharedTarget, east,
                true, true, true);
        assertEquals(DebugItemEndpoint.Status.PENDING, first.reply().status());
        assertEquals(DebugItemEndpoint.Status.PENDING, otherFace.reply().status());
        assertNotEquals(first.request().operation(), otherFace.request().operation());
        assertEquals(2, endpoint.submissions);
    }

    @Test void sameTickDifferentSessionWorldAndBodyKeepSeparateGuards() {
        Endpoint endpoint = new Endpoint();
        DebugItemEndpoint.Identity[] variants = {
                new DebugItemEndpoint.Identity(identity.boot(), actor, UUID.randomUUID(), identity.world(),
                        identity.worldName(), identity.dimension(), identity.entityId(), identity.pod(), identity.body()),
                new DebugItemEndpoint.Identity(identity.boot(), actor, identity.session(), UUID.randomUUID(),
                        "other-world", 2, identity.entityId(), identity.pod(), identity.body()),
                new DebugItemEndpoint.Identity(identity.boot(), actor, identity.session(), identity.world(),
                        identity.worldName(), identity.dimension(), identity.entityId(), identity.pod(), UUID.randomUUID()),
                new DebugItemEndpoint.Identity(identity.boot(), actor, identity.session(), identity.world(),
                        identity.worldName(), identity.dimension(), 8, UUID.randomUUID(), UUID.randomUUID())
        };
        DebugItemEndpoint.Identity[] identities = {identity, variants[0], variants[1], variants[2], variants[3]};
        java.util.List<DebugItemDispatch.Result> results = new java.util.ArrayList<>();
        for (int index = 0; index < identities.length; index++) {
            endpoint.pod = new DebugItemEndpoint.CachedPod(identities[index], actor, "WorldIdle", true,
                    "Committed", DebugItemEndpoint.Reason.NONE);
            results.add(invoke(endpoint, identities[index], DebugItemDispatch.Hand.MAIN_HAND,
                    DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.INSPECT,
                    10, null, null, true, false, true));
        }
        for (int i = 0; i < results.size(); i++) {
            assertEquals(DebugItemEndpoint.Status.INSPECTED, results.get(i).reply().status());
            for (int j = i + 1; j < results.size(); j++)
                assertNotEquals(results.get(i).request().operation(), results.get(j).request().operation());
        }
        assertEquals(5, endpoint.inspections);
    }

    @Test void placementRequiresActualAdministratorAndProtection() {
        Endpoint endpoint = new Endpoint();
        assertEquals(DebugItemEndpoint.Status.REJECTED,
                invoke(endpoint, placementIdentity, DebugItemDispatch.Hand.MAIN_HAND,
                        DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.PLACE, 1,
                        position(DebugItemEndpoint.Action.PLACE), context(DebugItemEndpoint.Action.PLACE),
                        true, false, true).reply().status());
        DebugItemDispatch.Result noProtection = invoke(endpoint, placementIdentity,
                DebugItemDispatch.Hand.OFF_HAND, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemEndpoint.Action.PLACE, 2, position(DebugItemEndpoint.Action.PLACE),
                context(DebugItemEndpoint.Action.PLACE), true, true, false);
        assertEquals(DebugItemEndpoint.Status.REJECTED, noProtection.reply().status());
        assertEquals(0, endpoint.submissions);
    }

    @Test void ownerCheckNeverTreatsAdministratorAsOwner() {
        Endpoint endpoint = new Endpoint();
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, null, "WorldIdle", true,
                "Committed", DebugItemEndpoint.Reason.NONE);
        assertEquals(DebugItemEndpoint.Status.REJECTED,
                invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND, DebugItemEndpoint.Action.TOGGLE, 1).reply().status());
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, UUID.randomUUID(), "Normal", true,
                "Committed", DebugItemEndpoint.Reason.NONE);
        assertEquals(DebugItemEndpoint.Status.REJECTED,
                invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.TOGGLE, 2).reply().status());
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, actor, "Normal", true,
                "Committed", DebugItemEndpoint.Reason.NONE);
        DebugItemDispatch.Facts ownerWithoutAdmin =
                new DebugItemDispatch.Facts(true, true, DebugItemDispatch.Hand.OFF_HAND, true,
                        true, true, true, false);
        DebugItemDispatch.Result allowed = dispatch.handleActual(endpoint, ownerWithoutAdmin, identity,
                DebugItemDispatch.Hand.OFF_HAND, 3, DebugItemEndpoint.Action.TOGGLE, null, null);
        assertEquals(DebugItemEndpoint.Status.PENDING, allowed.reply().status());
        assertEquals(DebugItemDispatch.Hand.OFF_HAND, allowed.request().hand());
        assertEquals(1, endpoint.submissions);
    }

    @Test void inspectionReturnsOnlyNonSensitiveProjectionAndDoesNotWrite() {
        Endpoint endpoint = new Endpoint();
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, actor, "WorldIdle", true,
                "Committed", DebugItemEndpoint.Reason.NONE);
        DebugItemDispatch.Result result = invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemEndpoint.Action.INSPECT, 1);
        assertEquals(new DebugItemEndpoint.View(identity.pod(), actor, "WorldIdle", true,
                "Committed", DebugItemEndpoint.Reason.NONE), result.reply().view());
        assertEquals(DebugItemEndpoint.Status.INSPECTED, result.reply().status());
        assertEquals(0, endpoint.submissions);
    }

    @Test void realOwnerCannotTogglePendingOrInactiveBody() {
        Endpoint endpoint = new Endpoint();
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, actor, "WorldIdle", true,
                "Prepared", DebugItemEndpoint.Reason.NONE);
        assertEquals(DebugItemEndpoint.Status.REJECTED,
                invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND, DebugItemEndpoint.Action.TOGGLE, 1).reply().status());
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, actor, "WorldIdle", false,
                "Committed", DebugItemEndpoint.Reason.NONE);
        assertEquals(DebugItemEndpoint.Status.REJECTED,
                invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.TOGGLE, 2).reply().status());
        assertEquals(0, endpoint.submissions);
    }

    @Test void staleExactIdentityIsRejected() {
        Endpoint endpoint = new Endpoint();
        DebugItemEndpoint.Identity stale = new DebugItemEndpoint.Identity(identity.boot(), actor, UUID.randomUUID(),
                identity.world(), "test-world", 0, 7, identity.pod(), identity.body());
        endpoint.pod = new DebugItemEndpoint.CachedPod(stale, actor, "Normal", true,
                "Committed", DebugItemEndpoint.Reason.NONE);
        assertEquals(DebugItemEndpoint.Status.REJECTED,
                invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND, DebugItemEndpoint.Action.TOGGLE, 1).reply().status());
        assertEquals(0, endpoint.submissions);
    }

    @Test void unexpectedCausePropagatesAndDualHandReplayCannotRepeatFailure() {
        Endpoint endpoint = new Endpoint();
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, actor, "Normal", true,
                "Committed", DebugItemEndpoint.Reason.NONE);
        endpoint.failure = new IllegalStateException("original");
        assertSame(endpoint.failure, assertThrows(IllegalStateException.class,
                () -> invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND, DebugItemEndpoint.Action.TOGGLE, 1)));
        DebugItemEndpoint.Request failed = dispatch.findRequest(identity, DebugItemEndpoint.Action.TOGGLE,
                null, null, 1);
        assertNotNull(failed);
        DebugItemDispatch.Result replay = invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemEndpoint.Action.TOGGLE, 1);
        assertEquals(DebugItemEndpoint.Status.FAULTED, replay.reply().status());
        assertSame(failed, replay.request());
        assertEquals(DebugItemDispatch.Hand.OFF_HAND, replay.request().hand());
        assertTrue(replay.duplicate());
        assertEquals(1, endpoint.submissions);
    }

    @Test void invalidMainSessionReachHeldToolAndExactBodyFailBeforeEndpoint() {
        Endpoint endpoint = new Endpoint();
        assertThrows(IllegalStateException.class, () -> dispatch.handleActual(endpoint,
                new DebugItemDispatch.Facts(false, true, DebugItemDispatch.Hand.MAIN_HAND, true,
                        true, true, true, true), placementIdentity, DebugItemDispatch.Hand.MAIN_HAND, 1,
                DebugItemEndpoint.Action.PLACE, position(DebugItemEndpoint.Action.PLACE),
                context(DebugItemEndpoint.Action.PLACE)));
        DebugItemDispatch.Facts[] denied = {
                new DebugItemDispatch.Facts(true, false, DebugItemDispatch.Hand.MAIN_HAND, true, true, true, true, true),
                new DebugItemDispatch.Facts(true, true, DebugItemDispatch.Hand.MAIN_HAND, false, true, true, true, true),
                new DebugItemDispatch.Facts(true, true, DebugItemDispatch.Hand.MAIN_HAND, true, false, true, true, true),
                new DebugItemDispatch.Facts(true, true, DebugItemDispatch.Hand.MAIN_HAND, true, true, false, true, true)
        };
        long tick = 1;
        for (DebugItemDispatch.Facts facts : denied) {
            assertEquals(DebugItemEndpoint.Status.REJECTED,
                    dispatch.handleActual(endpoint, facts, placementIdentity, DebugItemDispatch.Hand.MAIN_HAND, tick++,
                            DebugItemEndpoint.Action.PLACE, position(DebugItemEndpoint.Action.PLACE),
                            context(DebugItemEndpoint.Action.PLACE)).reply().status());
        }
        assertEquals(0, endpoint.submissions);
    }

    @Test void sameTickDifferentActionStillRunsOwnerGuard() {
        Endpoint endpoint = new Endpoint();
        endpoint.pod = new DebugItemEndpoint.CachedPod(identity, null, "WorldIdle", true,
                "Committed", DebugItemEndpoint.Reason.NONE);
        DebugItemDispatch.Result inspect = invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemEndpoint.Action.INSPECT, 1);
        assertEquals(DebugItemEndpoint.Status.INSPECTED, inspect.reply().status());
        DebugItemDispatch.Result toggle = invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemEndpoint.Action.TOGGLE, 1);
        assertNotEquals(inspect.request().operation(), toggle.request().operation());
        assertEquals(DebugItemEndpoint.Status.REJECTED, toggle.reply().status());
        assertEquals(0, endpoint.submissions);
    }

    @Test void sameTickDifferentPlacementTargetHasIndependentGuardAndSameTargetDeduplicatesYaw() {
        Endpoint endpoint = new Endpoint();
        DebugItemEndpoint.Position firstPosition = new DebugItemEndpoint.Position(1.5, 65, 1.5, 0);
        DebugItemEndpoint.PlacementContext firstContext =
                new DebugItemEndpoint.PlacementContext(1, 64, 1, DebugItemEndpoint.PlacementFace.UP);
        DebugItemDispatch.Result first = invoke(endpoint, placementIdentity, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.PLACE, 1,
                firstPosition, firstContext, true, true, true);
        DebugItemEndpoint.Position secondPosition = new DebugItemEndpoint.Position(2.5, 65, 1.5, 0);
        DebugItemEndpoint.PlacementContext secondContext =
                new DebugItemEndpoint.PlacementContext(2, 64, 1, DebugItemEndpoint.PlacementFace.UP);
        DebugItemDispatch.Result second = invoke(endpoint, placementIdentity, DebugItemDispatch.Hand.OFF_HAND,
                DebugItemDispatch.Hand.OFF_HAND, DebugItemEndpoint.Action.PLACE, 1,
                secondPosition, secondContext, true, true, true);
        assertNotEquals(first.request().operation(), second.request().operation());
        DebugItemDispatch.Result sameTargetNewYaw = invoke(endpoint, placementIdentity,
                DebugItemDispatch.Hand.MAIN_HAND, DebugItemDispatch.Hand.MAIN_HAND,
                DebugItemEndpoint.Action.PLACE, 1, new DebugItemEndpoint.Position(2.5, 65, 1.5, 90),
                secondContext, true, true, true);
        assertSame(second.request(), sameTargetNewYaw.request());
        assertTrue(sameTargetNewYaw.duplicate());
        assertEquals(2, endpoint.submissions);
    }

    @Test void unavailableAndPendingNeverManufactureCommittedAcknowledgement() {
        assertEquals(DebugItemEndpoint.Status.UNAVAILABLE,
                invoke(null, DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.PLACE, 1).reply().status());
        Endpoint endpoint = new Endpoint();
        endpoint.ready = false;
        assertEquals(DebugItemEndpoint.Status.UNAVAILABLE,
                invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND, DebugItemEndpoint.Action.PLACE, 2).reply().status());
        endpoint.ready = true;
        assertEquals(DebugItemEndpoint.Status.PENDING,
                invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.PLACE, 3).reply().status());
        endpoint.result = new DebugItemEndpoint.Reply(DebugItemEndpoint.Status.INSPECTED,
                new DebugItemEndpoint.View(identity.pod(), null, "WorldIdle", true,
                        "Committed", DebugItemEndpoint.Reason.NONE));
        assertThrows(IllegalStateException.class,
                () -> invoke(endpoint, DebugItemDispatch.Hand.MAIN_HAND, DebugItemEndpoint.Action.PLACE, 4));
    }

    @Test void ownedUnclaimedCleanupAndFaultedStatesHaveExactPublicProjection() {
        Endpoint endpoint = new Endpoint();
        DebugItemEndpoint.CachedPod[] states = {
                new DebugItemEndpoint.CachedPod(identity, actor, "Normal", true, "Committed", DebugItemEndpoint.Reason.NONE),
                new DebugItemEndpoint.CachedPod(identity, null, "WorldIdle", true, "Prepared", DebugItemEndpoint.Reason.INITIALIZATION_PENDING),
                new DebugItemEndpoint.CachedPod(identity, null, "WorldIdle", true, "PendingCleanup", DebugItemEndpoint.Reason.CLEANUP_PENDING),
                new DebugItemEndpoint.CachedPod(identity, actor, "WorldIdle", true, "Faulted", DebugItemEndpoint.Reason.FAULTED)
        };
        long tick = 1;
        for (DebugItemEndpoint.CachedPod state : states) {
            endpoint.pod = state;
            DebugItemDispatch.Result result = invoke(endpoint, DebugItemDispatch.Hand.OFF_HAND,
                    DebugItemEndpoint.Action.INSPECT, tick++);
            assertEquals(new DebugItemEndpoint.View(identity.pod(), state.owner(), state.mode(), state.active(),
                    state.phase(), state.reason()), result.reply().view());
        }
        assertEquals(0, endpoint.submissions);
    }
}

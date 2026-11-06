package com.wi.floatingpod.debug;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** 实际回调共用的权限和同 tick 去重责任；不持有游戏对象。仅由 Main 串行调用。 */
public final class DebugItemDispatch {
    /** 自有不可变手别值；不把 Minecraft EnumHand 或玩家对象带进请求/IO命令。 */
    public enum Hand { MAIN_HAND, OFF_HAND }

    public record Facts(boolean mainThread, boolean actualSession, Hand actualHand, boolean heldTool,
                        boolean exactWorldAndBody, boolean reachable, boolean protectedPlacement,
                        boolean administrator) { }

    public record PlacementTarget(double x, double y, double z) { }

    /** 手别保存在首次获接纳的 Request；同一原始手势的副手回调共享此去重键。 */
    public record Gesture(DebugItemEndpoint.Identity identity, Hand hand, DebugItemEndpoint.Action action,
                         PlacementTarget placement, DebugItemEndpoint.PlacementContext placementContext,
                         long tick) {
        private GestureKey key() {
            return new GestureKey(identity, action, placement, placementContext, tick);
        }
    }

    private record GestureKey(DebugItemEndpoint.Identity identity, DebugItemEndpoint.Action action,
                             PlacementTarget placement, DebugItemEndpoint.PlacementContext placementContext,
                             long tick) { }

    private record Attempt(UUID operation, DebugItemEndpoint.Request request, DebugItemEndpoint.Reply reply) { }

    /** duplicate=true 时返回首次 Request 与其原结果，不会产生第二个 Request 或 operation。 */
    public record Result(DebugItemEndpoint.Request request, DebugItemEndpoint.Reply reply, boolean duplicate) {
        public Result {
            if (reply == null) throw new IllegalArgumentException("Debug dispatch reply required");
        }
    }

    private final Map<GestureKey, Attempt> attempts = new HashMap<>();

    /**
     * 兼容旧的纯业务调用方；实际 vanilla 回调应使用 handleActual，以便去重前不创建新 Request。
     */
    public UUID begin(Gesture gesture) {
        if (gesture == null || gesture.identity() == null || gesture.hand() == null || gesture.action() == null)
            throw new IllegalArgumentException("Complete debug gesture required");
        attempts.keySet().removeIf(key -> key.tick() != gesture.tick());
        Attempt prior = attempts.get(gesture.key());
        if (prior != null) return prior.operation();
        UUID operation = UUID.randomUUID();
        attempts.put(gesture.key(), new Attempt(operation, null, null));
        return operation;
    }

    public static Gesture gesture(DebugItemEndpoint.Identity identity, Hand hand,
                                  DebugItemEndpoint.Action action, DebugItemEndpoint.Position position,
                                  DebugItemEndpoint.PlacementContext context, long tick) {
        if (hand == null) throw new IllegalArgumentException("Actual debug hand required");
        return new Gesture(identity, hand, action,
                position == null ? null : new PlacementTarget(position.x(), position.y(), position.z()), context, tick);
    }

    /** Source-compatible helper for historic main-hand callers. */
    public static Gesture gesture(DebugItemEndpoint.Identity identity, DebugItemEndpoint.Action action,
                                  DebugItemEndpoint.Position position, DebugItemEndpoint.PlacementContext context,
                                  long tick) {
        return gesture(identity, Hand.MAIN_HAND, action, position, context, tick);
    }

    /** Real callback path: validates the callback hand before lookup, then reuses the first immutable request. */
    public Result handleActual(DebugItemEndpoint endpoint, Facts facts, DebugItemEndpoint.Identity identity,
                               Hand hand, long tick, DebugItemEndpoint.Action action,
                               DebugItemEndpoint.Position position,
                               DebugItemEndpoint.PlacementContext context) {
        return handleInternal(endpoint, facts, identity, hand, tick, action, position, context, null);
    }

    /** Compatibility entry for callers that already own an immutable Request. */
    public DebugItemEndpoint.Reply handle(DebugItemEndpoint endpoint, Facts facts,
                                          DebugItemEndpoint.Request request) {
        if (request == null) throw new IllegalArgumentException("Original debug request required");
        return handleInternal(endpoint, facts, request.identity(), request.hand(), request.tick(), request.action(),
                request.placement(), request.placementContext(), request).reply();
    }

    private Result handleInternal(DebugItemEndpoint endpoint, Facts facts,
                                  DebugItemEndpoint.Identity identity, Hand hand, long tick,
                                  DebugItemEndpoint.Action action, DebugItemEndpoint.Position position,
                                  DebugItemEndpoint.PlacementContext context, DebugItemEndpoint.Request supplied) {
        if (facts == null) throw new IllegalArgumentException("Actual callback facts required");
        if (!facts.mainThread()) throw new IllegalStateException("Debug item callback requires Main thread");
        if (identity == null || hand == null || action == null
                || facts.actualHand() != hand || !facts.heldTool())
            return rejected();

        Gesture gesture = gesture(identity, hand, action, position, context, tick);
        attempts.keySet().removeIf(key -> key.tick() != tick);
        GestureKey key = gesture.key();
        Attempt prior = attempts.get(key);
        if (prior != null && prior.request() != null)
            return new Result(prior.request(), prior.reply(), true);

        UUID operation = prior != null ? prior.operation()
                : supplied != null ? supplied.operation() : UUID.randomUUID();
        if (supplied != null && !operation.equals(supplied.operation()))
            throw new IllegalStateException("Preallocated debug operation differs from retained gesture");
        DebugItemEndpoint.Request request = supplied != null ? supplied
                : new DebugItemEndpoint.Request(identity, tick, operation, action, position, context, hand);
        if (request.hand() != hand)
            return rejected();

        // Reserve a terminal fault result first so an unexpected failure cannot be repeated by the other hand.
        attempts.put(key, new Attempt(request.operation(), request,
                DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.FAULTED)));
        DebugItemEndpoint.Reply reply = dispatch(endpoint, facts, request);
        attempts.put(key, new Attempt(request.operation(), request, reply));
        return new Result(request, reply, false);
    }

    private static Result rejected() {
        return new Result(null, DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.REJECTED), false);
    }

    /** Finds the original accepted context for fault reporting; does not create or reauthorize a request. */
    public DebugItemEndpoint.Request findRequest(DebugItemEndpoint.Identity identity,
            DebugItemEndpoint.Action action, DebugItemEndpoint.Position position,
            DebugItemEndpoint.PlacementContext context, long tick) {
        Gesture gesture = gesture(identity, Hand.MAIN_HAND, action, position, context, tick);
        Attempt attempt = attempts.get(gesture.key());
        return attempt == null ? null : attempt.request();
    }

    private DebugItemEndpoint.Reply dispatch(DebugItemEndpoint endpoint, Facts facts,
                                              DebugItemEndpoint.Request request) {
        if (!facts.actualSession() || !facts.exactWorldAndBody() || !facts.reachable())
            return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.REJECTED);
        if (request.action() == DebugItemEndpoint.Action.PLACE
                && (!facts.administrator() || !facts.protectedPlacement()))
            return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.REJECTED);
        if (endpoint == null || !endpoint.isReady())
            return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.UNAVAILABLE);
        if (request.action() != DebugItemEndpoint.Action.PLACE) {
            DebugItemEndpoint.CachedPod pod = endpoint.inspect(request);
            if (pod == null) return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.UNAVAILABLE);
            if (!request.identity().equals(pod.identity()))
                return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.REJECTED);
            if (request.action() == DebugItemEndpoint.Action.INSPECT)
                return new DebugItemEndpoint.Reply(DebugItemEndpoint.Status.INSPECTED,
                        new DebugItemEndpoint.View(pod.identity().pod(), pod.owner(), pod.mode(), pod.active(),
                                pod.phase(), pod.reason()));
            if (pod.owner() == null || !pod.owner().equals(request.identity().player()))
                return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.REJECTED);
            if ("PendingRecall".equals(pod.phase()))
                return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.PENDING);
            if (!pod.active() || !"Committed".equals(pod.phase()))
                return DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.REJECTED);
        }
        DebugItemEndpoint.Reply reply = endpoint.submit(request);
        if (reply == null || (reply.status() != DebugItemEndpoint.Status.PENDING
                && reply.status() != DebugItemEndpoint.Status.REJECTED
                && reply.status() != DebugItemEndpoint.Status.UNAVAILABLE))
            throw new IllegalStateException("Async debug submission returned an invalid acknowledgement");
        return reply;
    }

    /** 断线/boot teardown 必须释放短期输入状态。 */
    public void forget(UUID session) {
        attempts.keySet().removeIf(key -> key.identity().session().equals(session));
    }
    public void clear() { attempts.clear(); }
}

package com.wi.floatingpod.entity;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

final class NormalStopEntityFreezeTest {
    private final UUID pod = UUID.randomUUID(), attempt = UUID.randomUUID(), request = UUID.randomUUID();
    private FloatingPodControlSnapshot state(UUID owner, String mode, long version, UUID actualAttempt) {
        return new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED,
                pod, owner, mode, true, version, "world", 1, 64, 1, 0, 0, 0, null, actualAttempt, null, null);
    }
    @Test void actualFreezeAllowsInflightModeAckButRejectsForeignAttemptRequestAndFault() {
        UUID owner = UUID.randomUUID();
        FloatingPodControlSnapshot original = state(owner, "Normal", 1, attempt);
        NormalStopEntityFreeze freeze = new NormalStopEntityFreeze(request, original);
        freeze.requireCurrent(request, state(owner, "WorldIdle", 2, attempt));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(UUID.randomUUID(), original));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(request, state(owner, "Normal", 1, UUID.randomUUID())));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(request, original.asFaulted(new Exception("original"))));
    }
    @Test void ownerlessWorldIdleFreezeIsMainBound() throws InterruptedException {
        FloatingPodControlSnapshot original = state(null, "WorldIdle", 1, attempt);
        NormalStopEntityFreeze freeze = new NormalStopEntityFreeze(request, original);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try { freeze.requireCurrent(request, original); } catch (Throwable cause) { failure.set(cause); }
        });
        other.start(); other.join(3000);
        assertFalse(other.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        freeze.requireCurrent(request, original);
    }
}

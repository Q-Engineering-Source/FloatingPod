package com.wi.floatingpod.entity;

import java.util.Objects;
import java.util.UUID;

/** 原实体的本次停服冻结；在途 mode/drift ACK 更新控制态时仍保持冻结，不签关闭许可。 */
public final class NormalStopEntityFreeze {
    private final Thread main;
    private final UUID request;
    private final UUID pod;
    private final UUID attempt;

    public NormalStopEntityFreeze(UUID request, FloatingPodControlSnapshot original) {
        this.main = Thread.currentThread();
        this.request = Objects.requireNonNull(request);
        requireCommitted(original);
        this.pod = original.podUuid();
        this.attempt = Objects.requireNonNull(original.attemptUuid());
    }

    public void requireCurrent(UUID request, FloatingPodControlSnapshot current) {
        if (Thread.currentThread() != main || !this.request.equals(request))
            throw new IllegalStateException("Normal stop freeze belongs to another Main/request");
        requireCommitted(current);
        if (!pod.equals(current.podUuid()) || !attempt.equals(current.attemptUuid()))
            throw new IllegalStateException("Normal stop freeze body/attempt changed");
    }

    private static void requireCommitted(FloatingPodControlSnapshot snapshot) {
        if (snapshot == null || snapshot.phase() != FloatingPodControlSnapshot.Phase.COMMITTED || !snapshot.active())
            throw new IllegalStateException("Normal stop freeze requires original committed active control");
    }
}

package com.wi.floatingpod.entity;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

final class DbFirstCheckpointFreezeTest {
    private final UUID operation = new UUID(0, 11), pod = new UUID(0, 12), owner = new UUID(0, 13);
    private final UUID binding = new UUID(0, 14), attempt = new UUID(0, 15), creation = new UUID(0, 16);
    private final DbFirstPodSnapshot.Pose pose = new DbFirstPodSnapshot.Pose("world", 0, 4, 65, 8, 3, 4);
    private FloatingPodControlSnapshot state(UUID owner, String mode, long version, UUID actualBinding, UUID actualAttempt,
            double x) {
        return state(pod, owner, mode, version, actualBinding, actualAttempt, creation, x);
    }
    private FloatingPodControlSnapshot state(UUID actualPod, UUID owner, String mode, long version,
            UUID actualBinding, UUID actualAttempt, UUID actualOperation, double x) {
        return new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED, actualPod, owner, mode, true,
                version, "world", x, 65, 8, 3, 4, 0, actualBinding, actualAttempt, actualOperation, null);
    }

    @Test void permitsOwnerlessWorldIdleAndPreservesActiveSuccessorAndOriginalStopRequest() {
        var original = state(null, "WorldIdle", 7, binding, attempt, 4);
        UUID stop = new UUID(0, 17);
        var freeze = new DbFirstCheckpointFreeze(operation, 7, original, pose, stop);
        freeze.requireCurrent(operation, 7, original, pose);
        freeze.requireCommittedSuccessor(state(null, "WorldIdle", 8, binding, attempt, 4));
        assertSame(original, freeze.original()); assertEquals(stop, freeze.normalStopRequest());
        assertEquals(operation, freeze.operationUuid()); assertEquals(pose, freeze.pose());
        var normal = state(owner, "Normal", 7, binding, attempt, 4);
        var ordinary = new DbFirstCheckpointFreeze(operation, 7, normal, pose, null);
        ordinary.requireCommittedSuccessor(state(owner, "Normal", 8, binding, attempt, 4));
        assertNull(ordinary.normalStopRequest());
    }

    @Test void rejectsReplacementControlWrongOperationVersionOrChangedPhysicalPose() {
        var original = state(owner, "Normal", 7, binding, attempt, 4);
        var freeze = new DbFirstCheckpointFreeze(operation, 7, original, pose, null);
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(new UUID(0, 99), 7, original, pose));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 8, original, pose));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 7,
                state(owner, "Normal", 7, binding, attempt, 4), pose));
        assertThrows(IllegalStateException.class, () -> freeze.requireCurrent(operation, 7, original,
                new DbFirstPodSnapshot.Pose("world", 0, 5, 65, 8, 3, 4)));
        freeze.requireCurrent(operation, 7, original, pose);
    }

    @Test void boundDatabaseRecoveryControlCanCheckpointWithoutInventingCreationStamp() {
        var original = state(pod, null, "WorldIdle", 7, binding, attempt, null, 4);
        var freeze = new DbFirstCheckpointFreeze(operation, 7, original, pose, null);
        freeze.requireCurrent(operation, 7, original, pose);
        freeze.requireCommittedSuccessor(state(pod, null, "WorldIdle", 8, binding, attempt, null, 4));
        assertThrows(IllegalStateException.class, () -> freeze.requireCommittedSuccessor(
                state(pod, null, "WorldIdle", 8, new UUID(0, 99), attempt, null, 4)));
        assertThrows(IllegalStateException.class, () -> freeze.requireCommittedSuccessor(
                state(pod, null, "WorldIdle", 8, binding, new UUID(0, 99), null, 4)));
        assertThrows(IllegalStateException.class, () -> freeze.requireCommittedSuccessor(
                state(new UUID(0, 99), null, "WorldIdle", 8, binding, attempt, null, 4)));
        assertThrows(IllegalStateException.class, () -> freeze.requireCommittedSuccessor(
                new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED, pod, null,
                        "WorldIdle", true, 8, "other", 4, 65, 8, 3, 4, 0, binding, attempt, null, null)));
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7, original,
                new DbFirstPodSnapshot.Pose("other", 0, 4, 65, 8, 3, 4), null));
        // 此处只验证纯不可变freeze合同；运行期来源仍由FloatingPodEntity核验。
    }

    @Test void rejectsFaultedIncompleteOrUnincrementableInitialControl() {
        var original = state(owner, "Normal", 7, binding, attempt, 4);
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7,
                original.asFaulted(new Exception("actual defect")), pose, null));
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7,
                state(owner, "Normal", 7, null, attempt, 4), pose, null));
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7,
                state(owner, "Normal", 7, binding, null, 4), pose, null));
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, Long.MAX_VALUE,
                state(owner, "Normal", Long.MAX_VALUE, binding, attempt, 4), pose, null));
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 8, original, pose, null));
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7,
                state(owner, "Normal", 7, binding, attempt, -1), pose, null));
    }

    @Test void checkpointCannotChangeOwnerModeAttemptBindingVersionOrFinalPose() {
        var original = state(owner, "Normal", 7, binding, attempt, 4);
        var freeze = new DbFirstCheckpointFreeze(operation, 7, original, pose, null);
        for (var successor : new FloatingPodControlSnapshot[]{
                state(new UUID(0, 99), "Normal", 8, binding, attempt, 4),
                state(owner, "WorldIdle", 8, binding, attempt, 4),
                state(owner, "Normal", 8, new UUID(0, 99), attempt, 4),
                state(owner, "Normal", 8, binding, new UUID(0, 99), 4),
                state(owner, "Normal", 9, binding, attempt, 4),
                state(pod, owner, "Normal", 8, binding, attempt, new UUID(0, 99), 4),
                state(owner, "Normal", 8, binding, attempt, 5),
                state(owner, "Normal", 8, binding, attempt, 4).asFaulted(new Exception("actual defect"))}) {
            assertThrows(IllegalStateException.class, () -> freeze.requireCommittedSuccessor(successor));
        }
    }

    @Test void sameCenterChunkDriftCapturesActualPoseAndRequiresThatExactCommittedSuccessor() {
        var original = state(owner, "Normal", 7, binding, attempt, 1);
        var freeze = new DbFirstCheckpointFreeze(operation, 7, original, pose, null);
        freeze.requireCurrent(operation, 7, original, pose);
        freeze.requireCommittedSuccessor(state(owner, "Normal", 8, binding, attempt, 4));
        assertThrows(IllegalStateException.class, () -> freeze.requireCommittedSuccessor(
                state(owner, "Normal", 8, binding, attempt, 1)));
        for (double x : new double[]{-0.01, 16, -16, 32, Double.MAX_VALUE}) {
            var outside = new DbFirstPodSnapshot.Pose("world", 0, x, 65, 8, 3, 4);
            assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7, original, outside, null));
        }
        var wrongWorld = new DbFirstPodSnapshot.Pose("other", 0, 4, 65, 8, 3, 4);
        var wrongDimension = new DbFirstPodSnapshot.Pose("world", 1, 4, 65, 8, 3, 4);
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7, original, wrongWorld, null));
        assertThrows(IllegalStateException.class, () -> new DbFirstCheckpointFreeze(operation, 7, original, wrongDimension, null));
        assertEquals(1, original.posX());
    }

    @Test void anotherThreadCannotCaptureOrConfirmTheOriginalFreeze() throws Exception {
        var original = state(owner, "Normal", 7, binding, attempt, 4);
        var freeze = new DbFirstCheckpointFreeze(operation, 7, original, pose, null);
        var failure = new AtomicReference<Throwable>();
        Thread thread = new Thread(() -> {
            try { freeze.requireCommittedSuccessor(state(owner, "Normal", 8, binding, attempt, 4)); }
            catch (Throwable cause) { failure.set(cause); }
        });
        thread.start(); thread.join(3000);
        assertFalse(thread.isAlive()); assertInstanceOf(IllegalStateException.class, failure.get());
        freeze.requireCurrent(operation, 7, original, pose);
    }
}

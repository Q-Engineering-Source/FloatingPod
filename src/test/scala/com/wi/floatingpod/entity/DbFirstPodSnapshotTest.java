package com.wi.floatingpod.entity;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.Arrays;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;
import net.minecraft.nbt.NBTSizeTracker;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class DbFirstPodSnapshotTest {
    /** Shared-ledger core 回归通过相同包的真实 NBT capture 入口，不启动 Entity/game。 */
    public static DbFirstPodSnapshot captureForCore(UUID operation, DbFirstCaptureAllocation allocation, int payloadBytes)
            throws IOException {
        return captureForCore(operation, allocation, payloadBytes, (byte)0,
                new DbFirstPodSnapshot.Pose("world", 0, 1, 64, 2, 3, 4));
    }

    public static DbFirstPodSnapshot captureForCore(UUID operation, DbFirstCaptureAllocation allocation,
            int payloadBytes, byte payloadValue, DbFirstPodSnapshot.Pose pose) throws IOException {
        UUID owner = new UUID(0, 501), pod = new UUID(0, 502);
        FloatingPodControlSnapshot control = new FloatingPodControlSnapshot(
                FloatingPodControlSnapshot.Phase.COMMITTED, pod, owner, "Normal", true, 7, "world",
                1, 64, 2, 3, 4, 0, new UUID(0, 503), new UUID(0, 504), null, null);
        byte[] payload = new byte[payloadBytes]; Arrays.fill(payload, payloadValue);
        NBTTagCompound nbt = new NBTTagCompound(); nbt.setByteArray("actual", payload);
        return DbFirstPodSnapshot.capture(new DbFirstRecallFreeze(operation, owner, 7, control),
                pose, nbt, allocation);
    }

    /** 相同真实codec/core seam；停服资格来自本包原NormalStopEntityFreeze，不传认证owner。 */
    public static DbFirstPodSnapshot captureShutdownForCore(UUID operation, DbFirstCaptureAllocation allocation,
            int payloadBytes) throws IOException {
        var original = new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED,
                new UUID(0, 502), new UUID(0, 501), "Normal", true, 7, "world",
                1, 64, 2, 3, 4, 0, new UUID(0, 503), new UUID(0, 504), null, null);
        UUID request = new UUID(0, 506);
        var stop = new NormalStopEntityFreeze(request, original);
        var recall = DbFirstRecallFreeze.forNormalStop(operation, request, 7, original, stop);
        var nbt = new NBTTagCompound(); nbt.setByteArray("actual", new byte[payloadBytes]);
        return DbFirstPodSnapshot.capture(recall, new DbFirstPodSnapshot.Pose("world", 0, 1, 64, 2, 3, 4), nbt, allocation);
    }

    public static DbFirstPodSnapshot captureInitialForCore(UUID operation, DbFirstCaptureAllocation allocation)
            throws IOException {
        return captureInitialForCore(operation, allocation, new UUID(0, 505));
    }

    public static DbFirstPodSnapshot captureInitialForCore(UUID operation, DbFirstCaptureAllocation allocation,
            UUID creationOperation) throws IOException {
        return captureInitialForCore(operation, allocation, creationOperation,
                new UUID(0, 502), new UUID(0, 503), new UUID(0, 504));
    }

    public static DbFirstPodSnapshot captureInitialForCore(UUID operation, DbFirstCaptureAllocation allocation,
            UUID creationOperation, UUID pod, UUID binding, UUID attempt) throws IOException {
        FloatingPodControlSnapshot pending = new FloatingPodControlSnapshot(
                FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION, pod, null, "WorldIdle",
                false, 0, "world", 1, 64, 2, 3, 4, 0, binding, attempt, creationOperation, null);
        NBTTagCompound nbt = new NBTTagCompound();
        NBTTagCompound caps = new NBTTagCompound(); caps.setByteArray("opaque", new byte[]{1, 3, 7});
        nbt.setTag("ForgeCaps", caps);
        return DbFirstPodSnapshot.captureInitialActivation(operation, pending,
                new DbFirstPodSnapshot.Pose("world", 0, 1, 64, 2, 3, 4), nbt, allocation);
    }

    private final UUID operation = UUID.randomUUID(), pod = UUID.randomUUID(), owner = UUID.randomUUID();
    private final UUID binding = UUID.randomUUID(), attempt = UUID.randomUUID();
    private final FloatingPodControlSnapshot control = new FloatingPodControlSnapshot(
            FloatingPodControlSnapshot.Phase.COMMITTED, pod, owner, "Normal", true, 7, "world",
            1, 64, 2, 3, 4, 0, binding, attempt, null, null);
    private final DbFirstRecallFreeze freeze = new DbFirstRecallFreeze(operation, owner, 7, control);
    private final DbFirstPodSnapshot.Pose pose = new DbFirstPodSnapshot.Pose("world", 0, 1, 64, 2, 3, 4);

    private SourceLimitProfile limits(long bytes, long depth, long members, long retained, long packet) {
        return SourceLimitProfile.of(100000, 100000, bytes, depth, members, retained, packet, 100000);
    }
    private DbFirstPodSnapshot capture(NBTTagCompound nbt, SourceLimitProfile profile) throws IOException {
        TestAllocation allocation = new TestAllocation(profile);
        try {
            return DbFirstPodSnapshot.capture(freeze, pose, nbt, allocation);
        } catch (IOException | RuntimeException | Error cause) {
            assertEquals(0L, allocation.retained);
            assertEquals(0L, allocation.temporary);
            throw cause;
        } finally { allocation.active = false; }
    }
    private static final class TestAllocation implements DbFirstCaptureAllocation {
        final SourceLimitProfile profile;
        long retained, temporary;
        boolean active = true, begun;
        DbFirstPodSnapshot snapshot;
        TestAllocation(SourceLimitProfile profile) { this.profile = profile; }
        public SourceLimitProfile profile() { return profile; }
        private void check() { if (!active) throw new IllegalStateException("Test allocation ended"); }
        public void beginCapture() { check(); if (begun) throw new IllegalStateException("Consumed"); begun = true; }
        private void capacity(long bytes) throws IOException {
            check();
            if (bytes < 0 || bytes > profile.maxRetainedBytes() - retained - temporary)
                throw new IOException("Explicit test shared budget exceeded");
        }
        public void reserveRetained(long bytes, String responsibility) throws IOException { capacity(bytes); retained += bytes; }
        public void releaseRetained(long bytes, String responsibility) { check(); assertTrue(bytes <= retained); retained -= bytes; }
        public void reserveTemporary(long bytes, String responsibility) throws IOException { capacity(bytes); temporary += bytes; }
        public void releaseTemporary(long bytes, String responsibility) { check(); assertTrue(bytes <= temporary); temporary -= bytes; }
        public void retainSnapshot(DbFirstPodSnapshot value) {
            check(); assertNull(snapshot); assertTrue(value.belongsTo(this));
            assertEquals(value.retainedBytes(), retained); assertEquals(0L, temporary); snapshot = value;
        }
    }

    /** 真实codec的pure fixture，不发行游戏冻结、SQL或commit确认。 */
    public static DbFirstPodSnapshot captureCheckpointForCore(UUID operation, DbFirstCaptureAllocation allocation,
            UUID pod, UUID binding, UUID attempt) throws IOException {
        return captureCheckpointForCore(operation, allocation, pod, binding, attempt, null, "WorldIdle");
    }

    public static DbFirstPodSnapshot captureCheckpointForCore(UUID operation, DbFirstCaptureAllocation allocation,
            UUID pod, UUID binding, UUID attempt, UUID owner, String mode) throws IOException {
        return captureCheckpointForCoreAt(operation, allocation, pod, binding, attempt, owner, mode, 1);
    }

    public static DbFirstPodSnapshot captureCheckpointForCoreAt(UUID operation, DbFirstCaptureAllocation allocation,
            UUID pod, UUID binding, UUID attempt, UUID owner, String mode, double x) throws IOException {
        var control = new FloatingPodControlSnapshot(FloatingPodControlSnapshot.Phase.COMMITTED, pod, owner,
                mode, true, 7, "world", x, 64, 2, 3, 4, 0, binding, attempt, null, null);
        var pose = new DbFirstPodSnapshot.Pose("world", 0, x, 64, 2, 3, 4);
        var nbt = new NBTTagCompound();
        var caps = new NBTTagCompound(); caps.setByteArray("opaque", new byte[]{1, 3, 7});
        nbt.setTag("ForgeCaps", caps);
        return DbFirstPodSnapshot.captureCheckpoint(new DbFirstCheckpointFreeze(operation, 7, control, pose, null), nbt, allocation);
    }

    @Test void originalFaultRevokesCachedFreezeBeforeCaptureConsumesAllocation() {
        var failure = new IllegalStateException("Original recall failure");
        var fault = freeze.retainFault(control, failure);
        var allocation = new TestAllocation(limits(50000, 64, 1000, 1000000, 50000));
        assertThrows(IllegalStateException.class,
                () -> DbFirstPodSnapshot.capture(freeze, pose, tree(), allocation));
        assertFalse(allocation.begun);
        assertEquals(0L, allocation.retained);
        assertEquals(0L, allocation.temporary);
        assertNull(allocation.snapshot);
        freeze.requireRetainedFailure(operation, 7, fault, failure);
    }

    @Test void activeCheckpointCapturesWholeTreeWithoutRecallOrInitialActivationPurpose() throws Exception {
        var allocation = new TestAllocation(limits(50000, 64, 1000, 1000000, 50000));
        var original = tree();
        var freeze = new DbFirstCheckpointFreeze(operation, 7, control, pose, UUID.randomUUID());
        var captured = DbFirstPodSnapshot.captureCheckpoint(freeze, original, allocation);
        assertEquals(DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT, captured.purpose());
        assertNull(captured.initialCreationOperationUuid());
        assertEquals(owner, captured.ownerUuid()); assertEquals(7, captured.stateVersion());
        assertSame(captured, allocation.snapshot); assertEquals(0, allocation.temporary);
        var output = new ByteArrayOutputStream(); captured.writeTo(output);
        assertEquals(original, CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(output.toByteArray())),
                new NBTSizeTracker(1000000)));
        original.getCompoundTag("ForgeCaps").setByteArray("opaque", new byte[]{0});
        var again = new ByteArrayOutputStream(); captured.writeTo(again);
        assertArrayEquals(output.toByteArray(), again.toByteArray());
    }

    private NBTTagCompound tree() {
        NBTTagCompound root = new NBTTagCompound();
        root.setString("id", "floatingpod:floating_pod"); root.setUniqueId("UUID", pod);
        NBTTagCompound caps = new NBTTagCompound();
        caps.setByteArray("opaque", new byte[]{1, 3, 7, -1}); root.setTag("ForgeCaps", caps);
        NBTTagCompound data = new NBTTagCompound(); data.setString("unknown", "完整保留");
        NBTTagList list = new NBTTagList();
        NBTTagCompound nested = new NBTTagCompound(); nested.setIntArray("ints", new int[]{2, 4, 8});
        list.appendTag(nested); data.setTag("nested", list); root.setTag("ForgeData", data);
        return root;
    }

    @Test void shutdownRecallCapturesCompleteTreeUnderSameRecallPurpose() throws Exception {
        UUID request = UUID.randomUUID();
        var stop = new NormalStopEntityFreeze(request, control);
        var recall = DbFirstRecallFreeze.forNormalStop(operation, request, 7, control, stop);
        var allocation = new TestAllocation(limits(50000, 64, 1000, 1000000, 50000));
        var original = tree();
        var snapshot = DbFirstPodSnapshot.capture(recall, pose, original, allocation);
        assertEquals(DbFirstPodSnapshot.Purpose.RECALL, snapshot.purpose());
        assertEquals(operation, snapshot.operationUuid()); assertEquals(owner, snapshot.ownerUuid());
        assertSame(snapshot, allocation.snapshot); assertEquals(0L, allocation.temporary);
        var output = new ByteArrayOutputStream(); snapshot.writeTo(output);
        assertEquals(original, CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(output.toByteArray())), new NBTSizeTracker(1000000)));
        original.getCompoundTag("ForgeCaps").setByteArray("opaque", new byte[]{0});
        var again = new ByteArrayOutputStream(); snapshot.writeTo(again);
        assertArrayEquals(output.toByteArray(), again.toByteArray());
        recall.requireCurrent(operation, 7, control); recall.requireNormalStopOwner(stop);
    }

    @Test void initialActivationCapturesCompletePendingContentWithoutRecallAuthority() throws Exception {
        FloatingPodControlSnapshot pending = new FloatingPodControlSnapshot(
                FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION, pod, null, "WorldIdle", false, 0, "world",
                1, 64, 2, 3, 4, 0, binding, attempt, UUID.randomUUID(), null);
        TestAllocation allocation = new TestAllocation(limits(50000, 64, 1000, 1000000, 50000));
        NBTTagCompound original = tree();
        DbFirstPodSnapshot snapshot = DbFirstPodSnapshot.captureInitialActivation(operation, pending, pose, original, allocation);
        assertEquals(DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION, snapshot.purpose());
        assertEquals(operation, snapshot.operationUuid());
        assertEquals(pod, snapshot.podUuid()); assertEquals(binding, snapshot.bindingUuid());
        assertEquals(attempt, snapshot.attemptUuid()); assertNull(snapshot.ownerUuid());
        assertEquals(0, snapshot.stateVersion()); assertSame(snapshot, allocation.snapshot);
        ByteArrayOutputStream output = new ByteArrayOutputStream(); snapshot.writeTo(output);
        assertEquals(original, CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(output.toByteArray())),
                new NBTSizeTracker(1000000)));
        assertEquals(DbFirstPodSnapshot.Purpose.RECALL,
                capture(tree(), limits(50000, 64, 1000, 1000000, 50000)).purpose());
    }

    @Test void initialActivationRejectsCommittedStateBeforeConsumingAllocation() {
        TestAllocation allocation = new TestAllocation(limits(50000, 64, 1000, 1000000, 50000));
        assertThrows(IllegalStateException.class,
                () -> DbFirstPodSnapshot.captureInitialActivation(operation, control, pose, tree(), allocation));
        assertFalse(allocation.begun); assertEquals(0L, allocation.retained);
    }

    @Test void actualNbtCodecPreservesCompleteTreeAndImmutableBytesAndMetadata() throws Exception {
        NBTTagCompound original = tree();
        DbFirstPodSnapshot snapshot = capture(original,
                limits(50000, 64, 1000, 1000000, 50000));
        ByteArrayOutputStream output = new ByteArrayOutputStream(); snapshot.writeTo(output);
        byte[] bytes = output.toByteArray();
        assertEquals(original, CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(bytes)),
                new NBTSizeTracker(1000000)));
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(bytes), snapshot.sha256());
        assertEquals(1, snapshot.formatVersion()); assertEquals(bytes.length, snapshot.byteLength());
        assertEquals(operation, snapshot.operationUuid()); assertEquals(pod, snapshot.podUuid());
        assertEquals(owner, snapshot.ownerUuid()); assertEquals(attempt, snapshot.attemptUuid());
        assertEquals(binding, snapshot.bindingUuid()); assertEquals(7, snapshot.stateVersion());
        assertEquals(pose, snapshot.pose());
        byte first = snapshot.byteAt(0); bytes[0] ^= 1;
        byte[] digest = snapshot.sha256(); digest[0] ^= 1;
        original.getCompoundTag("ForgeCaps").setByteArray("opaque", new byte[]{99});
        ByteArrayOutputStream second = new ByteArrayOutputStream();
        snapshot.writeTo(new OutputStream() {
            @Override public void write(int value) { second.write(value); }
            @Override public void write(byte[] value, int offset, int length) {
                fail("Consumer must never receive the snapshot owned array");
            }
        });
        assertEquals(first, snapshot.byteAt(0));
        assertNotEquals(bytes[0], second.toByteArray()[0]);
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(second.toByteArray()), snapshot.sha256());
    }

    @Test void streamingModifiedUtfMatchesVanillaAndRejectsOversizedUtfWithoutRetainedPayload() throws Exception {
        NBTTagCompound root = new NBTTagCompound();
        root.setString("null\u0000/surrogate", "A\u0000世界\ud83d\ude00\ud800");
        DbFirstPodSnapshot captured = capture(root, limits(100000, 64, 1000, 1000000, 100000));
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        CompressedStreamTools.write(root, new java.io.DataOutputStream(expected));
        ByteArrayOutputStream actual = new ByteArrayOutputStream(); captured.writeTo(actual);
        assertArrayEquals(expected.toByteArray(), actual.toByteArray());
        root.setString("oversized", "界".repeat(22000));
        assertThrows(java.io.UTFDataFormatException.class, () ->
                capture(root, limits(100000, 64, 1000, 1000000, 100000)));
        freeze.requireCurrent(operation, 7, control);
    }

    @Test void explicitByteDepthMemberAndRetainedLimitsRejectWithoutReleasingFreeze() {
        NBTTagCompound original = tree();
        assertThrows(IOException.class, () -> capture(original,
                limits(20, 64, 1000, 1000000, 50000)));
        assertThrows(IOException.class, () -> capture(original,
                limits(50000, 64, 1000, 1000000, 20)));
        assertThrows(IOException.class, () -> capture(original,
                limits(50000, 2, 1000, 1000000, 50000)));
        assertThrows(IOException.class, () -> capture(original,
                limits(50000, 64, 2, 1000000, 50000)));
        assertThrows(IOException.class, () -> capture(original,
                limits(50000, 64, 1000, 2000, 50000)));
        NBTTagCompound cycle = new NBTTagCompound(); cycle.setTag("self", cycle);
        assertThrows(IOException.class, () -> capture(cycle,
                limits(50000, 8, 1000, 1000000, 50000)));
        freeze.requireCurrent(operation, 7, control);
    }
}

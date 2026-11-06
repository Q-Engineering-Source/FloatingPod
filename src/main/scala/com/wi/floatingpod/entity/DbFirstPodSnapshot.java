package com.wi.floatingpod.entity;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTBase;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;

/** 完整 serializeNBT 的不可变捕获；不是数据库收据、恢复资格或非序列化 capability 证明。 */
public final class DbFirstPodSnapshot {
    public static final int FORMAT_VERSION = 1;
    public enum Purpose { RECALL, INITIAL_ACTIVATION, ACTIVE_CHECKPOINT }
    private final Purpose purpose;
    public record Pose(String worldName, int dimension, double x, double y, double z, float yaw, float pitch) {
        public Pose {
            if (worldName == null || worldName.isBlank() || !Double.isFinite(x) || !Double.isFinite(y)
                    || !Double.isFinite(z) || !Float.isFinite(yaw) || !Float.isFinite(pitch)) {
                throw new IllegalArgumentException("Recall snapshot requires a finite world pose");
            }
        }
    }

    private final UUID operation, pod, owner, attempt, binding;
    private final UUID initialCreationOperation;
    private final long version;
    private final Pose pose;
    private final byte[] bytes, sha256;
    private final SourceLimitProfile profile;
    private final DbFirstCaptureAllocation allocation;

    private DbFirstPodSnapshot(Purpose purpose, UUID operation, FloatingPodControlSnapshot state,
                               Pose pose, byte[] bytes, byte[] sha256, DbFirstCaptureAllocation allocation) {
        this.purpose = purpose;
        this.initialCreationOperation = purpose == Purpose.INITIAL_ACTIVATION ? state.operationUuid() : null;
        this.operation = operation; this.pod = state.podUuid(); this.owner = state.ownerUuid();
        this.attempt = state.attemptUuid(); this.binding = state.bindingUuid(); this.version = state.stateVersion();
        this.pose = pose; this.bytes = bytes; this.sha256 = sha256; this.profile = allocation.profile(); this.allocation = allocation;
    }

    /** 消费 Main 持有的真实 NBT；不返回可变树或内部数组。 */
    static DbFirstPodSnapshot capture(DbFirstRecallFreeze freeze, Pose pose, NBTTagCompound nbt,
                                      DbFirstCaptureAllocation allocation) throws IOException {
        Objects.requireNonNull(freeze); Objects.requireNonNull(pose); Objects.requireNonNull(nbt);
        Objects.requireNonNull(allocation);
        freeze.requireCurrent(freeze.operationUuid(), freeze.original().stateVersion(), freeze.original());
        return capture(Purpose.RECALL, freeze.operationUuid(), freeze.original(), pose, nbt, allocation);
    }

    /** 初建使用真实 pending 控制态；不得借用要求已提交活动态的收回 token。 */
    static DbFirstPodSnapshot captureInitialActivation(UUID operation, FloatingPodControlSnapshot pending,
            Pose pose, NBTTagCompound nbt, DbFirstCaptureAllocation allocation) throws IOException {
        Objects.requireNonNull(operation); Objects.requireNonNull(pending);
        Objects.requireNonNull(pose); Objects.requireNonNull(nbt); Objects.requireNonNull(allocation);
        if (operation.equals(new UUID(0, 0)) || pending.phase() != FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION
                || pending.faultType() != null || pending.bindingUuid() == null || pending.attemptUuid() == null
                || pending.operationUuid() == null || !pose.equals(new Pose(pending.worldName(), pending.dimensionId(),
                    pending.posX(), pending.posY(), pending.posZ(), pending.yaw(), pending.pitch())))
            throw new IllegalStateException("Initial activation capture requires original pending creation and exact pose");
        return capture(Purpose.INITIAL_ACTIVATION, operation, pending, pose, nbt, allocation);
    }

    /** active checkpoint复用完整有界codec，但不借用要求所有者的recall token。 */
    static DbFirstPodSnapshot captureCheckpoint(DbFirstCheckpointFreeze freeze, NBTTagCompound nbt,
            DbFirstCaptureAllocation allocation) throws IOException {
        Objects.requireNonNull(freeze); Objects.requireNonNull(nbt); Objects.requireNonNull(allocation);
        freeze.requireCurrent(freeze.operationUuid(), freeze.original().stateVersion(), freeze.original(), freeze.pose());
        return capture(Purpose.ACTIVE_CHECKPOINT, freeze.operationUuid(), freeze.original(), freeze.pose(), nbt, allocation);
    }

    private static DbFirstPodSnapshot capture(Purpose purpose, UUID operation, FloatingPodControlSnapshot state,
            Pose pose, NBTTagCompound nbt, DbFirstCaptureAllocation allocation) throws IOException {
        allocation.beginCapture();
        SourceLimitProfile profile = Objects.requireNonNull(allocation.profile());
        long temporary = 0L, retained = 0L;
        Throwable primary = null;
        try {
            allocation.reserveTemporary(2048L, "Recall codec/digest/two-pass fixed workspace"); temporary = 2048L;
            preflight(nbt, profile, allocation);
            long limit = Math.min(Math.min(profile.maxDecompressedBytes(), profile.maxSqlPacketBytes()),
                    Math.min(profile.maxRetainedBytes(), Integer.MAX_VALUE - 8L));
            Sink count = new Sink(limit, null);
            CompressedStreamTools.write(nbt, new StreamingDataOutput(count));
            byte[] firstSha = count.digest.digest();
            allocation.reserveRetained(640L, "Recall immutable snapshot metadata/SHA"); retained = 640L;
            allocation.reserveRetained(count.length, "Recall exact owned NBT bytes"); retained += count.length;
            byte[] owned = new byte[Math.toIntExact(count.length)];
            preflight(nbt, profile, allocation);
            Sink exact = new Sink(count.length, owned);
            CompressedStreamTools.write(nbt, new StreamingDataOutput(exact));
            byte[] secondSha = exact.digest.digest();
            if (count.length != exact.length || !MessageDigest.isEqual(firstSha, secondSha))
                throw new IOException("Recall NBT changed between bounded count and exact write");
            DbFirstPodSnapshot result = new DbFirstPodSnapshot(purpose, operation, state, pose, owned, secondSha, allocation);
            allocation.releaseTemporary(temporary, "Recall codec workspace complete"); temporary = 0L;
            allocation.retainSnapshot(result);
            retained = 0L;
            return result;
        } catch (IOException | RuntimeException | Error cause) {
            primary = cause; throw cause;
        } finally {
            Throwable cleanupFailure = null;
            if (temporary != 0L) try { allocation.releaseTemporary(temporary, "Recall failed codec workspace"); }
            catch (Throwable cleanup) { cleanupFailure = cleanup; }
            if (retained != 0L) try { allocation.releaseRetained(retained, "Recall untransferred snapshot"); }
            catch (Throwable cleanup) {
                if (cleanupFailure == null) cleanupFailure = cleanup; else if (cleanup != cleanupFailure) cleanupFailure.addSuppressed(cleanup);
            }
            if (cleanupFailure != null) {
                if (primary != null) { if (primary != cleanupFailure) primary.addSuppressed(cleanupFailure); }
                else if (cleanupFailure instanceof IOException cause) throw cause;
                else if (cleanupFailure instanceof RuntimeException cause) throw cause;
                else if (cleanupFailure instanceof Error cause) throw cause;
                else throw new IOException("Recall capture cleanup failed", cleanupFailure);
            }
        }
    }

    private static final class Frame {
        final NBTBase tag;
        final Iterator<String> keys;
        int index;
        Frame(NBTBase tag) {
            this.tag = tag;
            keys = tag instanceof NBTTagCompound compound ? compound.getKeySet().iterator() : null;
        }
        NBTBase next() throws IOException {
            if (keys != null) {
                if (!keys.hasNext()) return null;
                NBTBase value = ((NBTTagCompound) tag).getTag(keys.next());
                if (value == null || value.getId() == 0) throw new IOException("Recall NBT contains an invalid named tag");
                return value;
            }
            if (tag instanceof NBTTagList list && index < list.tagCount()) {
                NBTBase value = list.get(index++);
                if (value == null || value.getId() == 0 || value.getId() != list.getTagType()) {
                    throw new IOException("Recall NBT list type differs from its actual member");
                }
                return value;
            }
            return null;
        }
    }

    private static void preflight(NBTTagCompound root, SourceLimitProfile profile,
                                  DbFirstCaptureAllocation allocation) throws IOException {
        long depthLimit = Math.min(profile.maxNbtDepth(), 512L);
        long charged = 0L;
        Throwable primary = null;
        try {
            allocation.reserveTemporary(128L, "Recall NBT preflight stack"); charged = 128L;
            ArrayDeque<Frame> stack = new ArrayDeque<>();
            allocation.reserveTemporary(192L, "Recall NBT root frame"); charged += 192L;
            stack.push(new Frame(root));
            long members = 1;
            while (!stack.isEmpty()) {
                NBTBase child = stack.peek().next();
                if (child == null) { stack.pop(); continue; }
                members = Math.addExact(members, 1L);
                if (members > profile.maxMembers()) throw new IOException("Recall NBT exceeds member budget");
                if (stack.size() + 1L > depthLimit) throw new IOException("Recall NBT exceeds depth budget");
                if (child.getId() < 1 || child.getId() > 12) throw new IOException("Recall NBT contains an unsupported tag");
                if (child instanceof NBTTagCompound || child instanceof NBTTagList) {
                    allocation.reserveTemporary(192L, "Recall NBT bounded traversal frame/stack capacity"); charged += 192L;
                    stack.push(new Frame(child));
                }
            }
        } catch (IOException | RuntimeException | Error cause) { primary = cause; throw cause; }
        finally {
            if (charged != 0L) try { allocation.releaseTemporary(charged, "Recall NBT preflight complete"); }
            catch (IOException | RuntimeException | Error cleanup) {
                if (primary == null) throw cleanup;
                if (cleanup != primary) primary.addSuppressed(cleanup);
            }
        }
    }

    /** 与 DataOutput.writeUTF 相同的 modified UTF framing，不分配按字符串长度增长的副本。 */
    private static final class StreamingDataOutput implements DataOutput {
        private final DataOutputStream out;
        StreamingDataOutput(OutputStream sink) { out = new DataOutputStream(sink); }
        public void write(int value) throws IOException { out.write(value); }
        public void write(byte[] value) throws IOException { out.write(value); }
        public void write(byte[] value, int offset, int length) throws IOException { out.write(value, offset, length); }
        public void writeBoolean(boolean value) throws IOException { out.writeBoolean(value); }
        public void writeByte(int value) throws IOException { out.writeByte(value); }
        public void writeShort(int value) throws IOException { out.writeShort(value); }
        public void writeChar(int value) throws IOException { out.writeChar(value); }
        public void writeInt(int value) throws IOException { out.writeInt(value); }
        public void writeLong(long value) throws IOException { out.writeLong(value); }
        public void writeFloat(float value) throws IOException { out.writeFloat(value); }
        public void writeDouble(double value) throws IOException { out.writeDouble(value); }
        public void writeBytes(String value) throws IOException {
            for (int index = 0; index < value.length(); index++) out.writeByte(value.charAt(index));
        }
        public void writeChars(String value) throws IOException {
            for (int index = 0; index < value.length(); index++) out.writeChar(value.charAt(index));
        }
        public void writeUTF(String value) throws IOException {
            long length = 0L;
            for (int index = 0; index < value.length(); index++) {
                int ch = value.charAt(index);
                length += ch >= 1 && ch <= 127 ? 1L : ch <= 2047 ? 2L : 3L;
                if (length > 65535L) throw new UTFDataFormatException("Recall NBT modified UTF exceeds 65535 bytes");
            }
            out.writeShort((int) length);
            for (int index = 0; index < value.length(); index++) {
                int ch = value.charAt(index);
                if (ch >= 1 && ch <= 127) out.writeByte(ch);
                else if (ch <= 2047) { out.writeByte(192 | ch >> 6); out.writeByte(128 | ch & 63); }
                else { out.writeByte(224 | ch >> 12); out.writeByte(128 | ch >> 6 & 63); out.writeByte(128 | ch & 63); }
            }
        }
    }

    private static final class Sink extends OutputStream {
        final long limit;
        final byte[] target;
        final MessageDigest digest;
        long length;
        Sink(long limit, byte[] target) {
            this.limit = limit; this.target = target;
            try { digest = MessageDigest.getInstance("SHA-256"); }
            catch (NoSuchAlgorithmException cause) { throw new IllegalStateException("SHA-256 unavailable", cause); }
        }
        private void reserve(int count) throws IOException {
            if (count < 0 || length > limit - count) throw new IOException("Recall NBT exceeds explicit byte budget");
        }
        @Override public void write(int value) throws IOException {
            reserve(1);
            if (target != null) target[(int) length] = (byte) value;
            digest.update((byte) value); length++;
        }
        @Override public void write(byte[] value, int offset, int count) throws IOException {
            Objects.checkFromIndexSize(offset, count, value.length); reserve(count);
            if (target != null) System.arraycopy(value, offset, target, (int) length, count);
            digest.update(value, offset, count); length += count;
        }
    }

    public Purpose purpose() { return purpose; }
    public UUID initialCreationOperationUuid() { return initialCreationOperation; }
    public boolean belongsTo(DbFirstCaptureAllocation actual) { return allocation == actual; }
    public long retainedBytes() { return bytes.length + 640L; }
    public int formatVersion() { return FORMAT_VERSION; }
    public UUID operationUuid() { return operation; }
    public UUID podUuid() { return pod; }
    public UUID ownerUuid() { return owner; }
    public UUID attemptUuid() { return attempt; }
    public UUID bindingUuid() { return binding; }
    public long stateVersion() { return version; }
    public Pose pose() { return pose; }
    public SourceLimitProfile limitProfile() { return profile; }
    public int byteLength() { return bytes.length; }
    public byte byteAt(int index) { return bytes[index]; }
    public byte[] sha256() { return sha256.clone(); }
    public void writeTo(OutputStream output) throws IOException {
        Objects.requireNonNull(output);
        for (byte value : bytes) output.write(value & 255);
    }
}

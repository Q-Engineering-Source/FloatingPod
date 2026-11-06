package com.wi.floatingpod.server;

import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * FirstBinding6 的原 worker native store：锁住实际 save/data 与独占 guard，按固定角色永久保留证据。
 * 调用者先登记 holder；所有 SQL/阶段判定均属于上层 actor。本类不创建世界、数据库或 Ready。
 */
public final class WindowsDbFirstAdoptionStore implements AutoCloseable {
    public enum Role {
        SOURCE, OWNER, PREPARE_INTENT, PREPARED, MARKER_INTENT, MARKER_DURABLE,
        CERTIFY_INTENT, CERTIFIED, RELEASE_INTENT, RELEASED, COMPLETE,
        PREPARE_BEFORE, PREPARE_TARGET, CERTIFY_BEFORE, CERTIFY_TARGET, RELEASE_BEFORE, RELEASE_TARGET,
        HANDOFF_INTENT, HANDOFF_CONFIRMED, HANDOFF_BEFORE, HANDOFF_TARGET, READBACK,
        STARTUP_INCIDENT, STARTUP_RETIRE_INTENT, STARTUP_RETIRE_BEFORE, STARTUP_RETIRE_TARGET, STARTUP_RETIRE_CONFIRMED
    }
    public record Key(Role role, UUID ownerBoot, String contentSha256) {
        public Key(Role role, UUID ownerBoot) { this(role, ownerBoot, null); }
        public Key {
            Objects.requireNonNull(role);
            boolean perBoot = switch (role) {
                case SOURCE, MARKER_INTENT, MARKER_DURABLE, COMPLETE -> false;
                default -> true;
            };
            if (perBoot != (ownerBoot != null))
                throw new IllegalArgumentException("Owner/SQL-phase artifacts require an exact boot-derived filename");
            if (ownerBoot != null && ownerBoot.equals(new UUID(0, 0)))
                throw new IllegalArgumentException("Nonzero owner boot required");
            if ((role == Role.READBACK) != (contentSha256 != null)
                || (contentSha256 != null && !contentSha256.matches("[0-9a-f]{64}")))
                throw new IllegalArgumentException("Only a readback witness has an exact canonical content SHA filename");
        }
        public static Key of(Role role) { return new Key(role, null); }
    }
    @FunctionalInterface public interface Decoder<T> { T decode(byte[] bytes) throws IOException; }

    /** 不可由 DATA 重建的同 store native durable observation；不包含 SQL/业务权限。 */
    public static final class DurableArtifact {
        private final WindowsDbFirstAdoptionStore store;
        private final Key key;
        private final WindowsMarkerIo.FileIdentity identity;
        private final String sha256;
        private final int byteLength;
        private DurableArtifact(WindowsDbFirstAdoptionStore store, Key key,
                WindowsMarkerIo.FileIdentity identity, byte[] bytes) {
            this.store = store; this.key = key; this.identity = identity;
            this.sha256 = sha(bytes); this.byteLength = bytes.length;
        }
        public Key key() { return key; }
        public String sha256() { return sha256; }
        public int byteLength() { return byteLength; }
        public String fileId() { return identity.versionedIdentity(); }
        public void rebarrier() throws IOException {
            store.requireLive();
            try (var actual = store.read(key)) {
                if (actual == null || !identity.equals(actual.ack.identity)
                    || !sha256.equals(actual.ack.sha256) || byteLength != actual.ack.byteLength)
                    throw new IOException("FirstBinding6 durable artifact identity/content changed");
            }
        }
    }

    /** 原 store 对读取的实际 body 计费，decoder 返回前后检查 body 未被修改。 */
    public final class ReadArtifact implements AutoCloseable {
        private byte[] bytes;
        private long reservation;
        private final DurableArtifact ack;
        private ReadArtifact(Key key, WindowsMarkerIo.FileIdentity identity, byte[] bytes, long reservation) {
            this.bytes = bytes; this.reservation = reservation;
            this.ack = new DurableArtifact(WindowsDbFirstAdoptionStore.this, key, identity, bytes);
        }
        public DurableArtifact acknowledgement() throws IOException { requireBody(); return ack; }
        public <T> T decode(Decoder<T> decoder) throws IOException {
            requireBody();
            T result = Objects.requireNonNull(decoder).decode(bytes);
            if (!ack.sha256.equals(sha(bytes)))
                throw new IOException("FirstBinding6 decoder modified original durable body");
            return result;
        }
        private void requireBody() throws IOException {
            requireLive();
            if (bytes == null) throw new IOException("FirstBinding6 read body already retired");
        }
        @Override public void close() throws IOException {
            requireThread();
            bytes = null;
            if (reservation != 0L) {
                allocation.releaseTemporary(reservation, "FirstBinding6 actual artifact body retired");
                reservation = 0L;
            }
            reads.remove(this);
        }
    }

    private static final long PATH_WORKSPACE = 393216L;
    private final Thread thread = Thread.currentThread();
    private final WindowsMarkerIo io;
    private final SourceForestAllocationBudget allocation;
    private final DbFirstWorldAdoptionPlanData plan;
    private final WindowsWorldProvisioner.BootIdentity boot;
    private final Path world, data;
    private final long artifactLimit;
    private final List<ReadArtifact> reads = new LinkedList<>();
    private WindowsMarkerIo.Handle parentPin, worldPin, dataPin, guard, pending;
    private WindowsMarkerIo.FileIdentity parentId, worldId, dataId, guardId;
    private String nativeParent, nativeWorld, nativeData;
    private boolean attempted, closed;
    private Throwable failure;
    private long pendingFee;
    private ReadArtifact pendingRead;
    private List<Key> inventory;
    private long inventoryFee;

    /** holder/固定控制结构由上层原 root 在构造前预收；这里不打开 native。 */
    public WindowsDbFirstAdoptionStore(WindowsMarkerIo io, SourceForestAllocationBudget allocation,
            Path selectedWorld, DbFirstWorldAdoptionPlanData plan, WindowsWorldProvisioner.BootIdentity boot) {
        this.io = Objects.requireNonNull(io); this.allocation = Objects.requireNonNull(allocation);
        this.plan = Objects.requireNonNull(plan); this.boot = Objects.requireNonNull(boot);
        this.world = Objects.requireNonNull(selectedWorld).toAbsolutePath().normalize();
        if (world.getParent() == null) throw new IllegalArgumentException("Existing selected save requires a real parent");
        this.data = world.resolve("data");
        this.artifactLimit = allocation.exactProfile().maxNativeArtifactBytes();
        if (artifactLimit <= 0) throw new IllegalArgumentException("Explicit native artifact limit required");
    }
    private void requireThread() {
        if (Thread.currentThread() != thread) throw new IllegalStateException("Original FirstBinding6 native worker required");
    }
    private void requireLive() throws IOException {
        requireThread();
        if (!attempted || closed || guard == null || pending != null || pendingFee != 0L || failure != null)
            throw new IOException("Original settled FirstBinding6 native store required", failure);
    }
    public void initialize() throws IOException {
        requireThread();
        if (attempted) throw new IllegalStateException("FirstBinding6 store initialize is one-shot");
        attempted = true;
        allocation.reserveTemporary(PATH_WORKSPACE, "FirstBinding6 native save/guard pin workspace BEFORE open");
        Throwable primary = null;
        try {
            parentPin = io.directory(world.getParent(), plan.bootstrapOperationUuid());
            parentId = io.identity(parentPin); nativeParent = io.finalPath(parentPin);
            worldPin = io.directory(world, plan.bootstrapOperationUuid());
            worldId = io.identity(worldPin); nativeWorld = io.finalPath(worldPin);
            dataPin = io.directory(data, plan.bootstrapOperationUuid());
            dataId = io.identity(dataPin); nativeData = io.finalPath(dataPin);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(nativeParent, nativeWorld);
            if (!nativeWorld.equals(child(nativeParent, world.getFileName().toString()))
                || !nativeData.equals(child(nativeWorld, "data")))
                throw new IOException("FirstBinding6 selected save/data path aliases differ from actual native parent");
            Path guardPath = data.resolve(".floatingpod-adoption6.guard");
            guard = openIfPresent(guardPath, WindowsMarkerIo.READ | WindowsMarkerIo.WRITE);
            if (guard == null) guard = io.open(guardPath, WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.CREATE_NEW, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, plan.bootstrapOperationUuid());
            io.metadata(guard, false, 0L); guardId = io.identity(guard);
            io.flushChecked(guard);
            if (!child(nativeData, guardPath.getFileName().toString()).equals(io.finalPath(guard)))
                throw new IOException("FirstBinding6 guard is outside the pinned actual data directory");
            verifyPins();
        } catch (IOException | RuntimeException | Error cause) {
            primary = cause; failure = cause; throw cause;
        } finally {
            try { allocation.releaseTemporary(PATH_WORKSPACE, "FirstBinding6 save/guard pin workspace retired"); }
            catch (Throwable cleanup) { if (primary != null) primary.addSuppressed(cleanup); else throw cleanup; }
        }
    }
    private void verifyPins() throws IOException {
        if (parentPin == null || worldPin == null || dataPin == null || guard == null)
            throw new IOException("FirstBinding6 native pins are incomplete");
        if (!parentId.equals(io.identity(parentPin)) || !worldId.equals(io.identity(worldPin))
            || !dataId.equals(io.identity(dataPin)) || !guardId.equals(io.identity(guard))
            || !nativeParent.equals(io.finalPath(parentPin)) || !nativeWorld.equals(io.finalPath(worldPin))
            || !nativeData.equals(io.finalPath(dataPin)))
            throw new IOException("FirstBinding6 selected save/custody native identity changed");
        if (parentId.volumeSerialNumber() != worldId.volumeSerialNumber()
            || worldId.volumeSerialNumber() != dataId.volumeSerialNumber()
            || dataId.volumeSerialNumber() != guardId.volumeSerialNumber())
            throw new IOException("FirstBinding6 source/custody must remain on the pinned volume");
    }
    private WindowsMarkerIo.Handle openIfPresent(Path path, int access) throws IOException {
        try { return io.open(path, access, 0, WindowsMarkerIo.OPEN_EXISTING,
            WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, plan.bootstrapOperationUuid()); }
        catch (WindowsMarkerIo.NativeFailure absent) { if (absent.win32Error() == 2) return null; throw absent; }
    }
    private String basename(Key key) {
        Objects.requireNonNull(key);
        return ".floatingpod-adoption6-" + plan.bootstrapOperationUuid() + "-" +
            key.role.name().toLowerCase(java.util.Locale.ROOT) + (key.ownerBoot == null ? "" : "-" + key.ownerBoot)
            + (key.contentSha256 == null ? "" : "-" + key.contentSha256) + ".bin";
    }
    private Path path(Key key) { return data.resolve(basename(key)); }
    private static String child(String parent, String name) { return (parent.endsWith("\\") ? parent : parent + "\\") + name; }

    /** 实际固定目录的有界名字 inventory；内容和 FileId 仍逐一由 read() 的 native HANDLE 验证。 */
    public List<Key> discoverKeys() throws IOException {
        requireLive();
        if (inventory != null) return inventory;
        long workspace = PATH_WORKSPACE;
        allocation.reserveTemporary(workspace, "FirstBinding6 bounded fixed artifact inventory BEFORE enumeration");
        Throwable primary = null;
        try {
            verifyPins();
            var result = new ArrayList<Key>();
            String prefix = ".floatingpod-adoption6-" + plan.bootstrapOperationUuid() + "-";
            long examined = 0;
            try (var entries = Files.newDirectoryStream(data)) {
                for (Path entry : entries) {
                    if (++examined > allocation.exactProfile().maxMembers())
                        throw new IOException("FirstBinding6 data inventory exceeds the original source member limit");
                    String name = entry.getFileName().toString();
                    if (name.startsWith(".floatingpod-b1-"))
                        throw new IOException("Existing legacy B1 native history requires its original owner reconciliation");
                    if (name.startsWith(".floatingpod-adoption6-") && !name.startsWith(prefix)
                        && !name.equals(".floatingpod-adoption6-" + plan.bootstrapOperationUuid() + ".old-binding.bin"))
                        throw new IOException("Another FirstBinding6 operation owns native history in this selected save");
                    if (!name.startsWith(prefix)) continue;
                    // 未发布 temp 只保留；不将其内容/存在性当作已提交阶段或自动清理依据。
                    if (name.contains(".bin.tmp-")) continue;
                    if (name.endsWith(".old-binding.bin")) continue;
                    Key key = null;
                    for (Role role : Role.values()) {
                        String fixed = prefix + role.name().toLowerCase(java.util.Locale.ROOT);
                        if (name.equals(fixed + ".bin")) {
                            try { key = new Key(role, null); }
                            catch (IllegalArgumentException invalid) { throw new IOException("FirstBinding6 SQL phase lacks its owner boot", invalid); }
                        } else if (name.startsWith(fixed + "-") && name.endsWith(".bin")) {
                            String text = name.substring(fixed.length() + 1, name.length() - 4);
                            try {
                                if (role == Role.READBACK && (text.length() != 101 || text.charAt(36) != '-'))
                                    throw new IllegalArgumentException("Readback filename lacks its full content SHA");
                                String ownerText = role == Role.READBACK ? text.substring(0, 36) : text;
                                UUID owner = UUID.fromString(ownerText);
                                if (!owner.toString().equals(ownerText)) throw new IllegalArgumentException("Noncanonical boot UUID");
                                key = new Key(role, owner, role == Role.READBACK ? text.substring(37) : null);
                            } catch (IllegalArgumentException invalid) { throw new IOException("Malformed FirstBinding6 role/boot basename", invalid); }
                        }
                    }
                    if (key == null || result.contains(key)) throw new IOException("Unexplained or duplicate FirstBinding6 canonical artifact");
                    long nextFee = Math.addExact(inventoryFee, 512L);
                    allocation.reserveTemporary(512L, "FirstBinding6 immutable inventory key BEFORE retention");
                    inventoryFee = nextFee;
                    result.add(key);
                }
            }
            verifyPins();
            inventory = List.copyOf(result);
            return inventory;
        } catch (IOException | RuntimeException | Error cause) { primary = cause; failure = cause; throw cause; }
        finally {
            try { allocation.releaseTemporary(workspace, "FirstBinding6 bounded directory inventory retired"); }
            catch (Throwable cleanup) { if (primary != null) primary.addSuppressed(cleanup); else throw cleanup; }
        }
    }

    public DbFirstWorldAdoptionReceiptCodec.SourceFacts captureOriginalSource(String configurationSha256,
            String securityGuardSha256) throws IOException {
        requireLive();
        allocation.reserveTemporary(PATH_WORKSPACE, "FirstBinding6 original source observation workspace BEFORE allocation");
        Throwable primary = null;
        try {
            verifyPins();
            pending = openIfPresent(data.resolve("floatingpod-world-binding.bin"), WindowsMarkerIo.READ);
            DbFirstWorldAdoptionReceiptCodec.PreviousMarker marker;
            if (pending == null) marker = new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(false, null, 0, null);
            else {
                io.metadata(pending, false);
                var file = io.identity(pending);
                if (!child(nativeData, "floatingpod-world-binding.bin").equals(io.finalPath(pending)))
                    throw new IOException("Original FP marker resolved outside the selected save");
                byte[] raw = io.readBackExact(pending);
                var bytes = ByteBuffer.wrap(raw);
                if (bytes.getInt() != 0x46505742) throw new IOException("Original FP marker magic differs");
                int format = bytes.getInt(); UUID token = new UUID(bytes.getLong(), bytes.getLong());
                marker = new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(true, file.versionedIdentity(), format, token);
                if (!Arrays.equals(raw, io.readBackExact(pending)) || !file.equals(io.identity(pending)))
                    throw new IOException("Original FP marker changed during observation");
                closePending();
            }
            var process = io.captureCurrentProcessIdentity(plan.bootstrapOperationUuid());
            verifyPins();
            return new DbFirstWorldAdoptionReceiptCodec.SourceFacts(plan, worldId.versionedIdentity(), parentId.versionedIdentity(),
                dataId.versionedIdentity(), guardId.versionedIdentity(), marker, process.pid(), process.creationFileTime(), boot,
                configurationSha256, securityGuardSha256);
        } catch (IOException | RuntimeException | Error cause) { primary = cause; failure = cause; throw cause; }
        finally {
            try { allocation.releaseTemporary(PATH_WORKSPACE, "FirstBinding6 original source observation workspace retired"); }
            catch (Throwable cleanup) { if (primary != null) primary.addSuppressed(cleanup); else throw cleanup; }
        }
    }
    public void requireOriginalSource(DbFirstWorldAdoptionReceiptCodec.SourceFacts facts) throws IOException {
        requireLive(); Objects.requireNonNull(facts);
        if (!plan.equals(facts.plan()) || !worldId.versionedIdentity().equals(facts.selectedSaveFileId())
            || !parentId.versionedIdentity().equals(facts.parentSaveFileId())
            || !dataId.versionedIdentity().equals(facts.dataDirectoryFileId())
            || !guardId.versionedIdentity().equals(facts.custodyFileId()))
            throw new IOException("FirstBinding6 original source receipt differs from actual selected save/custody");
    }

    /** 仅读取固定 canonical，真实 writable flush/readback/CloseHandle 后才返回 durable observation。 */
    public ReadArtifact read(Key key) throws IOException {
        requireLive();
        long workspace = PATH_WORKSPACE;
        allocation.reserveTemporary(workspace, "FirstBinding6 artifact path/observation workspace BEFORE open");
        long bodyFee = 0L;
        Throwable primary = null;
        try {
            verifyPins();
            pending = openIfPresent(path(key), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE);
            if (pending == null) return null;
            var observed = io.observeHeldAdoptionArtifact(pending, artifactLimit);
            int length = Math.toIntExact(observed.byteLength());
            bodyFee = Math.addExact(512L, Math.multiplyExact(3L, length));
            allocation.reserveTemporary(bodyFee, "FirstBinding6 bounded actual artifact body/native transfer BEFORE read");
            pendingFee = bodyFee;
            if (!child(nativeData, basename(key)).equals(io.finalPath(pending)))
                throw new IOException("FirstBinding6 canonical artifact native path differs");
            io.flushChecked(pending);
            byte[] bytes = io.readWitnessArtifactExact(pending, length, artifactLimit);
            io.compareWitnessArtifactExact(pending, bytes, observed.identity(), artifactLimit);
            verifyPins(); closePending();
            long retained = Math.addExact(512L, length);
            allocation.releaseTemporary(bodyFee - retained, "FirstBinding6 artifact transfer workspace retired");
            pendingFee = retained;
            pendingRead = new ReadArtifact(key, observed.identity(), bytes, 0L);
            reads.add(pendingRead);
            var result = pendingRead;
            result.reservation = retained;
            pendingFee = 0L; pendingRead = null;
            return result;
        } catch (IOException | RuntimeException | Error cause) { primary = cause; failure = cause; throw cause; }
        finally {
            try { allocation.releaseTemporary(workspace, "FirstBinding6 artifact observation path workspace retired"); }
            catch (Throwable cleanup) { if (primary != null) primary.addSuppressed(cleanup); else throw cleanup; }
        }
    }

    /** 完整输入由 caller 的原 root 计费；发布不覆盖任何既有 canonical 或失败 temp。 */
    public DurableArtifact publish(Key key, byte[] bytes) throws IOException {
        requireLive(); Objects.requireNonNull(bytes);
        WindowsMarkerIo.witnessArtifactBound(bytes.length, artifactLimit);
        try (var existing = read(key)) {
            if (existing != null) {
                if (!existing.decode(actual -> Arrays.equals(actual, bytes)))
                    throw new IOException("FirstBinding6 immutable artifact collides with different body");
                return existing.acknowledgement();
            }
        }
        long workspace = Math.addExact(PATH_WORKSPACE, Math.multiplyExact(2L, bytes.length));
        allocation.reserveTemporary(workspace, "FirstBinding6 complete native publication workspace BEFORE CREATE_NEW");
        pendingFee = workspace;
        Throwable primary = null;
        Path temporary = data.resolve(basename(key) + ".tmp-" + boot.bootId());
        try {
            verifyPins();
            pending = io.open(temporary, WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, WindowsMarkerIo.SHARE_DELETE,
                WindowsMarkerIo.CREATE_NEW, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, plan.bootstrapOperationUuid());
            io.metadata(pending, false, 0L);
            var identity = io.identity(pending);
            if (!child(nativeData, temporary.getFileName().toString()).equals(io.finalPath(pending)))
                throw new IOException("FirstBinding6 new artifact escaped its pinned data directory");
            io.writeWitnessArtifact(pending, bytes, artifactLimit); io.flushChecked(pending);
            io.compareWitnessArtifactExact(pending, bytes, identity, artifactLimit);
            io.moveWriteThroughNoReplace(temporary, path(key), plan.bootstrapOperationUuid());
            closePending();
            pending = io.open(path(key), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, plan.bootstrapOperationUuid());
            io.flushChecked(pending);
            io.compareWitnessArtifactExact(pending, bytes, identity, artifactLimit);
            if (!child(nativeData, basename(key)).equals(io.finalPath(pending)))
                throw new IOException("Published FirstBinding6 artifact canonical path differs");
            verifyPins(); closePending();
            allocation.releaseTemporary(workspace, "FirstBinding6 complete native publication workspace retired");
            pendingFee = 0L;
            return new DurableArtifact(this, key, identity, bytes);
        } catch (IOException | RuntimeException | Error cause) { primary = cause; failure = cause; throw cause; }
        // 失败 temp/canonical 与原 handle/全部预算留在本 holder；不删除、不重试 SQL 或改写证据。
    }
    public void requirePriorProcessEnded(long pid, long creationTime) throws IOException {
        requireLive();
        allocation.reserveTemporary(PATH_WORKSPACE, "FirstBinding6 prior native process lifetime observation");
        Throwable primary = null;
        try {
            var proof = io.observePreviousProcessLifetime(pid, creationTime, plan.bootstrapOperationUuid());
            io.claimPreviousProcessLifetimeObservation(proof, pid, creationTime, plan.bootstrapOperationUuid());
            io.requireClaimedPreviousProcessLifetimeObservation(proof, pid, creationTime, plan.bootstrapOperationUuid());
        } catch (IOException | RuntimeException | Error cause) { primary = cause; failure = cause; throw cause; }
        finally {
            try { allocation.releaseTemporary(PATH_WORKSPACE, "FirstBinding6 prior native process observation retired"); }
            catch (Throwable cleanup) { if (primary != null) primary.addSuppressed(cleanup); else throw cleanup; }
        }
    }
    public WindowsMarkerIo.FileIdentity dataDirectoryIdentity() throws IOException { requireLive(); return dataId; }
    private void closePending() throws IOException {
        if (pending != null) { io.closeChecked(pending); pending = null; }
    }
    @Override public void close() throws IOException {
        requireThread();
        if (closed) return;
        // 不重试已经失败的 CloseHandle；原 io 的 owned map 保留该义务并阻止 arena/root 假释放。
        if (pending != null) {
            if (pending.closeAttempted) throw new IOException("FirstBinding6 artifact handle close remains unknown", failure);
            closePending();
        }
        if (io.retainedWitnessWorkspaceFailure() != null)
            throw new IOException("FirstBinding6 native artifact Arena close remains unknown; original transfer charge retained",
                io.retainedWitnessWorkspaceFailure());
        if (pendingFee != 0L) {
            if (pendingRead != null) { pendingRead.close(); pendingRead = null; }
            allocation.releaseTemporary(pendingFee, "FirstBinding6 failed artifact body/workspace retired after handle close");
            pendingFee = 0L;
        }
        while (!reads.isEmpty()) reads.getFirst().close();
        inventory = null;
        if (inventoryFee != 0L) {
            allocation.releaseTemporary(inventoryFee, "FirstBinding6 native inventory references retired");
            inventoryFee = 0L;
        }
        guard = closeOne(guard); dataPin = closeOne(dataPin); worldPin = closeOne(worldPin); parentPin = closeOne(parentPin);
        closed = true;
    }
    private WindowsMarkerIo.Handle closeOne(WindowsMarkerIo.Handle handle) throws IOException {
        if (handle != null) {
            if (handle.closeAttempted) throw new IOException("FirstBinding6 native pin close remains unknown", failure);
            io.closeChecked(handle);
        }
        return null;
    }
    private static String sha(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}

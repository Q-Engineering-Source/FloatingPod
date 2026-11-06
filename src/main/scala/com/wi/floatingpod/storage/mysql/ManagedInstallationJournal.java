package com.wi.floatingpod.storage.mysql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 有界的本地安装进度 DATA，不授权业务写入，也不签发 runtime Ready。
 * 打开日志前，调用方必须创建目录并落实其 ACL；本类不接收或持久化凭据及秘密字节。
 */
public final class ManagedInstallationJournal implements AutoCloseable {
    public static final String FILE_NAME = "pending.journal";
    public static final int MAX_BYTES = 2 * 1024 * 1024;
    public static final int MAX_STEPS = 4096;
    public static final int MAX_STEP_KEY_LENGTH = 128;

    private static final int MAGIC = 0x46504D49; // FPMI
    private static final int VERSION = 1;
    private static final int HASH_BYTES = 32;
    private static final int RECORD_PAYLOAD_BYTES = 1 + Integer.BYTES + HASH_BYTES;
    private static final int RECORD_BYTES = Integer.BYTES + RECORD_PAYLOAD_BYTES + HASH_BYTES;
    private static final int INTENT = 1;
    private static final int CONFIRMED = 2;
    private static final int SEAL_CONFIRMED = 3;
    private static final int BUNDLE_PUBLISHED = 4;
    private static final Pattern SCHEMA = Pattern.compile("[A-Za-z0-9_]{1,64}");
    private static final Pattern HASH = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern STEP_KEY = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");

    public record Identity(UUID installationId, UUID serverId, String schema,
                           String configurationSha256, String recipeSha256) {
        public Identity {
            Objects.requireNonNull(installationId, "installationId");
            Objects.requireNonNull(serverId, "serverId");
            if (installationId.equals(new UUID(0L, 0L)) || serverId.equals(new UUID(0L, 0L)))
                throw new IllegalArgumentException("installation and server identities must be nonzero");
            if (schema == null || !SCHEMA.matcher(schema).matches())
                throw new IllegalArgumentException("managed installation schema identifier is invalid");
            if (configurationSha256 == null || !HASH.matcher(configurationSha256).matches()
                    || recipeSha256 == null || !HASH.matcher(recipeSha256).matches())
                throw new IllegalArgumentException("managed installation digests must be lowercase SHA-256");
        }
        @Override public String toString() { return "ManagedInstallationIdentity[redacted]"; }
    }

    public record Step(String key, String sha256) {
        public Step {
            if (key == null || !STEP_KEY.matcher(key).matches())
                throw new IllegalArgumentException("managed installation step key is invalid");
            if (sha256 == null || !HASH.matcher(sha256).matches())
                throw new IllegalArgumentException("managed installation step digest must be lowercase SHA-256");
        }
        @Override public String toString() { return "ManagedInstallationStep[redacted]"; }
    }

    public enum Phase { READY_FOR_STEP, INTENT_REQUIRES_INSPECTION, ALL_STEPS_CONFIRMED, SEALED, BUNDLE_PUBLISHED }

    /** 不可变进度视图；pending intent 不表示 SQL 副作用已经成功。 */
    public record Snapshot(int nextStepIndex, OptionalInt pendingIntentIndex, Phase phase,
                           boolean tornSuffixNeedsRecovery) {
        public Snapshot {
            Objects.requireNonNull(pendingIntentIndex, "pendingIntentIndex");
            Objects.requireNonNull(phase, "phase");
        }
    }

    private final Path journalPath;
    private final Thread owner = Thread.currentThread();
    private final FileChannel channel;
    private final FileLock lock;
    private final Identity identity;
    private final List<Step> steps;
    private int nextStepIndex;
    private int pendingIntent = -1;
    private boolean installationSealed;
    private boolean published;
    private boolean tornSuffix;
    private boolean unresolvedAppend;
    private boolean closeAttempted;
    private Throwable closeFailure;
    private byte[] previousDigest;
    private long validLength;

    private ManagedInstallationJournal(Path journalPath, FileChannel channel, FileLock lock,
                                       Identity identity, List<Step> steps) {
        this.journalPath = journalPath;
        this.channel = channel;
        this.lock = lock;
        this.identity = identity;
        this.steps = steps;
    }

    /**
     * 打开唯一的固定 pending 日志，或以不覆盖方式创建；独占文件锁持续到返回的 owner 关闭。
     * 此锁只保护本地文件，SQL 目标的串行化仍须使用独立、有界的 MySQL advisory lock。
     */
    public static ManagedInstallationJournal openOrCreate(Path protectedDirectory, Identity expected,
                                                          List<Step> expectedSteps) throws IOException {
        Objects.requireNonNull(protectedDirectory, "protectedDirectory");
        Objects.requireNonNull(expected, "expected");
        List<Step> steps = validateSteps(expectedSteps);
        if (!protectedDirectory.isAbsolute()) throw new IllegalArgumentException("journal directory must be absolute");
        Path directory = protectedDirectory.normalize();
        BasicFileAttributes directoryAttributes = Files.readAttributes(directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!directoryAttributes.isDirectory() || directoryAttributes.isSymbolicLink())
            throw new IOException("managed installation journal directory must be an existing non-link directory");
        Path path = directory.resolve(FILE_NAME);
        FileChannel channel;
        boolean newlyCreated = false;
        try {
            channel = openExisting(path);
        } catch (FileAlreadyExistsException race) {
            channel = openExisting(path);
        } catch (java.nio.file.NoSuchFileException missing) {
            try {
                channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                        StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                newlyCreated = true;
            } catch (FileAlreadyExistsException race) {
                channel = openExisting(path);
            }
        }

        FileLock lock = null;
        ManagedInstallationJournal result = null;
        Throwable primary = null;
        try {
            try { lock = channel.tryLock(); }
            catch (OverlappingFileLockException busy) { throw new IOException("managed installation journal is already open", busy); }
            if (lock == null) throw new IOException("managed installation journal is locked by another process");
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile() || attributes.isSymbolicLink())
                throw new IOException("managed installation journal must be a regular non-link file");
            if (channel.size() == 0L && newlyCreated) {
                result = new ManagedInstallationJournal(path, channel, lock, expected, steps);
                result.writeHeader();
            } else {
                if (channel.size() == 0L)
                    throw new IOException("existing managed installation journal is empty and cannot be reinitialized");
                result = new ManagedInstallationJournal(path, channel, lock, expected, steps);
                result.loadExisting();
            }
            return result;
        } catch (Throwable failure) {
            primary = failure;
            throw rethrow(failure);
        } finally {
            if (primary != null) closeAfterFailedOpen(lock, channel, primary);
        }
    }

    private static FileChannel openExisting(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    }

    private static List<Step> validateSteps(List<Step> input) {
        if (input == null || input.isEmpty() || input.size() > MAX_STEPS)
            throw new IllegalArgumentException("managed installation requires a bounded nonempty ordered step plan");
        List<Step> copy = List.copyOf(input);
        if (copy.stream().anyMatch(Objects::isNull)) throw new IllegalArgumentException("null managed installation step");
        if (copy.stream().map(Step::key).distinct().count() != copy.size())
            throw new IllegalArgumentException("managed installation step keys must be unique");
        return copy;
    }

    public synchronized Identity identity() {
        requireOwner();
        requireUsable();
        return identity;
    }

    public synchronized Snapshot snapshot() {
        requireOwner();
        requireUsable();
        Phase phase = published ? Phase.BUNDLE_PUBLISHED : installationSealed ? Phase.SEALED
                : pendingIntent >= 0 ? Phase.INTENT_REQUIRES_INSPECTION
                : nextStepIndex == steps.size() ? Phase.ALL_STEPS_CONFIRMED : Phase.READY_FOR_STEP;
        return new Snapshot(nextStepIndex, pendingIntent < 0 ? OptionalInt.empty() : OptionalInt.of(pendingIntent), phase, tornSuffix);
    }

    /** 调用方发送对应的外部副作用前，先追加并 force intent。 */
    public synchronized void recordIntent(int index) throws IOException {
        requireWritableProgress();
        if (installationSealed || published || pendingIntent >= 0 || index != nextStepIndex || index >= steps.size())
            throw new IllegalStateException("managed installation intent is out of order or already unresolved");
        append(INTENT, index);
        pendingIntent = index;
    }

    /** 只有调用方独立核对精确副作用后才确认。 */
    public synchronized void confirmEffect(int index) throws IOException {
        requireWritableProgress();
        if (installationSealed || published || pendingIntent != index || nextStepIndex != index)
            throw new IllegalStateException("managed installation confirmation requires its exact pending intent");
        append(CONFIRMED, index);
        nextStepIndex++;
        pendingIntent = -1;
    }

    /** 数据库的一次精确 bootstrap seal 已读回后，再持久化独立确认。 */
    public synchronized void recordSealConfirmed() throws IOException {
        requireWritableProgress();
        if (installationSealed || published || pendingIntent >= 0 || nextStepIndex != steps.size())
            throw new IllegalStateException("managed installation seal requires every planned step confirmed");
        append(SEAL_CONFIRMED, steps.size());
        installationSealed = true;
    }

    /** 只有独立数据库 seal 已确认后才发布本地 active bundle。 */
    public synchronized void recordBundlePublished() throws IOException {
        requireWritableProgress();
        if (!installationSealed || published) throw new IllegalStateException("managed active bundle requires the exact sealed installation");
        append(BUNDLE_PUBLISHED, steps.size());
        published = true;
    }

    private void requireWritableProgress() {
        requireOwner();
        requireUsable();
        if (unresolvedAppend) throw new IllegalStateException("journal append outcome is unknown; close and reopen to inspect the durable prefix");
        if (closeAttempted) throw new IllegalStateException("journal close was already attempted", closeFailure);
    }

    private void requireUsable() {
        if (closeAttempted || closeFailure != null || !channel.isOpen() || !lock.isValid())
            throw new IllegalStateException("managed installation journal owner is not live", closeFailure);
    }

    private void requireOwner() {
        if (Thread.currentThread() != owner) throw new IllegalStateException("managed installation journal requires its exact owner thread");
    }

    private void writeHeader() throws IOException {
        byte[] payload = encodeHeader(identity, steps);
        byte[] digest = sha256(payload);
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES * 2 + payload.length + digest.length);
        frame.putInt(MAGIC).putInt(payload.length).put(payload).put(digest).flip();
        writeFully(channel, frame, 0L);
        channel.force(true);
        byte[] persisted = readBounded(channel);
        Header readback = readHeader(persisted);
        if (channel.size() != frame.limit() || persisted.length != frame.limit())
            throw new IOException("managed installation journal header file size readback differs");
        if (!readback.identity.equals(identity) || !readback.steps.equals(steps))
            throw new IOException("managed installation journal header readback differs");
        previousDigest = digest;
        validLength = Integer.BYTES * 2L + payload.length + digest.length;
    }

    private void loadExisting() throws IOException {
        byte[] bytes = readBounded(channel);
        Header header = readHeader(bytes);
        if (!header.identity.equals(identity) || !header.steps.equals(steps))
            throw new IOException("existing managed installation journal belongs to a different identity or exact plan");
        previousDigest = header.digest;
        validLength = header.length;
        int position = Math.toIntExact(header.length);
        while (position < bytes.length) {
            int remaining = bytes.length - position;
            if (remaining < Integer.BYTES) { tornSuffix = true; break; }
            int frameLength = ByteBuffer.wrap(bytes, position, Integer.BYTES).getInt();
            if (frameLength != RECORD_PAYLOAD_BYTES + HASH_BYTES)
                throw new IOException("managed installation journal contains an invalid complete frame length");
            if (remaining < Integer.BYTES + frameLength) { tornSuffix = true; break; }
            ByteBuffer frame = ByteBuffer.wrap(bytes, position + Integer.BYTES, frameLength);
            byte[] payload = new byte[RECORD_PAYLOAD_BYTES]; frame.get(payload);
            byte[] digest = new byte[HASH_BYTES]; frame.get(digest);
            if (!MessageDigest.isEqual(sha256(payload), digest))
                throw new IOException("managed installation journal contains a complete frame with an invalid digest");
            ByteBuffer record = ByteBuffer.wrap(payload);
            byte type = record.get();
            int index = record.getInt();
            byte[] parent = new byte[HASH_BYTES]; record.get(parent);
            if (!MessageDigest.isEqual(previousDigest, parent))
                throw new IOException("managed installation journal digest chain is invalid");
            applyRecord(type, index);
            previousDigest = digest;
            position += Integer.BYTES + frameLength;
            validLength = position;
        }
        if (bytes.length > MAX_BYTES) throw new IOException("managed installation journal exceeds its fixed bound");
    }

    private void applyRecord(byte type, int index) throws IOException {
        switch (type) {
            case INTENT -> {
                if (installationSealed || published || pendingIntent >= 0 || index != nextStepIndex || index >= steps.size())
                    throw new IOException("managed installation journal contains an out-of-order intent");
                pendingIntent = index;
            }
            case CONFIRMED -> {
                if (installationSealed || published || pendingIntent != index || nextStepIndex != index)
                    throw new IOException("managed installation journal contains an unmatched confirmation");
                nextStepIndex++;
                pendingIntent = -1;
            }
            case SEAL_CONFIRMED -> {
                if (installationSealed || published || pendingIntent >= 0 || index != steps.size() || nextStepIndex != steps.size())
                    throw new IOException("managed installation journal contains an invalid seal acknowledgment");
                installationSealed = true;
            }
            case BUNDLE_PUBLISHED -> {
                if (!installationSealed || published || index != steps.size())
                    throw new IOException("managed installation journal contains an invalid bundle publication acknowledgment");
                published = true;
            }
            default -> throw new IOException("managed installation journal contains an unknown record kind");
        }
    }

    private void append(int type, int index) throws IOException {
        if (tornSuffix) {
            channel.truncate(validLength);
            channel.force(true);
            if (channel.size() != validLength) throw new IOException("managed installation torn suffix truncation was not confirmed");
            tornSuffix = false;
        }
        ByteBuffer payload = ByteBuffer.allocate(RECORD_PAYLOAD_BYTES);
        payload.put((byte) type).putInt(index).put(previousDigest).flip();
        byte[] raw = new byte[RECORD_PAYLOAD_BYTES]; payload.get(raw);
        byte[] digest = sha256(raw);
        ByteBuffer frame = ByteBuffer.allocate(RECORD_BYTES);
        frame.putInt(RECORD_PAYLOAD_BYTES + HASH_BYTES).put(raw).put(digest).flip();
        long newLength = Math.addExact(validLength, RECORD_BYTES);
        if (newLength > MAX_BYTES) throw new IOException("managed installation journal would exceed its fixed bound");
        unresolvedAppend = true;
        writeFully(channel, frame, validLength);
        channel.force(true);
        if (channel.size() != newLength) throw new IOException("managed installation journal append length readback differs");
        byte[] readback = new byte[RECORD_BYTES];
        readFully(channel, ByteBuffer.wrap(readback), validLength);
        if (!Arrays.equals(frame.array(), readback)) throw new IOException("managed installation journal append readback differs");
        previousDigest = digest;
        validLength = newLength;
        unresolvedAppend = false;
    }

    private record Header(Identity identity, List<Step> steps, byte[] digest, long length) { }

    private static byte[] encodeHeader(Identity identity, List<Step> steps) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(VERSION);
            writeUuid(out, identity.installationId());
            writeUuid(out, identity.serverId());
            writeAscii(out, identity.schema(), 64);
            writeAscii(out, identity.configurationSha256(), 64);
            writeAscii(out, identity.recipeSha256(), 64);
            out.writeInt(steps.size());
            for (Step step : steps) { writeAscii(out, step.key(), MAX_STEP_KEY_LENGTH); writeAscii(out, step.sha256(), 64); }
        }
        byte[] payload = bytes.toByteArray();
        if (payload.length + Integer.BYTES * 2L + HASH_BYTES > MAX_BYTES)
            throw new IOException("managed installation journal header exceeds its fixed bound");
        return payload;
    }

    private static Header readHeader(byte[] bytes) throws IOException {
        if (bytes.length < Integer.BYTES * 2 + HASH_BYTES) throw new IOException("managed installation journal header is incomplete");
        ByteBuffer prefix = ByteBuffer.wrap(bytes);
        if (prefix.getInt() != MAGIC) throw new IOException("managed installation journal magic is invalid");
        int length = prefix.getInt();
        if (length < 0 || length > MAX_BYTES - Integer.BYTES * 2 - HASH_BYTES)
            throw new IOException("managed installation journal header length is out of bounds");
        long completeHeader = Integer.BYTES * 2L + length + HASH_BYTES;
        if (bytes.length < completeHeader) throw new IOException("managed installation journal header is incomplete");
        byte[] payload = Arrays.copyOfRange(bytes, Integer.BYTES * 2, Integer.BYTES * 2 + length);
        byte[] digest = Arrays.copyOfRange(bytes, Integer.BYTES * 2 + length, Math.toIntExact(completeHeader));
        if (!MessageDigest.isEqual(sha256(payload), digest)) throw new IOException("managed installation journal header digest is invalid");
        try (var in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int version = in.readInt();
            if (version != VERSION) throw new IOException("managed installation journal version is unsupported");
            Identity identity = new Identity(readUuid(in), readUuid(in), readAscii(in, 64), readAscii(in, 64), readAscii(in, 64));
            int count = in.readInt();
            if (count <= 0 || count > MAX_STEPS) throw new IOException("managed installation journal step count is out of bounds");
            var steps = new java.util.ArrayList<Step>(count);
            for (int i = 0; i < count; i++) steps.add(new Step(readAscii(in, MAX_STEP_KEY_LENGTH), readAscii(in, 64)));
            if (in.read() != -1) throw new IOException("managed installation journal header has trailing bytes");
            List<Step> validated = validateSteps(steps);
            return new Header(identity, validated, digest, completeHeader);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("managed installation journal header contains invalid identity data", malformed);
        }
    }

    private static void writeUuid(DataOutputStream out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits()); out.writeLong(value.getLeastSignificantBits());
    }
    private static UUID readUuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }
    private static void writeAscii(DataOutputStream out, String value, int maximum) throws IOException {
        byte[] raw = value.getBytes(StandardCharsets.US_ASCII);
        if (raw.length > maximum || !StandardCharsets.US_ASCII.newEncoder().canEncode(value))
            throw new IOException("managed installation journal ASCII field is out of bounds");
        out.writeInt(raw.length); out.write(raw);
    }
    private static String readAscii(DataInputStream in, int maximum) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maximum) throw new IOException("managed installation journal ASCII field length is out of bounds");
        byte[] raw = in.readNBytes(length);
        if (raw.length != length) throw new IOException("managed installation journal header is truncated");
        for (byte value : raw) if ((value & 0x80) != 0) throw new IOException("managed installation journal requires ASCII fields");
        return new String(raw, StandardCharsets.US_ASCII);
    }

    private static byte[] readBounded(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size < 0 || size > MAX_BYTES) throw new IOException("managed installation journal exceeds its fixed bound");
        byte[] bytes = new byte[Math.toIntExact(size)];
        readFully(channel, ByteBuffer.wrap(bytes), 0L);
        return bytes;
    }
    private static void readFully(FileChannel channel, ByteBuffer target, long position) throws IOException {
        while (target.hasRemaining()) {
            int read = channel.read(target, position);
            if (read < 0) throw new IOException("managed installation journal truncated during readback");
            if (read == 0) throw new IOException("managed installation journal read made no progress");
            position += read;
        }
    }
    private static void writeFully(FileChannel channel, ByteBuffer source, long position) throws IOException {
        while (source.hasRemaining()) {
            int written = channel.write(source, position);
            if (written <= 0) throw new IOException("managed installation journal write made no progress");
            position += written;
        }
    }
    private static byte[] sha256(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError("SHA-256 must be available", impossible); }
    }
    private static void closeAfterFailedOpen(FileLock lock, FileChannel channel, Throwable primary) {
        Throwable cleanupFailure = null;
        try { if (lock != null && lock.isValid()) lock.release(); }
        catch (Throwable cleanup) { cleanupFailure = cleanup; }
        try { if (channel != null && channel.isOpen()) channel.close(); }
        catch (Throwable cleanup) {
            if (cleanupFailure == null) cleanupFailure = cleanup;
            else if (cleanup != cleanupFailure) cleanupFailure.addSuppressed(cleanup);
        }
        if (cleanupFailure != null && cleanupFailure != primary) {
            primary.addSuppressed(new UnresolvedOpenResources("managed installation journal open cleanup was not confirmed",
                    lock, channel, cleanupFailure));
        }
    }

    /** 通过主异常保留打开失败的原始 handle，不把未确认关闭写成成功。 */
    private static final class UnresolvedOpenResources extends IOException {
        @SuppressWarnings("unused") private final FileLock lock;
        @SuppressWarnings("unused") private final FileChannel channel;
        private UnresolvedOpenResources(String message, FileLock lock, FileChannel channel, Throwable cause) {
            super(message, cause); this.lock = lock; this.channel = channel;
        }
    }
    @SuppressWarnings("unchecked") private static <T extends Throwable> RuntimeException rethrow(Throwable failure) throws T { throw (T) failure; }

    @Override public synchronized void close() throws IOException {
        requireOwner();
        if (closeAttempted) {
            if (closeFailure != null) throw new IOException("managed installation journal close remains unresolved", closeFailure);
            return;
        }
        closeAttempted = true;
        Throwable primary = null;
        try { if (lock.isValid()) lock.release(); }
        catch (Throwable failure) { primary = failure; }
        try { if (channel.isOpen()) channel.close(); }
        catch (Throwable failure) { if (primary == null) primary = failure; else if (failure != primary) primary.addSuppressed(failure); }
        if (primary != null) {
            closeFailure = primary;
            throw new IOException("managed installation journal resource close was not fully confirmed", primary);
        }
    }
}

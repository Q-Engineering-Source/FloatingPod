package com.wi.floatingpod.storage.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 非秘密、可跨重启读取的安装数据包。发布要求本地持有已封存的进度日志；本类不验证 SQL 内容，
 * 也不宣称数据库或服务器处于 Ready 或存活状态。
 */
public final class ManagedInstallationBundle {
    public static final String BASELINE_FILE = "baseline.json";
    public static final String RUNTIME_CONTRACT_FILE = "runtime-contract.json";
    public static final String SOURCE_CAPTURE_FILE = "source-capture.json";
    public static final String RUNTIME_METADATA_FILE = "runtime-metadata.properties";
    public static final String PENDING_POINTER_FILE = "active-pointer.pending";
    public static final String ACTIVE_POINTER_FILE = "active.bundle";
    public static final int MAX_PAYLOAD_BYTES = ManagedInstallationFiles.MAX_BUNDLE_BYTES;
    public static final int MAX_POINTER_BYTES = 4096;

    private static final String FORMAT = "FP_MANAGED_ACTIVE_BUNDLE_V1";
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final List<String> IDENTITY_KEYS = List.of("installationUuid", "serverUuid", "bindingUuid",
            "provisioningUuid", "creationOperationUuid", "bootstrapOperationUuid", "prepareWriteUuid",
            "certifyWriteUuid", "schema", "configurationSha256", "recipeSha256");

    /** 发布副作用前复制输入字节，并核对调用方提供的实际摘要。 */
    public record Payload(byte[] bytes, String sha256) {
        public Payload {
            if (bytes == null || bytes.length == 0 || bytes.length > MAX_PAYLOAD_BYTES)
                throw new IllegalArgumentException("managed non-secret bundle payload is outside its fixed byte bound");
            if (sha256 == null || !SHA256.matcher(sha256).matches())
                throw new IllegalArgumentException("managed payload requires lowercase actual SHA-256");
        }
        @Override public byte[] bytes() { return bytes.clone(); }
        @Override public String toString() { return "ManagedInstallationBundle.Payload[bounded DATA; bytes omitted]"; }
    }

    /** 路径始终使用四个编译时固定文件名；指针不能选择任意本地文件。 */
    public record Loaded(ManagedInstallationIdentity.Record identity, String configurationDigestSha256,
                         String baselineSha256, String runtimeContractSha256,
                         String sourceCaptureSha256, String metadataSha256,
                         Path baselinePath, Path runtimeContractPath,
                         Path sourceCapturePath, Path metadataPath) {
        public Loaded {
            Objects.requireNonNull(identity, "persisted installation identity");
            Objects.requireNonNull(baselinePath, "baseline path");
            Objects.requireNonNull(runtimeContractPath, "runtime contract path");
            Objects.requireNonNull(sourceCapturePath, "source capture path");
            Objects.requireNonNull(metadataPath, "metadata path");
            requireSha(configurationDigestSha256, "configuration digest");
            requireSha(baselineSha256, "baseline payload digest");
            requireSha(runtimeContractSha256, "runtime contract payload digest");
            requireSha(sourceCaptureSha256, "source capture payload digest");
            requireSha(metadataSha256, "metadata payload digest");
        }
        @Override public String toString() { return "ManagedInstallationBundle.Loaded[non-secret DATA; no Ready]"; }
    }

    private ManagedInstallationBundle() { }

    /**
     * 持久化四个已核对的不可变载荷后，原子创建 active 硬链接指针；再次核对指针与载荷，
     * 最后在同一份仍持有的本地日志中确认 BUNDLE_PUBLISHED。
     */
    public static Loaded publish(ManagedInstallationFiles files, ManagedInstallationJournal journal,
            ManagedInstallationIdentity.Record identity, Payload baseline, Payload runtimeContract,
            Payload sourceCapture, Payload metadata, String configurationDigestSha256) throws IOException {
        Objects.requireNonNull(files, "managed private files");
        Objects.requireNonNull(journal, "held managed installation journal");
        Objects.requireNonNull(identity, "persisted managed identity");
        Objects.requireNonNull(baseline, "baseline payload");
        Objects.requireNonNull(runtimeContract, "runtime contract payload");
        Objects.requireNonNull(sourceCapture, "source capture payload");
        Objects.requireNonNull(metadata, "runtime metadata payload");
        requireSha(configurationDigestSha256, "configuration digest");
        if (!journal.identity().equals(journalIdentity(identity)))
            throw new IOException("managed bundle journal identity differs from its persisted installation identity");

        ManagedInstallationJournal.Snapshot before = journal.snapshot();
        if (before.tornSuffixNeedsRecovery() || before.pendingIntentIndex().isPresent())
            throw new IllegalStateException("managed installation journal must have no unresolved append or pending effect before bundle publication");
        if (before.phase() != ManagedInstallationJournal.Phase.SEALED &&
                before.phase() != ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED)
            throw new IllegalStateException("managed bundle publication requires the exact persisted schema seal");

        byte[][] snapshots = new byte[4][];
        byte[] pointer = null;
        try {
            Payload[] inputs = {baseline, runtimeContract, sourceCapture, metadata};
            String[] hashes = new String[4];
            for (int i = 0; i < inputs.length; i++) {
                snapshots[i] = inputs[i].bytes();
                hashes[i] = sha256(snapshots[i]);
                if (!hashes[i].equals(inputs[i].sha256()))
                    throw new IOException("managed payload input bytes differ from their caller-pinned actual SHA-256");
            }
            pointer = encodePointer(identity, hashes, configurationDigestSha256);

            Path directory = files.directory();
            verifyActiveOrPendingBeforeEffects(files, directory, identity, hashes, configurationDigestSha256,
                    pointer, before.phase());
            if (before.phase() == ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED) {
                Loaded loaded = load(files, identity).orElseThrow(() ->
                        new IOException("journal records bundle publication but the active pointer is absent"));
                requireSameLoaded(loaded, identity, hashes, configurationDigestSha256);
                return loaded;
            }

            publishOrVerifyPayload(files, BASELINE_FILE, snapshots[0], hashes[0]);
            publishOrVerifyPayload(files, RUNTIME_CONTRACT_FILE, snapshots[1], hashes[1]);
            publishOrVerifyPayload(files, SOURCE_CAPTURE_FILE, snapshots[2], hashes[2]);
            publishOrVerifyPayload(files, RUNTIME_METADATA_FILE, snapshots[3], hashes[3]);

            Path activePath = directory.resolve(ACTIVE_POINTER_FILE);
            if (!Files.exists(activePath, LinkOption.NOFOLLOW_LINKS)) {
                publishOrVerifyPayload(files, PENDING_POINTER_FILE, pointer, sha256(pointer));
                try {
                    files.publishAtomicLink(PENDING_POINTER_FILE, ACTIVE_POINTER_FILE);
                } catch (FileAlreadyExistsException raced) {
                    // 并发发布者仅可先创建完全相同的字节；绝不替换其他目标。
                    verifyExactFile(files, ACTIVE_POINTER_FILE, pointer, sha256(pointer));
                }
            }

            Loaded loaded = load(files, identity).orElseThrow(() ->
                    new IOException("active managed bundle was not visible after hard-link publication"));
            requireSameLoaded(loaded, identity, hashes, configurationDigestSha256);
            ManagedInstallationJournal.Snapshot current = journal.snapshot();
            if (current.phase() == ManagedInstallationJournal.Phase.SEALED) journal.recordBundlePublished();
            else if (current.phase() != ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED)
                throw new IllegalStateException("managed installation journal changed during active bundle publication");
            if (journal.snapshot().phase() != ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED)
                throw new IOException("managed bundle journal acknowledgement failed its durable readback");
            return loaded;
        } catch (Throwable failure) {
            throw rethrow(failure);
        } finally {
            for (byte[] snapshot : snapshots) if (snapshot != null) Arrays.fill(snapshot, (byte) 0);
            if (pointer != null) Arrays.fill(pointer, (byte) 0);
        }
    }

    /** 只加载 active bundle；没有 active 链接的 pending 指针交由恢复协调器处理。 */
    public static Optional<Loaded> load(ManagedInstallationFiles files,
            ManagedInstallationIdentity.Record expectedIdentity) throws IOException {
        Objects.requireNonNull(files, "managed private files");
        Objects.requireNonNull(expectedIdentity, "expected persisted identity");
        Path directory = files.directory();
        Path pointerPath = directory.resolve(ACTIVE_POINTER_FILE);
        if (!Files.exists(pointerPath, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
        byte[] pointer = files.readPrivateFile(ACTIVE_POINTER_FILE, MAX_POINTER_BYTES);
        try {
            Decoded decoded = decodePointer(pointer);
            if (!decoded.identity().equals(expectedIdentity))
                throw new IOException("active managed bundle identity differs from the persisted installation identity");
            verifyFileHash(files, BASELINE_FILE, decoded.hashes()[0]);
            verifyFileHash(files, RUNTIME_CONTRACT_FILE, decoded.hashes()[1]);
            verifyFileHash(files, SOURCE_CAPTURE_FILE, decoded.hashes()[2]);
            verifyFileHash(files, RUNTIME_METADATA_FILE, decoded.hashes()[3]);
            Path normalized = directory.toAbsolutePath().normalize();
            return Optional.of(new Loaded(decoded.identity(), decoded.configurationDigestSha256(),
                    decoded.hashes()[0], decoded.hashes()[1], decoded.hashes()[2], decoded.hashes()[3],
                    normalized.resolve(BASELINE_FILE), normalized.resolve(RUNTIME_CONTRACT_FILE),
                    normalized.resolve(SOURCE_CAPTURE_FILE), normalized.resolve(RUNTIME_METADATA_FILE)));
        } catch (Throwable failure) {
            throw rethrow(failure);
        } finally {
            Arrays.fill(pointer, (byte) 0);
        }
    }

    private static void verifyActiveOrPendingBeforeEffects(ManagedInstallationFiles files, Path directory,
            ManagedInstallationIdentity.Record identity, String[] hashes, String configurationDigest,
            byte[] expectedPointer, ManagedInstallationJournal.Phase phase) throws IOException {
        Path active = directory.resolve(ACTIVE_POINTER_FILE);
        if (Files.exists(active, LinkOption.NOFOLLOW_LINKS)) {
            verifyExactFile(files, ACTIVE_POINTER_FILE, expectedPointer, sha256(expectedPointer));
            Loaded loaded = load(files, identity).orElseThrow(() -> new IOException("active bundle disappeared during verification"));
            requireSameLoaded(loaded, identity, hashes, configurationDigest);
            return;
        }
        if (phase == ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED)
            throw new IOException("published journal has no active bundle pointer; refusing to recreate or replace it");
        Path pending = directory.resolve(PENDING_POINTER_FILE);
        if (Files.exists(pending, LinkOption.NOFOLLOW_LINKS))
            verifyExactFile(files, PENDING_POINTER_FILE, expectedPointer, sha256(expectedPointer));
    }

    private static void publishOrVerifyPayload(ManagedInstallationFiles files, String name,
            byte[] expectedBytes, String expectedSha256) throws IOException {
        Path path = files.directory().resolve(name);
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            verifyExactFile(files, name, expectedBytes, expectedSha256);
            return;
        }
        try {
            files.publishNewFile(name, expectedBytes);
        } catch (FileAlreadyExistsException raced) {
            verifyExactFile(files, name, expectedBytes, expectedSha256);
        }
        verifyExactFile(files, name, expectedBytes, expectedSha256);
    }

    private static void verifyExactFile(ManagedInstallationFiles files, String name,
            byte[] expectedBytes, String expectedSha256) throws IOException {
        byte[] actual = files.readPrivateFile(name, Math.min(expectedBytes.length, MAX_PAYLOAD_BYTES));
        try {
            if (!sha256(actual).equals(expectedSha256) || !Arrays.equals(actual, expectedBytes))
                throw new IOException("existing managed bundle file differs from the exact expected payload; no replacement was attempted: " + name);
        } finally { Arrays.fill(actual, (byte) 0); }
    }

    private static void verifyFileHash(ManagedInstallationFiles files, String name, String expectedSha256) throws IOException {
        byte[] actual = files.readPrivateFile(name, MAX_PAYLOAD_BYTES);
        try {
            if (!sha256(actual).equals(expectedSha256))
                throw new IOException("active managed bundle payload digest mismatch: " + name);
        } finally { Arrays.fill(actual, (byte) 0); }
    }

    private static void requireSameLoaded(Loaded loaded, ManagedInstallationIdentity.Record identity,
            String[] hashes, String configurationDigest) throws IOException {
        if (!loaded.identity().equals(identity) || !loaded.configurationDigestSha256().equals(configurationDigest) ||
                !loaded.baselineSha256().equals(hashes[0]) || !loaded.runtimeContractSha256().equals(hashes[1]) ||
                !loaded.sourceCaptureSha256().equals(hashes[2]) || !loaded.metadataSha256().equals(hashes[3]))
            throw new IOException("active managed bundle differs from this exact sealed installation payload set");
    }

    private static ManagedInstallationJournal.Identity journalIdentity(ManagedInstallationIdentity.Record identity) {
        return new ManagedInstallationJournal.Identity(identity.installationUuid(), identity.serverUuid(), identity.schema(),
                identity.configurationSha256(), identity.recipeSha256());
    }

    private record Decoded(ManagedInstallationIdentity.Record identity, String[] hashes,
                           String configurationDigestSha256) { }

    private static byte[] encodePointer(ManagedInstallationIdentity.Record identity, String[] hashes,
            String configurationDigestSha256) throws IOException {
        if (hashes == null || hashes.length != 4) throw new IllegalArgumentException("four fixed payload digests are required");
        StringBuilder prefix = new StringBuilder(1400).append(FORMAT).append('\n');
        List<String> values = identityValues(identity);
        for (int i = 0; i < IDENTITY_KEYS.size(); i++) line(prefix, IDENTITY_KEYS.get(i), values.get(i));
        line(prefix, "baselineSha256", hashes[0]);
        line(prefix, "runtimeContractSha256", hashes[1]);
        line(prefix, "sourceCaptureSha256", hashes[2]);
        line(prefix, "metadataSha256", hashes[3]);
        line(prefix, "configurationDigestSha256", configurationDigestSha256);
        byte[] prefixBytes = prefix.toString().getBytes(StandardCharsets.US_ASCII);
        String checksum = sha256(prefixBytes);
        prefix.append("pointerSha256=").append(checksum).append('\n');
        byte[] result = prefix.toString().getBytes(StandardCharsets.US_ASCII);
        Arrays.fill(prefixBytes, (byte) 0);
        if (result.length > MAX_POINTER_BYTES) {
            Arrays.fill(result, (byte) 0);
            throw new IOException("managed active pointer exceeds its fixed byte bound");
        }
        return result;
    }

    private static Decoded decodePointer(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_POINTER_BYTES)
            throw new IOException("active managed bundle pointer is outside its fixed byte bound");
        final String text;
        try {
            text = StandardCharsets.US_ASCII.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException failure) {
            throw new IOException("active managed bundle pointer is not canonical ASCII", failure);
        }
        if (!text.endsWith("\n") || text.indexOf('\r') >= 0 || text.indexOf('\0') >= 0)
            throw new IOException("active managed bundle pointer requires canonical LF framing");
        String[] lines = text.split("\n", -1);
        if (lines.length != 19 || !FORMAT.equals(lines[0]) || !lines[18].isEmpty())
            throw new IOException("active managed bundle pointer has an unexpected field count or format");
        String[] keys = new String[17];
        String[] values = new String[17];
        for (int i = 0; i < 17; i++) {
            int separator = lines[i + 1].indexOf('=');
            if (separator <= 0 || separator != lines[i + 1].lastIndexOf('='))
                throw new IOException("active managed bundle pointer has malformed key/value framing");
            keys[i] = lines[i + 1].substring(0, separator);
            values[i] = lines[i + 1].substring(separator + 1);
        }
        for (int i = 0; i < IDENTITY_KEYS.size(); i++)
            if (!IDENTITY_KEYS.get(i).equals(keys[i])) throw new IOException("active managed bundle identity field order differs");
        String[] tail = {"baselineSha256", "runtimeContractSha256", "sourceCaptureSha256",
                "metadataSha256", "configurationDigestSha256", "pointerSha256"};
        for (int i = 0; i < tail.length; i++) if (!tail[i].equals(keys[11 + i]))
            throw new IOException("active managed bundle payload field order differs");
        for (int i = 11; i < values.length; i++) requireShaIo(values[i], "active pointer digest");
        String expectedChecksum = sha256(Arrays.copyOf(bytes, checksumPrefixLength(bytes)));
        if (!expectedChecksum.equals(values[16])) throw new IOException("active managed bundle pointer checksum differs");

        ManagedInstallationIdentity.Record identity;
        try {
            identity = new ManagedInstallationIdentity.Record(UUID.fromString(values[0]), UUID.fromString(values[1]),
                    UUID.fromString(values[2]), UUID.fromString(values[3]), UUID.fromString(values[4]),
                    UUID.fromString(values[5]), UUID.fromString(values[6]), UUID.fromString(values[7]),
                    values[8], values[9], values[10]);
        } catch (RuntimeException failure) {
            throw new IOException("active managed bundle identity is malformed", failure);
        }
        byte[] canonical = encodePointer(identity, Arrays.copyOfRange(values, 11, 15), values[15]);
        try {
            if (!Arrays.equals(canonical, bytes)) throw new IOException("active managed bundle pointer is noncanonical");
        } finally { Arrays.fill(canonical, (byte) 0); }
        return new Decoded(identity, Arrays.copyOfRange(values, 11, 15), values[15]);
    }

    private static int checksumPrefixLength(byte[] bytes) throws IOException {
        byte[] marker = "pointerSha256=".getBytes(StandardCharsets.US_ASCII);
        for (int i = bytes.length - marker.length; i >= 0; i--) {
            boolean found = true;
            for (int j = 0; j < marker.length; j++) if (bytes[i + j] != marker[j]) { found = false; break; }
            if (found) {
                if (i == 0 || bytes[i - 1] != '\n') throw new IOException("active pointer checksum is not a final field");
                return i;
            }
        }
        throw new IOException("active managed bundle pointer checksum is missing");
    }

    private static List<String> identityValues(ManagedInstallationIdentity.Record identity) {
        return List.of(identity.installationUuid().toString(), identity.serverUuid().toString(), identity.bindingUuid().toString(),
                identity.provisioningUuid().toString(), identity.creationOperationUuid().toString(),
                identity.bootstrapOperationUuid().toString(), identity.prepareWriteUuid().toString(),
                identity.certifyWriteUuid().toString(), identity.schema(), identity.configurationSha256(), identity.recipeSha256());
    }

    private static void line(StringBuilder output, String key, String value) {
        output.append(key).append('=').append(value).append('\n');
    }

    private static void requireSha(String value, String description) {
        if (value == null || !SHA256.matcher(value).matches()) throw new IllegalArgumentException(description + " must be lowercase SHA-256");
    }

    private static void requireShaIo(String value, String description) throws IOException {
        if (value == null || !SHA256.matcher(value).matches()) throw new IOException(description + " is not lowercase SHA-256");
    }

    private static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 is unavailable", impossible); }
    }

    private static RuntimeException rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException io) throw io;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        throw new IOException("managed bundle operation failed", failure);
    }
}

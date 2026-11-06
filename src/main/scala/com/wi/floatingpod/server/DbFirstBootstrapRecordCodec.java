package com.wi.floatingpod.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/** FirstBinding6 owner/phase DATA 格式；调用者预留有界工作区，本 codec 只封闭结构而不授予 SQL/native 权限。 */
public final class DbFirstBootstrapRecordCodec {
    public enum Phase { PREPARE, HANDOFF, CERTIFY, RELEASE }
    public enum Kind { INTENT, CONFIRMED }
    public enum Outcome { APPLIED, NOT_APPLIED }

    public static final int SCHEMA_FORMAT = 6;
    public static final String SNAPSHOT_SELECTOR = "FULL27";

    private static final long OWNER_MAGIC = 0x465042364F574E31L; // FPB6OWN1
    private static final long RECORD_MAGIC = 0x4650423652434431L; // FPB6RCD1
    private static final long CUTOVER_INTENT_MAGIC = 0x465042364D493031L; // FPB6MI01
    private static final long CUTOVER_CONFIRMED_MAGIC = 0x465042364D433031L; // FPB6MC01
    private static final long SETTLEMENT_MAGIC = 0x4650423653544C31L; // FPB6STL1
    private static final int VERSION = 1;
    private static final int MAX_TEXT_BYTES = 256;
    public static final int MAX_RECORD_BYTES = 262_144;
    /** Reserve per codec call for bounded nested encode/decode copies; excludes caller-owned input/output references. */
    public static final int MAX_CODEC_WORKSPACE_BYTES = 8 * MAX_RECORD_BYTES;
    private static final int MAX_BYTES = MAX_RECORD_BYTES;
    private static final int HEADER_BYTES = Long.BYTES + Integer.BYTES + Integer.BYTES;
    private static final int SHA_BYTES = 32;
    private static final int MAX_PAYLOAD_BYTES = MAX_BYTES - HEADER_BYTES - SHA_BYTES;
    private static final String OWNER_DOMAIN = "FP_DB_FIRST_OWNER_DATA_V1";
    private static final String RECORD_DOMAIN = "FP_DB_FIRST_PHASE_RECORD_V1";
    private static final String CUTOVER_INTENT_DOMAIN = "FP_DB_FIRST_MARKER_CUTOVER_INTENT_V1";
    private static final String CUTOVER_CONFIRMED_DOMAIN = "FP_DB_FIRST_MARKER_CUTOVER_CONFIRMED_V1";
    private static final String SETTLEMENT_DOMAIN = "FP_DB_FIRST_PHASE_SETTLEMENT_V1";
    private static final UUID ZERO_UUID = new UUID(0L, 0L);

    private DbFirstBootstrapRecordCodec() { }

    public record WitnessRef(String fileId, String sha256, long byteLength) {
        public WitnessRef {
            validateFileId(fileId, "witness fileId");
            requireLowerSha256(sha256, "witness SHA-256");
            if (byteLength <= 0) throw new IllegalArgumentException("Positive witness byte length required");
        }

        @Override public String toString() { return "WitnessRef[actual FileId/SHA/length]"; }
    }

    public record OwnerData(UUID boot, long generation, long pid, long creationTime, String sourceReceiptSha256) {
        public OwnerData {
            requireNonzero(boot, "owner boot UUID");
            if (generation <= 0) throw new IllegalArgumentException("Positive owner generation required");
            validateProcessIdentity(pid, creationTime);
            requireLowerSha256(sourceReceiptSha256, "owner source receipt SHA-256");
        }

        @Override public String toString() { return "OwnerData[retained owner DATA; no native handle]"; }
    }

    /** Marker intent DATA 仍須由 actor 對原來源收據及部署計畫做精確核對。 */
    public record CutoverIntentData(String sourceReceiptSha256, OwnerData owner,
            UUID operationUuid, UUID newBindingUuid) {
        public CutoverIntentData {
            requireLowerSha256(sourceReceiptSha256, "cutover source receipt SHA-256");
            Objects.requireNonNull(owner, "owner");
            if (!sourceReceiptSha256.equals(owner.sourceReceiptSha256()))
                throw new IllegalArgumentException("Cutover intent and owner must bind the same source receipt");
            requireNonzero(operationUuid, "cutover operation UUID");
            if (newBindingUuid == null || newBindingUuid.version() != 4 || newBindingUuid.variant() != 2)
                throw new IllegalArgumentException("Cutover binding must be an RFC UUIDv4");
        }

        @Override public String toString() { return "CutoverIntentData[original source/owner and new binding DATA]"; }
    }

    /** preservedOldMarkerPresent=false 必须与 null fileId 配对；actor 仍须核对原 marker 与真实 24-byte marker。 */
    public record MarkerData(String markerFileId, String markerSha256,
            boolean preservedOldMarkerPresent, String preservedOldFileId) {
        public MarkerData {
            validateFileId(markerFileId, "actual marker fileId");
            requireLowerSha256(markerSha256, "canonical marker24B SHA-256");
            if (preservedOldMarkerPresent) validateFileId(preservedOldFileId, "preserved old marker fileId");
            else if (preservedOldFileId != null)
                throw new IllegalArgumentException("Absent preserved old marker must not carry a FileId");
        }

        @Override public String toString() { return "MarkerData[actual marker identity/hash; preserved-old=" + preservedOldMarkerPresent + "]"; }
    }

    /** 确认 DATA 引用精确 intent 编码并附实际 marker 观察字段，但不签发 native ACK。 */
    public record CutoverConfirmedData(CutoverIntentData intent, String intentSha256, MarkerData marker) {
        public CutoverConfirmedData {
            Objects.requireNonNull(intent, "intent");
            requireLowerSha256(intentSha256, "cutover intent SHA-256");
            Objects.requireNonNull(marker, "actual marker DATA");
        }

        @Override public String toString() { return "CutoverConfirmedData[actual marker DATA; no native ACK]"; }
    }

    /** 旧 INTENT 与新实际观察分别保留；这份 DATA 不替代 actor 对实际 SQL/Full27 的核验。 */
    public record SettlementData(RecordData intent, String intentSha256, OwnerData observer,
            String sessionBarrierNamespace, long actualConnectionId, LocalDateTime databaseNow,
            UUID connectionGeneration, WitnessRef actualFull27, Outcome outcome) {
        public SettlementData {
            Objects.requireNonNull(intent, "intent");
            if (intent.kind() != Kind.INTENT)
                throw new IllegalArgumentException("Settlement must retain the original INTENT record");
            requireLowerSha256(intentSha256, "settled intent SHA-256");
            Objects.requireNonNull(observer, "observer");
            if (!intent.sourceReceiptSha256().equals(observer.sourceReceiptSha256()))
                throw new IllegalArgumentException("Settlement observer must bind the intent source receipt");
            if (sessionBarrierNamespace == null || !sessionBarrierNamespace.matches("fpb1:[A-Za-z0-9_-]{43}"))
                throw new IllegalArgumentException("Canonical observed session barrier namespace required");
            if (actualConnectionId <= 0) throw new IllegalArgumentException("Positive actual observer connection ID required");
            validateDatabaseNow(databaseNow);
            requireNonzero(connectionGeneration, "actual observer connection generation UUID");
            Objects.requireNonNull(actualFull27, "actualFull27");
            Objects.requireNonNull(outcome, "outcome");
        }

        @Override public String toString() { return "SettlementData[actual re-observation DATA; " + outcome + "]"; }
    }

    /** CONFIRMED 的 intent SHA 只是引用；actor 必须与原 durable INTENT 字节的实际 SHA 核对。 */
    public record RecordData(Phase phase, Kind kind, UUID operationUuid, String sourceReceiptSha256,
            OwnerData owner, String sessionBarrierNamespace, long actualConnectionId, LocalDateTime databaseNow,
            UUID connectionGeneration,
            WitnessRef beforeFull27, WitnessRef targetFull27, String intentSha256, MarkerData marker) {
        public RecordData {
            Objects.requireNonNull(phase, "phase");
            Objects.requireNonNull(kind, "kind");
            requireNonzero(operationUuid, "phase operation UUID");
            requireLowerSha256(sourceReceiptSha256, "source receipt SHA-256");
            Objects.requireNonNull(owner, "owner");
            if (!sourceReceiptSha256.equals(owner.sourceReceiptSha256()))
                throw new IllegalArgumentException("Phase record and owner must bind the exact same source receipt");
            if (sessionBarrierNamespace == null || !sessionBarrierNamespace.matches("fpb1:[A-Za-z0-9_-]{43}"))
                throw new IllegalArgumentException("Canonical MySQL session barrier namespace required");
            if (actualConnectionId <= 0) throw new IllegalArgumentException("Positive actual connection ID required");
            validateDatabaseNow(databaseNow);
            requireNonzero(connectionGeneration, "connection generation UUID");
            Objects.requireNonNull(beforeFull27, "beforeFull27");
            Objects.requireNonNull(targetFull27, "targetFull27");

            if (kind == Kind.INTENT) {
                if (intentSha256 != null) throw new IllegalArgumentException("INTENT cannot refer to a confirmed intent SHA");
            } else {
                requireLowerSha256(intentSha256, "confirmed intent SHA-256");
            }

            if (phase == Phase.PREPARE && marker != null)
                throw new IllegalArgumentException("PREPARE record must not carry marker data");
            if ((phase == Phase.CERTIFY || phase == Phase.RELEASE) && marker == null)
                throw new IllegalArgumentException("CERTIFY and RELEASE records require marker data");
        }

        public int schemaFormat() { return SCHEMA_FORMAT; }
        public String snapshotSelector() { return SNAPSHOT_SELECTOR; }

        @Override public String toString() {
            return "RecordData[" + phase + "," + kind + ",schema6,Full27; assertions only]";
        }
    }

    /** 调用方须在调用前为有界编码工作区预留内存。 */
    public static byte[] encodeOwner(OwnerData owner) throws IOException {
        Objects.requireNonNull(owner, "owner");
        var payloadBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(payloadBuffer)) {
            text(out, OWNER_DOMAIN);
            writeOwner(out, owner);
        }
        return frame(OWNER_MAGIC, payloadBuffer.toByteArray());
    }

    public static OwnerData decodeOwner(byte[] original) throws IOException {
        byte[] payload = unframe(original, OWNER_MAGIC);
        try (var in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (!OWNER_DOMAIN.equals(text(in))) throw new IOException("Unknown FirstBinding6 owner DATA domain");
            OwnerData owner = readOwner(in);
            if (in.read() != -1) throw new IOException("Trailing FirstBinding6 owner DATA fields");
            if (!Arrays.equals(original, encodeOwner(owner)))
                throw new IOException("Noncanonical FirstBinding6 owner DATA encoding");
            return owner;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException failure) {
            throw new IOException("Invalid FirstBinding6 owner DATA", failure);
        }
    }

    /** Full27 snapshot 仅记录独立 witness 引用；语义验证仍由 actor 调用真实 Full27 codec 完成。 */
    public static byte[] encodeRecord(RecordData record) throws IOException {
        Objects.requireNonNull(record, "record");
        var payloadBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(payloadBuffer)) {
            text(out, RECORD_DOMAIN);
            out.writeInt(SCHEMA_FORMAT);
            text(out, SNAPSHOT_SELECTOR);
            text(out, record.phase().name());
            text(out, record.kind().name());
            uuid(out, record.operationUuid());
            hash(out, record.sourceReceiptSha256());
            writeOwner(out, record.owner());
            text(out, record.sessionBarrierNamespace());
            out.writeLong(record.actualConnectionId());
            databaseNow(out, record.databaseNow());
            uuid(out, record.connectionGeneration());
            writeWitness(out, record.beforeFull27());
            writeWitness(out, record.targetFull27());
            out.writeBoolean(record.intentSha256() != null);
            if (record.intentSha256() != null) hash(out, record.intentSha256());
            out.writeBoolean(record.marker() != null);
            if (record.marker() != null) writeMarker(out, record.marker());
        }
        return frame(RECORD_MAGIC, payloadBuffer.toByteArray());
    }

    public static RecordData decodeRecord(byte[] original) throws IOException {
        byte[] payload = unframe(original, RECORD_MAGIC);
        try (var in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (!RECORD_DOMAIN.equals(text(in))) throw new IOException("Unknown FirstBinding6 phase record domain");
            if (in.readInt() != SCHEMA_FORMAT) throw new IOException("FirstBinding6 phase record must select schema6");
            if (!SNAPSHOT_SELECTOR.equals(text(in)))
                throw new IOException("FirstBinding6 phase record must select Full27 snapshots");
            Phase phase = Phase.valueOf(text(in));
            Kind kind = Kind.valueOf(text(in));
            UUID operation = uuid(in);
            String sourceSha = hash(in);
            OwnerData owner = readOwner(in);
            String namespace = text(in);
            long connectionId = in.readLong();
            LocalDateTime databaseNow = databaseNow(in);
            UUID connectionGeneration = uuid(in);
            WitnessRef before = readWitness(in);
            WitnessRef target = readWitness(in);
            String intentSha = readBoolean(in) ? hash(in) : null;
            MarkerData marker = readBoolean(in) ? readMarker(in) : null;
            if (in.read() != -1) throw new IOException("Trailing FirstBinding6 phase record fields");

            RecordData record = new RecordData(phase, kind, operation, sourceSha, owner, namespace,
                connectionId, databaseNow, connectionGeneration, before, target, intentSha, marker);
            if (!Arrays.equals(original, encodeRecord(record)))
                throw new IOException("Noncanonical FirstBinding6 phase record encoding");
            return record;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException failure) {
            throw new IOException("Invalid FirstBinding6 phase record DATA", failure);
        }
    }

    /** 调用方预留 workspace；返回字节仍是普通 DATA，需由 native owner 另行取得实际 ACK。 */
    public static byte[] encodeCutoverIntent(CutoverIntentData intent) throws IOException {
        Objects.requireNonNull(intent, "intent");
        var payloadBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(payloadBuffer)) {
            text(out, CUTOVER_INTENT_DOMAIN);
            hash(out, intent.sourceReceiptSha256());
            writeOwner(out, intent.owner());
            uuid(out, intent.operationUuid());
            uuid(out, intent.newBindingUuid());
        }
        return frame(CUTOVER_INTENT_MAGIC, payloadBuffer.toByteArray());
    }

    public static CutoverIntentData decodeCutoverIntent(byte[] original) throws IOException {
        byte[] payload = unframe(original, CUTOVER_INTENT_MAGIC);
        try (var in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (!CUTOVER_INTENT_DOMAIN.equals(text(in)))
                throw new IOException("Unknown FirstBinding6 marker cutover intent domain");
            String sourceSha = hash(in);
            OwnerData owner = readOwner(in);
            UUID operation = uuid(in);
            UUID newBinding = uuid(in);
            if (in.read() != -1) throw new IOException("Trailing FirstBinding6 marker cutover intent fields");
            CutoverIntentData intent = new CutoverIntentData(sourceSha, owner, operation, newBinding);
            if (!Arrays.equals(original, encodeCutoverIntent(intent)))
                throw new IOException("Noncanonical FirstBinding6 marker cutover intent encoding");
            return intent;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException failure) {
            throw new IOException("Invalid FirstBinding6 marker cutover intent DATA", failure);
        }
    }

    public static byte[] encodeCutoverConfirmed(CutoverConfirmedData confirmed) throws IOException {
        Objects.requireNonNull(confirmed, "confirmed");
        byte[] intentBytes = encodeCutoverIntent(confirmed.intent());
        if (!confirmed.intentSha256().equals(shaHex(intentBytes)))
            throw new IOException("Cutover confirmation does not reference the exact encoded intent SHA-256");

        var payloadBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(payloadBuffer)) {
            text(out, CUTOVER_CONFIRMED_DOMAIN);
            blob(out, intentBytes);
            hash(out, confirmed.intentSha256());
            writeMarker(out, confirmed.marker());
        }
        return frame(CUTOVER_CONFIRMED_MAGIC, payloadBuffer.toByteArray());
    }

    public static CutoverConfirmedData decodeCutoverConfirmed(byte[] original) throws IOException {
        byte[] payload = unframe(original, CUTOVER_CONFIRMED_MAGIC);
        try (var in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (!CUTOVER_CONFIRMED_DOMAIN.equals(text(in)))
                throw new IOException("Unknown FirstBinding6 marker cutover confirmation domain");
            byte[] intentBytes = blob(in);
            String intentSha = hash(in);
            MarkerData marker = readMarker(in);
            if (in.read() != -1) throw new IOException("Trailing FirstBinding6 marker cutover confirmation fields");
            CutoverIntentData intent = decodeCutoverIntent(intentBytes);
            CutoverConfirmedData confirmed = new CutoverConfirmedData(intent, intentSha, marker);
            if (!intentSha.equals(shaHex(intentBytes)))
                throw new IOException("Cutover confirmation intent SHA-256 differs from exact encoded intent");
            if (!Arrays.equals(original, encodeCutoverConfirmed(confirmed)))
                throw new IOException("Noncanonical FirstBinding6 marker cutover confirmation encoding");
            return confirmed;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException failure) {
            throw new IOException("Invalid FirstBinding6 marker cutover confirmation DATA", failure);
        }
    }

    /** Caller pre-reserves workspace; only a new actual Full27 witness is carried here. */
    public static byte[] encodeSettlement(SettlementData settlement) throws IOException {
        Objects.requireNonNull(settlement, "settlement");
        byte[] intentBytes = encodeRecord(settlement.intent());
        if (!settlement.intentSha256().equals(shaHex(intentBytes)))
            throw new IOException("Settlement does not reference the exact original INTENT SHA-256");

        var payloadBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(payloadBuffer)) {
            text(out, SETTLEMENT_DOMAIN);
            blob(out, intentBytes);
            hash(out, settlement.intentSha256());
            writeOwner(out, settlement.observer());
            text(out, settlement.sessionBarrierNamespace());
            out.writeLong(settlement.actualConnectionId());
            databaseNow(out, settlement.databaseNow());
            uuid(out, settlement.connectionGeneration());
            writeWitness(out, settlement.actualFull27());
            text(out, settlement.outcome().name());
        }
        return frame(SETTLEMENT_MAGIC, payloadBuffer.toByteArray());
    }

    public static SettlementData decodeSettlement(byte[] original) throws IOException {
        byte[] payload = unframe(original, SETTLEMENT_MAGIC);
        try (var in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (!SETTLEMENT_DOMAIN.equals(text(in))) throw new IOException("Unknown FirstBinding6 settlement DATA domain");
            byte[] intentBytes = blob(in);
            String intentSha = hash(in);
            OwnerData observer = readOwner(in);
            String namespace = text(in);
            long connectionId = in.readLong();
            LocalDateTime databaseNow = databaseNow(in);
            UUID connectionGeneration = uuid(in);
            WitnessRef actualFull27 = readWitness(in);
            Outcome outcome = Outcome.valueOf(text(in));
            if (in.read() != -1) throw new IOException("Trailing FirstBinding6 settlement DATA fields");
            RecordData intent = decodeRecord(intentBytes);
            SettlementData settlement = new SettlementData(intent, intentSha, observer, namespace,
                connectionId, databaseNow, connectionGeneration, actualFull27, outcome);
            if (!intentSha.equals(shaHex(intentBytes)))
                throw new IOException("Settlement intent SHA-256 differs from the exact original INTENT bytes");
            if (!Arrays.equals(original, encodeSettlement(settlement)))
                throw new IOException("Noncanonical FirstBinding6 settlement DATA encoding");
            return settlement;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException failure) {
            throw new IOException("Invalid FirstBinding6 settlement DATA", failure);
        }
    }

    private static byte[] frame(long magic, byte[] payload) throws IOException {
        if (payload.length == 0 || payload.length > MAX_PAYLOAD_BYTES)
            throw new IOException("FirstBinding6 DATA payload exceeds its 256 KiB bound");
        var prefix = new ByteArrayOutputStream(HEADER_BYTES + payload.length);
        try (var out = new DataOutputStream(prefix)) {
            out.writeLong(magic);
            out.writeInt(VERSION);
            out.writeInt(payload.length);
            out.write(payload);
        }
        byte[] prefixBytes = prefix.toByteArray();
        var complete = new ByteArrayOutputStream(prefixBytes.length + SHA_BYTES);
        complete.write(prefixBytes);
        complete.write(digest(prefixBytes));
        return complete.toByteArray();
    }

    private static byte[] unframe(byte[] original, long expectedMagic) throws IOException {
        if (original == null || original.length < HEADER_BYTES + SHA_BYTES || original.length > MAX_BYTES)
            throw new IOException("Invalid FirstBinding6 DATA record size");
        byte[] complete = original.clone();
        byte[] prefix = Arrays.copyOf(complete, complete.length - SHA_BYTES);
        if (!MessageDigest.isEqual(digest(prefix), Arrays.copyOfRange(complete, prefix.length, complete.length)))
            throw new IOException("FirstBinding6 DATA whole-record SHA-256 mismatch");
        try (var in = new DataInputStream(new ByteArrayInputStream(prefix))) {
            if (in.readLong() != expectedMagic || in.readInt() != VERSION)
                throw new IOException("Unknown FirstBinding6 DATA magic or version");
            int payloadLength = in.readInt();
            if (payloadLength <= 0 || payloadLength > MAX_PAYLOAD_BYTES || payloadLength != in.available())
                throw new IOException("Invalid FirstBinding6 DATA payload length");
            byte[] payload = in.readNBytes(payloadLength);
            if (payload.length != payloadLength || in.read() != -1)
                throw new IOException("Truncated or trailing FirstBinding6 DATA frame bytes");
            return payload;
        }
    }

    private static void writeOwner(DataOutputStream out, OwnerData owner) throws IOException {
        uuid(out, owner.boot());
        out.writeLong(owner.generation());
        out.writeLong(owner.pid());
        out.writeLong(owner.creationTime());
        hash(out, owner.sourceReceiptSha256());
    }

    private static OwnerData readOwner(DataInputStream in) throws IOException {
        return new OwnerData(uuid(in), in.readLong(), in.readLong(), in.readLong(), hash(in));
    }

    private static void writeWitness(DataOutputStream out, WitnessRef witness) throws IOException {
        text(out, witness.fileId());
        hash(out, witness.sha256());
        out.writeLong(witness.byteLength());
    }

    private static WitnessRef readWitness(DataInputStream in) throws IOException {
        return new WitnessRef(text(in), hash(in), in.readLong());
    }

    private static void writeMarker(DataOutputStream out, MarkerData marker) throws IOException {
        text(out, marker.markerFileId());
        hash(out, marker.markerSha256());
        out.writeBoolean(marker.preservedOldMarkerPresent());
        if (marker.preservedOldMarkerPresent()) text(out, marker.preservedOldFileId());
    }

    private static MarkerData readMarker(DataInputStream in) throws IOException {
        String fileId = text(in);
        String sha = hash(in);
        boolean oldPresent = readBoolean(in);
        String oldFileId = oldPresent ? text(in) : null;
        return new MarkerData(fileId, sha, oldPresent, oldFileId);
    }

    private static void blob(DataOutputStream out, byte[] value) throws IOException {
        if (value == null || value.length == 0 || value.length > MAX_RECORD_BYTES)
            throw new IOException("Invalid bounded FirstBinding6 nested DATA length");
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] blob(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > MAX_RECORD_BYTES || length > in.available())
            throw new IOException("Invalid bounded FirstBinding6 nested DATA length");
        byte[] value = in.readNBytes(length);
        if (value.length != length) throw new IOException("Truncated nested FirstBinding6 DATA");
        return value;
    }

    private static boolean readBoolean(DataInputStream in) throws IOException {
        int value = in.readUnsignedByte();
        if (value > 1) throw new IOException("Noncanonical FirstBinding6 DATA boolean");
        return value == 1;
    }

    private static void uuid(DataOutputStream out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    private static UUID uuid(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    private static void hash(DataOutputStream out, String value) throws IOException {
        requireLowerSha256(value, "SHA-256");
        out.write(HexFormat.of().parseHex(value));
    }

    private static String hash(DataInputStream in) throws IOException {
        byte[] value = in.readNBytes(SHA_BYTES);
        if (value.length != SHA_BYTES) throw new IOException("Truncated FirstBinding6 DATA SHA-256");
        return HexFormat.of().formatHex(value);
    }

    private static void validateFileId(String value, String label) {
        if (value == null || !value.matches("FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}"))
            throw new IllegalArgumentException("Canonical " + label + " required");
    }

    private static void requireLowerSha256(String value, String label) {
        if (value == null || !value.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Canonical lowercase " + label + " required");
    }

    private static void requireNonzero(UUID value, String label) {
        if (value == null || value.equals(ZERO_UUID)) throw new IllegalArgumentException("Nonzero " + label + " required");
    }

    private static void validateProcessIdentity(long pid, long creationTime) {
        if (pid <= 0 || pid > 0xffff_ffffL || creationTime <= 0)
            throw new IllegalArgumentException("Exact Windows PID/FILETIME required");
        Instant.ofEpochSecond(creationTime / 10_000_000L - 11_644_473_600L,
            (creationTime % 10_000_000L) * 100L);
    }

    private static void validateDatabaseNow(LocalDateTime value) {
        Objects.requireNonNull(value, "databaseNow");
        if (value.getYear() < 1000 || value.getYear() > 9999 || value.getNano() % 1000 != 0)
            throw new IllegalArgumentException("Actual MySQL DATETIME(6) in years 1000..9999 required");
    }

    private static void databaseNow(DataOutputStream out, LocalDateTime value) throws IOException {
        validateDatabaseNow(value);
        out.writeInt(value.getYear());
        out.writeByte(value.getMonthValue());
        out.writeByte(value.getDayOfMonth());
        out.writeByte(value.getHour());
        out.writeByte(value.getMinute());
        out.writeByte(value.getSecond());
        out.writeInt(value.getNano());
    }

    private static LocalDateTime databaseNow(DataInputStream in) throws IOException {
        return LocalDateTime.of(in.readInt(), in.readUnsignedByte(), in.readUnsignedByte(),
            in.readUnsignedByte(), in.readUnsignedByte(), in.readUnsignedByte(), in.readInt());
    }

    private static void text(DataOutputStream out, String value) throws IOException {
        var encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
        if (encoded.remaining() > MAX_TEXT_BYTES) throw new IOException("FirstBinding6 DATA UTF-8 text exceeds its bound");
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String text(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_TEXT_BYTES || length > in.available())
            throw new IOException("Invalid FirstBinding6 DATA UTF-8 length");
        byte[] bytes = in.readNBytes(length);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException failure) {
            throw new IOException("Invalid canonical UTF-8 in FirstBinding6 DATA", failure);
        }
    }

    private static byte[] digest(byte[] bytes) throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException failure) {
            throw new IOException("Required SHA-256 unavailable for FirstBinding6 DATA", failure);
        }
    }

    private static String shaHex(byte[] bytes) throws IOException {
        return HexFormat.of().formatHex(digest(bytes));
    }
}

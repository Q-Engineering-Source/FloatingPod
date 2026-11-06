package com.wi.floatingpod.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * FirstBinding6 世界承接来源事实的闭合 DATA 格式；只记录可持久化观察，不产生 issuer、native proof、ACK 或权威。
 */
public final class DbFirstWorldAdoptionReceiptCodec {
    public static final String SOURCE_TOKEN = "ExistingWorldAdoption6";

    private static final long MAGIC = 0x4650574144365231L; // FPWAD6R1
    private static final int VERSION = 1;
    private static final int MARKER_MAGIC = 0x46505742; // FPWB
    private static final int MARKER_BYTES = 24;
    private static final int MAX_TEXT_BYTES = 256;
    private static final int MAX_BYTES = 4096;
    private static final UUID ZERO_UUID = new UUID(0L, 0L);

    private DbFirstWorldAdoptionReceiptCodec() { }

    /** 旧 marker 必须明确表示缺席，或同时记录精确 native FileId 与固定 wire bytes。 */
    public record PreviousMarker(boolean present, String fileId, int format, UUID markerUuid) {
        public PreviousMarker {
            if (!present) {
                if (fileId != null || format != 0 || markerUuid != null)
                    throw new IllegalArgumentException("Absent old marker must have no identity or marker bytes");
            } else {
                validateFileId(fileId);
                if (format != 1 && format != 2)
                    throw new IllegalArgumentException("Old marker format must be FPWB format 1 or 2");
                Objects.requireNonNull(markerUuid, "markerUuid");
                if (markerUuid.equals(ZERO_UUID)
                        || (format == 2 && (markerUuid.version() != 4 || markerUuid.variant() != 2)))
                    throw new IllegalArgumentException("Old marker UUID does not satisfy its format proof");
            }
        }

        public Optional<byte[]> markerBytes() {
            if (!present) return Optional.empty();
            return Optional.of(encodeMarker(format, markerUuid));
        }

        @Override public String toString() {
            return present ? "PreviousMarker[present=true, format=" + format + "]" : "PreviousMarker[present=false]";
        }
    }

    /** 不含秘密的不可变来源 DATA；source token 固定属于本 codec 版本。 */
    public record SourceFacts(DbFirstWorldAdoptionPlanData plan,
            String selectedSaveFileId, String parentSaveFileId, String dataDirectoryFileId, String custodyFileId,
            PreviousMarker oldMarker, long creatorPid, long creatorCreationTime,
            WindowsWorldProvisioner.BootIdentity creationBoot,
            String configurationSha256, String securityGuardSha256) {
        public SourceFacts {
            Objects.requireNonNull(plan, "plan");
            validatePlan(plan);
            validateFileId(selectedSaveFileId);
            validateFileId(parentSaveFileId);
            validateFileId(dataDirectoryFileId);
            validateFileId(custodyFileId);
            Objects.requireNonNull(oldMarker, "oldMarker");
            validateProcessIdentity(creatorPid, creatorCreationTime);
            Objects.requireNonNull(creationBoot, "creationBoot");
            if (creationBoot.bootId().equals(ZERO_UUID) || creationBoot.generation() <= 0)
                throw new IllegalArgumentException("Exact nonzero boot identity and positive generation required");
            requireLowerSha256(configurationSha256, "configurationSha256");
            requireLowerSha256(securityGuardSha256, "securityGuardSha256");
        }

        public String sourceToken() { return SOURCE_TOKEN; }

        @Override public String toString() {
            return "SourceFacts[sourceToken=" + SOURCE_TOKEN + ", schemaFormat=6, source DATA only]";
        }
    }

    public static byte[] encode(SourceFacts data) throws IOException {
        Objects.requireNonNull(data, "data");
        var bodyBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bodyBuffer)) {
            out.writeLong(MAGIC);
            out.writeInt(VERSION);
            text(out, SOURCE_TOKEN);
            var plan = data.plan();
            uuid(out, plan.creationOperationUuid());
            uuid(out, plan.bootstrapOperationUuid());
            uuid(out, plan.prepareWriteUuid());
            uuid(out, plan.certifyWriteUuid());
            uuid(out, plan.bindingUuid());
            out.writeInt(plan.physicalFormat());
            uuid(out, plan.mysqlServerUuid());
            text(out, plan.schema());
            out.writeInt(plan.schemaFormat());
            out.write(HexFormat.of().parseHex(plan.ddlSha256()));
            uuid(out, plan.provisioningUuid());

            text(out, data.selectedSaveFileId());
            text(out, data.parentSaveFileId());
            text(out, data.dataDirectoryFileId());
            text(out, data.custodyFileId());
            var marker = data.oldMarker();
            out.writeBoolean(marker.present());
            if (marker.present()) {
                text(out, marker.fileId());
                out.write(encodeMarker(marker.format(), marker.markerUuid()));
            }

            out.writeLong(data.creatorPid());
            out.writeLong(data.creatorCreationTime());
            uuid(out, data.creationBoot().bootId());
            out.writeLong(data.creationBoot().generation());
            out.write(HexFormat.of().parseHex(data.configurationSha256()));
            out.write(HexFormat.of().parseHex(data.securityGuardSha256()));
            out.flush();
        }

        byte[] body = bodyBuffer.toByteArray();
        if (body.length + 32 > MAX_BYTES) throw new IOException("FirstBinding6 source receipt exceeds its closed size bound");
        var complete = new ByteArrayOutputStream(body.length + 32);
        complete.write(body);
        complete.write(digest(body));
        return complete.toByteArray();
    }

    public static SourceFacts decode(byte[] original) throws IOException {
        if (original == null || original.length < 32 || original.length > MAX_BYTES)
            throw new IOException("Invalid FirstBinding6 source receipt size");
        byte[] complete = original.clone();
        int bodyLength = complete.length - 32;
        byte[] body = Arrays.copyOf(complete, bodyLength);
        if (!MessageDigest.isEqual(digest(body), Arrays.copyOfRange(complete, bodyLength, complete.length)))
            throw new IOException("FirstBinding6 source receipt full-body SHA-256 mismatch");

        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            if (in.readLong() != MAGIC || in.readInt() != VERSION)
                throw new IOException("Unknown FirstBinding6 source receipt domain or version");
            if (!SOURCE_TOKEN.equals(text(in)))
                throw new IOException("Source receipt is not ExistingWorldAdoption6");

            UUID creation = uuid(in);
            UUID bootstrap = uuid(in);
            UUID prepare = uuid(in);
            UUID certify = uuid(in);
            UUID binding = uuid(in);
            int physicalFormat = in.readInt();
            UUID mysqlServer = uuid(in);
            String schema = text(in);
            int schemaFormat = in.readInt();
            byte[] ddlSha = exactBytes(in, 32, "DDL SHA-256");
            UUID provisioning = uuid(in);
            var plan = new DbFirstWorldAdoptionPlanData(creation, bootstrap, prepare, certify, binding,
                physicalFormat, mysqlServer, schema, schemaFormat, HexFormat.of().withUpperCase().formatHex(ddlSha), provisioning);

            String selectedSave = text(in);
            String parentSave = text(in);
            String dataDirectory = text(in);
            String custody = text(in);
            boolean markerPresent = in.readBoolean();
            PreviousMarker marker;
            if (markerPresent) {
                String markerFileId = text(in);
                marker = decodeMarker(markerFileId, exactBytes(in, MARKER_BYTES, "old FPWB marker"));
            } else {
                marker = new PreviousMarker(false, null, 0, null);
            }

            long creatorPid = in.readLong();
            long creatorTime = in.readLong();
            var boot = new WindowsWorldProvisioner.BootIdentity(uuid(in), in.readLong());
            String configurationSha = HexFormat.of().formatHex(exactBytes(in, 32, "configuration SHA-256"));
            String securityGuardSha = HexFormat.of().formatHex(exactBytes(in, 32, "security/guard SHA-256"));
            if (in.read() != -1) throw new IOException("Trailing FirstBinding6 source receipt fields");

            var data = new SourceFacts(plan, selectedSave, parentSave, dataDirectory, custody, marker,
                creatorPid, creatorTime, boot, configurationSha, securityGuardSha);
            if (!Arrays.equals(complete, encode(data)))
                throw new IOException("Noncanonical FirstBinding6 source receipt encoding");
            return data;
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IOException("Invalid FirstBinding6 source receipt facts", failure);
        }
    }

    private static void validatePlan(DbFirstWorldAdoptionPlanData plan) {
        if (plan.physicalFormat() != 2 || plan.schemaFormat() != 6)
            throw new IllegalArgumentException("FirstBinding6 receipt requires schema6 and physical2 plan");
        if (!plan.schema().matches("[a-z][a-z0-9_]{0,63}"))
            throw new IllegalArgumentException("FirstBinding6 receipt requires canonical lowercase schema");
        if (plan.bindingUuid().version() != 4 || plan.bindingUuid().variant() != 2)
            throw new IllegalArgumentException("FirstBinding6 binding identity must be UUIDv4");
        for (UUID id : new UUID[]{plan.creationOperationUuid(), plan.bootstrapOperationUuid(), plan.prepareWriteUuid(),
                plan.certifyWriteUuid(), plan.bindingUuid(), plan.mysqlServerUuid(), plan.provisioningUuid()}) {
            if (id == null || id.equals(ZERO_UUID))
                throw new IllegalArgumentException("FirstBinding6 plan UUIDs must be present and nonzero");
        }
        if (plan.prepareWriteUuid().equals(plan.certifyWriteUuid()))
            throw new IllegalArgumentException("FirstBinding6 prepare and certify operation UUIDs must differ");
        if (plan.ddlSha256() == null || !plan.ddlSha256().matches("[0-9A-F]{64}"))
            throw new IllegalArgumentException("FirstBinding6 DDL hash must use canonical uppercase SHA-256");
    }

    private static PreviousMarker decodeMarker(String fileId, byte[] bytes) throws IOException {
        validateFileId(fileId);
        if (bytes.length != MARKER_BYTES) throw new IOException("Old FPWB marker must be exactly 24 bytes");
        var in = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (in.getInt() != MARKER_MAGIC) throw new IOException("Old marker has the wrong FPWB format proof");
        int format = in.getInt();
        UUID markerUuid = new UUID(in.getLong(), in.getLong());
        try {
            return new PreviousMarker(true, fileId, format, markerUuid);
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IOException("Invalid old FPWB marker identity or format", failure);
        }
    }

    private static byte[] encodeMarker(int format, UUID markerUuid) {
        if (format != 1 && format != 2) throw new IllegalArgumentException("Unsupported FPWB marker format");
        Objects.requireNonNull(markerUuid, "markerUuid");
        return ByteBuffer.allocate(MARKER_BYTES).order(ByteOrder.BIG_ENDIAN).putInt(MARKER_MAGIC).putInt(format)
            .putLong(markerUuid.getMostSignificantBits()).putLong(markerUuid.getLeastSignificantBits()).array();
    }

    private static void validateFileId(String value) {
        if (value == null || !value.matches("FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}"))
            throw new IllegalArgumentException("Canonical Win32 FileId128 required");
    }

    private static void requireLowerSha256(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException(field + " must be canonical lowercase SHA-256");
    }

    private static void validateProcessIdentity(long pid, long creationTime) {
        if (pid <= 0 || pid > 0xffff_ffffL || creationTime <= 0)
            throw new IllegalArgumentException("Exact Windows PID/FILETIME required");
        Instant.ofEpochSecond(creationTime / 10_000_000L - 11_644_473_600L,
            (creationTime % 10_000_000L) * 100L);
    }

    private static byte[] digest(byte[] bytes) throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException failure) {
            throw new IOException("Required SHA-256 unavailable for FirstBinding6 receipt", failure);
        }
    }

    private static void uuid(DataOutputStream out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    private static UUID uuid(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    private static void text(DataOutputStream out, String value) throws IOException {
        var encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
        if (encoded.remaining() > MAX_TEXT_BYTES) throw new IOException("FirstBinding6 receipt text exceeds its closed bound");
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String text(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_TEXT_BYTES || length > in.available())
            throw new IOException("Invalid FirstBinding6 receipt text length");
        byte[] bytes = in.readNBytes(length);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException failure) {
            throw new IOException("Invalid canonical UTF-8 in FirstBinding6 receipt", failure);
        }
    }

    private static byte[] exactBytes(DataInputStream in, int length, String label) throws IOException {
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) throw new IOException("Truncated " + label + " in FirstBinding6 receipt");
        return bytes;
    }
}

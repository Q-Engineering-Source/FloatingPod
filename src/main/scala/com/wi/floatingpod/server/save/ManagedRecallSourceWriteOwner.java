package com.wi.floatingpod.server.save;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import com.wi.floatingpod.server.WindowsMarkerIo;
import com.wi.floatingpod.storage.mysql.MySqlRecoveryResourceKey;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/** 仅保留历史 Region force-evidence DATA 的编码与校验。 */
public final class ManagedRecallSourceWriteOwner {
    private static final String FORCE_DATA_DOMAIN = "FP_REGION_FD_SYNC_EVIDENCE_BE_V1";
    private static final byte[] FORCE_DATA_DOMAIN_BYTES = FORCE_DATA_DOMAIN.getBytes(StandardCharsets.US_ASCII);
    private static final int FORCE_DATA_HASH_BYTES = 32;
    private static final int FORCE_DATA_STAGE_FRAME_BYTES = 20;
    private static final int FORCE_DATA_MIN_STAGE_COUNT = 17;
    private static final int FORCE_DATA_MAX_STAGE_COUNT = 18;




    /** Actual native region stamp from one pinned HANDLE; FILETIME is preserved as raw bits. */
    public record RegionFileStamp(WindowsMarkerIo.FileIdentity fileIdentity,
            long byteLength, long lastWriteFileTimeBits) {
        public RegionFileStamp {
            Objects.requireNonNull(fileIdentity, "actual native source region FILE_ID_INFO");
            if (byteLength < 8_192L) throw new IllegalArgumentException("Complete native source region stamp required");
        }
    }

    /** Pure DATA progress tuple; it never asserts that the named operation actually occurred. */
    public record CompletedStage(int stageCode, long enteredOrdinal, long returnedOrdinal) {
        public CompletedStage {
            if (stageCode < 1 || stageCode > 18 || enteredOrdinal <= 0L || returnedOrdinal <= enteredOrdinal)
                throw new IllegalArgumentException("A canonical SOURCE force stage requires its frozen code and increasing positive event ordinals");
        }
    }

    /**
     * Strictly decoded, callback-scoped force-wire DATA. This class carries no private evidence,
     * native handle, region writer, or publication authority. Its contents expire when the exact
     * consumeForceData callback returns.
     */
    public static final class DecodedForceData {
        private boolean active = true;
        private SourceLimitProfile profile;
        private UUID binding, workflow, write, boot;
        private long bootGeneration, custodyGeneration, writeGeneration;
        private byte[] carrierKeyBytes;
        private byte[] limitProfileSha256, payloadDescriptorSha256, undoDescriptorSha256,
                memberCoverageSha256, resultFactsSha256;
        private WindowsMarkerIo.FileIdentity regionIdentity;
        private RegionFileStamp before, after;
        private CompletedStage[] stages;

        private DecodedForceData(SourceLimitProfile profile, UUID binding, UUID workflow, UUID write, UUID boot,
                long bootGeneration, long custodyGeneration, UUID loaderObservationId, UUID regionInstanceId,
                UUID flightId, UUID outputObservationId, long writeGeneration, byte[] carrierKeyBytes,
                byte[] limitProfileSha256, byte[] payloadDescriptorSha256, byte[] undoDescriptorSha256,
                byte[] memberCoverageSha256, WindowsMarkerIo.FileIdentity regionIdentity,
                RegionFileStamp before, RegionFileStamp after, byte[] resultFactsSha256, CompletedStage[] stages) {
            this.profile = Objects.requireNonNull(profile);
            this.binding = Objects.requireNonNull(binding);
            this.workflow = Objects.requireNonNull(workflow);
            this.write = Objects.requireNonNull(write);
            this.boot = Objects.requireNonNull(boot);
            this.bootGeneration = bootGeneration;
            this.custodyGeneration = custodyGeneration;
            this.loaderObservationId = Objects.requireNonNull(loaderObservationId);
            this.regionInstanceId = Objects.requireNonNull(regionInstanceId);
            this.flightId = Objects.requireNonNull(flightId);
            this.outputObservationId = Objects.requireNonNull(outputObservationId);
            this.writeGeneration = writeGeneration;
            this.carrierKeyBytes = Objects.requireNonNull(carrierKeyBytes);
            this.limitProfileSha256 = Objects.requireNonNull(limitProfileSha256);
            this.payloadDescriptorSha256 = Objects.requireNonNull(payloadDescriptorSha256);
            this.undoDescriptorSha256 = Objects.requireNonNull(undoDescriptorSha256);
            this.memberCoverageSha256 = Objects.requireNonNull(memberCoverageSha256);
            this.regionIdentity = Objects.requireNonNull(regionIdentity);
            this.before = Objects.requireNonNull(before);
            this.after = Objects.requireNonNull(after);
            this.resultFactsSha256 = Objects.requireNonNull(resultFactsSha256);
            this.stages = Objects.requireNonNull(stages);
        }

        private UUID loaderObservationId;
        private UUID regionInstanceId;
        private UUID flightId;
        private UUID outputObservationId;

        public UUID binding() { requireActive(); return binding; }
        public UUID workflow() { requireActive(); return workflow; }
        public UUID write() { requireActive(); return write; }
        public UUID boot() { requireActive(); return boot; }
        public long bootGeneration() { requireActive(); return bootGeneration; }
        public long custodyGeneration() { requireActive(); return custodyGeneration; }
        public UUID loaderObservationId() { requireActive(); return loaderObservationId; }
        public UUID regionInstanceId() { requireActive(); return regionInstanceId; }
        public UUID flightId() { requireActive(); return flightId; }
        public UUID outputObservationId() { requireActive(); return outputObservationId; }
        public long writeGeneration() { requireActive(); return writeGeneration; }
        public int carrierKeyLength() { requireActive(); return carrierKeyBytes.length; }
        public boolean carrierKeyMatches(byte[] exactCanonicalCarrierKey) {
            requireActive();
            return exactCanonicalCarrierKey != null && MessageDigest.isEqual(carrierKeyBytes, exactCanonicalCarrierKey);
        }
        public void writeCarrierKeyTo(DataOutput destination) throws IOException {
            requireActive();
            Objects.requireNonNull(destination, "borrowed canonical carrier-key destination");
            destination.writeInt(carrierKeyBytes.length);
            destination.write(carrierKeyBytes, 0, carrierKeyBytes.length);
        }
        public boolean limitProfileSha256Matches(byte[] expected) { requireActive(); return matches(limitProfileSha256, expected); }
        public boolean payloadDescriptorSha256Matches(byte[] expected) { requireActive(); return matches(payloadDescriptorSha256, expected); }
        public boolean undoDescriptorSha256Matches(byte[] expected) { requireActive(); return matches(undoDescriptorSha256, expected); }
        public boolean memberCoverageSha256Matches(byte[] expected) { requireActive(); return matches(memberCoverageSha256, expected); }
        public WindowsMarkerIo.FileIdentity regionIdentity() { requireActive(); return regionIdentity; }
        public RegionFileStamp before() { requireActive(); return before; }
        public RegionFileStamp after() { requireActive(); return after; }
        public boolean resultFactsSha256Matches(byte[] expected) { requireActive(); return matches(resultFactsSha256, expected); }
        public int completedStageCount() { requireActive(); return stages.length; }
        public CompletedStage completedStage(int index) { requireActive(); return stages[Objects.checkIndex(index, stages.length)]; }

        private void requireActive() {
            if (!active) throw new IllegalStateException("Decoded SOURCE force DATA is valid only within its exact lexical consumer");
        }

        private void expire() {
            active = false;
            profile = null;
            binding = workflow = write = boot = loaderObservationId = regionInstanceId = flightId = outputObservationId = null;
            bootGeneration = custodyGeneration = writeGeneration = 0L;
            carrierKeyBytes = null;
            limitProfileSha256 = payloadDescriptorSha256 = undoDescriptorSha256 = memberCoverageSha256 = resultFactsSha256 = null;
            regionIdentity = null;
            before = after = null;
            stages = null;
        }
    }

    /**
     * Writes only the frozen force DATA frame. This is a pure serializer; it does not mint or
     * publish RegionWriteEvidence. The DATA value must still be inside its decoder's lexical callback.
     */
    public static void encodeForceData(DecodedForceData exactData, DataOutputStream output,
            SourceLimitProfile exactProfile, SourceForestAllocationBudget originalAllocation) throws IOException {
        Objects.requireNonNull(exactData, "strictly decoded SOURCE force DATA");
        Objects.requireNonNull(output, "borrowed force DATA destination");
        requireForceAllocation(exactProfile, originalAllocation);
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(originalAllocation, exactProfile)) {
            work.charge(1_024L, "SOURCE force DATA encoder identity/hash/file-ID and canonical-writer workspace BEFORE access");
            validateForceData(exactData, exactProfile, originalAllocation, work);
            long encodedLength = forceDataEncodedLength(exactData.carrierKeyBytes.length, exactData.stages.length);
            requireForceFrameLength(encodedLength, exactProfile, "encoded SOURCE force DATA");
            writeForceData(exactData, output);
        }
    }

    /**
     * Strictly consumes one complete frozen force DATA frame and exposes only transient DATA to the
     * lexical consumer. The caller must already have charged the borrowed frame to the original
     * allocation; it must remain stable until this returns and is never closed or refunded here.
     */
    public static void consumeForceData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation, Consumer<DecodedForceData> consumer) throws IOException {
        Objects.requireNonNull(exactFrame, "complete frozen SOURCE force DATA frame");
        Objects.requireNonNull(consumer, "lexical force DATA consumer");
        requireForceAllocation(exactProfile, originalAllocation);
        requireForceFrameLength(exactFrame.length, exactProfile, "SOURCE force DATA frame");

        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(originalAllocation, exactProfile)) {
            work.charge(1_024L, "SOURCE force DATA decoder, cursor, fixed DATA and canonical comparator workspace BEFORE allocation");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            readForceToken(input, FORCE_DATA_DOMAIN);
            UUID binding = readForceUuid(input, "binding");
            UUID workflow = readForceUuid(input, "workflow");
            UUID write = readForceUuid(input, "write");
            UUID boot = readForceUuid(input, "boot");
            long bootGeneration = readForceNonnegative(input, "boot generation");
            if (bootGeneration == 0L) throw new IOException("SOURCE force DATA boot generation must be positive");
            long custodyGeneration = readForceNonnegative(input, "custody generation");
            if (custodyGeneration == 0L) throw new IOException("SOURCE force DATA custody generation must be positive");
            UUID loaderObservationId = readForceUuid(input, "loader observation");
            UUID regionInstanceId = readForceUuid(input, "Region instance");
            UUID flightId = readForceUuid(input, "flight");
            UUID outputObservationId = readForceUuid(input, "output observation");
            long writeGeneration = readForceNonnegative(input, "write generation");

            int carrierKeyLength = readForceLength(input, "complete CarrierKey", exactProfile);
            work.charge(Math.addExact((long) carrierKeyLength, 128L),
                    "complete original CarrierKey frame retained by decoded force DATA BEFORE allocation");
            byte[] carrierKeyBytes = input.readNBytes(carrierKeyLength);
            if (carrierKeyBytes.length != carrierKeyLength) throw new IOException("Truncated complete SOURCE force CarrierKey frame");
            validateCanonicalCarrierKey(carrierKeyBytes, exactProfile, originalAllocation);

            work.charge(5L * FORCE_DATA_HASH_BYTES,
                    "five frozen SOURCE force DATA SHA-256 values BEFORE allocation");
            byte[] limitProfileSha256 = readForceHash(input, "limit profile SHA-256");
            byte[] payloadDescriptorSha256 = readForceHash(input, "payload descriptor SHA-256");
            byte[] undoDescriptorSha256 = readForceHash(input, "undo descriptor SHA-256");
            byte[] memberCoverageSha256 = readForceHash(input, "member coverage SHA-256");
            work.charge(FORCE_DATA_HASH_BYTES, "exact original profile SHA-256 comparison copy BEFORE allocation");
            if (!MessageDigest.isEqual(limitProfileSha256, exactProfile.sha256()))
                throw new IOException("SOURCE force DATA limit-profile SHA-256 differs from the exact original profile");

            work.charge(384L, "three exact SOURCE force FileIdentity and stamp DATA values BEFORE allocation");
            WindowsMarkerIo.FileIdentity regionIdentity = readForceFileIdentity(input);
            RegionFileStamp before = readForceStamp(input, exactProfile, "before");
            RegionFileStamp after = readForceStamp(input, exactProfile, "after");
            if (!regionIdentity.equals(before.fileIdentity()) || !regionIdentity.equals(after.fileIdentity()))
                throw new IOException("SOURCE force DATA region and before/after FileIdentity values disagree");

            byte[] resultFactsSha256 = readForceHash(input, "result facts SHA-256");
            int stageCount = input.readInt();
            if (stageCount != FORCE_DATA_MIN_STAGE_COUNT && stageCount != FORCE_DATA_MAX_STAGE_COUNT)
                throw new IOException("SOURCE force DATA must contain exactly the frozen 17 or 18 completed stages");
            work.charge(Math.addExact(256L, Math.multiplyExact((long) stageCount, 64L)),
                    "complete frozen SOURCE force stage array and records BEFORE allocation");
            CompletedStage[] stages = new CompletedStage[stageCount];
            for (int index = 0; index < stageCount; index++) {
                int stageLength = input.readInt();
                if (stageLength != FORCE_DATA_STAGE_FRAME_BYTES)
                    throw new IOException("SOURCE force stage must use the exact frozen B20 frame");
                int stageCode = input.readInt();
                long enteredOrdinal = readForceNonnegative(input, "stage entered ordinal");
                long returnedOrdinal = readForceNonnegative(input, "stage returned ordinal");
                if (stageCode < 1 || stageCode > 18 || enteredOrdinal == 0L || returnedOrdinal <= enteredOrdinal)
                    throw new IOException("SOURCE force stage must have increasing positive actual event ordinals");
                stages[index] = new CompletedStage(stageCode, enteredOrdinal, returnedOrdinal);
            }
            validateForceStageOrder(stages);
            if (input.read() != -1) throw new IOException("SOURCE force DATA frame has trailing bytes");

            work.charge(512L, "transient strictly decoded SOURCE force DATA holder BEFORE allocation");
            DecodedForceData decoded = new DecodedForceData(exactProfile, binding, workflow, write, boot,
                    bootGeneration, custodyGeneration, loaderObservationId, regionInstanceId, flightId,
                    outputObservationId, writeGeneration, carrierKeyBytes, limitProfileSha256,
                    payloadDescriptorSha256, undoDescriptorSha256, memberCoverageSha256,
                    regionIdentity, before, after, resultFactsSha256, stages);
            try {
                long canonicalLength = forceDataEncodedLength(decoded.carrierKeyBytes.length, decoded.stages.length);
                if (canonicalLength != exactFrame.length)
                    throw new IOException("SOURCE force DATA canonical field lengths differ from the complete input frame");
                work.charge(256L, "SOURCE force DATA strict full-field writer comparison BEFORE allocation");
                ComparingOutputStream comparison = new ComparingOutputStream(exactFrame);
                writeForceData(decoded, new DataOutputStream(comparison));
                comparison.requireComplete();
                consumer.accept(decoded);
            } finally {
                decoded.expire();
            }
        }
    }

    private static void requireForceAllocation(SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation) throws IOException {
        Objects.requireNonNull(exactProfile, "exact SOURCE force DATA profile");
        Objects.requireNonNull(originalAllocation, "original SOURCE force DATA allocation");
        if (originalAllocation.exactProfile() != exactProfile)
            throw new IOException("SOURCE force DATA must use the exact original allocation profile instance");
    }

    private static boolean matches(byte[] actual, byte[] expected) {
        return actual != null && expected != null && MessageDigest.isEqual(actual, expected);
    }

    private static void requireForceFrameLength(long frameLength, SourceLimitProfile exactProfile,
            String field) throws IOException {
        long maximum = Math.min(exactProfile.maxSqlPacketBytes(), exactProfile.maxRetainedBytes());
        if (frameLength <= 0L || frameLength > maximum || frameLength > Integer.MAX_VALUE)
            throw new IOException(field + " is outside the active SQL-packet and retained-byte profile");
    }

    private static long forceDataEncodedLength(int carrierKeyLength, int stageCount) {
        long length = Math.addExact(4L, FORCE_DATA_DOMAIN_BYTES.length);
        length = Math.addExact(length, 8L * 2L * Long.BYTES); // eight UUIDs are each two BE-i64 values.
        length = Math.addExact(length, 3L * Long.BYTES);
        length = Math.addExact(length, Integer.BYTES + (long) carrierKeyLength);
        length = Math.addExact(length, 4L * FORCE_DATA_HASH_BYTES);
        length = Math.addExact(length, 24L + 40L + 40L); // region identity plus before/after stamps.
        length = Math.addExact(length, FORCE_DATA_HASH_BYTES);
        length = Math.addExact(length, Integer.BYTES + Math.multiplyExact((long) stageCount, 4L + FORCE_DATA_STAGE_FRAME_BYTES));
        return length;
    }

    private static void validateForceData(DecodedForceData data, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation, SourceForestAllocationBudget.Scope work) throws IOException {
        data.requireActive();
        if (data.profile != exactProfile || !isNonzero(data.binding) || !isNonzero(data.workflow)
                || !isNonzero(data.write) || !isNonzero(data.boot) || !isNonzero(data.loaderObservationId)
                || !isNonzero(data.regionInstanceId) || !isNonzero(data.flightId)
                || !isNonzero(data.outputObservationId) || data.bootGeneration <= 0L
                || data.custodyGeneration <= 0L || data.writeGeneration < 0L)
            throw new IOException("SOURCE force DATA UUID, generation, or exact profile fields are invalid");
        if (data.carrierKeyBytes == null || data.carrierKeyBytes.length <= 0)
            throw new IOException("SOURCE force DATA requires the full original CARRIER key frame");
        requireForceFrameLength(data.carrierKeyBytes.length, exactProfile, "SOURCE force CarrierKey");
        validateForceHash(data.limitProfileSha256, "limit profile SHA-256");
        validateForceHash(data.payloadDescriptorSha256, "payload descriptor SHA-256");
        validateForceHash(data.undoDescriptorSha256, "undo descriptor SHA-256");
        validateForceHash(data.memberCoverageSha256, "member coverage SHA-256");
        validateForceHash(data.resultFactsSha256, "result facts SHA-256");
        work.charge(FORCE_DATA_HASH_BYTES, "exact original profile SHA-256 comparison copy BEFORE allocation");
        if (!MessageDigest.isEqual(data.limitProfileSha256, exactProfile.sha256()))
            throw new IOException("SOURCE force DATA limit-profile SHA-256 differs from the exact original profile");
        if (data.regionIdentity == null || data.before == null || data.after == null
                || !data.regionIdentity.equals(data.before.fileIdentity())
                || !data.regionIdentity.equals(data.after.fileIdentity()))
            throw new IOException("SOURCE force DATA region identity differs from its before/after native stamps");
        validateForceStamp(data.before, exactProfile, "before");
        validateForceStamp(data.after, exactProfile, "after");
        validateForceStageOrder(data.stages);
        validateCanonicalCarrierKey(data.carrierKeyBytes, exactProfile, originalAllocation);
    }

    private static void validateForceHash(byte[] hash, String field) throws IOException {
        if (hash == null || hash.length != FORCE_DATA_HASH_BYTES)
            throw new IOException("SOURCE force DATA " + field + " must contain exactly 32 bytes");
    }

    private static void validateForceStamp(RegionFileStamp stamp, SourceLimitProfile exactProfile,
            String field) throws IOException {
        if (stamp.byteLength() < 8_192L || stamp.byteLength() > exactProfile.maxRegionBytes()
                || stamp.byteLength() % 4_096L != 0L)
            throw new IOException("SOURCE force DATA " + field + " region stamp is outside the actual aligned reader profile");
    }

    private static void validateForceStageOrder(CompletedStage[] stages) throws IOException {
        if (stages == null || (stages.length != FORCE_DATA_MIN_STAGE_COUNT
                && stages.length != FORCE_DATA_MAX_STAGE_COUNT))
            throw new IOException("SOURCE force DATA must contain exactly the frozen 17 or 18 completed stages");
        int expectedStage = 1;
        long previousReturnedOrdinal = 0L;
        for (CompletedStage stage : stages) {
            if (stages.length == FORCE_DATA_MIN_STAGE_COUNT && expectedStage == 2) expectedStage = 3;
            if (stage == null || stage.stageCode() != expectedStage
                    || stage.enteredOrdinal() <= previousReturnedOrdinal
                    || stage.returnedOrdinal() <= stage.enteredOrdinal())
                throw new IOException("SOURCE force DATA stages must follow the frozen unique order and actual monotonic event ordinals");
            previousReturnedOrdinal = stage.returnedOrdinal();
            expectedStage++;
        }
        if (expectedStage != 19) throw new IOException("SOURCE force DATA does not contain the complete frozen stage sequence");
    }

    private static void validateCanonicalCarrierKey(byte[] exactCarrierKey, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation) throws IOException {
        try {
            MySqlRecoveryResourceKey.consumeCarrierKeyData(exactCarrierKey, exactProfile, originalAllocation, ignored -> { });
        } catch (Throwable malformed) {
            if (malformed instanceof Error fatal) throw fatal;
            throw new IOException("SOURCE force DATA contains a noncanonical or out-of-profile full CARRIER key", malformed);
        }
    }

    private static int readForceLength(DataInputStream input, String field, SourceLimitProfile exactProfile) throws IOException {
        int length = input.readInt();
        long maximum = Math.min(exactProfile.maxSqlPacketBytes(), exactProfile.maxRetainedBytes());
        if (length <= 0 || length > input.available() || length > maximum)
            throw new IOException("SOURCE force DATA " + field + " length is outside the complete frame/profile bounds");
        return length;
    }

    private static byte[] readForceHash(DataInputStream input, String field) throws IOException {
        byte[] value = input.readNBytes(FORCE_DATA_HASH_BYTES);
        if (value.length != FORCE_DATA_HASH_BYTES)
            throw new IOException("Truncated SOURCE force DATA " + field);
        return value;
    }

    private static WindowsMarkerIo.FileIdentity readForceFileIdentity(DataInputStream input) throws IOException {
        long volume = input.readLong();
        byte[] fileId = input.readNBytes(16);
        if (fileId.length != 16) throw new IOException("Truncated SOURCE force DATA FileIdentity");
        return new WindowsMarkerIo.FileIdentity(volume, fileId);
    }

    private static RegionFileStamp readForceStamp(DataInputStream input, SourceLimitProfile exactProfile,
            String field) throws IOException {
        WindowsMarkerIo.FileIdentity identity = readForceFileIdentity(input);
        long byteLength = readForceNonnegative(input, field + " region byte length");
        long lastWriteFileTimeBits = input.readLong();
        if (byteLength < 8_192L || byteLength > exactProfile.maxRegionBytes() || byteLength % 4_096L != 0L)
            throw new IOException("SOURCE force DATA " + field + " stamp is outside the actual aligned reader profile");
        return new RegionFileStamp(identity, byteLength, lastWriteFileTimeBits);
    }

    private static UUID readForceUuid(DataInputStream input, String field) throws IOException {
        UUID value = new UUID(input.readLong(), input.readLong());
        if (!isNonzero(value)) throw new IOException("SOURCE force DATA " + field + " UUID must be nonzero");
        return value;
    }

    private static long readForceNonnegative(DataInputStream input, String field) throws IOException {
        long value = input.readLong();
        if (value < 0L) throw new IOException("SOURCE force DATA " + field + " must use nonnegative BE-i64 framing");
        return value;
    }

    private static boolean isNonzero(UUID value) {
        return value != null && (value.getMostSignificantBits() != 0L || value.getLeastSignificantBits() != 0L);
    }

    private static void readForceToken(DataInputStream input, String expected) throws IOException {
        int length = input.readInt();
        if (length != expected.length()) throw new IOException("Unsupported SOURCE force DATA domain token length");
        for (int index = 0; index < expected.length(); index++) {
            if (input.readUnsignedByte() != expected.charAt(index))
                throw new IOException("Unsupported SOURCE force DATA domain token");
        }
    }

    private static void writeForceData(DecodedForceData data, DataOutputStream output) throws IOException {
        data.requireActive();
        output.writeInt(FORCE_DATA_DOMAIN_BYTES.length);
        output.write(FORCE_DATA_DOMAIN_BYTES);
        writeForceUuid(output, data.binding);
        writeForceUuid(output, data.workflow);
        writeForceUuid(output, data.write);
        writeForceUuid(output, data.boot);
        output.writeLong(data.bootGeneration);
        output.writeLong(data.custodyGeneration);
        writeForceUuid(output, data.loaderObservationId);
        writeForceUuid(output, data.regionInstanceId);
        writeForceUuid(output, data.flightId);
        writeForceUuid(output, data.outputObservationId);
        output.writeLong(data.writeGeneration);
        output.writeInt(data.carrierKeyBytes.length);
        output.write(data.carrierKeyBytes);
        output.write(data.limitProfileSha256);
        output.write(data.payloadDescriptorSha256);
        output.write(data.undoDescriptorSha256);
        output.write(data.memberCoverageSha256);
        writeForceFileIdentity(output, data.regionIdentity);
        writeForceStamp(output, data.before);
        writeForceStamp(output, data.after);
        output.write(data.resultFactsSha256);
        output.writeInt(data.stages.length);
        for (CompletedStage stage : data.stages) {
            output.writeInt(FORCE_DATA_STAGE_FRAME_BYTES);
            output.writeInt(stage.stageCode());
            output.writeLong(stage.enteredOrdinal());
            output.writeLong(stage.returnedOrdinal());
        }
    }

    private static void writeForceUuid(DataOutputStream output, UUID value) throws IOException {
        output.writeLong(value.getMostSignificantBits());
        output.writeLong(value.getLeastSignificantBits());
    }

    private static void writeForceFileIdentity(DataOutputStream output,
            WindowsMarkerIo.FileIdentity identity) throws IOException {
        output.writeLong(identity.volumeSerialNumber());
        byte[] fileId = identity.fileId128();
        output.write(fileId);
    }

    private static void writeForceStamp(DataOutputStream output, RegionFileStamp stamp) throws IOException {
        writeForceFileIdentity(output, stamp.fileIdentity());
        output.writeLong(stamp.byteLength());
        output.writeLong(stamp.lastWriteFileTimeBits());
    }

    private static final class ComparingOutputStream extends OutputStream {
        private final byte[] expected;
        private int offset;

        private ComparingOutputStream(byte[] expected) { this.expected = expected; }

        @Override public void write(int value) throws IOException {
            if (offset >= expected.length || (expected[offset] & 0xff) != (value & 0xff))
                throw new IOException("SOURCE force DATA failed exact canonical byte roundtrip at offset " + offset);
            offset++;
        }

        @Override public void write(byte[] bytes, int sourceOffset, int length) throws IOException {
            Objects.checkFromIndexSize(sourceOffset, length, bytes.length);
            if (length > expected.length - offset)
                throw new IOException("SOURCE force DATA canonical writer exceeded the exact input frame");
            for (int index = 0; index < length; index++) {
                if (expected[offset] != bytes[sourceOffset + index])
                    throw new IOException("SOURCE force DATA failed exact canonical byte roundtrip at offset " + offset);
                offset++;
            }
        }

        private void requireComplete() throws IOException {
            if (offset != expected.length)
                throw new IOException("SOURCE force DATA canonical writer returned " + offset + " bytes for " + expected.length + " input bytes");
        }
    }

    private ManagedRecallSourceWriteOwner() { }
}

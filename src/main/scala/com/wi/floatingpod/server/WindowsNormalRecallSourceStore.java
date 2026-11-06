package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.storage.mysql.MySqlRecoveryResourceKey;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import com.wi.floatingpod.server.save.ManagedRecallSourceWriteOwner;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/** Historical artifact identity and publication DATA codecs for normal Recall source receipts. */
public final class WindowsNormalRecallSourceStore {
    private static final String ARTIFACT_ID_DOMAIN = "FP_SOURCE_ARTIFACT_ID_BE_V1";
    private static final byte[] ARTIFACT_ID_DOMAIN_BYTES = ARTIFACT_ID_DOMAIN.getBytes(StandardCharsets.US_ASCII);
    private static final String ARTIFACT_ID_TEXT_PREFIX = "fp-source-artifact-v1:";
    private static final int ARTIFACT_ID_MAX_FRAME_BYTES = 256;
    private static final int ARTIFACT_ID_FIXED_WORK_BYTES = 1_024;
    private static final int INLINE_ARTIFACT_MAX_BYTES = 256;
    private static final int SHA256_BYTES = 32;
    private static final String SOURCE_ARTIFACT_BASENAME_PREFIX = ".floatingpod-source-";
    private static final String SOURCE_ARTIFACT_BASENAME_SUFFIX = ".bin";
    private static final String PAYLOAD_DESCRIPTOR_DOMAIN = "FP_SOURCE_PAYLOAD_DESCRIPTOR_BE_V1";
    private static final String FULL_UNDO_DESCRIPTOR_DOMAIN = "FP_SOURCE_UNDO_FULL_CHUNK_BE_V1";
    private static final String ABSENT_UNDO_DESCRIPTOR_DOMAIN = "FP_SOURCE_UNDO_SLOT_ABSENT_BE_V1";
    private static final String RESULT_PUBLICATION_PROTOCOL = "FP_WIN32_RESULT_CORE_PUBLISH_V1";
    private static final String RESULT_PUBLICATION_EVIDENCE_DOMAIN = "FP_WIN32_RESULT_PUBLICATION_BE_V1";
    private static final String SOURCE_WRITE_RECEIPT_DOMAIN = "FP_SOURCE_WRITE_RECEIPT_BE_V1";
    private static final String REGION_FORCE_PROTOCOL = "FP_REGION_FD_SYNC_READBACK_V1";
    private static final String MYSQL_RESOURCE_KEY_ENCODING = "FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1";
    private static final int PUBLICATION_STAGE_COUNT = 11;
    private static final int PUBLICATION_STAGE_FRAME_BYTES = 20;
    public WindowsNormalRecallSourceStore() { }

    /** DATA role only; this enum does not authorize a native file operation. */
    public enum ArtifactKind {
        PAYLOAD("payload"),
        UNDO("undo");

        private final String basenamePart;
        ArtifactKind(String basenamePart) { this.basenamePart = basenamePart; }
    }

    /** Inline receipt-leaf role. This is DATA framing only and never authorizes a file operation. */
    public enum InlineArtifactRole { PAYLOAD, UNDO, RESULT_CORE }

    /** Historical Store publication-stage receipt DATA; this enum grants no runtime authority. */
    public enum ArtifactPublicationStage {
        OWNER_REGISTERED,
        HOLDER_RESERVED,
        HOLDER_INITIALIZE,
        PARENT_PIN,
        PAYLOAD_CREATE,
        PAYLOAD_BODY_STREAM,
        PAYLOAD_BODY_CLOSE,
        PAYLOAD_FINISH_BODY,
        PAYLOAD_CREATE_CLOSE,
        PAYLOAD_READBACK_OPEN,
        PAYLOAD_COMPARE_STREAM,
        PAYLOAD_COMPARE_BODY,
        PAYLOAD_COMPARE_CLOSE,
        PAYLOAD_FINISH_COMPARE,
        PAYLOAD_READBACK_CLOSE,
        PAYLOAD_IDENTITY_ISSUE,
        UNDO_CREATE,
        UNDO_BODY_STREAM,
        UNDO_BODY_CLOSE,
        UNDO_FINISH_BODY,
        UNDO_CREATE_CLOSE,
        UNDO_READBACK_OPEN,
        UNDO_COMPARE_STREAM,
        UNDO_COMPARE_BODY,
        UNDO_COMPARE_CLOSE,
        UNDO_FINISH_COMPARE,
        UNDO_READBACK_CLOSE,
        UNDO_IDENTITY_ISSUE,
        PARENT_CLOSE,
        REALM_CLOSE,
        PAIR_PUBLISH
    }

    /** Undo kind is receipt DATA only; it never authorizes restoring a native slot. */
    public enum UndoKind {
        FULL_CHUNK(1), SLOT_ABSENT(2);
        private final int wireCode;
        UndoKind(int wireCode) { this.wireCode = wireCode; }
        public int wireCode() { return wireCode; }
        private static UndoKind fromWire(int value) throws IOException {
            return switch (value) {
                case 1 -> FULL_CHUNK;
                case 2 -> SLOT_ABSENT;
                default -> throw new IOException("Unknown frozen SOURCE undo kind");
            };
        }
    }

    /** RegionFileStamp projection used by the frozen descriptor and receipt DATA codecs. */
    public record RegionFileStampData(WindowsMarkerIo.FileIdentity identity, long byteLength,
            long lastWriteFileTimeBits) {
        public RegionFileStampData {
            Objects.requireNonNull(identity, "region FileIdentity");
            if (byteLength < 8192L) throw new IllegalArgumentException("A source region stamp must include both 4096-byte headers");
        }
    }

    /** Strictly decoded payload descriptor DATA. Arrays are owned by the synchronous consumer scope. */
    public static final class DecodedPayloadDescriptorData {
        private final SourceLimitProfile profile;
        private final UUID binding, workflow, write, loaderObservation;
        private final long writeGeneration;
        private final String saveIdentity, loaderIdentity;
        private final MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey;
        private final byte[] profileSha256;
        private final RegionFileStampData before;
        private final int regionX, regionZ, slot;
        private final long payloadLength;
        private final byte[] payloadSha256, undoDescriptorSha256, memberCoverageSha256, obligationRevisionsSha256;
        private final byte[] descriptorSha256;

        private DecodedPayloadDescriptorData(SourceLimitProfile profile, UUID binding, UUID workflow, UUID write, UUID loaderObservation,
                long writeGeneration, String saveIdentity, String loaderIdentity,
                MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey, byte[] profileSha256,
                RegionFileStampData before, int regionX, int regionZ, int slot, long payloadLength,
                byte[] payloadSha256, byte[] undoDescriptorSha256, byte[] memberCoverageSha256,
                byte[] obligationRevisionsSha256, byte[] descriptorSha256) {
            this.profile = profile; this.binding = binding; this.workflow = workflow; this.write = write; this.loaderObservation = loaderObservation;
            this.writeGeneration = writeGeneration; this.saveIdentity = saveIdentity; this.loaderIdentity = loaderIdentity;
            this.carrierKey = carrierKey; this.profileSha256 = profileSha256; this.before = before;
            this.regionX = regionX; this.regionZ = regionZ; this.slot = slot; this.payloadLength = payloadLength;
            this.payloadSha256 = payloadSha256; this.undoDescriptorSha256 = undoDescriptorSha256;
            this.memberCoverageSha256 = memberCoverageSha256; this.obligationRevisionsSha256 = obligationRevisionsSha256;
            this.descriptorSha256 = descriptorSha256;
        }

        public UUID bindingUuid() { return binding; }
        public UUID childWorkflowUuid() { return workflow; }
        public UUID writeUuid() { return write; }
        public UUID loaderObservationId() { return loaderObservation; }
        public long writeGeneration() { return writeGeneration; }
        public String saveIdentity() { return saveIdentity; }
        public String loaderIdentity() { return loaderIdentity; }
        public MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey() { return carrierKey; }
        public boolean profileSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(profileSha256, expected); }
        public byte[] copyProfileSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(profileSha256, account, profile, "retained payload descriptor profile SHA-256");
        }
        public RegionFileStampData before() { return before; }
        public int regionX() { return regionX; }
        public int regionZ() { return regionZ; }
        public int slot() { return slot; }
        public long payloadLength() { return payloadLength; }
        public boolean payloadSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(payloadSha256, expected); }
        public boolean undoDescriptorSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(undoDescriptorSha256, expected); }
        public boolean memberCoverageSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(memberCoverageSha256, expected); }
        public boolean obligationRevisionsSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(obligationRevisionsSha256, expected); }
        public boolean descriptorSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(descriptorSha256, expected); }
        public byte[] copyPayloadSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(payloadSha256, account, profile, "retained payload body SHA-256");
        }
        public byte[] copyUndoDescriptorSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(undoDescriptorSha256, account, profile, "retained undo descriptor SHA-256");
        }
        public byte[] copyMemberCoverageSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(memberCoverageSha256, account, profile, "retained member coverage SHA-256");
        }
        public byte[] copyObligationRevisionsSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(obligationRevisionsSha256, account, profile, "retained obligation revisions SHA-256");
        }
        public byte[] copyDescriptorSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(descriptorSha256, account, profile, "retained payload descriptor SHA-256");
        }
    }

    /** Strictly decoded FULL_CHUNK or SLOT_ABSENT undo descriptor DATA; no native restore capability. */
    public static final class DecodedUndoDescriptorData {
        private final SourceLimitProfile profile;
        private final UndoKind kind;
        private final UUID binding, workflow, write, loaderObservation;
        private final long writeGeneration;
        private final String saveIdentity, loaderIdentity;
        private final MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey;
        private final byte[] profileSha256;
        private final RegionFileStampData before;
        private final int regionX, regionZ, slot;
        private final byte[] originalHeader;
        private final int sectorOffset, spanSectors, timestampBits, compressionId;
        private final long compressedBodyLength, decodedRootLength;
        private final byte[] compressedBodySha256, decodedRootSha256;
        private final byte[] descriptorSha256;

        private DecodedUndoDescriptorData(SourceLimitProfile profile, UndoKind kind, UUID binding, UUID workflow, UUID write,
                UUID loaderObservation, long writeGeneration, String saveIdentity, String loaderIdentity,
                MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey, byte[] profileSha256,
                RegionFileStampData before, int regionX, int regionZ, int slot, byte[] originalHeader,
                int sectorOffset, int spanSectors, int timestampBits, int compressionId,
                long compressedBodyLength, byte[] compressedBodySha256,
                long decodedRootLength, byte[] decodedRootSha256, byte[] descriptorSha256) {
            this.profile = profile; this.kind = kind; this.binding = binding; this.workflow = workflow; this.write = write;
            this.loaderObservation = loaderObservation; this.writeGeneration = writeGeneration;
            this.saveIdentity = saveIdentity; this.loaderIdentity = loaderIdentity; this.carrierKey = carrierKey;
            this.profileSha256 = profileSha256; this.before = before; this.regionX = regionX; this.regionZ = regionZ;
            this.slot = slot; this.originalHeader = originalHeader; this.sectorOffset = sectorOffset;
            this.spanSectors = spanSectors; this.timestampBits = timestampBits; this.compressionId = compressionId;
            this.compressedBodyLength = compressedBodyLength; this.compressedBodySha256 = compressedBodySha256;
            this.decodedRootLength = decodedRootLength; this.decodedRootSha256 = decodedRootSha256;
            this.descriptorSha256 = descriptorSha256;
        }

        public UndoKind kind() { return kind; }
        public UUID bindingUuid() { return binding; }
        public UUID childWorkflowUuid() { return workflow; }
        public UUID writeUuid() { return write; }
        public UUID loaderObservationId() { return loaderObservation; }
        public long writeGeneration() { return writeGeneration; }
        public String saveIdentity() { return saveIdentity; }
        public String loaderIdentity() { return loaderIdentity; }
        public MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey() { return carrierKey; }
        public boolean profileSha256Matches(byte[] expected) { return profileSha256 != null && java.security.MessageDigest.isEqual(profileSha256, expected); }
        public RegionFileStampData before() { return before; }
        public int regionX() { return regionX; }
        public int regionZ() { return regionZ; }
        public int slot() { return slot; }
        public byte[] copyProfileSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return profileSha256 == null ? null : copyForRetention(profileSha256, account, profile, "retained undo profile SHA-256");
        }
        public byte[] copyOriginalHeaderForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return originalHeader == null ? null : copyForRetention(originalHeader, account, profile, "retained original undo header");
        }
        public int sectorOffset() { return sectorOffset; }
        public int spanSectors() { return spanSectors; }
        public int timestampBits() { return timestampBits; }
        public int compressionId() { return compressionId; }
        public long compressedBodyLength() { return compressedBodyLength; }
        public boolean compressedBodySha256Matches(byte[] expected) { return compressedBodySha256 != null && java.security.MessageDigest.isEqual(compressedBodySha256, expected); }
        public long decodedRootLength() { return decodedRootLength; }
        public boolean decodedRootSha256Matches(byte[] expected) { return decodedRootSha256 != null && java.security.MessageDigest.isEqual(decodedRootSha256, expected); }
        public boolean descriptorSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(descriptorSha256, expected); }
        public byte[] copyCompressedBodySha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return compressedBodySha256 == null ? null : copyForRetention(compressedBodySha256, account, profile, "retained compressed undo SHA-256");
        }
        public byte[] copyDecodedRootSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return decodedRootSha256 == null ? null : copyForRetention(decodedRootSha256, account, profile, "retained decoded undo root SHA-256");
        }
        public byte[] copyDescriptorSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(descriptorSha256, account, profile, "retained undo descriptor SHA-256");
        }
    }

    /** Complete budgeted forest/index DATA graph; it proves membership structure, never absence authority. */
    public static final class DecodedMemberCoverageData {
        private final SourceLimitProfile profile;
        private final byte[] exactFrame, forestBytes, forestSha256, indexBytes, indexSha256, frameSha256;
        private final com.wi.floatingpod.server.source.SourceForestCodec.Encoded forest;
        private final com.wi.floatingpod.server.source.SourceForestMembershipCodec.IndexData index;

        private DecodedMemberCoverageData(SourceLimitProfile profile, byte[] exactFrame, byte[] forestBytes, byte[] forestSha256,
                byte[] indexBytes, byte[] indexSha256, byte[] frameSha256,
                com.wi.floatingpod.server.source.SourceForestCodec.Encoded forest,
                com.wi.floatingpod.server.source.SourceForestMembershipCodec.IndexData index) {
            this.profile = profile; this.exactFrame = exactFrame; this.forestBytes = forestBytes; this.forestSha256 = forestSha256;
            this.indexBytes = indexBytes; this.indexSha256 = indexSha256; this.frameSha256 = frameSha256;
            this.forest = forest; this.index = index;
        }

        public long byteLength() { return exactFrame.length; }
        public boolean contentSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(frameSha256, expected); }
        public boolean forestSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(forestSha256, expected); }
        public boolean indexSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(indexSha256, expected); }
        public byte[] copyContentSha256ForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(frameSha256, allocation, profile, "retained member coverage SHA-256");
        }
        public byte[] copyForestSha256ForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(forestSha256, allocation, profile, "retained forest SHA-256");
        }
        public byte[] copyIndexSha256ForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(indexSha256, allocation, profile, "retained membership index SHA-256");
        }
        public com.wi.floatingpod.server.source.SourceForestCodec.Encoded forest() { return forest; }
        public com.wi.floatingpod.server.source.SourceForestMembershipCodec.IndexData index() { return index; }
        /** Copy for retention; the returned bytes remain charged to this exact owner until released. */
        public byte[] copyFrameForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            reserveForCopy(allocation, profile, exactFrame.length, "retained complete SOURCE member coverage frame BEFORE copy");
            try { return exactFrame.clone(); }
            catch (Throwable failure) {
                releasePreserving(allocation, exactFrame.length,
                    "failed retained SOURCE member coverage frame copy", failure);
                rethrow(failure, "copy SOURCE member coverage frame");
                throw new AssertionError("unreachable");
            }
        }
        public byte[] copyForestForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            reserveForCopy(allocation, profile, forestBytes.length, "retained complete SOURCE forest bytes BEFORE copy");
            try { return forestBytes.clone(); }
            catch (Throwable failure) {
                releasePreserving(allocation, forestBytes.length, "failed retained SOURCE forest bytes copy", failure);
                rethrow(failure, "copy SOURCE forest bytes");
                throw new AssertionError("unreachable");
            }
        }
        public byte[] copyIndexForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            reserveForCopy(allocation, profile, indexBytes.length, "retained complete SOURCE membership index BEFORE copy");
            try { return indexBytes.clone(); }
            catch (Throwable failure) {
                releasePreserving(allocation, indexBytes.length, "failed retained SOURCE membership index copy", failure);
                rethrow(failure, "copy SOURCE membership index");
                throw new AssertionError("unreachable");
            }
        }
    }

    /** One immutable primary-key/revision item from the frozen receipt L(ObligationRevision) field. */
    public static final class DecodedObligationRevisionData {
        private final SourceLimitProfile profile;
        private final UUID binding, unit, forest;
        private final byte[] carrierKeyHash, carrierKey;
        private final String purpose;
        private final long revision;

        private DecodedObligationRevisionData(SourceLimitProfile profile, UUID binding, UUID unit, UUID forest,
                byte[] carrierKeyHash, byte[] carrierKey, String purpose, long revision) {
            this.profile = profile; this.binding = binding; this.unit = unit; this.forest = forest;
            this.carrierKeyHash = carrierKeyHash; this.carrierKey = carrierKey;
            this.purpose = purpose; this.revision = revision;
        }

        public UUID bindingUuid() { return binding; }
        public UUID unitUuid() { return unit; }
        public UUID forestUuid() { return forest; }
        public String purpose() { return purpose; }
        public long revision() { return revision; }
        public boolean carrierKeyHashMatches(byte[] expected) { return java.security.MessageDigest.isEqual(carrierKeyHash, expected); }
        public boolean carrierKeyMatches(byte[] expected) { return Arrays.equals(carrierKey, expected); }
        public byte[] copyCarrierKeyHashForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(carrierKeyHash, allocation, profile, "retained obligation CARRIER key hash");
        }
        public byte[] copyCarrierKeyForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(carrierKey, allocation, profile, "retained full obligation CARRIER key");
        }
    }

    /** Complete strict L-field DATA; only safe to inspect synchronously unless copied with a charge. */
    public static final class DecodedObligationRevisionsData {
        private final SourceLimitProfile profile;
        private final byte[] exactFrame, frameSha256;
        private final List<DecodedObligationRevisionData> values;

        private DecodedObligationRevisionsData(SourceLimitProfile profile, byte[] exactFrame,
                byte[] frameSha256, List<DecodedObligationRevisionData> values) {
            this.profile = profile; this.exactFrame = exactFrame; this.frameSha256 = frameSha256;
            this.values = values;
        }

        public long byteLength() { return exactFrame.length; }
        public int size() { return values.size(); }
        public DecodedObligationRevisionData valueAt(int index) { return values.get(index); }
        public boolean contentSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(frameSha256, expected); }
        public byte[] copyContentSha256ForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(frameSha256, allocation, profile, "retained obligation revisions SHA-256");
        }
        public byte[] copyFrameForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(exactFrame, allocation, profile, "retained exact obligation revisions L field");
        }
    }

    /** One actually completed native stage in the result-core publication evidence. */
    public record PublicationStageData(int stageCode, long enteredOrdinal, long returnedOrdinal) { }

    /** Strict publication DATA. It describes evidence but does not prove that this process performed it. */
    public static final class DecodedPublicationData {
        private final SourceLimitProfile profile;
        private final SourceForestAllocationBudget originalAllocation;
        private final Thread consumerThread;
        private final UUID binding, workflow, write, publicationAttempt, boot;
        private final long bootGeneration, custodyGeneration, writeGeneration, coreLength;
        private final MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey;
        private final byte[] profileSha256, coreSha256, originalEvidence;
        private final String temporaryBasename, canonicalBasename;
        private final WindowsMarkerIo.FileIdentity parentIdentity, temporaryIdentity, canonicalIdentity;
        private final int moveFlags;
        private final List<PublicationStageData> completedStages;
        private final DecodedInlineArtifactIdentityData coreArtifact;
        private boolean consumerActive = true;

        private DecodedPublicationData(SourceLimitProfile profile, UUID binding, UUID workflow, UUID write,
                UUID publicationAttempt, UUID boot, long bootGeneration, long custodyGeneration,
                long writeGeneration, MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey,
                byte[] profileSha256, String temporaryBasename, String canonicalBasename,
                WindowsMarkerIo.FileIdentity parentIdentity, WindowsMarkerIo.FileIdentity temporaryIdentity,
                WindowsMarkerIo.FileIdentity canonicalIdentity, long coreLength, byte[] coreSha256,
                int moveFlags, List<PublicationStageData> completedStages, byte[] originalEvidence,
                DecodedInlineArtifactIdentityData coreArtifact, SourceForestAllocationBudget originalAllocation) {
            this.profile = profile; this.binding = binding; this.workflow = workflow; this.write = write;
            this.originalAllocation = originalAllocation; this.consumerThread = Thread.currentThread();
            this.publicationAttempt = publicationAttempt; this.boot = boot; this.bootGeneration = bootGeneration;
            this.custodyGeneration = custodyGeneration; this.writeGeneration = writeGeneration;
            this.carrierKey = carrierKey; this.profileSha256 = profileSha256;
            this.temporaryBasename = temporaryBasename; this.canonicalBasename = canonicalBasename;
            this.parentIdentity = parentIdentity; this.temporaryIdentity = temporaryIdentity;
            this.canonicalIdentity = canonicalIdentity; this.coreLength = coreLength; this.coreSha256 = coreSha256;
            this.moveFlags = moveFlags; this.completedStages = completedStages;
            this.originalEvidence = originalEvidence; this.coreArtifact = coreArtifact;
        }

        public UUID bindingUuid() { return binding; }
        public UUID childWorkflowUuid() { return workflow; }
        public UUID writeUuid() { return write; }
        public UUID publicationAttemptUuid() { return publicationAttempt; }
        public UUID bootUuid() { return boot; }
        public long bootGeneration() { return bootGeneration; }
        public long custodyGeneration() { return custodyGeneration; }
        public long writeGeneration() { return writeGeneration; }
        public MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierKey() { return carrierKey; }
        public boolean profileSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(profileSha256, expected); }
        public byte[] copyProfileSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(profileSha256, account, profile, "retained publication profile SHA-256");
        }
        public String temporaryBasename() { return temporaryBasename; }
        public String canonicalBasename() { return canonicalBasename; }
        public WindowsMarkerIo.FileIdentity parentDirectoryIdentity() { return parentIdentity; }
        public WindowsMarkerIo.FileIdentity temporaryIdentity() { return temporaryIdentity; }
        public WindowsMarkerIo.FileIdentity canonicalIdentity() { return canonicalIdentity; }
        public long coreLength() { return coreLength; }
        public boolean coreSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(coreSha256, expected); }
        public byte[] copyCoreSha256ForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(coreSha256, account, profile, "retained published core SHA-256");
        }
        public int moveFlags() { return moveFlags; }
        public int completedStageCount() { return completedStages.size(); }
        public PublicationStageData completedStageAt(int index) { return completedStages.get(index); }
        public DecodedInlineArtifactIdentityData coreArtifact() { return coreArtifact; }
        public byte[] copyOriginalEvidenceForRetention(SourceForestAllocationBudget.Account account) throws IOException {
            return copyForRetention(originalEvidence, account, profile, "retained original result publication evidence");
        }
        private boolean isActiveOnCurrentThread(SourceLimitProfile exactProfile,
                SourceForestAllocationBudget allocation) {
            return consumerActive && consumerThread == Thread.currentThread()
                && profile == exactProfile && originalAllocation == allocation;
        }
        private void finishSynchronousConsumer() { consumerActive = false; }
    }

    /**
     * Strictly decoded original receipt DATA. This view exists only inside the exact lexical consumer
     * on the decoding thread and original profile/allocation pair; it never carries a private issuer
     * value or publication capability.
     */
    public static final class DecodedReceiptCoreData {
        private final ReceiptData data;
        private DecodedReceiptCoreData(ReceiptData data) { this.data = data; }
        public UUID writeUuid() { data.requireActive(false); return data.write; }
        public UUID childWorkflowUuid() { data.requireActive(false); return data.workflow; }
        public UUID bindingUuid() { data.requireActive(false); return data.binding; }
        public String saveIdentity() { data.requireActive(false); return data.saveIdentity; }
        public String loaderIdentity() { data.requireActive(false); return data.loaderIdentity; }
        public boolean canonicalCarrierKeyMatches(byte[] expected) {
            data.requireActive(false); return expected != null && Arrays.equals(data.carrierKeyBytes, expected);
        }
        public int slot() { data.requireActive(false); return data.slot; }
        public long writeGeneration() { data.requireActive(false); return data.writeGeneration; }
        public long payloadLength() { data.requireActive(false); return data.payloadLength; }
        public boolean payloadSha256Matches(byte[] expected) {
            data.requireActive(false);
            return expected != null && java.security.MessageDigest.isEqual(data.payloadSha256, expected);
        }
        public DecodedInlineArtifactView payloadArtifact() {
            data.requireActive(false); return new DecodedInlineArtifactView(data, true);
        }
        public UndoKind undoKind() { data.requireActive(false); return data.undoKind; }
        public long undoLength() { data.requireActive(false); return data.undoLength; }
        public boolean undoContentSha256Matches(byte[] expected) {
            data.requireActive(false);
            return data.undoContentSha256 == null ? expected == null
                : expected != null && java.security.MessageDigest.isEqual(data.undoContentSha256, expected);
        }
        public DecodedInlineArtifactView undoArtifact() {
            data.requireActive(false); return new DecodedInlineArtifactView(data, false);
        }
        public DecodedMemberCoverageView memberCoverage() {
            data.requireActive(false); return new DecodedMemberCoverageView(data);
        }
        public int obligationCount() { data.requireActive(false); return data.obligations.size(); }
        public DecodedObligationRevisionView obligationAt(int index) {
            data.requireActive(false);
            data.obligations.valueAt(index);
            return new DecodedObligationRevisionView(data, index);
        }
        public int completedForceStageCount() {
            data.requireActive(false); return data.force.completedStageCount();
        }
        public int completedForceStageCodeAt(int index) {
            data.requireActive(false); return data.force.completedStage(index).stageCode();
        }
        public long completedForceStageEnteredOrdinalAt(int index) {
            data.requireActive(false); return data.force.completedStage(index).enteredOrdinal();
        }
        public long completedForceStageReturnedOrdinalAt(int index) {
            data.requireActive(false); return data.force.completedStage(index).returnedOrdinal();
        }
        public boolean readbackTargetRecordMatches(byte[] expected) {
            data.requireActive(false); return expected != null && Arrays.equals(data.readbackCompressedChunk, expected);
        }
        public boolean readbackRootNbtMatches(byte[] expected) {
            data.requireActive(false); return expected != null && Arrays.equals(data.readbackDecodedRootNbt, expected);
        }
        public long readbackDecodedRootNbtLength() {
            data.requireActive(false); return data.readbackDecodedRootNbt.length;
        }
        public long dataWriteCallsReturned() { data.requireActive(false); return data.dataWriteCallsReturned; }
        public long headerWriteCallsReturned() { data.requireActive(false); return data.headerWriteCallsReturned; }
        public long encoderSubmittedBytes() { data.requireActive(false); return data.encoderSubmittedBytes; }
        public long outputCloseCallsReturned() { data.requireActive(false); return data.outputCloseCallsReturned; }
        public boolean originalDiagnosticPresent() {
            data.requireActive(false); return data.exactFrame[data.diagnosticOffset] == 1;
        }
        public int originalDiagnosticLength() {
            data.requireActive(false); return data.diagnosticValueLength;
        }
        public boolean originalDiagnosticMatches(boolean expectedPresent, byte[] expectedBytes) {
            data.requireActive(false);
            boolean present = data.exactFrame[data.diagnosticOffset] == 1;
            if (present != expectedPresent) return false;
            if (!present) return expectedBytes == null;
            return expectedBytes != null && expectedBytes.length == data.diagnosticValueLength
                && rangeEquals(expectedBytes, data.exactFrame, data.diagnosticValueOffset, data.diagnosticValueLength);
        }
        public boolean readbackHeaderMatches(byte[] expected) {
            data.requireActive(false); return expected != null && Arrays.equals(data.readbackHeader, expected);
        }
        public boolean readbackPayloadSha256Matches(byte[] expected) {
            data.requireActive(false);
            return expected != null && java.security.MessageDigest.isEqual(data.readbackPayloadSha256, expected);
        }
        public boolean regionIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(false); return expected != null && data.regionIdentity.equals(expected);
        }
        public long beforeByteLength() { data.requireActive(false); return data.before.byteLength(); }
        public long beforeLastWriteFileTimeBits() { data.requireActive(false); return data.before.lastWriteFileTimeBits(); }
        public boolean beforeFileIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(false); return expected != null && data.before.identity().equals(expected);
        }
        public long afterByteLength() { data.requireActive(false); return data.after.byteLength(); }
        public long afterLastWriteFileTimeBits() { data.requireActive(false); return data.after.lastWriteFileTimeBits(); }
        public boolean afterFileIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(false); return expected != null && data.after.identity().equals(expected);
        }
        public boolean forceBindingUuidMatches(UUID expected) { data.requireActive(false); return expected != null && data.force.binding().equals(expected); }
        public boolean forceWorkflowUuidMatches(UUID expected) { data.requireActive(false); return expected != null && data.force.workflow().equals(expected); }
        public boolean forceWriteUuidMatches(UUID expected) { data.requireActive(false); return expected != null && data.force.write().equals(expected); }
        public boolean forceBootUuidMatches(UUID expected) { data.requireActive(false); return expected != null && data.force.boot().equals(expected); }
        public long forceBootGeneration() { data.requireActive(false); return data.force.bootGeneration(); }
        public long forceCustodyGeneration() { data.requireActive(false); return data.force.custodyGeneration(); }
        public boolean forcePayloadDescriptorSha256Matches(byte[] expected) {
            data.requireActive(false); return data.force.payloadDescriptorSha256Matches(expected);
        }
        public boolean forceUndoDescriptorSha256Matches(byte[] expected) {
            data.requireActive(false); return data.force.undoDescriptorSha256Matches(expected);
        }
        public boolean forceMemberCoverageSha256Matches(byte[] expected) {
            data.requireActive(false); return data.force.memberCoverageSha256Matches(expected);
        }
        public boolean forceCanonicalCarrierKeyMatches(byte[] expected) {
            data.requireActive(false); return data.force.carrierKeyMatches(expected);
        }
        public boolean forceResultFactsSha256Matches(byte[] expected) {
            data.requireActive(false); return data.force.resultFactsSha256Matches(expected);
        }
    }

    /** Complete outer receipt DATA; publication DATA remains descriptive evidence only. */
    public static final class DecodedSourceWriteReceiptData {
        private final ReceiptData data;
        private DecodedSourceWriteReceiptData(ReceiptData data) { this.data = data; }
        public UUID writeUuid() { data.requireActive(true); return data.write; }
        public UUID childWorkflowUuid() { data.requireActive(true); return data.workflow; }
        public UUID bindingUuid() { data.requireActive(true); return data.binding; }
        public String saveIdentity() { data.requireActive(true); return data.saveIdentity; }
        public String loaderIdentity() { data.requireActive(true); return data.loaderIdentity; }
        public boolean canonicalCarrierKeyMatches(byte[] expected) {
            data.requireActive(true); return expected != null && Arrays.equals(data.carrierKeyBytes, expected);
        }
        public int slot() { data.requireActive(true); return data.slot; }
        public long writeGeneration() { data.requireActive(true); return data.writeGeneration; }
        public long payloadLength() { data.requireActive(true); return data.payloadLength; }
        public boolean payloadSha256Matches(byte[] expected) {
            data.requireActive(true);
            return expected != null && java.security.MessageDigest.isEqual(data.payloadSha256, expected);
        }
        public DecodedInlineArtifactView payloadArtifact() {
            data.requireActive(true); return new DecodedInlineArtifactView(data, true);
        }
        public UndoKind undoKind() { data.requireActive(true); return data.undoKind; }
        public long undoLength() { data.requireActive(true); return data.undoLength; }
        public boolean undoContentSha256Matches(byte[] expected) {
            data.requireActive(true);
            return data.undoContentSha256 == null ? expected == null
                : expected != null && java.security.MessageDigest.isEqual(data.undoContentSha256, expected);
        }
        public DecodedInlineArtifactView undoArtifact() {
            data.requireActive(true); return new DecodedInlineArtifactView(data, false);
        }
        public DecodedMemberCoverageView memberCoverage() {
            data.requireActive(true); return new DecodedMemberCoverageView(data);
        }
        public int obligationCount() { data.requireActive(true); return data.obligations.size(); }
        public DecodedObligationRevisionView obligationAt(int index) {
            data.requireActive(true);
            data.obligations.valueAt(index);
            return new DecodedObligationRevisionView(data, index);
        }
        public int completedForceStageCount() {
            data.requireActive(true); return data.force.completedStageCount();
        }
        public int completedForceStageCodeAt(int index) {
            data.requireActive(true); return data.force.completedStage(index).stageCode();
        }
        public long completedForceStageEnteredOrdinalAt(int index) {
            data.requireActive(true); return data.force.completedStage(index).enteredOrdinal();
        }
        public long completedForceStageReturnedOrdinalAt(int index) {
            data.requireActive(true); return data.force.completedStage(index).returnedOrdinal();
        }
        public String temporaryCoreBasename() { data.requireActive(true); return data.publication.temporaryBasename(); }
        public String canonicalCoreBasename() { data.requireActive(true); return data.publication.canonicalBasename(); }
        public int publicationStageCount() { data.requireActive(true); return data.publication.completedStageCount(); }
        public int publicationStageCodeAt(int index) {
            data.requireActive(true); return data.publication.completedStageAt(index).stageCode();
        }
        public long publicationStageEnteredOrdinalAt(int index) {
            data.requireActive(true); return data.publication.completedStageAt(index).enteredOrdinal();
        }
        public long publicationStageReturnedOrdinalAt(int index) {
            data.requireActive(true); return data.publication.completedStageAt(index).returnedOrdinal();
        }
        public boolean publicationCoreSha256Matches(byte[] expected) {
            data.requireActive(true); return data.publication.coreSha256Matches(expected);
        }
        public long publicationCoreLength() { data.requireActive(true); return data.publication.coreLength(); }
        public int publicationMoveFlags() { data.requireActive(true); return data.publication.moveFlags(); }
        public boolean publicationAttemptUuidMatches(UUID expected) {
            data.requireActive(true); return expected != null && data.publication.publicationAttemptUuid().equals(expected);
        }
        public boolean publicationParentIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(true); return expected != null && data.publication.parentDirectoryIdentity().equals(expected);
        }
        public boolean publicationTemporaryIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(true); return expected != null && data.publication.temporaryIdentity().equals(expected);
        }
        public boolean publicationCanonicalIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(true); return expected != null && data.publication.canonicalIdentity().equals(expected);
        }
        public boolean readbackTargetRecordMatches(byte[] expected) {
            data.requireActive(true); return expected != null && Arrays.equals(data.readbackCompressedChunk, expected);
        }
        public boolean readbackRootNbtMatches(byte[] expected) {
            data.requireActive(true); return expected != null && Arrays.equals(data.readbackDecodedRootNbt, expected);
        }
        public long readbackDecodedRootNbtLength() {
            data.requireActive(true); return data.readbackDecodedRootNbt.length;
        }
        public long dataWriteCallsReturned() { data.requireActive(true); return data.dataWriteCallsReturned; }
        public long headerWriteCallsReturned() { data.requireActive(true); return data.headerWriteCallsReturned; }
        public long encoderSubmittedBytes() { data.requireActive(true); return data.encoderSubmittedBytes; }
        public long outputCloseCallsReturned() { data.requireActive(true); return data.outputCloseCallsReturned; }
        public boolean originalDiagnosticPresent() {
            data.requireActive(true); return data.exactFrame[data.diagnosticOffset] == 1;
        }
        public int originalDiagnosticLength() {
            data.requireActive(true); return data.diagnosticValueLength;
        }
        public boolean originalDiagnosticMatches(boolean expectedPresent, byte[] expectedBytes) {
            data.requireActive(true);
            boolean present = data.exactFrame[data.diagnosticOffset] == 1;
            if (present != expectedPresent) return false;
            if (!present) return expectedBytes == null;
            return expectedBytes != null && expectedBytes.length == data.diagnosticValueLength
                && rangeEquals(expectedBytes, data.exactFrame, data.diagnosticValueOffset, data.diagnosticValueLength);
        }
        public boolean readbackHeaderMatches(byte[] expected) {
            data.requireActive(true); return expected != null && Arrays.equals(data.readbackHeader, expected);
        }
        public boolean readbackPayloadSha256Matches(byte[] expected) {
            data.requireActive(true);
            return expected != null && java.security.MessageDigest.isEqual(data.readbackPayloadSha256, expected);
        }
        public boolean regionIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(true); return expected != null && data.regionIdentity.equals(expected);
        }
        public long beforeByteLength() { data.requireActive(true); return data.before.byteLength(); }
        public long beforeLastWriteFileTimeBits() { data.requireActive(true); return data.before.lastWriteFileTimeBits(); }
        public boolean beforeFileIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(true); return expected != null && data.before.identity().equals(expected);
        }
        public long afterByteLength() { data.requireActive(true); return data.after.byteLength(); }
        public long afterLastWriteFileTimeBits() { data.requireActive(true); return data.after.lastWriteFileTimeBits(); }
        public boolean afterFileIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            data.requireActive(true); return expected != null && data.after.identity().equals(expected);
        }
        public boolean forceBindingUuidMatches(UUID expected) { data.requireActive(true); return expected != null && data.force.binding().equals(expected); }
        public boolean forceWorkflowUuidMatches(UUID expected) { data.requireActive(true); return expected != null && data.force.workflow().equals(expected); }
        public boolean forceWriteUuidMatches(UUID expected) { data.requireActive(true); return expected != null && data.force.write().equals(expected); }
        public boolean forceBootUuidMatches(UUID expected) { data.requireActive(true); return expected != null && data.force.boot().equals(expected); }
        public long forceBootGeneration() { data.requireActive(true); return data.force.bootGeneration(); }
        public long forceCustodyGeneration() { data.requireActive(true); return data.force.custodyGeneration(); }
        public boolean forcePayloadDescriptorSha256Matches(byte[] expected) {
            data.requireActive(true); return data.force.payloadDescriptorSha256Matches(expected);
        }
        public boolean forceUndoDescriptorSha256Matches(byte[] expected) {
            data.requireActive(true); return data.force.undoDescriptorSha256Matches(expected);
        }
        public boolean forceMemberCoverageSha256Matches(byte[] expected) {
            data.requireActive(true); return data.force.memberCoverageSha256Matches(expected);
        }
        public boolean forceCanonicalCarrierKeyMatches(byte[] expected) {
            data.requireActive(true); return data.force.carrierKeyMatches(expected);
        }
        public boolean forceResultFactsSha256Matches(byte[] expected) {
            data.requireActive(true); return data.force.resultFactsSha256Matches(expected);
        }
    }

    /** Active-checked inline identity projection, with no operation or publication authority. */
    public static final class DecodedInlineArtifactView {
        private final ReceiptData owner;
        private final boolean payload;
        private DecodedInlineArtifactView(ReceiptData owner, boolean payload) {
            this.owner = owner; this.payload = payload;
        }
        private DecodedInlineArtifactIdentityData value() {
            owner.requireActive(owner.outer);
            DecodedInlineArtifactIdentityData value = payload ? owner.payloadArtifact : owner.undoArtifact;
            if (value == null) throw new IllegalStateException("Receipt inline artifact view has expired");
            return value;
        }
        public InlineArtifactRole role() { return value().role(); }
        public UUID writeUuid() { return value().writeUuid(); }
        public String basename() { return value().basename(); }
        public boolean parentDirectoryIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            DecodedInlineArtifactIdentityData value = value();
            return expected != null && value.parentDirectoryIdentity().equals(expected);
        }
        public boolean fileIdentityMatches(WindowsMarkerIo.FileIdentity expected) {
            DecodedInlineArtifactIdentityData value = value();
            return expected != null && value.fileIdentity().equals(expected);
        }
        public long byteLength() { return value().byteLength(); }
        public boolean contentSha256Matches(byte[] expected) {
            DecodedInlineArtifactIdentityData value = value();
            return expected != null && java.security.MessageDigest.isEqual(value.contentSha256, expected);
        }
    }

    /** Active-checked member coverage hashes for comparison with the authentic locked forest snapshot. */
    public static final class DecodedMemberCoverageView {
        private final ReceiptData owner;
        private DecodedMemberCoverageView(ReceiptData owner) { this.owner = owner; }
        public long byteLength() { owner.requireActive(owner.outer); return owner.memberCoverage.byteLength(); }
        public boolean contentSha256Matches(byte[] expected) {
            owner.requireActive(owner.outer); return owner.memberCoverage.contentSha256Matches(expected);
        }
        public boolean forestSha256Matches(byte[] expected) {
            owner.requireActive(owner.outer); return owner.memberCoverage.forestSha256Matches(expected);
        }
        public boolean indexSha256Matches(byte[] expected) {
            owner.requireActive(owner.outer); return owner.memberCoverage.indexSha256Matches(expected);
        }
        public boolean fullForestBytesMatch(byte[] expected) {
            owner.requireActive(owner.outer);
            return expected != null && Arrays.equals(owner.memberCoverage.forestBytes, expected);
        }
        public boolean fullIndexBytesMatch(byte[] expected) {
            owner.requireActive(owner.outer);
            return expected != null && Arrays.equals(owner.memberCoverage.indexBytes, expected);
        }
    }

    /** Active-checked full primary-key/revision projection; ordering and duplicate checks ran in the strict codec. */
    public static final class DecodedObligationRevisionView {
        private final ReceiptData owner;
        private final int index;
        private DecodedObligationRevisionView(ReceiptData owner, int index) {
            this.owner = owner; this.index = index;
        }
        private DecodedObligationRevisionData value() {
            owner.requireActive(owner.outer);
            if (owner.obligations == null) throw new IllegalStateException("Receipt obligation view has expired");
            return owner.obligations.valueAt(index);
        }
        public UUID bindingUuid() { return value().bindingUuid(); }
        public UUID unitUuid() { return value().unitUuid(); }
        public UUID forestUuid() { return value().forestUuid(); }
        public String purpose() { return value().purpose(); }
        public long revision() { return value().revision(); }
        public boolean carrierKeyHashMatches(byte[] expected) {
            return value().carrierKeyHashMatches(expected);
        }
        public boolean carrierKeyMatches(byte[] expected) {
            return value().carrierKeyMatches(expected);
        }
    }

    private static final class ReceiptData {
        private SourceLimitProfile profile;
        private SourceForestAllocationBudget allocation;
        private final Thread consumerThread;
        private final boolean outer;
        private byte[] exactFrame, carrierKeyBytes, payloadSha256, undoContentSha256;
        private final UUID write, workflow, binding;
        private String saveIdentity, loaderIdentity;
        private MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier;
        private WindowsMarkerIo.FileIdentity regionIdentity;
        private final int slot;
        private final long writeGeneration, payloadLength, undoLength;
        private DecodedInlineArtifactIdentityData payloadArtifact, undoArtifact;
        private final UndoKind undoKind;
        private DecodedMemberCoverageData memberCoverage;
        private DecodedObligationRevisionsData obligations;
        private final long dataWriteCallsReturned, headerWriteCallsReturned, encoderSubmittedBytes, outputCloseCallsReturned;
        private RegionFileStampData before, after;
        private byte[] readbackHeader, readbackCompressedChunk, readbackDecodedRootNbt, readbackPayloadSha256;
        private String forceProtocol;
        private byte[] forceEvidence;
        private ManagedRecallSourceWriteOwner.DecodedForceData force;
        private DecodedPublicationData publication;
        private final int prefixEnd, diagnosticOffset, diagnosticLength, diagnosticValueOffset, diagnosticValueLength;
        private boolean active = true;

        private ReceiptData(SourceLimitProfile profile, SourceForestAllocationBudget allocation, boolean outer,
                byte[] exactFrame, UUID write, UUID workflow, UUID binding, String saveIdentity, String loaderIdentity,
                byte[] carrierKeyBytes, MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier,
                WindowsMarkerIo.FileIdentity regionIdentity, int slot, long writeGeneration, long payloadLength,
                byte[] payloadSha256, DecodedInlineArtifactIdentityData payloadArtifact, UndoKind undoKind,
                long undoLength, byte[] undoContentSha256, DecodedInlineArtifactIdentityData undoArtifact,
                DecodedMemberCoverageData memberCoverage, DecodedObligationRevisionsData obligations,
                long dataWriteCallsReturned, long headerWriteCallsReturned, long encoderSubmittedBytes,
                long outputCloseCallsReturned, RegionFileStampData before, RegionFileStampData after,
                byte[] readbackHeader, byte[] readbackCompressedChunk, byte[] readbackDecodedRootNbt,
                byte[] readbackPayloadSha256, String forceProtocol, byte[] forceEvidence,
                ManagedRecallSourceWriteOwner.DecodedForceData force, DecodedPublicationData publication,
                int prefixEnd, int diagnosticOffset, int diagnosticLength, int diagnosticValueOffset,
                int diagnosticValueLength) {
            this.profile = profile; this.allocation = allocation; this.consumerThread = Thread.currentThread();
            this.outer = outer; this.exactFrame = exactFrame; this.write = write; this.workflow = workflow;
            this.binding = binding; this.saveIdentity = saveIdentity; this.loaderIdentity = loaderIdentity;
            this.carrierKeyBytes = carrierKeyBytes; this.carrier = carrier; this.regionIdentity = regionIdentity;
            this.slot = slot; this.writeGeneration = writeGeneration; this.payloadLength = payloadLength;
            this.payloadSha256 = payloadSha256; this.payloadArtifact = payloadArtifact; this.undoKind = undoKind;
            this.undoLength = undoLength; this.undoContentSha256 = undoContentSha256; this.undoArtifact = undoArtifact;
            this.memberCoverage = memberCoverage; this.obligations = obligations;
            this.dataWriteCallsReturned = dataWriteCallsReturned; this.headerWriteCallsReturned = headerWriteCallsReturned;
            this.encoderSubmittedBytes = encoderSubmittedBytes; this.outputCloseCallsReturned = outputCloseCallsReturned;
            this.before = before; this.after = after; this.readbackHeader = readbackHeader;
            this.readbackCompressedChunk = readbackCompressedChunk; this.readbackDecodedRootNbt = readbackDecodedRootNbt;
            this.readbackPayloadSha256 = readbackPayloadSha256; this.forceProtocol = forceProtocol;
            this.forceEvidence = forceEvidence; this.force = force; this.publication = publication;
            this.prefixEnd = prefixEnd; this.diagnosticOffset = diagnosticOffset;
            this.diagnosticLength = diagnosticLength; this.diagnosticValueOffset = diagnosticValueOffset;
            this.diagnosticValueLength = diagnosticValueLength;
        }

        private void requireActive(boolean requireOuter) {
            if (!active || consumerThread != Thread.currentThread() || profile == null || allocation == null
                || allocation.exactProfile() != profile || outer != requireOuter || (outer && publication == null))
                throw new IllegalStateException("Decoded SOURCE receipt DATA is valid only inside its exact lexical consumer/profile/allocation");
        }

        private void expire() {
            active = false;
            profile = null; allocation = null;
            exactFrame = carrierKeyBytes = payloadSha256 = undoContentSha256 = null;
            saveIdentity = loaderIdentity = null; carrier = null; regionIdentity = null;
            payloadArtifact = undoArtifact = null; memberCoverage = null; obligations = null;
            before = after = null;
            readbackHeader = readbackCompressedChunk = readbackDecodedRootNbt = readbackPayloadSha256 = null;
            forceProtocol = null; forceEvidence = null; force = null; publication = null;
        }
    }

    /** Strict graph-preserving failure diagnostic DATA; it never constructs or reflects into Throwable. */
    public static final class DecodedFailureDiagnosticData {
        private final SourceLimitProfile profile;
        private final byte[] exactFrame, frameSha256;
        private final UUID diagnostic, binding, workflow, write, producerAttempt, boot;
        private final long bootGeneration;
        private final int producer, failedStage, effectKnowledge, resourceKnowledge, primaryNode;
        private final List<FailureStageProgressData> stages;
        private final List<FailureThrowableNodeData> nodes;

        private DecodedFailureDiagnosticData(SourceLimitProfile profile, byte[] exactFrame, byte[] frameSha256,
                UUID diagnostic, UUID binding, UUID workflow, UUID write, UUID producerAttempt, UUID boot,
                long bootGeneration, int producer, int failedStage, int effectKnowledge, int resourceKnowledge,
                List<FailureStageProgressData> stages, int primaryNode, List<FailureThrowableNodeData> nodes) {
            this.profile = profile; this.exactFrame = exactFrame; this.frameSha256 = frameSha256;
            this.diagnostic = diagnostic; this.binding = binding; this.workflow = workflow; this.write = write;
            this.producerAttempt = producerAttempt; this.boot = boot; this.bootGeneration = bootGeneration;
            this.producer = producer; this.failedStage = failedStage; this.effectKnowledge = effectKnowledge;
            this.resourceKnowledge = resourceKnowledge; this.stages = stages; this.primaryNode = primaryNode;
            this.nodes = nodes;
        }

        public UUID diagnosticUuid() { return diagnostic; }
        public UUID bindingUuid() { return binding; }
        public UUID childWorkflowUuid() { return workflow; }
        public UUID writeUuid() { return write; }
        public UUID producerAttemptUuid() { return producerAttempt; }
        public UUID bootUuid() { return boot; }
        public long bootGeneration() { return bootGeneration; }
        public int producer() { return producer; }
        public int failedStage() { return failedStage; }
        public int effectKnowledge() { return effectKnowledge; }
        public int resourceKnowledge() { return resourceKnowledge; }
        public int stageCount() { return stages.size(); }
        public FailureStageProgressData stageAt(int index) { return stages.get(index); }
        public int primaryNode() { return primaryNode; }
        public int throwableNodeCount() { return nodes.size(); }
        public FailureThrowableNodeData throwableNodeAt(int index) { return nodes.get(index); }
        public boolean contentSha256Matches(byte[] expected) { return java.security.MessageDigest.isEqual(frameSha256, expected); }
        public byte[] copyContentSha256ForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(frameSha256, allocation, profile, "retained failure diagnostic SHA-256");
        }
        public byte[] copyFrameForRetention(SourceForestAllocationBudget.Account allocation) throws IOException {
            return copyForRetention(exactFrame, allocation, profile, "retained complete failure diagnostic frame");
        }
    }

    public record FailureStageProgressData(int stageCode, int state, long enteredOrdinal,
            long terminalOrdinal, Integer failureNode) { }

    /** Stack-frame DATA preserves only the specified class/method/line/native fields. */
    public record FailureStackFrameData(String declaringClass, String methodName, int lineNumber, boolean nativeMethod) { }

    /** Native error DATA contains only the exact API, operation UUID, and observed Win32 bits. */
    public record FailureNativeErrorData(String api, UUID operation, int win32ErrorBits) { }

    public static final class FailureThrowableNodeData {
        private final int nodeId;
        private final String exceptionClass;
        private final int messagePolicy;
        private final Integer causeNode;
        private final List<Integer> suppressedNodes;
        private final List<FailureStackFrameData> stack;
        private final FailureNativeErrorData nativeError;

        private FailureThrowableNodeData(int nodeId, String exceptionClass, int messagePolicy, Integer causeNode,
                List<Integer> suppressedNodes, List<FailureStackFrameData> stack,
                FailureNativeErrorData nativeError) {
            this.nodeId = nodeId; this.exceptionClass = exceptionClass; this.messagePolicy = messagePolicy;
            this.causeNode = causeNode; this.suppressedNodes = suppressedNodes; this.stack = stack;
            this.nativeError = nativeError;
        }

        public int nodeId() { return nodeId; }
        public String exceptionClass() { return exceptionClass; }
        public int messagePolicy() { return messagePolicy; }
        public Integer causeNode() { return causeNode; }
        public int suppressedCount() { return suppressedNodes.size(); }
        public int suppressedNodeAt(int index) { return suppressedNodes.get(index); }
        public int stackFrameCount() { return stack.size(); }
        public FailureStackFrameData stackFrameAt(int index) { return stack.get(index); }
        public boolean hasNativeError() { return nativeError != null; }
        public FailureNativeErrorData nativeErrorData() { return nativeError; }
    }

    /** Exact domainless ArtifactIdentity leaf embedded in the outer receipt/publication wire. */
    public static final class DecodedInlineArtifactIdentityData {
        private final InlineArtifactRole role;
        private final UUID writeUuid;
        private final String basename;
        private final WindowsMarkerIo.FileIdentity parentDirectoryIdentity;
        private final WindowsMarkerIo.FileIdentity fileIdentity;
        private final long byteLength;
        private final byte[] contentSha256;

        private DecodedInlineArtifactIdentityData(InlineArtifactRole role, UUID writeUuid, String basename,
                WindowsMarkerIo.FileIdentity parentDirectoryIdentity, WindowsMarkerIo.FileIdentity fileIdentity,
                long byteLength, byte[] contentSha256) {
            this.role = Objects.requireNonNull(role);
            this.writeUuid = requireUuid(writeUuid, "inline artifact write UUID");
            this.basename = Objects.requireNonNull(basename);
            this.parentDirectoryIdentity = Objects.requireNonNull(parentDirectoryIdentity);
            this.fileIdentity = Objects.requireNonNull(fileIdentity);
            if (byteLength <= 0L) throw new IllegalArgumentException("Inline artifact length must be positive");
            this.byteLength = byteLength;
            this.contentSha256 = exactSha256(contentSha256);
            if (!basename.equals(inlineArtifactBasename(role, writeUuid)))
                throw new IllegalArgumentException("Inline artifact basename does not match its fixed role and write UUID");
            if (parentDirectoryIdentity.volumeSerialNumber() != fileIdentity.volumeSerialNumber())
                throw new IllegalArgumentException("Inline artifact file and parent are not on the same volume");
            if (allZero(fileIdentity.fileId128()))
                throw new IllegalArgumentException("Inline artifact file identity is unobservable");
        }

        public InlineArtifactRole role() { return role; }
        public UUID writeUuid() { return writeUuid; }
        public String basename() { return basename; }
        public WindowsMarkerIo.FileIdentity parentDirectoryIdentity() { return parentDirectoryIdentity; }
        public WindowsMarkerIo.FileIdentity fileIdentity() { return fileIdentity; }
        public long byteLength() { return byteLength; }
        public byte[] contentSha256() { return contentSha256.clone(); }
    }

    /** Immutable decoded DATA. Its private constructor and callbacks do not issue publication authority. */
    public static final class DecodedArtifactIdentityData {
        private final ArtifactKind kind;
        private final UUID writeUuid;
        private final String basename;
        private final WindowsMarkerIo.FileIdentity parentDirectoryIdentity;
        private final WindowsMarkerIo.FileIdentity fileIdentity;
        private final long byteLength;
        private final byte[] contentSha256;

        private DecodedArtifactIdentityData(ArtifactKind kind, UUID writeUuid, String basename,
                WindowsMarkerIo.FileIdentity parentDirectoryIdentity, WindowsMarkerIo.FileIdentity fileIdentity,
                long byteLength, byte[] contentSha256) {
            this.kind = Objects.requireNonNull(kind);
            this.writeUuid = requireUuid(writeUuid, "artifact write UUID");
            this.basename = Objects.requireNonNull(basename);
            this.parentDirectoryIdentity = Objects.requireNonNull(parentDirectoryIdentity);
            this.fileIdentity = Objects.requireNonNull(fileIdentity);
            if (byteLength <= 0) throw new IllegalArgumentException("Source artifact length must be positive");
            this.byteLength = byteLength;
            this.contentSha256 = exactSha256(contentSha256);
            if (!basename.equals(sourceArtifactBasename(kind, writeUuid)))
                throw new IllegalArgumentException("Source artifact basename does not match its fixed role and write UUID");
            if (parentDirectoryIdentity.volumeSerialNumber() != fileIdentity.volumeSerialNumber()
                || allZero(parentDirectoryIdentity.fileId128()) || allZero(fileIdentity.fileId128()))
                throw new IllegalArgumentException("Source artifact FileIdentity must be observable and on its exact parent volume");
        }

        public ArtifactKind kind() { return kind; }
        public UUID writeUuid() { return writeUuid; }
        public String basename() { return basename; }
        public WindowsMarkerIo.FileIdentity parentDirectoryIdentity() { return parentDirectoryIdentity; }
        public WindowsMarkerIo.FileIdentity fileIdentity() { return fileIdentity; }
        public long byteLength() { return byteLength; }
        public byte[] contentSha256() { return contentSha256.clone(); }
    }






    /**
     * Strictly emits the fixed artifact identity DATA. T is BE-i32-length strict UTF-8,
     * FileIdentity is raw BE-i64 volume plus 16 file-ID bytes, P is positive BE-i64, H is 32 bytes.
     */
    public static void encodeArtifactIdentityData(DecodedArtifactIdentityData data, DataOutputStream output,
            SourceLimitProfile profile, SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(data, "decoded artifact identity DATA");
        Objects.requireNonNull(output, "artifact identity output");
        requireAllocation(profile, allocation);
        validateArtifactData(data, profile);
        long work = identityWorkCharge(data.basename.length());
        allocation.reserveTemporary(work, "source artifact identity encode workspace BEFORE allocation");
        Throwable primary = null;
        try {
            writeArtifactIdentityData(output, data);
        } catch (Throwable failure) {
            primary = failure;
        } finally {
            releasePreserving(allocation, work, "source artifact identity encode workspace", primary);
        }
        rethrow(primary, "encode source artifact identity DATA");
    }




    private static boolean isLowercaseSha256Text(String value) {
        if (value == null || value.length() != SHA256_BYTES * 2) return false;
        for (int index = 0; index < value.length(); index++)
            if (lowercaseHex(value.charAt(index)) < 0) return false;
        return true;
    }


















    private static void addSuppressedPreserving(Throwable primary, Throwable secondary) {
        if (primary != null && secondary != null && primary != secondary) {
            try { primary.addSuppressed(secondary); }
            catch (Throwable ignored) { /* The original failure remains authoritative if suppression itself fails. */ }
        }
    }

    private static long stageBit(ArtifactPublicationStage stage) {
        int ordinal = Objects.requireNonNull(stage).ordinal();
        if (ordinal >= Long.SIZE) throw new IllegalArgumentException("Too many SOURCE artifact publication stages");
        return 1L << ordinal;
    }

    public static void writePayloadDescriptorData(DataOutputStream output, UUID binding, UUID workflow,
            UUID write, UUID loaderObservation, long writeGeneration, String saveIdentity, String loaderIdentity,
            String regionIdentity, byte[] exactCarrierKey, RegionFileStampData before,
            int regionX, int regionZ, int slot, long payloadLength, byte[] payloadSha256,
            byte[] undoDescriptorSha256, byte[] memberCoverageSha256, byte[] obligationRevisionsSha256,
            SourceLimitProfile profile, SourceForestAllocationBudget.Account allocation) throws IOException {
        Objects.requireNonNull(output, "payload descriptor output");
        requireDescriptorInputs(binding, workflow, write, loaderObservation, writeGeneration, saveIdentity,
            loaderIdentity, regionIdentity, exactCarrierKey, before, regionX, regionZ, slot, profile, allocation);
        requireHash(payloadSha256, "payload SHA-256");
        requireHash(undoDescriptorSha256, "undo descriptor SHA-256");
        requireHash(memberCoverageSha256, "member coverage SHA-256");
        requireHash(obligationRevisionsSha256, "obligation revisions SHA-256");
        if (payloadLength <= 0L || payloadLength > profile.maxNativeArtifactBytes()
            || payloadLength > profile.maxRetainedBytes())
            throw new IOException("Payload descriptor length is outside the active native/retained profile");
        validateCarrierFrameForSlot(exactCarrierKey, profile, allocation, saveIdentity, loaderIdentity,
            regionIdentity, regionX, regionZ, slot);
        validateRegionStamp(before, profile);
        long workspace = descriptorWriterCharge(saveIdentity, loaderIdentity);
        allocation.reserveTemporary(workspace, "payload descriptor UTF-8/hash writer workspace BEFORE allocation");
        Throwable primary = null;
        try {
            writeText(output, PAYLOAD_DESCRIPTOR_DOMAIN);
            writeUuid(output, binding); writeUuid(output, workflow); writeUuid(output, write);
            writeUuid(output, loaderObservation); output.writeLong(writeGeneration);
            writeText(output, saveIdentity); writeText(output, loaderIdentity);
            writeCarrierFrame(output, exactCarrierKey);
            output.write(profile.sha256());
            writeRegionStamp(output, before);
            output.writeInt(regionX); output.writeInt(regionZ); output.writeInt(slot);
            output.writeLong(payloadLength); output.write(payloadSha256);
            output.write(undoDescriptorSha256); output.write(memberCoverageSha256);
            output.write(obligationRevisionsSha256);
        } catch (Throwable failure) {
            primary = failure;
        } finally {
            releasePreserving(allocation, workspace, "payload descriptor writer workspace", primary);
        }
        rethrow(primary, "write payload descriptor DATA");
    }

    /**
     * Strictly consumes the complete frozen payload descriptor as synchronous DATA. The input frame
     * is borrowed and must already be charged to the supplied original account.
     */
    public static void consumePayloadDescriptorData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget allocation, Consumer<DecodedPayloadDescriptorData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous payload descriptor DATA consumer");
        requireDataFrame(exactFrame, exactProfile, allocation, "payload descriptor");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(allocation, exactProfile)) {
            work.charge(1_024L, "payload descriptor parser and DATA wrapper workspace");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            String domain = readAccountedText(input, maxTextFrame(exactProfile), work, "payload descriptor domain");
            if (!PAYLOAD_DESCRIPTOR_DOMAIN.equals(domain)) throw new IOException("Wrong SOURCE payload descriptor domain");
            UUID binding = readUuid(input, "payload binding UUID");
            UUID workflow = readUuid(input, "payload child workflow UUID");
            UUID write = readUuid(input, "payload write UUID");
            UUID loaderObservation = readUuid(input, "payload loader lifetime observation UUID");
            long generation = readNonnegative(input, "payload write generation");
            String save = readWireText(input, exactProfile, work, "payload save identity");
            String loader = readWireText(input, exactProfile, work, "payload loader identity");
            byte[] carrierBytes = readProfileFrame(input, exactProfile, work, "payload canonical CARRIER key");
            byte[] profileSha = readAccountedHash(input, work, "payload source profile SHA-256");
            work.charge(64L, "payload profile digest comparison copies");
            if (!java.security.MessageDigest.isEqual(profileSha, exactProfile.sha256()))
                throw new IOException("Payload descriptor profile hash differs from the exact original profile");
            RegionFileStampData before = readRegionStamp(input, exactProfile, work, "payload original RegionFileStamp");
            int regionX = input.readInt(), regionZ = input.readInt(), slot = input.readInt();
            requireSlotCoordinates(regionX, regionZ, slot);
            long payloadLength = readPositive(input, "payload length");
            if (payloadLength > exactProfile.maxNativeArtifactBytes() || payloadLength > exactProfile.maxRetainedBytes())
                throw new IOException("Payload descriptor length exceeds the exact native/retained profile");
            byte[] payloadSha = readAccountedHash(input, work, "payload body SHA-256");
            byte[] undoSha = readAccountedHash(input, work, "undo descriptor SHA-256");
            byte[] memberSha = readAccountedHash(input, work, "member coverage SHA-256");
            byte[] obligationsSha = readAccountedHash(input, work, "obligation revisions SHA-256");
            requireEnd(input, "payload descriptor");
            validateRegionStamp(before, exactProfile);
            byte[] descriptorSha = digestSha256(exactFrame, work, "payload descriptor SHA-256");
            MySqlRecoveryResourceKey.consumeCarrierKeyData(carrierBytes, exactProfile, allocation, carrier -> {
                try { validateCarrierFields(carrier, save, loader, carrier.regionIdentity(), regionX, regionZ, slot); }
                catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
                consumer.accept(new DecodedPayloadDescriptorData(exactProfile, binding, workflow, write, loaderObservation,
                    generation, save, loader, carrier, profileSha, before, regionX, regionZ, slot,
                    payloadLength, payloadSha, undoSha, memberSha, obligationsSha, descriptorSha));
            });
        } catch (java.io.UncheckedIOException failure) {
            throw unwrapCheckedIoPreservingSuppressed(failure);
        }
    }

    /** Emits a strict FULL_CHUNK undo descriptor; body bytes are written separately from this descriptor. */
    public static void writeFullUndoDescriptorData(DataOutputStream output, UUID binding, UUID workflow,
            UUID write, UUID loaderObservation, long writeGeneration, String saveIdentity, String loaderIdentity,
            String regionIdentity, byte[] exactCarrierKey, RegionFileStampData before,
            int regionX, int regionZ, int slot, byte[] originalHeader, int sectorOffset, int spanSectors,
            int timestampBits, int compressionId, long compressedBodyLength, byte[] compressedBodySha256,
            long decodedRootLength, byte[] decodedRootSha256, SourceLimitProfile profile,
            SourceForestAllocationBudget.Account allocation) throws IOException {
        Objects.requireNonNull(output, "FULL_CHUNK undo descriptor output");
        requireDescriptorInputs(binding, workflow, write, loaderObservation, writeGeneration, saveIdentity,
            loaderIdentity, regionIdentity, exactCarrierKey, before, regionX, regionZ, slot, profile, allocation);
        requireHash(compressedBodySha256, "compressed undo body SHA-256");
        requireHash(decodedRootSha256, "decoded undo root SHA-256");
        if (originalHeader == null || originalHeader.length != 8192)
            throw new IOException("FULL_CHUNK undo requires the exact original 8192-byte region header");
        if (compressedBodyLength <= 0L || compressedBodyLength > profile.maxChunkBytes()
            || compressedBodyLength > profile.maxRetainedBytes())
            throw new IOException("FULL_CHUNK compressed body length is outside the active profile");
        if (decodedRootLength <= 0L || decodedRootLength > profile.maxDecompressedBytes()
            || decodedRootLength > profile.maxRetainedBytes())
            throw new IOException("FULL_CHUNK decoded root length is outside the active profile");
        if (compressionId != 1 && compressionId != 2)
            throw new IOException("FULL_CHUNK compression id must be the original supported gzip/zlib id");
        validateCarrierFrameForSlot(exactCarrierKey, profile, allocation, saveIdentity, loaderIdentity,
            regionIdentity, regionX, regionZ, slot);
        validateRegionStamp(before, profile);
        validateFullUndoHeader(originalHeader, before, slot, sectorOffset, spanSectors,
            timestampBits, compressedBodyLength);
        long workspace = descriptorWriterCharge(saveIdentity, loaderIdentity);
        allocation.reserveTemporary(workspace, "FULL_CHUNK descriptor/header writer workspace BEFORE allocation");
        Throwable primary = null;
        try {
            writeText(output, FULL_UNDO_DESCRIPTOR_DOMAIN);
            writeUuid(output, binding); writeUuid(output, workflow); writeUuid(output, write);
            writeUuid(output, loaderObservation); output.writeLong(writeGeneration);
            writeText(output, saveIdentity); writeText(output, loaderIdentity);
            writeCarrierFrame(output, exactCarrierKey); output.write(profile.sha256());
            writeRegionStamp(output, before);
            output.writeInt(regionX); output.writeInt(regionZ); output.writeInt(slot);
            output.write(originalHeader);
            output.writeInt(sectorOffset); output.writeInt(spanSectors); output.writeInt(timestampBits);
            output.writeByte(compressionId); output.writeLong(compressedBodyLength); output.write(compressedBodySha256);
            output.writeLong(decodedRootLength); output.write(decodedRootSha256);
        } catch (Throwable failure) {
            primary = failure;
        } finally {
            releasePreserving(allocation, workspace, "FULL_CHUNK descriptor writer workspace", primary);
        }
        rethrow(primary, "write FULL_CHUNK undo descriptor DATA");
    }

    /** Strictly consumes either frozen undo descriptor without producing a restore capability. */
    public static void consumeUndoDescriptorData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget allocation, Consumer<DecodedUndoDescriptorData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous undo descriptor DATA consumer");
        requireDataFrame(exactFrame, exactProfile, allocation, "undo descriptor");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(allocation, exactProfile)) {
            work.charge(1_024L, "undo descriptor parser and DATA wrapper workspace");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            String domain = readAccountedText(input, maxTextFrame(exactProfile), work, "undo descriptor domain");
            boolean full = FULL_UNDO_DESCRIPTOR_DOMAIN.equals(domain);
            if (!full && !ABSENT_UNDO_DESCRIPTOR_DOMAIN.equals(domain))
                throw new IOException("Wrong SOURCE undo descriptor domain");
            UUID binding = readUuid(input, "undo binding UUID");
            UUID workflow = readUuid(input, "undo child workflow UUID");
            UUID write = readUuid(input, "undo write UUID");
            UUID loaderObservation = readUuid(input, "undo loader lifetime observation UUID");
            long generation = readNonnegative(input, "undo write generation");
            String save = readWireText(input, exactProfile, work, "undo save identity");
            String loader = readWireText(input, exactProfile, work, "undo loader identity");
            byte[] carrierBytes = readProfileFrame(input, exactProfile, work, "undo canonical CARRIER key");
            byte[] profileSha = full ? readAccountedHash(input, work, "undo source profile SHA-256") : null;
            if (full) work.charge(64L, "undo profile digest comparison copies");
            if (full && !java.security.MessageDigest.isEqual(profileSha, exactProfile.sha256()))
                throw new IOException("FULL_CHUNK descriptor profile hash differs from the exact original profile");
            work.charge(192L, "undo RegionFileStamp identity objects BEFORE allocation");
            WindowsMarkerIo.FileIdentity identity = readIdentity(input);
            long regionLength = readNonnegative(input, "undo original region length");
            long lastWrite = input.readLong();
            RegionFileStampData before;
            try { before = new RegionFileStampData(identity, regionLength, lastWrite); }
            catch (IllegalArgumentException malformed) { throw new IOException("Invalid undo RegionFileStamp", malformed); }
            validateRegionStamp(before, exactProfile);
            int regionX = input.readInt(), regionZ = input.readInt(), slot = input.readInt();
            requireSlotCoordinates(regionX, regionZ, slot);
            byte[] header = readAccountedFixed(input, full ? 8192 : 8192, work, "undo original region header");
            int offset = 0, span = 0, timestamp = 0, compression = 0;
            long bodyLength = 0L, rootLength = 0L;
            byte[] bodySha = null, rootSha = null;
            if (full) {
                offset = input.readInt(); span = input.readInt(); timestamp = input.readInt();
                compression = input.readUnsignedByte(); bodyLength = readPositive(input, "FULL_CHUNK compressed body length");
                bodySha = readAccountedHash(input, work, "FULL_CHUNK compressed body SHA-256");
                rootLength = readPositive(input, "FULL_CHUNK decoded root length");
                rootSha = readAccountedHash(input, work, "FULL_CHUNK decoded root SHA-256");
                if (bodyLength > exactProfile.maxChunkBytes() || bodyLength > exactProfile.maxRetainedBytes()
                    || rootLength > exactProfile.maxDecompressedBytes() || rootLength > exactProfile.maxRetainedBytes())
                    throw new IOException("FULL_CHUNK descriptor body/root length exceeds the exact profile");
                if (compression != 1 && compression != 2)
                    throw new IOException("FULL_CHUNK descriptor compression id must be 1 or 2");
                validateFullUndoHeader(header, before, slot, offset, span, timestamp, bodyLength);
            } else {
                int word = headerWord(header, slot);
                if (word != 0) throw new IOException("SLOT_ABSENT descriptor original header does not contain four zero location bytes");
            }
            requireEnd(input, "undo descriptor");
            byte[] descriptorSha = digestSha256(exactFrame, work, "undo descriptor SHA-256");
            final UndoKind kind = full ? UndoKind.FULL_CHUNK : UndoKind.SLOT_ABSENT;
            final byte[] exactProfileSha = profileSha;
            final byte[] exactHeader = header;
            final int exactOffset = offset, exactSpan = span, exactTimestamp = timestamp, exactCompression = compression;
            final long exactBodyLength = bodyLength, exactRootLength = rootLength;
            final byte[] exactBodySha = bodySha, exactRootSha = rootSha;
            MySqlRecoveryResourceKey.consumeCarrierKeyData(carrierBytes, exactProfile, allocation, carrier -> {
                try { validateCarrierFields(carrier, save, loader, carrier.regionIdentity(), regionX, regionZ, slot); }
                catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
                consumer.accept(new DecodedUndoDescriptorData(exactProfile, kind, binding, workflow, write, loaderObservation,
                    generation, save, loader, carrier, exactProfileSha, before, regionX, regionZ, slot, exactHeader,
                    exactOffset, exactSpan, exactTimestamp, exactCompression, exactBodyLength, exactBodySha,
                    exactRootLength, exactRootSha, descriptorSha));
            });
        } catch (java.io.UncheckedIOException failure) {
            throw unwrapCheckedIoPreservingSuppressed(failure);
        }
    }

    /**
     * Strictly consumes B(forest),H(forest SHA),B(index),H(index SHA) against a caller-supplied
     * expectation from the same locked/receipt-history snapshot. The expectation is never derived
     * from the bytes under verification.
     */
    public static void consumeMemberCoverageData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget allocation,
            com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation exactExpectation,
            Consumer<DecodedMemberCoverageData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous member coverage DATA consumer");
        Objects.requireNonNull(exactExpectation, "external same-snapshot full forest expectation");
        requireDataFrame(exactFrame, exactProfile, allocation, "SOURCE member coverage");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(allocation, exactProfile)) {
            work.charge(1_024L, "member coverage parser and graph DATA wrapper workspace");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            byte[] forestBytes = readProfileFrame(input, exactProfile, work, "complete member forest bytes");
            byte[] forestSha = readAccountedHash(input, work, "complete member forest SHA-256");
            byte[] indexBytes = readProfileFrame(input, exactProfile, work, "complete membership index bytes");
            byte[] indexSha = readAccountedHash(input, work, "complete membership index SHA-256");
            requireEnd(input, "SOURCE member coverage");
            byte[] frameSha = digestSha256(exactFrame, work, "SOURCE member coverage frame SHA-256");
            com.wi.floatingpod.server.source.SourceForestMembershipCodec.consumeVerifiedGraph(
                forestBytes, forestSha, indexBytes, indexSha, exactProfile, allocation, exactExpectation,
                (forest, index) -> consumer.accept(new DecodedMemberCoverageData(exactProfile, exactFrame, forestBytes,
                    forestSha, indexBytes, indexSha, frameSha, forest, index)));
        } catch (java.io.UncheckedIOException failure) {
            throw unwrapCheckedIoPreservingSuppressed(failure);
        }
    }

    /**
     * Strictly consumes the original receipt's count-plus-B(item) obligation vector. Every item is
     * checked against the exact binding and full canonical CARRIER key supplied by the locked caller.
     */
    public static void consumeObligationRevisionsData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget allocation, UUID exactBinding, byte[] exactCarrierKey,
            Consumer<DecodedObligationRevisionsData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous obligation revisions DATA consumer");
        requireDataFrame(exactFrame, exactProfile, allocation, "SOURCE obligation revisions");
        requireUuid(exactBinding, "exact obligation binding UUID");
        Objects.requireNonNull(exactCarrierKey, "exact full locked CARRIER key");
        if (exactCarrierKey.length == 0 || exactCarrierKey.length > exactProfile.maxSqlPacketBytes()
            || exactCarrierKey.length > exactProfile.maxRetainedBytes())
            throw new IOException("Exact obligation CARRIER key exceeds the source profile");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(allocation, exactProfile)) {
            work.charge(512L, "obligation list parser workspace");
            validateCanonicalCarrierKey(exactCarrierKey, exactProfile, allocation);
            byte[] expectedKeyHash = digestSha256(exactCarrierKey, work, "exact locked obligation CARRIER key hash");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            int count = input.readInt();
            if (count < 0 || (long) count > exactProfile.maxMembers()
                || count > input.available() / 98)
                throw new IOException("Obligation revisions count is negative, over-profile, or impossible for the remaining L frame");
            work.charge(Math.addExact(512L, Math.multiplyExact((long) count, 320L)),
                "obligation revision tuple/vector structures BEFORE allocation");
            List<DecodedObligationRevisionData> values = new ArrayList<>(count);
            DecodedObligationRevisionData previous = null;
            for (int index = 0; index < count; index++) {
                byte[] itemFrame = readProfileFrame(input, exactProfile, work, "B-framed obligation revision item");
                DataInputStream item = new DataInputStream(new ByteArrayInputStream(itemFrame));
                UUID binding = readUuid(item, "obligation binding UUID");
                UUID unit = readUuid(item, "obligation unit UUID");
                UUID forest = readUuid(item, "obligation forest UUID");
                byte[] keyHash = readAccountedHash(item, work, "obligation CARRIER key hash");
                byte[] fullKey = readProfileFrame(item, exactProfile, work, "full obligation CARRIER key");
                String purpose = readWireText(item, exactProfile, work, "obligation purpose");
                long revision = readNonnegative(item, "obligation revision");
                requireEnd(item, "B-framed obligation revision item");
                if (!binding.equals(exactBinding) || !Arrays.equals(fullKey, exactCarrierKey)
                    || !java.security.MessageDigest.isEqual(keyHash, expectedKeyHash))
                    throw new IOException("Obligation revision binding/full CARRIER key/hash differs from its locked scope");
                if (!isSourceObligationPurpose(purpose))
                    throw new IOException("Unknown frozen SOURCE obligation purpose");
                DecodedObligationRevisionData value = new DecodedObligationRevisionData(exactProfile,
                    binding, unit, forest, keyHash, fullKey, purpose, revision);
                if (previous != null) {
                    int order = compareObligationRevisionData(previous, value);
                    if (order >= 0) throw new IOException("Obligation revision primary-key tuples are duplicate or not in canonical order");
                    if (compareUnsignedBytes(previous.carrierKeyHash, value.carrierKeyHash) == 0
                        && !Arrays.equals(previous.carrierKey, value.carrierKey))
                        throw new IOException("Different full CARRIER keys collide on an obligation revision hash");
                }
                values.add(value);
                previous = value;
            }
            requireEnd(input, "SOURCE obligation revisions L field");
            byte[] frameSha = digestSha256(exactFrame, work, "SOURCE obligation revisions SHA-256");
            consumer.accept(new DecodedObligationRevisionsData(exactProfile, exactFrame, frameSha, values));
        } catch (java.io.UncheckedIOException failure) {
            throw unwrapCheckedIoPreservingSuppressed(failure);
        }
    }

    /**
     * Strictly consumes the NativeResultPublication leaf and its exact 12.8.7 evidence. This returns
     * DATA only; a decoded publication never becomes a Store-issued result/core publication value.
     */
    public static void consumePublicationData(byte[] exactFrame, UUID exactWrite,
            SourceLimitProfile exactProfile, SourceForestAllocationBudget allocation,
            Consumer<DecodedPublicationData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous result publication DATA consumer");
        requireDataFrame(exactFrame, exactProfile, allocation, "NativeResultPublication");
        requireUuid(exactWrite, "exact outer source write UUID");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(allocation, exactProfile)) {
            work.charge(1_024L, "result publication inline identity/parser workspace BEFORE allocation");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            DecodedInlineArtifactIdentityData coreArtifact = readInlineArtifactIdentityData(input,
                InlineArtifactRole.RESULT_CORE, exactWrite, exactProfile);
            String protocol = readAccountedText(input, maxTextFrame(exactProfile), work,
                "result core publication protocol");
            if (!"FP_WIN32_RESULT_CORE_PUBLISH_V1".equals(protocol))
                throw new IOException("Unknown result core publication protocol");
            byte[] evidence = readProfileFrame(input, exactProfile, work, "original result publication evidence");
            requireEnd(input, "NativeResultPublication");
            consumePublicationEvidence(evidence, exactWrite, coreArtifact, exactProfile, allocation, work, consumer);
        } catch (java.io.UncheckedIOException failure) {
            throw unwrapCheckedIoPreservingSuppressed(failure);
        }
    }

    /**
     * Re-encodes the complete frozen NativeResultPublication leaf from the exact synchronous
     * publication DATA consumer. This is a DATA codec only; it never issues a publication result,
     * evidence capability, or permit.
     */
    public static void encodePublicationData(DecodedPublicationData data, DataOutputStream output,
            SourceLimitProfile exactProfile, SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(data, "decoded result publication DATA");
        Objects.requireNonNull(output, "NativeResultPublication DATA output");
        requireAllocation(exactProfile, allocation);
        if (!data.isActiveOnCurrentThread(exactProfile, allocation))
            throw new IOException("Publication DATA encoder requires its exact live profile/account consumer callback");

        byte[] encodedLeaf = null;
        long retainedLeafBytes = 0L;
        Throwable primary = null;
        try {
            try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(allocation, exactProfile)) {
                work.charge(2_048L, "publication DATA identity/profile validation workspace BEFORE allocation");
                validatePublicationDataForEncoding(data, exactProfile);

                MySqlRecoveryResourceKey.DecodedCarrierKeyData carrierData = data.carrierKey;
                String saveIdentity = requireWireText(carrierData.saveIdentity(), exactProfile, "publication CARRIER save identity");
                String loaderIdentity = requireWireText(carrierData.loaderIdentity(), exactProfile, "publication CARRIER loader identity");
                String regionIdentity = requireWireText(carrierData.regionIdentity(), exactProfile, "publication CARRIER region identity");
                int saveBytes = utf8LengthStrict(saveIdentity, maxTextFrame(exactProfile));
                int loaderBytes = utf8LengthStrict(loaderIdentity, maxTextFrame(exactProfile));
                int regionBytes = utf8LengthStrict(regionIdentity, maxTextFrame(exactProfile));
                int carrierEncodingBytes = utf8LengthStrict(MYSQL_RESOURCE_KEY_ENCODING, maxTextFrame(exactProfile));
                int carrierKindBytes = utf8LengthStrict("CARRIER", maxTextFrame(exactProfile));
                long predictedCarrierLength = 1L + 8L;
                predictedCarrierLength = Math.addExact(predictedCarrierLength, 5L * Integer.BYTES);
                predictedCarrierLength = Math.addExact(predictedCarrierLength,
                    (long) carrierEncodingBytes + carrierKindBytes + saveBytes + loaderBytes + regionBytes);
                int carrierLength = boundedPublicationFrameLength(predictedCarrierLength, exactProfile,
                    "canonical publication CARRIER key");
                long carrierCharacters = Math.addExact((long) saveIdentity.length(),
                    Math.addExact(loaderIdentity.length(), regionIdentity.length()));
                long carrierWorkspace = Math.addExact(2_048L,
                    Math.addExact(Math.multiplyExact(3L, carrierLength), Math.multiplyExact(6L, carrierCharacters)));
                work.charge(carrierWorkspace,
                    "publication CARRIER canonical builder payload/text workspace BEFORE allocation");
                MySqlRecoveryResourceKey carrier = MySqlRecoveryResourceKey.carrier(saveIdentity, loaderIdentity,
                    regionIdentity, carrierData.chunkX(), carrierData.chunkZ(), exactProfile);
                if (carrier.canonicalLength() != carrierLength)
                    throw new IOException("Publication CARRIER canonical length differs from the exact typed DATA fields");

                int domainLength = strictTextFieldLength(RESULT_PUBLICATION_EVIDENCE_DOMAIN, exactProfile,
                    "result publication evidence domain");
                int temporaryLength = strictTextFieldLength(data.temporaryBasename, exactProfile,
                    "publication temporary basename");
                int canonicalLength = strictTextFieldLength(data.canonicalBasename, exactProfile,
                    "publication canonical basename");
                int inlineBasenameLength = strictTextFieldLength(data.coreArtifact.basename, exactProfile,
                    "inline result-core basename");
                int protocolLength = strictTextFieldLength(RESULT_PUBLICATION_PROTOCOL, exactProfile,
                    "result publication protocol");
                long evidenceLength = domainLength;
                evidenceLength = Math.addExact(evidenceLength, 5L * 16L);
                evidenceLength = Math.addExact(evidenceLength, 3L * Long.BYTES);
                evidenceLength = Math.addExact(evidenceLength, Integer.BYTES + (long) carrierLength);
                evidenceLength = Math.addExact(evidenceLength, SHA256_BYTES);
                evidenceLength = Math.addExact(evidenceLength, (long) temporaryLength + canonicalLength);
                evidenceLength = Math.addExact(evidenceLength, 3L * 24L);
                evidenceLength = Math.addExact(evidenceLength,
                    Long.BYTES + SHA256_BYTES + Integer.BYTES + Integer.BYTES);
                evidenceLength = Math.addExact(evidenceLength,
                    (long) PUBLICATION_STAGE_COUNT * (Integer.BYTES + PUBLICATION_STAGE_FRAME_BYTES));
                int evidenceFrameLength = boundedPublicationFrameLength(evidenceLength, exactProfile,
                    "complete result publication evidence");
                long inlineArtifactLength = Math.addExact(inlineBasenameLength, 2L * 24L + Long.BYTES + SHA256_BYTES);
                long leafLength = Math.addExact(inlineArtifactLength,
                    Math.addExact(protocolLength, Integer.BYTES + (long) evidenceFrameLength));
                int leafFrameLength = boundedPublicationFrameLength(leafLength, exactProfile,
                    "complete NativeResultPublication leaf");

                long allTextCharacters = RESULT_PUBLICATION_EVIDENCE_DOMAIN.length();
                allTextCharacters = Math.addExact(allTextCharacters, data.temporaryBasename.length());
                allTextCharacters = Math.addExact(allTextCharacters, data.canonicalBasename.length());
                allTextCharacters = Math.addExact(allTextCharacters, data.coreArtifact.basename.length());
                allTextCharacters = Math.addExact(allTextCharacters, RESULT_PUBLICATION_PROTOCOL.length());
                allTextCharacters = Math.addExact(allTextCharacters, carrierCharacters);
                long textWriterWorkspace = Math.addExact(4_096L,
                    Math.multiplyExact(8L, allTextCharacters));
                work.charge(Math.addExact((long) evidenceFrameLength, textWriterWorkspace),
                    "publication evidence re-encode and text-writer workspace BEFORE allocation");

                byte[] canonicalEvidence = new byte[evidenceFrameLength];
                ExactArrayOutputStream evidenceSink = new ExactArrayOutputStream(canonicalEvidence);
                DataOutputStream evidenceOutput = new DataOutputStream(evidenceSink);
                writeText(evidenceOutput, RESULT_PUBLICATION_EVIDENCE_DOMAIN);
                writeUuid(evidenceOutput, data.binding);
                writeUuid(evidenceOutput, data.workflow);
                writeUuid(evidenceOutput, data.write);
                writeUuid(evidenceOutput, data.publicationAttempt);
                writeUuid(evidenceOutput, data.boot);
                evidenceOutput.writeLong(data.bootGeneration);
                evidenceOutput.writeLong(data.custodyGeneration);
                evidenceOutput.writeLong(data.writeGeneration);
                evidenceOutput.writeInt(carrierLength);
                BoundedCountingOutputStream carrierSink = new BoundedCountingOutputStream(evidenceSink, carrierLength);
                DataOutputStream carrierOutput = new DataOutputStream(carrierSink);
                carrier.writeCanonicalTo(carrierOutput);
                carrierOutput.flush();
                carrierSink.requireComplete();
                evidenceOutput.write(data.profileSha256);
                writeText(evidenceOutput, data.temporaryBasename);
                writeText(evidenceOutput, data.canonicalBasename);
                writeIdentity(evidenceOutput, data.parentIdentity);
                writeIdentity(evidenceOutput, data.temporaryIdentity);
                writeIdentity(evidenceOutput, data.canonicalIdentity);
                evidenceOutput.writeLong(data.coreLength);
                evidenceOutput.write(data.coreSha256);
                evidenceOutput.writeInt(data.moveFlags);
                evidenceOutput.writeInt(PUBLICATION_STAGE_COUNT);
                for (PublicationStageData stage : data.completedStages) {
                    evidenceOutput.writeInt(PUBLICATION_STAGE_FRAME_BYTES);
                    evidenceOutput.writeInt(stage.stageCode());
                    evidenceOutput.writeLong(stage.enteredOrdinal());
                    evidenceOutput.writeLong(stage.returnedOrdinal());
                }
                evidenceOutput.flush();
                evidenceSink.requireComplete();
                if (!Arrays.equals(canonicalEvidence, data.originalEvidence))
                    throw new IOException("Re-encoded result publication evidence differs from its exact canonical original bytes");

                allocation.reserveTemporary(leafFrameLength,
                    "complete NativeResultPublication frame retained through account-scope close and output BEFORE allocation");
                retainedLeafBytes = leafFrameLength;
                encodedLeaf = new byte[leafFrameLength];
                ExactArrayOutputStream leafSink = new ExactArrayOutputStream(encodedLeaf);
                DataOutputStream leafOutput = new DataOutputStream(leafSink);
                writeInlineArtifactIdentityData(leafOutput, data.coreArtifact);
                writeText(leafOutput, RESULT_PUBLICATION_PROTOCOL);
                leafOutput.writeInt(evidenceFrameLength);
                leafOutput.write(canonicalEvidence);
                leafOutput.flush();
                leafSink.requireComplete();

                consumePublicationData(encodedLeaf, data.write, exactProfile, allocation, roundTrip -> {
                    if (!samePublicationData(data, roundTrip))
                        throw new java.io.UncheckedIOException(new IOException(
                            "Re-encoded NativeResultPublication leaf failed typed strict round-trip comparison"));
                });
            }
            output.write(encodedLeaf);
            encodedLeaf = null;
        } catch (Throwable failure) {
            primary = failure;
        } finally {
            if (retainedLeafBytes > 0L) {
                try {
                    allocation.releaseTemporary(retainedLeafBytes,
                        "complete NativeResultPublication frame dropped after strict output");
                } catch (Throwable cleanup) {
                    if (primary == null) primary = cleanup;
                    else if (cleanup != primary) primary.addSuppressed(cleanup);
                }
            }
        }
        if (primary instanceof java.io.UncheckedIOException unchecked)
            primary = unwrapCheckedIoPreservingSuppressed(unchecked);
        rethrow(primary, "encode strict NativeResultPublication DATA");
    }

    private static void validatePublicationDataForEncoding(DecodedPublicationData data,
            SourceLimitProfile exactProfile) throws IOException {
        if (data.profile != exactProfile || data.originalAllocation.exactProfile() != exactProfile)
            throw new IOException("Publication DATA profile/account differs from its exact original decode");
        try {
            requireUuid(data.binding, "publication binding UUID");
            requireUuid(data.workflow, "publication child workflow UUID");
            requireUuid(data.write, "publication write UUID");
            requireUuid(data.publicationAttempt, "publication attempt UUID");
            requireUuid(data.boot, "publication boot UUID");
        } catch (IllegalArgumentException malformed) {
            throw new IOException("Publication DATA contains an invalid frozen identity", malformed);
        }
        if (data.bootGeneration <= 0L || data.custodyGeneration <= 0L || data.writeGeneration < 0L)
            throw new IOException("Publication DATA generations are outside their frozen positive/nonnegative domains");
        if (data.carrierKey == null) throw new IOException("Publication DATA has no strict canonical CARRIER key");
        requireWireText(data.carrierKey.saveIdentity(), exactProfile, "publication CARRIER save identity");
        requireWireText(data.carrierKey.loaderIdentity(), exactProfile, "publication CARRIER loader identity");
        requireWireText(data.carrierKey.regionIdentity(), exactProfile, "publication CARRIER region identity");
        if (!WindowsMarkerIo.sourceResultTemporaryBasename(data.write, data.publicationAttempt)
                .equals(data.temporaryBasename)
            || !WindowsMarkerIo.sourceResultCanonicalBasename(data.write).equals(data.canonicalBasename))
            throw new IOException("Publication DATA basenames differ from the fixed write/attempt roles");
        requireWireText(data.temporaryBasename, exactProfile, "publication temporary basename");
        requireWireText(data.canonicalBasename, exactProfile, "publication canonical basename");
        if (data.parentIdentity == null || data.temporaryIdentity == null || data.canonicalIdentity == null
            || data.parentIdentity.volumeSerialNumber() != data.temporaryIdentity.volumeSerialNumber()
            || data.parentIdentity.volumeSerialNumber() != data.canonicalIdentity.volumeSerialNumber()
            || !data.temporaryIdentity.equals(data.canonicalIdentity))
            throw new IOException("Publication DATA parent/temp/canonical FileIdentity lineage differs");
        if (data.coreLength <= 0L || data.coreLength > exactProfile.maxNativeArtifactBytes()
            || data.coreLength > exactProfile.maxSqlPacketBytes() || data.coreLength > exactProfile.maxRetainedBytes())
            throw new IOException("Publication DATA core length exceeds the exact native/SQL/retained profile");
        requireHash(data.profileSha256, "publication profile SHA-256");
        if (!data.profileSha256Matches(exactProfile.sha256()))
            throw new IOException("Publication DATA profile SHA-256 differs from the exact original profile");
        requireHash(data.coreSha256, "published result core SHA-256");
        if (data.moveFlags != WindowsMarkerIo.WRITE_THROUGH)
            throw new IOException("Publication DATA must retain exact MOVEFILE_WRITE_THROUGH flags=8");
        if (data.completedStages == null || data.completedStages.size() != PUBLICATION_STAGE_COUNT)
            throw new IOException("Publication DATA must retain all 11 completed result-core stages");
        long previousReturn = 0L;
        for (int index = 0; index < PUBLICATION_STAGE_COUNT; index++) {
            PublicationStageData stage = data.completedStages.get(index);
            if (stage == null || stage.stageCode() != index + 1 || stage.enteredOrdinal() <= previousReturn
                || stage.returnedOrdinal() <= stage.enteredOrdinal())
                throw new IOException("Publication DATA stages are not the exact ordered 11 returned B20 events");
            previousReturn = stage.returnedOrdinal();
        }
        if (data.originalEvidence == null || data.originalEvidence.length == 0
            || data.originalEvidence.length > exactProfile.maxSqlPacketBytes()
            || data.originalEvidence.length > exactProfile.maxRetainedBytes())
            throw new IOException("Publication DATA original evidence is outside the exact SQL/retained frame profile");
        if (data.coreArtifact == null || data.coreArtifact.role != InlineArtifactRole.RESULT_CORE
            || !data.write.equals(data.coreArtifact.writeUuid))
            throw new IOException("Publication DATA does not retain the exact RESULT_CORE inline artifact leaf");
        validateInlineArtifactData(data.coreArtifact, exactProfile);
        if (data.coreArtifact.byteLength != data.coreLength
            || !java.security.MessageDigest.isEqual(data.coreArtifact.contentSha256, data.coreSha256)
            || !data.coreArtifact.parentDirectoryIdentity.equals(data.parentIdentity)
            || !data.coreArtifact.fileIdentity.equals(data.canonicalIdentity))
            throw new IOException("Publication DATA core ArtifactIdentity differs from its exact length/hash/identity evidence");
    }

    private static int strictTextFieldLength(String value, SourceLimitProfile profile, String field) throws IOException {
        requireWireText(value, profile, field);
        return Math.addExact(Integer.BYTES, utf8LengthStrict(value, maxTextFrame(profile)));
    }

    private static int boundedPublicationFrameLength(long length, SourceLimitProfile profile, String field)
            throws IOException {
        if (length <= 0L || length > Integer.MAX_VALUE || length > profile.maxSqlPacketBytes()
            || length > profile.maxRetainedBytes())
            throw new IOException("Complete " + field + " exceeds the exact SQL/retained or JVM array bound");
        return Math.toIntExact(length);
    }

    private static boolean samePublicationData(DecodedPublicationData expected, DecodedPublicationData actual) {
        if (expected.profile != actual.profile || expected.originalAllocation != actual.originalAllocation
            || expected.binding == null || !expected.binding.equals(actual.binding)
            || !expected.workflow.equals(actual.workflow) || !expected.write.equals(actual.write)
            || !expected.publicationAttempt.equals(actual.publicationAttempt) || !expected.boot.equals(actual.boot)
            || expected.bootGeneration != actual.bootGeneration || expected.custodyGeneration != actual.custodyGeneration
            || expected.writeGeneration != actual.writeGeneration
            || !expected.profileSha256Matches(actual.profileSha256)
            || !expected.temporaryBasename.equals(actual.temporaryBasename)
            || !expected.canonicalBasename.equals(actual.canonicalBasename)
            || !expected.parentIdentity.equals(actual.parentIdentity)
            || !expected.temporaryIdentity.equals(actual.temporaryIdentity)
            || !expected.canonicalIdentity.equals(actual.canonicalIdentity)
            || expected.coreLength != actual.coreLength
            || !java.security.MessageDigest.isEqual(expected.coreSha256, actual.coreSha256)
            || expected.moveFlags != actual.moveFlags
            || !Arrays.equals(expected.originalEvidence, actual.originalEvidence)
            || expected.completedStages.size() != actual.completedStages.size())
            return false;
        MySqlRecoveryResourceKey.DecodedCarrierKeyData expectedCarrier = expected.carrierKey;
        MySqlRecoveryResourceKey.DecodedCarrierKeyData actualCarrier = actual.carrierKey;
        if (!expectedCarrier.saveIdentity().equals(actualCarrier.saveIdentity())
            || !expectedCarrier.loaderIdentity().equals(actualCarrier.loaderIdentity())
            || !expectedCarrier.regionIdentity().equals(actualCarrier.regionIdentity())
            || expectedCarrier.chunkX() != actualCarrier.chunkX() || expectedCarrier.chunkZ() != actualCarrier.chunkZ())
            return false;
        DecodedInlineArtifactIdentityData expectedArtifact = expected.coreArtifact;
        DecodedInlineArtifactIdentityData actualArtifact = actual.coreArtifact;
        if (expectedArtifact.role != actualArtifact.role || !expectedArtifact.writeUuid.equals(actualArtifact.writeUuid)
            || !expectedArtifact.basename.equals(actualArtifact.basename)
            || !expectedArtifact.parentDirectoryIdentity.equals(actualArtifact.parentDirectoryIdentity)
            || !expectedArtifact.fileIdentity.equals(actualArtifact.fileIdentity)
            || expectedArtifact.byteLength != actualArtifact.byteLength
            || !java.security.MessageDigest.isEqual(expectedArtifact.contentSha256, actualArtifact.contentSha256))
            return false;
        for (int index = 0; index < expected.completedStages.size(); index++) {
            PublicationStageData left = expected.completedStages.get(index);
            PublicationStageData right = actual.completedStages.get(index);
            if (left.stageCode() != right.stageCode() || left.enteredOrdinal() != right.enteredOrdinal()
                || left.returnedOrdinal() != right.returnedOrdinal()) return false;
        }
        return true;
    }

    private static void consumePublicationEvidence(byte[] exactEvidence, UUID exactWrite,
            DecodedInlineArtifactIdentityData coreArtifact, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget allocation, SourceForestAllocationBudget.Scope outerWork,
            Consumer<DecodedPublicationData> consumer) throws IOException {
        outerWork.charge(768L, "publication evidence parser/work wrapper BEFORE allocation");
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactEvidence));
        String domain = readAccountedText(input, maxTextFrame(exactProfile), outerWork,
            "result publication evidence domain");
        if (!"FP_WIN32_RESULT_PUBLICATION_BE_V1".equals(domain))
            throw new IOException("Wrong original result publication evidence domain");
        UUID binding = readUuid(input, "publication binding UUID");
        UUID workflow = readUuid(input, "publication child workflow UUID");
        UUID write = readUuid(input, "publication write UUID");
        UUID attempt = readUuid(input, "publication attempt UUID");
        UUID boot = readUuid(input, "publication boot UUID");
        if (!write.equals(exactWrite)) throw new IOException("Publication evidence write UUID differs from the exact outer write");
        long bootGeneration = readPositive(input, "publication boot generation");
        long custodyGeneration = readPositive(input, "publication custody generation");
        long writeGeneration = readNonnegative(input, "publication write generation");
        byte[] carrierBytes = readProfileFrame(input, exactProfile, outerWork, "publication full canonical CARRIER key");
        byte[] profileSha = readAccountedHash(input, outerWork, "publication exact source profile SHA-256");
        outerWork.charge(64L, "publication profile digest comparison copies");
        if (!java.security.MessageDigest.isEqual(profileSha, exactProfile.sha256()))
            throw new IOException("Publication profile hash differs from the exact original profile");
        String temporaryBasename = readWireText(input, exactProfile, outerWork, "publication temporary basename");
        String canonicalBasename = readWireText(input, exactProfile, outerWork, "publication canonical basename");
        if (!temporaryBasename.equals(WindowsMarkerIo.sourceResultTemporaryBasename(exactWrite, attempt))
            || !canonicalBasename.equals(WindowsMarkerIo.sourceResultCanonicalBasename(exactWrite)))
            throw new IOException("Publication temporary/canonical names differ from fixed write/attempt roles");
        WindowsMarkerIo.FileIdentity parent = readAccountedIdentity(input, outerWork, "publication parent FileIdentity");
        WindowsMarkerIo.FileIdentity temporary = readAccountedIdentity(input, outerWork, "publication temporary FileIdentity");
        WindowsMarkerIo.FileIdentity canonical = readAccountedIdentity(input, outerWork, "publication canonical FileIdentity");
        if (parent.volumeSerialNumber() != temporary.volumeSerialNumber()
            || parent.volumeSerialNumber() != canonical.volumeSerialNumber() || !temporary.equals(canonical))
            throw new IOException("Publication parent/temp/canonical FileIdentity lineage differs");
        long coreLength = readPositive(input, "published result core length");
        if (coreLength > exactProfile.maxNativeArtifactBytes() || coreLength > exactProfile.maxSqlPacketBytes()
            || coreLength > exactProfile.maxRetainedBytes())
            throw new IOException("Published result core length exceeds exact native/SQL/retained profile");
        byte[] coreSha = readAccountedHash(input, outerWork, "published result core SHA-256");
        int moveFlags = input.readInt();
        if (moveFlags != WindowsMarkerIo.WRITE_THROUGH)
            throw new IOException("Result-core publication must use exact MOVEFILE_WRITE_THROUGH flags=8");
        int count = input.readInt();
        if (count != 11 || count > input.available() / 24)
            throw new IOException("Result-core publication evidence must contain exactly 11 complete stages");
        outerWork.charge(Math.addExact(256L, Math.multiplyExact((long) count, 96L)),
            "publication completed-stage DATA list BEFORE allocation");
        List<PublicationStageData> stages = new ArrayList<>(count);
        long previousReturn = 0L;
        for (int index = 0; index < count; index++) {
            byte[] stageFrame = readProfileFrame(input, exactProfile, outerWork, "B-framed completed publication stage");
            DataInputStream stageInput = new DataInputStream(new ByteArrayInputStream(stageFrame));
            int code = stageInput.readInt();
            long entered = readPositive(stageInput, "publication stage entered ordinal");
            long returned = readPositive(stageInput, "publication stage returned ordinal");
            requireEnd(stageInput, "completed publication stage");
            if (stageFrame.length != 20 || code != index + 1 || returned <= entered || entered <= previousReturn)
                throw new IOException("Publication completed stages are invalid, incomplete, duplicated, or out of actual event order");
            stages.add(new PublicationStageData(code, entered, returned));
            previousReturn = returned;
        }
        requireEnd(input, "result publication evidence");
        if (coreArtifact.byteLength() != coreLength
            || !java.security.MessageDigest.isEqual(coreArtifact.contentSha256, coreSha)
            || !coreArtifact.parentDirectoryIdentity().equals(parent)
            || !coreArtifact.fileIdentity().equals(canonical))
            throw new IOException("Result-core ArtifactIdentity differs from the original published core evidence");
        MySqlRecoveryResourceKey.consumeCarrierKeyData(carrierBytes, exactProfile, allocation, carrier -> {
            outerWork.charge(512L, "publication DATA callback value/account/lifetime binding BEFORE allocation");
            DecodedPublicationData decoded = new DecodedPublicationData(exactProfile, binding, workflow, write, attempt, boot,
                bootGeneration, custodyGeneration, writeGeneration, carrier, profileSha, temporaryBasename,
                canonicalBasename, parent, temporary, canonical, coreLength, coreSha, moveFlags, stages,
                exactEvidence, coreArtifact, allocation);
            try {
                consumer.accept(decoded);
            } finally {
                decoded.finishSynchronousConsumer();
            }
        });
    }

    /**
     * Strictly consumes the original receipt prefix plus its diagnostic option, with no publication leaf.
     * The complete borrowed input must already be charged to the exact original allocation and remain stable
     * until this method returns. The supplied forest expectation comes from the same external snapshot.
     */
    public static void consumeReceiptCoreData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation,
            com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation sameSnapshotExpectation,
            Consumer<DecodedReceiptCoreData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous source receipt core DATA consumer");
        consumeReceiptData(exactFrame, exactProfile, originalAllocation, sameSnapshotExpectation, false,
            data -> consumer.accept(new DecodedReceiptCoreData(data)));
    }

    /**
     * Strictly consumes the complete original outer receipt, including descriptive publication DATA.
     * The complete borrowed input must already be charged to the exact original allocation and remain stable
     * until this method returns. The supplied forest expectation comes from the same external snapshot.
     */
    public static void consumeSourceWriteReceiptData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation,
            com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation sameSnapshotExpectation,
            Consumer<DecodedSourceWriteReceiptData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous complete source receipt DATA consumer");
        consumeReceiptData(exactFrame, exactProfile, originalAllocation, sameSnapshotExpectation, true,
            data -> consumer.accept(new DecodedSourceWriteReceiptData(data)));
    }

    private static void consumeReceiptData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation,
            com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation sameSnapshotExpectation,
            boolean outer, Consumer<ReceiptData> consumer) throws IOException {
        Objects.requireNonNull(sameSnapshotExpectation, "external same-snapshot complete forest expectation");
        Objects.requireNonNull(consumer, "strict receipt DATA consumer");
        requireDataFrame(exactFrame, exactProfile, originalAllocation,
            outer ? "complete SOURCE write receipt" : "SOURCE receipt core");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(originalAllocation, exactProfile)) {
            work.charge(2_048L, "receipt DATA root cursor, typed wrapper, comparison, and digest workspace BEFORE allocation");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            String domain = readAccountedText(input, maxTextFrame(exactProfile), work, "SOURCE receipt domain");
            if (!SOURCE_WRITE_RECEIPT_DOMAIN.equals(domain))
                throw new IOException("Wrong frozen SOURCE write receipt domain");
            UUID write = readUuid(input, "receipt write UUID");
            UUID workflow = readUuid(input, "receipt child workflow UUID");
            UUID binding = readUuid(input, "receipt binding UUID");
            String saveIdentity = readWireText(input, exactProfile, work, "receipt save identity");
            String loaderIdentity = readWireText(input, exactProfile, work, "receipt loader identity");
            byte[] carrierKeyBytes = readProfileFrame(input, exactProfile, work, "receipt complete canonical CARRIER key");
            MySqlRecoveryResourceKey.consumeCarrierKeyData(carrierKeyBytes, exactProfile, originalAllocation, carrier -> {
                try {
                    validateCarrierRootFields(carrier, saveIdentity, loaderIdentity);
                    consumeReceiptAfterCarrier(exactFrame, input, exactProfile, originalAllocation,
                        sameSnapshotExpectation, outer, consumer, work, write, workflow, binding,
                        saveIdentity, loaderIdentity, carrierKeyBytes, carrier);
                } catch (IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        } catch (java.io.UncheckedIOException failure) {
            throw unwrapCheckedIoPreservingSuppressed(failure);
        }
    }

    private static void consumeReceiptAfterCarrier(byte[] exactFrame, DataInputStream input,
            SourceLimitProfile exactProfile, SourceForestAllocationBudget originalAllocation,
            com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation sameSnapshotExpectation,
            boolean outer, Consumer<ReceiptData> consumer, SourceForestAllocationBudget.Scope rootWork,
            UUID write, UUID workflow, UUID binding, String saveIdentity, String loaderIdentity,
            byte[] carrierKeyBytes, MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier) throws IOException {
        WindowsMarkerIo.FileIdentity regionIdentity = readAccountedIdentity(input, rootWork, "receipt native region FileIdentity");
        int slot = input.readInt();
        if (slot < 0 || slot >= 1024) throw new IOException("SOURCE receipt slot is outside the original Anvil 0..1023 range");
        long writeGeneration = readNonnegative(input, "receipt write generation");
        long payloadLength = readPositive(input, "receipt payload length");
        if (payloadLength > exactProfile.maxNativeArtifactBytes() || payloadLength > exactProfile.maxSqlPacketBytes()
            || payloadLength > exactProfile.maxRetainedBytes() || payloadLength > exactProfile.maxDecompressedBytes())
            throw new IOException("SOURCE receipt payload length exceeds the exact native/SQL/retained profile");
        byte[] payloadSha256 = readAccountedHash(input, rootWork, "receipt payload SHA-256");
        byte[] payloadArtifactLeaf = readInlineArtifactLeaf(input, exactProfile, rootWork,
            "complete raw PAYLOAD ArtifactIdentity leaf");
        int undoCode = input.readUnsignedByte();
        UndoKind undoKind = UndoKind.fromWire(undoCode);
        long undoLength = readNonnegative(input, "receipt undo content length");
        int undoHashTag = input.readUnsignedByte();
        byte[] undoContentSha256;
        if (undoHashTag == 0) undoContentSha256 = null;
        else if (undoHashTag == 1) undoContentSha256 = readAccountedHash(input, rootWork, "receipt undo content SHA-256");
        else throw new IOException("Unknown frozen SOURCE receipt undo-content option tag");
        byte[] undoArtifactLeaf = readInlineArtifactLeaf(input, exactProfile, rootWork,
            "complete raw UNDO ArtifactIdentity leaf");

        byte[] memberCoverageFrame = readProfileFrame(input, exactProfile, rootWork, "complete receipt member coverage frame");
        byte[] obligationRevisionsFrame = readObligationListFrame(input, exactProfile, rootWork);

        long dataWriteCallsReturned = readNonnegative(input, "RegionWriteResults dataWriteCallsReturned");
        long headerWriteCallsReturned = readNonnegative(input, "RegionWriteResults headerWriteCallsReturned");
        long encoderSubmittedBytes = readNonnegative(input, "RegionWriteResults encoderSubmittedBytes");
        long outputCloseCallsReturned = readNonnegative(input, "RegionWriteResults outputCloseCallsReturned");
        RegionFileStampData before = readRegionStamp(input, exactProfile, rootWork, "RegionWriteResults before stamp");
        RegionFileStampData after = readRegionStamp(input, exactProfile, rootWork, "RegionWriteResults after stamp");
        byte[] readbackHeader = readAccountedFixed(input, 8192, rootWork, "exact 8192-byte RegionWriteResults readback header");
        byte[] readbackCompressedChunk = readProfileFrame(input, exactProfile, rootWork, "complete target MCA record in readbackCompressedChunk");
        byte[] readbackDecodedRootNbt = readProfileFrame(input, exactProfile, rootWork, "complete decoded readback root NBT");
        byte[] readbackPayloadSha256 = readAccountedHash(input, rootWork, "RegionWriteResults readback payload SHA-256");
        String forceProtocol = readWireText(input, exactProfile, rootWork, "RegionWriteResults force protocol");
        byte[] forceEvidence = readProfileFrame(input, exactProfile, rootWork, "complete original SOURCE force evidence");
        int prefixEnd = exactFrame.length - input.available();

        byte[] publicationLeaf = null;
        if (outer) publicationLeaf = readPublicationLeaf(input, exactProfile, rootWork);
        int diagnosticOffset = exactFrame.length - input.available();
        int diagnosticTag = input.readUnsignedByte();
        int diagnosticValueOffset = -1, diagnosticValueLength = 0;
        if (diagnosticTag == 1) {
            int length = input.readInt();
            if (length < 0 || length > exactProfile.maxSqlPacketBytes() || length > exactProfile.maxRetainedBytes()
                || length > input.available())
                throw new IOException("SOURCE receipt original diagnostic option has an invalid/truncated exact length");
            diagnosticValueOffset = exactFrame.length - input.available();
            diagnosticValueLength = length;
            input.skipNBytes(length);
        } else if (diagnosticTag != 0) {
            throw new IOException("Unknown frozen SOURCE receipt original diagnostic option tag");
        }
        int diagnosticLength = exactFrame.length - diagnosticOffset;
        requireEnd(input, outer ? "complete SOURCE write receipt" : "SOURCE receipt core");

        if (writeGeneration < 0L || slot >= 1024) throw new IOException("Invalid SOURCE receipt generation or slot");
        final byte[] exactPublicationLeaf = publicationLeaf;
        final int exactDiagnosticValueOffset = diagnosticValueOffset, exactDiagnosticValueLength = diagnosticValueLength;
        consumeInlineArtifactIdentityData(payloadArtifactLeaf,
            InlineArtifactRole.PAYLOAD, write, exactProfile, originalAllocation, payload -> {
                try {
                    consumeInlineArtifactIdentityData(undoArtifactLeaf,
                        InlineArtifactRole.UNDO, write, exactProfile, originalAllocation, undo -> {
                            try {
                                validateReceiptTuple(write, workflow, binding, saveIdentity, loaderIdentity,
                                    carrier, slot, payloadLength, payloadSha256, payload, undoKind, undoLength,
                                    undoContentSha256, undo, before, after, regionIdentity, exactProfile);
                                consumeMemberCoverageData(memberCoverageFrame, exactProfile, originalAllocation,
                                    sameSnapshotExpectation, memberCoverage -> {
                                        try {
                                            consumeObligationRevisionsData(obligationRevisionsFrame, exactProfile,
                                                originalAllocation, binding, carrierKeyBytes, obligations -> {
                                                    try {
                                                        consumeForceAndFinish(exactFrame, exactProfile, originalAllocation,
                                                            outer, consumer, rootWork, write, workflow, binding,
                                                            saveIdentity, loaderIdentity, carrierKeyBytes, carrier,
                                                            regionIdentity, slot, writeGeneration, payloadLength,
                                                            payloadSha256, payload, undoKind, undoLength,
                                                            undoContentSha256, undo, memberCoverage, obligations,
                                                            dataWriteCallsReturned, headerWriteCallsReturned,
                                                            encoderSubmittedBytes, outputCloseCallsReturned, before, after,
                                                            readbackHeader, readbackCompressedChunk, readbackDecodedRootNbt,
                                                            readbackPayloadSha256, forceProtocol, forceEvidence,
                                                            exactPublicationLeaf, prefixEnd, diagnosticOffset, diagnosticLength,
                                                            exactDiagnosticValueOffset, exactDiagnosticValueLength);
                                                    } catch (IOException failure) {
                                                        throw new java.io.UncheckedIOException(failure);
                                                    }
                                                });
                                        } catch (IOException failure) {
                                            throw new java.io.UncheckedIOException(failure);
                                        }
                                    });
                            } catch (IOException failure) {
                                throw new java.io.UncheckedIOException(failure);
                            }
                        });
                } catch (IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
    }

    private static byte[] readInlineArtifactLeaf(DataInputStream input, SourceLimitProfile profile,
            SourceForestAllocationBudget.Scope work, String context) throws IOException {
        int available = input.available();
        input.mark(available);
        int textLength = input.readInt();
        if (textLength <= 0 || textLength > 128 || textLength > input.available())
            throw new IOException(context + " has an invalid basename length");
        input.skipNBytes(textLength);
        input.skipNBytes(24L + 24L + Long.BYTES + SHA256_BYTES);
        int leafLength = available - input.available();
        input.reset();
        if (leafLength <= 0 || leafLength > INLINE_ARTIFACT_MAX_BYTES
            || leafLength > profile.maxSqlPacketBytes() || leafLength > profile.maxRetainedBytes())
            throw new IOException(context + " exceeds the fixed bounded inline ArtifactIdentity size");
        return readAccountedFixed(input, leafLength, work, context);
    }

    private static byte[] readObligationListFrame(DataInputStream input, SourceLimitProfile profile,
            SourceForestAllocationBudget.Scope work) throws IOException {
        int available = input.available();
        if (available < Integer.BYTES) throw new EOFException("Truncated original SOURCE obligation L count");
        input.mark(available);
        int count = input.readInt();
        if (count < 0 || (long) count > profile.maxMembers() || count > input.available() / Integer.BYTES)
            throw new IOException("SOURCE obligation L count is negative, over-profile, or impossible for its remaining frame");
        for (int index = 0; index < count; index++) {
            int itemLength = input.readInt();
            if (itemLength <= 0 || itemLength > profile.maxSqlPacketBytes()
                || itemLength > profile.maxRetainedBytes() || itemLength > input.available())
                throw new IOException("SOURCE obligation L item has an invalid or truncated exact B length");
            input.skipNBytes(itemLength);
        }
        int frameLength = available - input.available();
        input.reset();
        if (frameLength < Integer.BYTES || frameLength > profile.maxSqlPacketBytes()
            || frameLength > profile.maxRetainedBytes())
            throw new IOException("Complete SOURCE obligation L field exceeds the exact SQL/retained profile");
        work.charge(Math.addExact(512L, Math.multiplyExact((long) count, 96L)),
            "original receipt obligation L typed vector-reference workspace BEFORE allocation");
        return readAccountedFixed(input, frameLength, work, "complete original SOURCE obligation L field");
    }

    private static byte[] readPublicationLeaf(DataInputStream input, SourceLimitProfile profile,
            SourceForestAllocationBudget.Scope work) throws IOException {
        int available = input.available();
        input.mark(available);
        int basenameLength = input.readInt();
        if (basenameLength <= 0 || basenameLength > 128 || basenameLength > input.available())
            throw new IOException("NativeResultPublication inline result-core basename length is invalid");
        input.skipNBytes(basenameLength);
        input.skipNBytes(24L + 24L + Long.BYTES + SHA256_BYTES);
        int protocolLength = input.readInt();
        if (protocolLength <= 0 || protocolLength > maxTextFrame(profile) || protocolLength > input.available())
            throw new IOException("NativeResultPublication protocol text length is invalid");
        input.skipNBytes(protocolLength);
        int evidenceLength = input.readInt();
        if (evidenceLength <= 0 || evidenceLength > profile.maxSqlPacketBytes()
            || evidenceLength > profile.maxRetainedBytes() || evidenceLength > input.available())
            throw new IOException("NativeResultPublication original evidence B length is invalid or truncated");
        input.skipNBytes(evidenceLength);
        int leafLength = available - input.available();
        input.reset();
        if (leafLength <= 0 || leafLength > profile.maxSqlPacketBytes() || leafLength > profile.maxRetainedBytes())
            throw new IOException("Complete NativeResultPublication leaf exceeds the exact SQL/retained profile");
        return readAccountedFixed(input, leafLength, work, "complete NativeResultPublication leaf");
    }

    private static void validateCarrierRootFields(MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier,
            String saveIdentity, String loaderIdentity) throws IOException {
        if (!carrier.saveIdentity().equals(saveIdentity) || !carrier.loaderIdentity().equals(loaderIdentity))
            throw new IOException("SOURCE receipt save/loader identities differ from its canonical CARRIER key");
    }

    private static void validateReceiptTuple(UUID write, UUID workflow, UUID binding,
            String saveIdentity, String loaderIdentity,
            MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier, int slot,
            long payloadLength, byte[] payloadSha256, DecodedInlineArtifactIdentityData payloadArtifact,
            UndoKind undoKind, long undoLength, byte[] undoContentSha256,
            DecodedInlineArtifactIdentityData undoArtifact, RegionFileStampData before,
            RegionFileStampData after, WindowsMarkerIo.FileIdentity regionIdentity,
            SourceLimitProfile profile) throws IOException {
        try {
            requireUuid(write, "SOURCE receipt write UUID");
            requireUuid(workflow, "SOURCE receipt child workflow UUID");
            requireUuid(binding, "SOURCE receipt binding UUID");
            requireWireText(saveIdentity, profile, "SOURCE receipt save identity");
            requireWireText(loaderIdentity, profile, "SOURCE receipt loader identity");
        } catch (IllegalArgumentException malformed) {
            throw new IOException("SOURCE receipt contains a malformed immutable identity", malformed);
        }
        if (carrier == null || !carrier.saveIdentity().equals(saveIdentity)
            || !carrier.loaderIdentity().equals(loaderIdentity))
            throw new IOException("SOURCE receipt canonical CARRIER key differs from its save/loader identity");
        int chunkX = carrier.chunkX(), chunkZ = carrier.chunkZ();
        long expectedSlot = (long) Math.floorMod(chunkX, 32) + 32L * Math.floorMod(chunkZ, 32);
        if (slot < 0 || slot >= 1024 || expectedSlot != slot)
            throw new IOException("SOURCE receipt slot differs from floorMod of its canonical CARRIER chunk coordinates");
        if (payloadArtifact.role != InlineArtifactRole.PAYLOAD || !write.equals(payloadArtifact.writeUuid)
            || payloadArtifact.byteLength != payloadLength
            || payloadArtifact.byteLength > profile.maxSqlPacketBytes()
            || payloadArtifact.byteLength > profile.maxRetainedBytes()
            || !java.security.MessageDigest.isEqual(payloadArtifact.contentSha256, payloadSha256))
            throw new IOException("PAYLOAD ArtifactIdentity differs from the complete receipt payload length/hash/write tuple");
        if (undoArtifact.role != InlineArtifactRole.UNDO || !write.equals(undoArtifact.writeUuid))
            throw new IOException("UNDO ArtifactIdentity differs from the fixed receipt write/role");
        if (undoArtifact.byteLength > profile.maxSqlPacketBytes() || undoArtifact.byteLength > profile.maxRetainedBytes())
            throw new IOException("UNDO ArtifactIdentity exceeds the exact SQL/retained artifact profile");
        if (undoKind == UndoKind.FULL_CHUNK) {
            if (undoLength <= 0L || undoContentSha256 == null || undoContentSha256.length != SHA256_BYTES
                || undoLength > profile.maxChunkBytes() || undoLength > profile.maxRetainedBytes()
                || undoArtifact.byteLength != undoLength
                || !java.security.MessageDigest.isEqual(undoArtifact.contentSha256, undoContentSha256))
                throw new IOException("FULL_CHUNK receipt undo length/hash differs from its complete UNDO ArtifactIdentity");
        } else if (undoKind == UndoKind.SLOT_ABSENT) {
            if (undoLength != 0L || undoContentSha256 != null || undoArtifact.byteLength <= 0L)
                throw new IOException("SLOT_ABSENT receipt requires zero content length/absent hash and a positive descriptor artifact");
        } else {
            throw new IOException("Unknown frozen SOURCE receipt undo kind");
        }
        validateRegionStamp(before, profile);
        validateRegionStamp(after, profile);
        if (before.byteLength() < 8192L || after.byteLength() < 8192L
            || before.byteLength() % 4096L != 0L || after.byteLength() % 4096L != 0L
            || !regionIdentity.equals(before.identity()) || !regionIdentity.equals(after.identity()))
            throw new IOException("RegionWriteResults before/after FileIdentity differs from the receipt region identity");
    }

    private static void consumeForceAndFinish(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation, boolean outer, Consumer<ReceiptData> consumer,
            SourceForestAllocationBudget.Scope rootWork, UUID write, UUID workflow, UUID binding,
            String saveIdentity, String loaderIdentity, byte[] carrierKeyBytes,
            MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier, WindowsMarkerIo.FileIdentity regionIdentity,
            int slot, long writeGeneration, long payloadLength, byte[] payloadSha256,
            DecodedInlineArtifactIdentityData payloadArtifact, UndoKind undoKind, long undoLength,
            byte[] undoContentSha256, DecodedInlineArtifactIdentityData undoArtifact,
            DecodedMemberCoverageData memberCoverage, DecodedObligationRevisionsData obligations,
            long dataWriteCallsReturned, long headerWriteCallsReturned, long encoderSubmittedBytes,
            long outputCloseCallsReturned, RegionFileStampData before, RegionFileStampData after,
            byte[] readbackHeader, byte[] readbackCompressedChunk, byte[] readbackDecodedRootNbt,
            byte[] readbackPayloadSha256, String forceProtocol, byte[] forceEvidence,
            byte[] publicationLeaf, int prefixEnd, int diagnosticOffset, int diagnosticLength,
            int diagnosticValueOffset, int diagnosticValueLength) throws IOException {
        if (!REGION_FORCE_PROTOCOL.equals(forceProtocol))
            throw new IOException("Unknown frozen RegionWriteResults force protocol");
        if (encoderSubmittedBytes != payloadLength)
            throw new IOException("RegionWriteResults encoderSubmittedBytes differs from the complete receipt payloadLength");
        if ((long) readbackDecodedRootNbt.length != payloadLength)
            throw new IOException("RegionWriteResults complete decoded root byte length differs from the receipt payloadLength");
        if (!java.security.MessageDigest.isEqual(payloadSha256, readbackPayloadSha256))
            throw new IOException("Receipt payload SHA-256 differs from RegionWriteResults readbackPayloadSha256");
        byte[] decodedRootSha = digestSha256(readbackDecodedRootNbt, rootWork,
            "complete decoded readback root NBT SHA-256");
        if (!java.security.MessageDigest.isEqual(payloadSha256, decodedRootSha))
            throw new IOException("Complete decoded readback root NBT bytes do not match the receipt payload SHA-256");
        ReadOnlyRegionCatalog.validateSourceReceiptTargetRecordData(readbackHeader, after.byteLength(), slot,
            readbackCompressedChunk, readbackDecodedRootNbt, exactProfile, originalAllocation);
        byte[] factsSha = computeReceiptFactsSha256(dataWriteCallsReturned, headerWriteCallsReturned,
            encoderSubmittedBytes, outputCloseCallsReturned, before, after, readbackHeader,
            readbackCompressedChunk, readbackDecodedRootNbt, readbackPayloadSha256, rootWork);

        ManagedRecallSourceWriteOwner.consumeForceData(forceEvidence, exactProfile, originalAllocation, force -> {
            try {
                validateForceCrossFields(force, write, workflow, binding, writeGeneration, carrierKeyBytes,
                    regionIdentity, before, after, memberCoverage, factsSha, exactProfile);
                if (outer) {
                    consumePublicationData(publicationLeaf, write, exactProfile, originalAllocation, publication -> {
                        try {
                            validatePublicationCrossFields(publication, force, carrier, write, workflow, binding,
                                writeGeneration, exactProfile);
                            ReceiptData data = new ReceiptData(exactProfile, originalAllocation, true, exactFrame,
                                write, workflow, binding, saveIdentity, loaderIdentity, carrierKeyBytes, carrier,
                                regionIdentity, slot, writeGeneration, payloadLength, payloadSha256, payloadArtifact,
                                undoKind, undoLength, undoContentSha256, undoArtifact, memberCoverage, obligations,
                                dataWriteCallsReturned, headerWriteCallsReturned, encoderSubmittedBytes,
                                outputCloseCallsReturned, before, after, readbackHeader, readbackCompressedChunk,
                                readbackDecodedRootNbt, readbackPayloadSha256, forceProtocol, forceEvidence, force,
                                publication, prefixEnd, diagnosticOffset, diagnosticLength, diagnosticValueOffset,
                                diagnosticValueLength);
                            deliverValidatedReceipt(data, consumer, rootWork, exactProfile, originalAllocation);
                        } catch (IOException failure) {
                            throw new java.io.UncheckedIOException(failure);
                        }
                    });
                } else {
                    ReceiptData data = new ReceiptData(exactProfile, originalAllocation, false, exactFrame,
                        write, workflow, binding, saveIdentity, loaderIdentity, carrierKeyBytes, carrier,
                        regionIdentity, slot, writeGeneration, payloadLength, payloadSha256, payloadArtifact,
                        undoKind, undoLength, undoContentSha256, undoArtifact, memberCoverage, obligations,
                        dataWriteCallsReturned, headerWriteCallsReturned, encoderSubmittedBytes,
                        outputCloseCallsReturned, before, after, readbackHeader, readbackCompressedChunk,
                        readbackDecodedRootNbt, readbackPayloadSha256, forceProtocol, forceEvidence, force,
                        null, prefixEnd, diagnosticOffset, diagnosticLength, diagnosticValueOffset,
                        diagnosticValueLength);
                    deliverValidatedReceipt(data, consumer, rootWork, exactProfile, originalAllocation);
                }
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        });
    }

    private static void validateForceCrossFields(ManagedRecallSourceWriteOwner.DecodedForceData force,
            UUID write, UUID workflow, UUID binding, long writeGeneration, byte[] carrierKeyBytes,
            WindowsMarkerIo.FileIdentity regionIdentity, RegionFileStampData before, RegionFileStampData after,
            DecodedMemberCoverageData memberCoverage, byte[] factsSha, SourceLimitProfile profile) throws IOException {
        if (!force.write().equals(write) || !force.workflow().equals(workflow) || !force.binding().equals(binding)
            || force.writeGeneration() != writeGeneration || !force.carrierKeyMatches(carrierKeyBytes)
            || !force.limitProfileSha256Matches(profile.sha256())
            || !force.memberCoverageSha256Matches(memberCoverage.frameSha256)
            || !force.regionIdentity().equals(regionIdentity)
            || !force.before().fileIdentity().equals(before.identity())
            || force.before().byteLength() != before.byteLength()
            || force.before().lastWriteFileTimeBits() != before.lastWriteFileTimeBits()
            || !force.after().fileIdentity().equals(after.identity())
            || force.after().byteLength() != after.byteLength()
            || force.after().lastWriteFileTimeBits() != after.lastWriteFileTimeBits()
            || !force.resultFactsSha256Matches(factsSha))
            throw new IOException("Region force DATA identity, profile, key, member coverage, stamp, or resultFactsSha256 differs from the typed receipt core");
    }

    private static void validatePublicationCrossFields(DecodedPublicationData publication,
            ManagedRecallSourceWriteOwner.DecodedForceData force,
            MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier, UUID write, UUID workflow, UUID binding,
            long writeGeneration, SourceLimitProfile profile) throws IOException {
        if (!publication.writeUuid().equals(write) || !publication.childWorkflowUuid().equals(workflow)
            || !publication.bindingUuid().equals(binding) || publication.writeGeneration() != writeGeneration
            || !publication.profileSha256Matches(profile.sha256())
            || !sameCarrierKeyFields(publication.carrierKey, carrier)
            || !publication.bootUuid().equals(force.boot())
            || publication.bootGeneration() != force.bootGeneration()
            || publication.custodyGeneration() != force.custodyGeneration())
            throw new IOException("NativeResultPublication DATA identity/profile/key/boot lineage differs from the exact force and receipt tuple");
    }

    private static boolean sameCarrierKeyFields(MySqlRecoveryResourceKey.DecodedCarrierKeyData left,
            MySqlRecoveryResourceKey.DecodedCarrierKeyData right) {
        return left != null && right != null && left.saveIdentity().equals(right.saveIdentity())
            && left.loaderIdentity().equals(right.loaderIdentity())
            && left.regionIdentity().equals(right.regionIdentity())
            && left.chunkX() == right.chunkX() && left.chunkZ() == right.chunkZ();
    }

    private static byte[] computeReceiptFactsSha256(long dataWriteCallsReturned, long headerWriteCallsReturned,
            long encoderSubmittedBytes, long outputCloseCallsReturned, RegionFileStampData before,
            RegionFileStampData after, byte[] header, byte[] targetRecord, byte[] decodedRoot,
            byte[] payloadSha256, SourceForestAllocationBudget.Scope work) throws IOException {
        work.charge(256L, "RegionWriteResults resultFacts canonical SHA-256 state BEFORE allocation");
        java.security.MessageDigest digest;
        try { digest = java.security.MessageDigest.getInstance("SHA-256"); }
        catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IOException("SHA-256 provider unavailable for RegionWriteResults resultFacts", impossible);
        }
        DataOutputStream canonical = new DataOutputStream(new java.security.DigestOutputStream(OutputStream.nullOutputStream(), digest));
        canonical.writeLong(dataWriteCallsReturned);
        canonical.writeLong(headerWriteCallsReturned);
        canonical.writeLong(encoderSubmittedBytes);
        canonical.writeLong(outputCloseCallsReturned);
        writeRegionStamp(canonical, before);
        writeRegionStamp(canonical, after);
        canonical.write(header);
        canonical.writeInt(targetRecord.length); canonical.write(targetRecord);
        canonical.writeInt(decodedRoot.length); canonical.write(decodedRoot);
        canonical.write(payloadSha256);
        canonical.flush();
        return digest.digest();
    }

    private static void deliverValidatedReceipt(ReceiptData data, Consumer<ReceiptData> consumer,
            SourceForestAllocationBudget.Scope rootWork, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        try {
            validateReceiptSerializedBytes(data, rootWork, profile, allocation);
            consumer.accept(data);
        } finally {
            data.expire();
        }
    }

    /** Re-encodes one strict core only while its original core callback is active. */
    public static void encodeReceiptCoreData(DecodedReceiptCoreData data, DataOutputStream output,
            SourceLimitProfile exactProfile, SourceForestAllocationBudget originalAllocation) throws IOException {
        Objects.requireNonNull(data, "strictly decoded receipt core DATA");
        encodeReceiptData(data.data, output, exactProfile, originalAllocation, false);
    }

    /** Re-encodes the complete outer receipt only while its original outer callback is active. */
    public static void encodeSourceWriteReceiptData(DecodedSourceWriteReceiptData data, DataOutputStream output,
            SourceLimitProfile exactProfile, SourceForestAllocationBudget originalAllocation) throws IOException {
        Objects.requireNonNull(data, "strictly decoded complete SOURCE write receipt DATA");
        encodeReceiptData(data.data, output, exactProfile, originalAllocation, true);
    }

    private static void encodeReceiptData(ReceiptData data, DataOutputStream output,
            SourceLimitProfile exactProfile, SourceForestAllocationBudget originalAllocation,
            boolean outer) throws IOException {
        Objects.requireNonNull(output, "borrowed strict receipt DATA destination");
        data.requireActive(outer);
        requireAllocation(exactProfile, originalAllocation);
        if (data.profile != exactProfile || data.allocation != originalAllocation)
            throw new IOException("Receipt encoder requires the exact original profile and allocation object");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(originalAllocation, exactProfile)) {
            work.charge(2_048L, "receipt encoder typed validation, digest, and bounded comparison state BEFORE allocation");
            validateReceiptSerializedBytes(data, work, exactProfile, originalAllocation);
            output.write(data.exactFrame);
        }
    }

    private static void validateReceiptSerializedBytes(ReceiptData data,
            SourceForestAllocationBudget.Scope work, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        data.requireActive(data.outer);
        long textCharacters = Math.addExact((long) data.saveIdentity.length(), data.loaderIdentity.length());
        textCharacters = Math.addExact(textCharacters, data.forceProtocol.length());
        textCharacters = Math.addExact(textCharacters, SOURCE_WRITE_RECEIPT_DOMAIN.length());
        textCharacters = Math.addExact(textCharacters, data.payloadArtifact.basename.length());
        textCharacters = Math.addExact(textCharacters, data.undoArtifact.basename.length());
        textCharacters = Math.addExact(textCharacters, data.carrier.regionIdentity().length());
        for (DecodedObligationRevisionData obligation : data.obligations.values)
            textCharacters = Math.addExact(textCharacters, obligation.purpose.length());
        long estimatedWriterWorkspace = Math.addExact(4_096L, Math.multiplyExact(textCharacters, 8L));
        estimatedWriterWorkspace = Math.addExact(estimatedWriterWorkspace,
            Math.multiplyExact((long) data.carrierKeyBytes.length, 3L));
        work.charge(estimatedWriterWorkspace,
            "typed receipt field writer UTF-8/CARRIER workspace BEFORE serialization");
        ExactRangeOutputStream wholeFrame = new ExactRangeOutputStream(data.exactFrame, 0, data.exactFrame.length,
            "complete strict SOURCE receipt canonical re-encode");
        ReceiptCoreDigest core = new ReceiptCoreDigest(data.exactFrame, data.prefixEnd,
            data.diagnosticOffset, data.diagnosticLength, work);
        DataOutputStream corePrefix = new DataOutputStream(new TeeOutputStream(wholeFrame, core.prefixSink()));
        writeReceiptCorePrefix(data, corePrefix, profile, allocation);
        corePrefix.flush();
        if (data.outer) {
            DataOutputStream publicationOutput = new DataOutputStream(wholeFrame);
            encodePublicationData(data.publication, publicationOutput, profile, allocation);
            publicationOutput.flush();
        } else if (data.publication != null) {
            throw new IOException("Core-only receipt DATA unexpectedly contains NativeResultPublication");
        }
        DataOutputStream diagnosticOutput = new DataOutputStream(new TeeOutputStream(wholeFrame, core.diagnosticSink()));
        writeReceiptDiagnostic(data, diagnosticOutput);
        diagnosticOutput.flush();
        wholeFrame.requireComplete();
        core.requireComplete();
        byte[] coreSha = core.finishSha256();
        long coreLength = core.byteLength();
        if (coreLength != Math.addExact((long) data.prefixEnd, data.diagnosticLength))
            throw new IOException("Typed receipt core reconstructed length differs from its exact prefix plus diagnostic option");
        if (data.outer) {
            DecodedInlineArtifactIdentityData artifact = data.publication.coreArtifact;
            if (data.publication.coreLength != coreLength || !data.publication.coreSha256Matches(coreSha)
                || artifact.byteLength != coreLength
                || !java.security.MessageDigest.isEqual(artifact.contentSha256, coreSha))
                throw new IOException("Typed receipt core reconstruction length/SHA-256 differs from NativeResultPublication DATA and RESULT_CORE ArtifactIdentity");
        } else if (coreLength != data.exactFrame.length) {
            throw new IOException("Core-only receipt reconstruction length differs from its complete exact frame");
        }
    }

    private static void writeReceiptCorePrefix(ReceiptData data, DataOutputStream output,
            SourceLimitProfile profile, SourceForestAllocationBudget allocation) throws IOException {
        writeText(output, SOURCE_WRITE_RECEIPT_DOMAIN);
        writeUuid(output, data.write); writeUuid(output, data.workflow); writeUuid(output, data.binding);
        writeText(output, data.saveIdentity); writeText(output, data.loaderIdentity);
        writeCanonicalCarrierFrame(output, data, profile);
        writeIdentity(output, data.regionIdentity);
        output.writeInt(data.slot);
        output.writeLong(data.writeGeneration);
        output.writeLong(data.payloadLength);
        output.write(data.payloadSha256);
        writeInlineArtifactIdentityData(output, data.payloadArtifact);
        output.writeByte(data.undoKind.wireCode());
        output.writeLong(data.undoLength);
        if (data.undoContentSha256 == null) output.writeByte(0);
        else { output.writeByte(1); output.write(data.undoContentSha256); }
        writeInlineArtifactIdentityData(output, data.undoArtifact);
        writeCanonicalMemberCoverage(output, data.memberCoverage);
        writeCanonicalObligationRevisions(output, data.obligations, profile);

        output.writeLong(data.dataWriteCallsReturned);
        output.writeLong(data.headerWriteCallsReturned);
        output.writeLong(data.encoderSubmittedBytes);
        output.writeLong(data.outputCloseCallsReturned);
        writeRegionStamp(output, data.before);
        writeRegionStamp(output, data.after);
        output.write(data.readbackHeader);
        output.writeInt(data.readbackCompressedChunk.length);
        output.write(data.readbackCompressedChunk);
        output.writeInt(data.readbackDecodedRootNbt.length);
        output.write(data.readbackDecodedRootNbt);
        output.write(data.readbackPayloadSha256);
        writeText(output, data.forceProtocol);
        output.writeInt(data.forceEvidence.length);
        BoundedExactCompareOutputStream forceSink = new BoundedExactCompareOutputStream(output,
            data.forceEvidence, 0, data.forceEvidence.length,
            "canonical nested original SOURCE force evidence");
        DataOutputStream forceOutput = new DataOutputStream(forceSink);
        ManagedRecallSourceWriteOwner.encodeForceData(data.force, forceOutput, profile, allocation);
        forceOutput.flush();
        forceSink.requireComplete();
    }

    private static void writeCanonicalCarrierFrame(DataOutputStream output, ReceiptData data,
            SourceLimitProfile profile) throws IOException {
        MySqlRecoveryResourceKey carrier;
        try {
            carrier = MySqlRecoveryResourceKey.carrier(data.saveIdentity, data.loaderIdentity,
                data.carrier.regionIdentity(), data.carrier.chunkX(), data.carrier.chunkZ(), profile);
        } catch (RuntimeException malformed) {
            throw new IOException("Cannot reconstruct the typed receipt canonical CARRIER key", malformed);
        }
        int length = carrier.canonicalLength();
        if (length != data.carrierKeyBytes.length)
            throw new IOException("Typed receipt canonical CARRIER length differs from its exact decoded key frame");
        output.writeInt(length);
        BoundedCountingOutputStream bounded = new BoundedCountingOutputStream(output, length);
        DataOutputStream carrierOutput = new DataOutputStream(bounded);
        carrier.writeCanonicalTo(carrierOutput);
        carrierOutput.flush();
        bounded.requireComplete();
    }

    private static void writeCanonicalMemberCoverage(DataOutputStream output,
            DecodedMemberCoverageData memberCoverage) throws IOException {
        output.writeInt(memberCoverage.exactFrame.length);
        output.writeInt(memberCoverage.forestBytes.length);
        output.write(memberCoverage.forestBytes);
        output.write(memberCoverage.forestSha256);
        output.writeInt(memberCoverage.indexBytes.length);
        output.write(memberCoverage.indexBytes);
        output.write(memberCoverage.indexSha256);
    }

    private static void writeCanonicalObligationRevisions(DataOutputStream output,
            DecodedObligationRevisionsData obligations, SourceLimitProfile profile) throws IOException {
        output.writeInt(obligations.values.size());
        for (DecodedObligationRevisionData item : obligations.values) {
            int purposeLength = utf8LengthStrict(item.purpose, maxTextFrame(profile));
            long itemLength = 3L * 16L + SHA256_BYTES + Integer.BYTES + (long) item.carrierKey.length
                + Integer.BYTES + purposeLength + Long.BYTES;
            if (itemLength <= 0L || itemLength > Integer.MAX_VALUE || itemLength > profile.maxSqlPacketBytes()
                || itemLength > profile.maxRetainedBytes())
                throw new IOException("Canonical obligation revision item exceeds the exact bounded B field");
            output.writeInt(Math.toIntExact(itemLength));
            writeUuid(output, item.binding); writeUuid(output, item.unit); writeUuid(output, item.forest);
            output.write(item.carrierKeyHash);
            output.writeInt(item.carrierKey.length); output.write(item.carrierKey);
            writeText(output, item.purpose);
            output.writeLong(item.revision);
        }
    }

    private static void writeReceiptDiagnostic(ReceiptData data, DataOutputStream output) throws IOException {
        if (data.diagnosticLength == 1) {
            if (data.exactFrame[data.diagnosticOffset] != 0)
                throw new IOException("Absent original diagnostic option is not canonical tag zero");
            output.writeByte(0);
            return;
        }
        if (data.diagnosticLength < 5 || data.diagnosticValueOffset < 0 || data.diagnosticValueLength < 0
            || data.exactFrame[data.diagnosticOffset] != 1
            || readInt(data.exactFrame, data.diagnosticOffset + 1) != data.diagnosticValueLength)
            throw new IOException("Present original diagnostic option is not an exact canonical O(B) frame");
        output.writeByte(1);
        output.writeInt(data.diagnosticValueLength);
        output.write(data.exactFrame, data.diagnosticValueOffset, data.diagnosticValueLength);
    }

    /** Strictly consumes the specified failure diagnostic DATA without constructing Throwable objects. */
    public static void consumeFailureDiagnosticData(byte[] exactFrame, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget allocation, Consumer<DecodedFailureDiagnosticData> consumer) throws IOException {
        Objects.requireNonNull(consumer, "synchronous failure diagnostic DATA consumer");
        requireDataFrame(exactFrame, exactProfile, allocation, "source failure diagnostic");
        try (SourceForestAllocationBudget.Scope work = SourceForestAllocationBudget.Scope.open(allocation, exactProfile)) {
            work.charge(1_024L, "failure diagnostic parser and graph workspace BEFORE allocation");
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
            if (!"FP_SOURCE_FAILURE_DIAGNOSTIC_BE_V1".equals(readAccountedText(input,
                    maxTextFrame(exactProfile), work, "failure diagnostic domain")))
                throw new IOException("Wrong source failure diagnostic domain");
            UUID diagnostic = readUuid(input, "failure diagnostic UUID");
            UUID binding = readUuid(input, "failure diagnostic binding UUID");
            UUID workflow = readUuid(input, "failure diagnostic child workflow UUID");
            UUID write = readUuid(input, "failure diagnostic write UUID");
            UUID producerAttempt = readUuid(input, "failure diagnostic producer attempt UUID");
            UUID boot = readUuid(input, "failure diagnostic boot UUID");
            long bootGeneration = readPositive(input, "failure diagnostic boot generation");
            int producer = input.readUnsignedByte();
            if (producer != 1 && producer != 2) throw new IOException("Unknown frozen SOURCE failure diagnostic producer");
            int stageLimit = producer == 1 ? 18 : 11;
            int failedStage = input.readInt();
            if (failedStage < 1 || failedStage > stageLimit)
                throw new IOException("Failure diagnostic failedStage is outside its producer's closed stage table");
            int effect = input.readUnsignedByte();
            int resource = input.readUnsignedByte();
            if (effect < 1 || effect > 3 || resource < 1 || resource > 2)
                throw new IOException("Failure diagnostic effect/resource knowledge code is unknown");
            int stageCount = input.readInt();
            if (stageCount <= 0 || stageCount > stageLimit || stageCount > input.available() / 26)
                throw new IOException("Failure diagnostic stage progress count is invalid or truncated");
            work.charge(Math.addExact(256L, Math.multiplyExact((long) stageCount, 96L)),
                "failure diagnostic stage progress DATA list BEFORE allocation");
            List<FailureStageProgressData> stages = new ArrayList<>(stageCount);
            int previousStageCode = 0;
            long previousOrdinal = 0L;
            FailureStageProgressData failed = null;
            for (int index = 0; index < stageCount; index++) {
                byte[] stageFrame = readProfileFrame(input, exactProfile, work, "B-framed failure stage progress");
                DataInputStream stageInput = new DataInputStream(new ByteArrayInputStream(stageFrame));
                int code = stageInput.readInt();
                int state = stageInput.readUnsignedByte();
                long entered = readNonnegative(stageInput, "failure stage entered ordinal");
                long terminal = readNonnegative(stageInput, "failure stage terminal ordinal");
                Integer failureNode = readOptionalInt(stageInput, "failure stage failure-node option");
                requireEnd(stageInput, "failure stage progress");
                if (code <= previousStageCode || code > stageLimit || state < 0 || state > 3)
                    throw new IOException("Failure diagnostic stage codes are unknown, duplicated, or unsorted");
                validateStageProgressState(state, entered, terminal, failureNode);
                if (entered > 0L) {
                    if (entered <= previousOrdinal) throw new IOException("Failure stage entered ordinals are not in actual event order");
                    previousOrdinal = entered;
                }
                if (terminal > 0L) {
                    if (terminal <= previousOrdinal) throw new IOException("Failure stage terminal ordinals are not in actual event order");
                    previousOrdinal = terminal;
                }
                FailureStageProgressData value = new FailureStageProgressData(code, state, entered, terminal, failureNode);
                stages.add(value);
                if (code == failedStage) failed = value;
                previousStageCode = code;
            }
            int primaryNode = input.readInt();
            if (primaryNode != 0)
                throw new IOException("Frozen Throwable graph primary node must be first-encounter id 0");
            int nodeCount = input.readInt();
            if (nodeCount <= 0 || (long) nodeCount > exactProfile.maxMembers() || nodeCount > input.available() / 24)
                throw new IOException("Failure diagnostic Throwable node count is invalid or truncated");
            work.charge(Math.addExact(512L, Math.multiplyExact((long) nodeCount, 256L)),
                "failure diagnostic Throwable graph nodes BEFORE allocation");
            List<FailureThrowableNodeData> nodes = new ArrayList<>(nodeCount);
            for (int index = 0; index < nodeCount; index++) {
                byte[] nodeFrame = readProfileFrame(input, exactProfile, work, "B-framed Throwable node");
                nodes.add(parseFailureThrowableNode(nodeFrame, index, exactProfile, work));
            }
            requireEnd(input, "source failure diagnostic");
            if (failed == null || failed.state() != 3 || failed.failureNode() == null
                || failed.failureNode() != primaryNode)
                throw new IOException("Failure diagnostic failedStage does not identify its primary thrown-node progress");
            validateFailureGraph(stages, nodes, primaryNode, work);
            byte[] frameSha = digestSha256(exactFrame, work, "failure diagnostic frame SHA-256");
            consumer.accept(new DecodedFailureDiagnosticData(exactProfile, exactFrame, frameSha, diagnostic,
                binding, workflow, write, producerAttempt, boot, bootGeneration, producer, failedStage,
                effect, resource, stages, primaryNode, nodes));
        } catch (java.io.UncheckedIOException failure) {
            throw unwrapCheckedIoPreservingSuppressed(failure);
        }
    }

    private static FailureThrowableNodeData parseFailureThrowableNode(byte[] exactFrame, int expectedNodeId,
            SourceLimitProfile profile, SourceForestAllocationBudget.Scope work) throws IOException {
        work.charge(256L, "Throwable node parser wrapper BEFORE allocation");
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(exactFrame));
        int nodeId = input.readInt();
        if (nodeId != expectedNodeId) throw new IOException("Throwable node IDs must be continuous first-encounter indices");
        String exceptionClass = readWireText(input, profile, work, "Throwable exception class");
        int messagePolicy = input.readUnsignedByte();
        if (messagePolicy != 1) throw new IOException("Throwable messages must use MESSAGE_NOT_SERIALIZED policy 1");
        Integer causeNode = readOptionalInt(input, "Throwable cause-node option");
        int suppressedCount = input.readInt();
        if (suppressedCount < 0 || (long) suppressedCount > profile.maxMembers()
            || suppressedCount > input.available() / Integer.BYTES)
            throw new IOException("Throwable suppressed-node vector count is invalid or truncated");
        work.charge(Math.addExact(128L, Math.multiplyExact((long) suppressedCount, 24L)),
            "Throwable suppressed graph edges BEFORE allocation");
        List<Integer> suppressed = new ArrayList<>(suppressedCount);
        for (int index = 0; index < suppressedCount; index++) {
            int value = input.readInt();
            if (value < 0) throw new IOException("Throwable suppressed-node reference must be nonnegative");
            suppressed.add(value);
        }
        int stackCount = input.readInt();
        if (stackCount < 0 || (long) stackCount > profile.maxMembers() || stackCount > input.available() / 19)
            throw new IOException("Throwable stack frame count is invalid or truncated");
        work.charge(Math.addExact(128L, Math.multiplyExact((long) stackCount, 128L)),
            "Throwable stack frame DATA list BEFORE allocation");
        List<FailureStackFrameData> stack = new ArrayList<>(stackCount);
        for (int index = 0; index < stackCount; index++) {
            byte[] frame = readProfileFrame(input, profile, work, "B-framed Throwable stack frame");
            DataInputStream frameInput = new DataInputStream(new ByteArrayInputStream(frame));
            String declaringClass = readWireText(frameInput, profile, work, "stack declaring class");
            String methodName = readWireText(frameInput, profile, work, "stack method name");
            int lineNumber = frameInput.readInt();
            int nativeCode = frameInput.readUnsignedByte();
            requireEnd(frameInput, "Throwable stack frame");
            if (nativeCode > 1) throw new IOException("Stack nativeMethod must be canonical boolean 0 or 1");
            stack.add(new FailureStackFrameData(declaringClass, methodName, lineNumber, nativeCode == 1));
        }
        FailureNativeErrorData nativeError = null;
        int nativeOption = input.readUnsignedByte();
        if (nativeOption == 1) {
            String api = readWireText(input, profile, work, "native failure API");
            UUID operation = readUuid(input, "native failure operation UUID");
            int errorBits = input.readInt();
            nativeError = new FailureNativeErrorData(api, operation, errorBits);
        } else if (nativeOption != 0) {
            throw new IOException("NativeError option must be canonical 0 or 1");
        }
        requireEnd(input, "Throwable node");
        return new FailureThrowableNodeData(nodeId, exceptionClass, messagePolicy, causeNode,
            suppressed, stack, nativeError);
    }

    private static void validateStageProgressState(int state, long entered, long terminal, Integer failureNode)
            throws IOException {
        switch (state) {
            case 0 -> {
                if (entered != 0L || terminal != 0L || failureNode != null)
                    throw new IOException("NOT_ENTERED stage must carry zero ordinals and no failure node");
            }
            case 1 -> {
                if (entered <= 0L || terminal != 0L || failureNode != null)
                    throw new IOException("ENTERED_NO_RETURN stage ordinals/failure reference are invalid");
            }
            case 2 -> {
                if (entered <= 0L || terminal <= entered || failureNode != null)
                    throw new IOException("RETURNED stage ordinals/failure reference are invalid");
            }
            case 3 -> {
                if (entered <= 0L || terminal <= entered || failureNode == null)
                    throw new IOException("THREW stage ordinals/failure reference are invalid");
            }
            default -> throw new IOException("Unknown failure stage progress state");
        }
    }

    private static void validateFailureGraph(List<FailureStageProgressData> stages,
            List<FailureThrowableNodeData> nodes, int primaryNode,
            SourceForestAllocationBudget.Scope work) throws IOException {
        int nodeCount = nodes.size();
        for (FailureStageProgressData stage : stages) {
            if (stage.failureNode() != null && stage.failureNode() >= nodeCount)
                throw new IOException("Failure stage refers to a Throwable node outside the complete graph");
        }
        for (FailureThrowableNodeData node : nodes) {
            if (node.causeNode() != null && node.causeNode() >= nodeCount)
                throw new IOException("Throwable cause reference is outside the complete graph");
            for (int index = 0; index < node.suppressedCount(); index++)
                if (node.suppressedNodeAt(index) >= nodeCount)
                    throw new IOException("Throwable suppressed reference is outside the complete graph");
        }
        long edgeCount = 0L;
        for (FailureThrowableNodeData node : nodes)
            edgeCount = Math.addExact(edgeCount, Math.addExact(node.suppressedCount(), node.causeNode() == null ? 0L : 1L));
        long traversalCharge = Math.addExact(Math.multiplyExact((long) nodeCount, 24L),
            Math.multiplyExact(edgeCount, 32L));
        work.charge(Math.addExact(256L, traversalCharge),
            "Throwable graph edge stack/first-encounter verification workspace BEFORE allocation");
        boolean[] visited = new boolean[nodeCount];
        java.util.ArrayDeque<Integer> pending = new java.util.ArrayDeque<>();
        pending.push(primaryNode);
        int nextEncounter = 0;
        while (!pending.isEmpty()) {
            int nodeId = pending.pop();
            if (visited[nodeId]) continue; // Back-references and cycles retain their original graph edges.
            if (nodeId != nextEncounter)
                throw new IOException("Throwable graph IDs do not follow first-encounter cause-before-suppressed order");
            visited[nodeId] = true;
            nextEncounter++;
            FailureThrowableNodeData node = nodes.get(nodeId);
            for (int index = node.suppressedCount() - 1; index >= 0; index--)
                pending.push(node.suppressedNodeAt(index));
            if (node.causeNode() != null) pending.push(node.causeNode());
        }
        if (nextEncounter != nodeCount)
            throw new IOException("Failure diagnostic contains unreachable Throwable nodes outside its exact primary graph");
    }

    private static Integer readOptionalInt(DataInputStream input, String field) throws IOException {
        int present = input.readUnsignedByte();
        if (present == 0) return null;
        if (present != 1) throw new IOException(field + " must use canonical option 0 or 1");
        int value = input.readInt();
        if (value < 0) throw new IOException(field + " reference must be nonnegative");
        return value;
    }

    private static void requireDescriptorInputs(UUID binding, UUID workflow, UUID write, UUID loaderObservation,
            long writeGeneration, String saveIdentity, String loaderIdentity, String regionIdentity,
            byte[] carrierKey, RegionFileStampData before, int regionX, int regionZ, int slot,
            SourceLimitProfile profile, SourceForestAllocationBudget allocation) throws IOException {
        requireAllocation(profile, allocation);
        requireUuid(binding, "source descriptor binding UUID");
        requireUuid(workflow, "source descriptor child workflow UUID");
        requireUuid(write, "source descriptor write UUID");
        requireUuid(loaderObservation, "source descriptor loader lifetime observation UUID");
        if (writeGeneration < 0L) throw new IOException("Source descriptor write generation must be nonnegative");
        requireWireText(saveIdentity, profile, "save identity");
        requireWireText(loaderIdentity, profile, "loader identity");
        requireWireText(regionIdentity, profile, "region identity");
        Objects.requireNonNull(carrierKey, "exact canonical CARRIER key frame");
        if (carrierKey.length == 0 || carrierKey.length > profile.maxSqlPacketBytes()
            || carrierKey.length > profile.maxRetainedBytes())
            throw new IOException("Exact canonical CARRIER key frame is outside the active profile");
        Objects.requireNonNull(before, "exact original RegionFileStamp");
        requireSlotCoordinates(regionX, regionZ, slot);
        validateRegionStamp(before, profile);
    }

    private static void validateCarrierFrameForSlot(byte[] exactCarrierKey, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation, String saveIdentity, String loaderIdentity,
            String regionIdentity, int regionX, int regionZ, int slot) throws IOException {
        try {
            MySqlRecoveryResourceKey.consumeCarrierKeyData(exactCarrierKey, profile, allocation, carrier -> {
                try { validateCarrierFields(carrier, saveIdentity, loaderIdentity, regionIdentity, regionX, regionZ, slot); }
                catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
            });
        } catch (java.io.UncheckedIOException failure) {
            throw failure.getCause();
        }
    }

    private static void validateCanonicalCarrierKey(byte[] exactCarrierKey, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        try { MySqlRecoveryResourceKey.consumeCarrierKeyData(exactCarrierKey, profile, allocation, ignored -> { }); }
        catch (java.io.UncheckedIOException failure) { throw unwrapCheckedIoPreservingSuppressed(failure); }
    }

    private static void validateCarrierFields(MySqlRecoveryResourceKey.DecodedCarrierKeyData carrier,
            String saveIdentity, String loaderIdentity, String regionIdentity, int regionX, int regionZ, int slot)
            throws IOException {
        Objects.requireNonNull(carrier, "strictly decoded canonical CARRIER key DATA");
        if (!Objects.equals(saveIdentity, carrier.saveIdentity())
            || !Objects.equals(loaderIdentity, carrier.loaderIdentity())
            || !Objects.equals(regionIdentity, carrier.regionIdentity()))
            throw new IOException("SOURCE descriptor text differs from its exact canonical CARRIER key DATA");
        requireSlotCoordinates(regionX, regionZ, slot);
        int expectedLocalX = slot & 31, expectedLocalZ = slot >>> 5;
        int expectedChunkX, expectedChunkZ;
        try {
            expectedChunkX = Math.toIntExact(Math.addExact(Math.multiplyExact((long) regionX, 32L), expectedLocalX));
            expectedChunkZ = Math.toIntExact(Math.addExact(Math.multiplyExact((long) regionZ, 32L), expectedLocalZ));
        } catch (ArithmeticException overflow) {
            throw new IOException("SOURCE descriptor region/slot coordinates overflow chunk coordinates", overflow);
        }
        if (carrier.chunkX() != expectedChunkX || carrier.chunkZ() != expectedChunkZ
            || Math.floorDiv(carrier.chunkX(), 32) != regionX || Math.floorDiv(carrier.chunkZ(), 32) != regionZ
            || Math.floorMod(carrier.chunkX(), 32) != expectedLocalX
            || Math.floorMod(carrier.chunkZ(), 32) != expectedLocalZ)
            throw new IOException("SOURCE descriptor region/slot coordinates differ from the full canonical CARRIER key");
    }

    private static void requireSlotCoordinates(int regionX, int regionZ, int slot) throws IOException {
        if (slot < 0 || slot >= 1024) throw new IOException("SOURCE region slot index must be in [0,1024)");
        int localX = slot & 31, localZ = slot >>> 5;
        try {
            int chunkX = Math.toIntExact(Math.addExact(Math.multiplyExact((long) regionX, 32L), localX));
            int chunkZ = Math.toIntExact(Math.addExact(Math.multiplyExact((long) regionZ, 32L), localZ));
            if (Math.floorDiv(chunkX, 32) != regionX || Math.floorDiv(chunkZ, 32) != regionZ
                || Math.floorMod(chunkX, 32) != localX || Math.floorMod(chunkZ, 32) != localZ)
                throw new IOException("SOURCE region/slot coordinate round trip failed");
        } catch (ArithmeticException overflow) {
            throw new IOException("SOURCE region/slot coordinate overflows chunk coordinates", overflow);
        }
    }

    private static boolean isSourceObligationPurpose(String value) {
        return "OLD_HOLD".equals(value) || "TARGET_STAGED".equals(value)
            || "CURRENT_HOLD".equals(value) || "OLD_RETIRE".equals(value) || "UNDO".equals(value);
    }

    private static int compareObligationRevisionData(DecodedObligationRevisionData left,
            DecodedObligationRevisionData right) {
        int compared = compareUuidUnsigned(left.binding, right.binding);
        if (compared != 0) return compared;
        compared = compareUuidUnsigned(left.unit, right.unit);
        if (compared != 0) return compared;
        compared = compareUuidUnsigned(left.forest, right.forest);
        if (compared != 0) return compared;
        compared = compareUnsignedBytes(left.carrierKeyHash, right.carrierKeyHash);
        if (compared != 0) return compared;
        return compareAscii(left.purpose, right.purpose);
    }

    private static int compareUuidUnsigned(UUID left, UUID right) {
        int high = Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
        return high != 0 ? high : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
    }

    private static int compareUnsignedBytes(byte[] left, byte[] right) {
        int limit = Math.min(left.length, right.length);
        for (int index = 0; index < limit; index++) {
            int compared = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (compared != 0) return compared;
        }
        return Integer.compare(left.length, right.length);
    }

    private static int compareAscii(String left, String right) {
        int limit = Math.min(left.length(), right.length());
        for (int index = 0; index < limit; index++) {
            int compared = Integer.compare(left.charAt(index), right.charAt(index));
            if (compared != 0) return compared;
        }
        return Integer.compare(left.length(), right.length());
    }

    private static void validateRegionStamp(RegionFileStampData stamp, SourceLimitProfile profile) throws IOException {
        if (stamp.byteLength() < 8192L || (stamp.byteLength() & 4095L) != 0L
            || stamp.byteLength() > profile.maxRegionBytes())
            throw new IOException("SOURCE RegionFileStamp length is not an aligned complete region within the exact profile");
    }

    private static void validateFullUndoHeader(byte[] header, RegionFileStampData before, int slot,
            int sectorOffset, int spanSectors, int timestampBits, long compressedBodyLength) throws IOException {
        if (header == null || header.length != 8192) throw new IOException("FULL_CHUNK requires exactly 8192 original header bytes");
        requireSlotCoordinates(0, 0, slot);
        if (sectorOffset < 2 || sectorOffset > 0x00ff_ffff || spanSectors <= 0)
            throw new IOException("FULL_CHUNK source sector offset/span is outside the native header representation");
        int word = headerWord(header, slot);
        int declaredOffset = (word >>> 8) & 0x00ff_ffff;
        int declaredCount = word & 0xff;
        if (declaredOffset != sectorOffset || declaredCount == 0)
            throw new IOException("FULL_CHUNK source header location entry differs from its exact retained slot descriptor");
        int timeOffset = Math.addExact(4096, Math.multiplyExact(slot, 4));
        int headerTimestamp = readInt(header, timeOffset);
        if (headerTimestamp != timestampBits)
            throw new IOException("FULL_CHUNK timestamp bits differ from the exact original timestamp table entry");
        if (compressedBodyLength <= 0L) throw new IOException("FULL_CHUNK compressed body must be nonempty");
        long recordLength;
        long sourceSpan;
        long recordStart;
        long recordEnd;
        try {
            recordLength = Math.addExact(compressedBodyLength, 1L); // MCA record length includes the compression byte.
            sourceSpan = Math.addExact((Math.addExact(recordLength, 4L) / 4096L), 1L); // Frozen patched 255-sector rule.
            recordStart = Math.multiplyExact((long) sectorOffset, 4096L);
            recordEnd = Math.addExact(recordStart, Math.addExact(4L, recordLength));
        } catch (ArithmeticException overflow) {
            throw new IOException("FULL_CHUNK source record length/span overflows", overflow);
        }
        if (sourceSpan != spanSectors)
            throw new IOException("FULL_CHUNK actual source span differs from the original patched record-length rule");
        if (declaredCount == 255 ? sourceSpan < 255L : sourceSpan != declaredCount)
            throw new IOException("FULL_CHUNK original header sector byte differs from the patched 255-sector rule");
        long regionSectors = before.byteLength() / 4096L;
        long endSector;
        try { endSector = Math.addExact((long) sectorOffset, spanSectors); }
        catch (ArithmeticException overflow) { throw new IOException("FULL_CHUNK source sector end overflows", overflow); }
        if (recordEnd > before.byteLength() || endSector > regionSectors
            || Math.addExact(recordLength, 4L) > Math.multiplyExact((long) spanSectors, 4096L))
            throw new IOException("FULL_CHUNK source record exceeds its exact original region file or slot span");
    }

    private static int headerWord(byte[] header, int slot) throws IOException {
        if (header == null || header.length != 8192 || slot < 0 || slot >= 1024)
            throw new IOException("Exact original region header and slot are required");
        return readInt(header, Math.multiplyExact(slot, 4));
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) | ((bytes[offset + 1] & 0xff) << 16)
            | ((bytes[offset + 2] & 0xff) << 8) | (bytes[offset + 3] & 0xff);
    }

    private static boolean rangeEquals(byte[] expected, byte[] source, int sourceOffset, int length) {
        if (expected == null || source == null || sourceOffset < 0 || length < 0
            || sourceOffset > source.length - length || expected.length != length) return false;
        for (int index = 0; index < length; index++)
            if (expected[index] != source[sourceOffset + index]) return false;
        return true;
    }

    private static long descriptorWriterCharge(String saveIdentity, String loaderIdentity) throws IOException {
        long bytes = Math.addExact((long) utf8LengthStrict(saveIdentity, Integer.MAX_VALUE),
            utf8LengthStrict(loaderIdentity, Integer.MAX_VALUE));
        long chars = Math.addExact((long) saveIdentity.length(), loaderIdentity.length());
        return Math.addExact(1_024L, Math.addExact(Math.multiplyExact(bytes, 4L),
            Math.multiplyExact(chars, 2L)));
    }

    private static byte[] readProfileFrame(DataInputStream input, SourceLimitProfile profile,
            SourceForestAllocationBudget.Scope work, String context) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > profile.maxSqlPacketBytes() || length > profile.maxRetainedBytes()
            || length > input.available()) throw new IOException(context + " has an invalid or truncated exact length");
        work.charge(Math.addExact((long) length, 64L), context + " bounded frame allocation BEFORE read");
        byte[] frame = input.readNBytes(length);
        if (frame.length != length) throw new EOFException("Truncated " + context);
        return frame;
    }

    private static byte[] readAccountedFixed(DataInputStream input, int length,
            SourceForestAllocationBudget.Scope work, String context) throws IOException {
        work.charge(Math.addExact((long) length, 64L), context + " exact byte allocation BEFORE read");
        byte[] value = input.readNBytes(length);
        if (value.length != length) throw new EOFException("Truncated " + context);
        return value;
    }

    private static byte[] readAccountedHash(DataInputStream input, SourceForestAllocationBudget.Scope work,
            String context) throws IOException {
        return readAccountedFixed(input, SHA256_BYTES, work, context);
    }

    private static UUID readUuid(DataInputStream input, String field) throws IOException {
        try { return requireUuid(new UUID(input.readLong(), input.readLong()), field); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid " + field, malformed); }
    }

    private static long readNonnegative(DataInputStream input, String field) throws IOException {
        long value = input.readLong();
        if (value < 0L) throw new IOException(field + " must be nonnegative");
        return value;
    }

    private static long readPositive(DataInputStream input, String field) throws IOException {
        long value = input.readLong();
        if (value <= 0L) throw new IOException(field + " must be positive");
        return value;
    }

    private static String readWireText(DataInputStream input, SourceLimitProfile profile,
            SourceForestAllocationBudget.Scope work, String field) throws IOException {
        String value = readAccountedText(input, maxTextFrame(profile), work, field);
        requireWireText(value, profile, field);
        return value;
    }

    private static int maxTextFrame(SourceLimitProfile profile) {
        return Math.toIntExact(Math.min(Math.min(profile.maxSqlPacketBytes(), profile.maxRetainedBytes()), Integer.MAX_VALUE));
    }

    private static void requireEnd(DataInputStream input, String context) throws IOException {
        if (input.available() != 0) throw new IOException("Trailing bytes after complete " + context + " DATA");
    }

    private static byte[] digestSha256(byte[] exactFrame, SourceForestAllocationBudget.Scope work, String context)
            throws IOException {
        work.charge(128L, context + " digest state/result BEFORE allocation");
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(exactFrame); }
        catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IOException("SHA-256 provider unavailable for " + context, impossible);
        }
    }

    private static void requireDataFrame(byte[] exactFrame, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation, String context) throws IOException {
        Objects.requireNonNull(exactFrame, "exact " + context + " frame");
        requireAllocation(profile, allocation);
        long maximum = Math.min(profile.maxSqlPacketBytes(), profile.maxRetainedBytes());
        if (exactFrame.length == 0 || exactFrame.length > maximum)
            throw new IOException("Complete " + context + " frame exceeds the exact SQL/retained byte profile");
    }

    private static void requireHash(byte[] value, String field) throws IOException {
        if (value == null || value.length != SHA256_BYTES) throw new IOException("Exact 32-byte " + field + " required");
    }

    private static void reserveForCopy(SourceForestAllocationBudget.Account allocation, SourceLimitProfile profile,
            long bytes, String context) throws IOException {
        Objects.requireNonNull(allocation, "original retained DATA allocation account");
        if (allocation.exactProfile() != profile) throw new IOException("Exact original profile account required to retain DATA copy");
        if (bytes <= 0L) throw new IOException("Positive retained DATA copy length required");
        allocation.reserveTemporary(bytes, context);
    }

    private static byte[] copyForRetention(byte[] source, SourceForestAllocationBudget.Account allocation,
            SourceLimitProfile profile, String context) throws IOException {
        Objects.requireNonNull(source, "immutable source DATA bytes");
        reserveForCopy(allocation, profile, source.length, context + " BEFORE copy");
        try { return source.clone(); }
        catch (Throwable failure) {
            releasePreserving(allocation, source.length, context + " failed copy", failure);
            rethrow(failure, context);
            throw new AssertionError("unreachable");
        }
    }

    private static IOException unwrapCheckedIoPreservingSuppressed(java.io.UncheckedIOException failure) {
        IOException cause = failure.getCause();
        for (Throwable suppressed : failure.getSuppressed())
            if (suppressed != cause) cause.addSuppressed(suppressed);
        return cause;
    }

    private static void writeCarrierFrame(DataOutputStream output, byte[] exactCarrierKey) throws IOException {
        output.writeInt(exactCarrierKey.length);
        output.write(exactCarrierKey);
    }

    private static void writeRegionStamp(DataOutputStream output, RegionFileStampData stamp) throws IOException {
        writeIdentity(output, stamp.identity());
        output.writeLong(stamp.byteLength());
        output.writeLong(stamp.lastWriteFileTimeBits());
    }

    private static RegionFileStampData readRegionStamp(DataInputStream input, SourceLimitProfile profile,
            SourceForestAllocationBudget.Scope work, String context) throws IOException {
        work.charge(192L, context + " FileIdentity/stamp objects BEFORE allocation");
        WindowsMarkerIo.FileIdentity identity = readIdentity(input);
        long byteLength = readNonnegative(input, context + " length");
        long lastWrite = input.readLong();
        RegionFileStampData result;
        try { result = new RegionFileStampData(identity, byteLength, lastWrite); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid " + context, malformed); }
        validateRegionStamp(result, profile);
        return result;
    }

    private static WindowsMarkerIo.FileIdentity readAccountedIdentity(DataInputStream input,
            SourceForestAllocationBudget.Scope work, String context) throws IOException {
        work.charge(192L, context + " FileIdentity object and 16-byte copy BEFORE allocation");
        return readIdentity(input);
    }

    /** Emits the frozen domainless receipt leaf: T basename, parent/file identity, N length, H SHA-256. */
    public static void encodeInlineArtifactIdentityData(DecodedInlineArtifactIdentityData data,
            DataOutputStream output, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(data, "inline artifact identity DATA");
        Objects.requireNonNull(output, "inline artifact identity output");
        requireAllocation(profile, allocation);
        validateInlineArtifactData(data, profile);
        long work = identityWorkCharge(data.basename.length());
        allocation.reserveTemporary(work, "inline source artifact leaf encode workspace BEFORE allocation");
        Throwable primary = null;
        try { writeInlineArtifactIdentityData(output, data); }
        catch (Throwable failure) { primary = failure; }
        finally { releasePreserving(allocation, work, "inline source artifact leaf encode workspace", primary); }
        rethrow(primary, "encode inline source artifact identity DATA");
    }


    /** Strictly decodes one inline artifact leaf; retained callback values remain DATA, never publication authority. */
    public static void consumeInlineArtifactIdentityData(byte[] exactLeaf, InlineArtifactRole expectedRole,
            UUID expectedWrite, SourceLimitProfile profile, SourceForestAllocationBudget allocation,
            Consumer<DecodedInlineArtifactIdentityData> consumer) throws IOException {
        Objects.requireNonNull(exactLeaf, "complete inline artifact identity leaf");
        Objects.requireNonNull(expectedRole, "expected inline artifact role");
        requireUuid(expectedWrite, "expected inline artifact write UUID");
        Objects.requireNonNull(consumer, "inline artifact identity DATA consumer");
        requireAllocation(profile, allocation);
        if (exactLeaf.length <= 0 || exactLeaf.length > INLINE_ARTIFACT_MAX_BYTES
            || exactLeaf.length > profile.maxSqlPacketBytes() || exactLeaf.length > profile.maxRetainedBytes())
            throw new IOException("Inline source artifact leaf is outside the active bounded DATA limits");
        long work = identityWorkCharge(exactLeaf.length);
        allocation.reserveTemporary(work, "inline source artifact leaf decode workspace BEFORE allocation");
        Throwable primary = null;
        try {
            DataInputStream exact = new DataInputStream(new ByteArrayInputStream(exactLeaf));
            DecodedInlineArtifactIdentityData decoded = readInlineArtifactIdentityData(
                exact, expectedRole, expectedWrite, profile);
            if (exact.read() != -1) throw new IOException("Inline artifact identity leaf has trailing bytes");
            IdentityCompareOutput comparison = new IdentityCompareOutput(exactLeaf);
            try (DataOutputStream canonical = new DataOutputStream(comparison)) {
                writeInlineArtifactIdentityData(canonical, decoded);
                canonical.flush();
            }
            if (!comparison.exact()) throw new IOException("Inline artifact identity leaf is not byte-canonical");
            consumer.accept(decoded);
        } catch (Throwable failure) { primary = failure; }
        finally { releasePreserving(allocation, work, "inline source artifact leaf decode workspace", primary); }
        rethrow(primary, "decode inline source artifact identity DATA");
    }

    /**
     * Decodes one exact canonical ArtifactIdentity DATA frame. The callback is synchronous; a caller
     * retaining any decoded values beyond it must first account that retention in its original owner.
     */
    public static void consumeArtifactIdentityData(byte[] exactFrame, ArtifactKind expectedKind,
            UUID expectedWrite, SourceLimitProfile profile, SourceForestAllocationBudget allocation,
            Consumer<DecodedArtifactIdentityData> consumer) throws IOException {
        Objects.requireNonNull(exactFrame, "complete artifact identity DATA frame");
        Objects.requireNonNull(expectedKind, "expected artifact role");
        requireUuid(expectedWrite, "expected artifact write UUID");
        Objects.requireNonNull(consumer, "artifact identity DATA consumer");
        requireAllocation(profile, allocation);
        validateFrameBounds(exactFrame.length, profile);
        long work = identityWorkCharge(exactFrame.length);
        allocation.reserveTemporary(work, "source artifact identity decode workspace BEFORE allocation");
        Throwable primary = null;
        try {
            DecodedArtifactIdentityData decoded = decodeFrame(exactFrame, expectedKind, expectedWrite, profile);
            consumer.accept(decoded);
        } catch (Throwable failure) {
            primary = failure;
        } finally {
            releasePreserving(allocation, work, "source artifact identity decode workspace", primary);
        }
        rethrow(primary, "decode source artifact identity DATA");
    }

    /** Strict SQL LONGTEXT decoder: fixed prefix, lowercase hex, expected role/write, canonical bytes. */
    public static void consumeArtifactIdentityText(String exactText, ArtifactKind expectedKind,
            UUID expectedWrite, SourceLimitProfile profile, SourceForestAllocationBudget allocation,
            Consumer<DecodedArtifactIdentityData> consumer) throws IOException {
        Objects.requireNonNull(exactText, "complete source artifact identity text");
        Objects.requireNonNull(expectedKind, "expected artifact role");
        requireUuid(expectedWrite, "expected artifact write UUID");
        Objects.requireNonNull(consumer, "artifact identity DATA consumer");
        requireAllocation(profile, allocation);
        int maxChars = Math.addExact(ARTIFACT_ID_TEXT_PREFIX.length(), Math.multiplyExact(ARTIFACT_ID_MAX_FRAME_BYTES, 2));
        if (exactText.length() > maxChars || !exactText.startsWith(ARTIFACT_ID_TEXT_PREFIX))
            throw new IOException("Source artifact identity text has an invalid prefix or exceeds its fixed bound");
        int hexLength = exactText.length() - ARTIFACT_ID_TEXT_PREFIX.length();
        if ((hexLength & 1) != 0 || hexLength == 0)
            throw new IOException("Source artifact identity text must contain a complete lowercase hex frame");
        int frameLength = hexLength / 2;
        validateFrameBounds(frameLength, profile);
        long work = Math.addExact(identityWorkCharge(frameLength), exactText.length() * 2L);
        allocation.reserveTemporary(work, "source artifact identity text decode workspace BEFORE allocation");
        Throwable primary = null;
        try {
            byte[] frame = new byte[frameLength];
            int source = ARTIFACT_ID_TEXT_PREFIX.length();
            for (int index = 0; index < frame.length; index++) {
                int high = lowercaseHex(exactText.charAt(source + index * 2));
                int low = lowercaseHex(exactText.charAt(source + index * 2 + 1));
                if (high < 0 || low < 0) throw new IOException("Source artifact identity text contains noncanonical lowercase hex");
                frame[index] = (byte) ((high << 4) | low);
            }
            consumeArtifactIdentityData(frame, expectedKind, expectedWrite, profile, allocation, consumer);
        } catch (Throwable failure) {
            primary = failure;
        } finally {
            releasePreserving(allocation, work, "source artifact identity text decode workspace", primary);
        }
        rethrow(primary, "decode source artifact identity text");
    }

    private static DecodedArtifactIdentityData decodeFrame(byte[] frame, ArtifactKind expectedKind,
            UUID expectedWrite, SourceLimitProfile profile) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame))) {
            String domain = readText(input, ARTIFACT_ID_DOMAIN_BYTES.length);
            if (!ARTIFACT_ID_DOMAIN.equals(domain)) throw new IOException("Unknown source artifact identity DATA domain");
            String basename = readText(input, 128);
            String expectedBasename = sourceArtifactBasename(expectedKind, expectedWrite);
            if (!expectedBasename.equals(basename))
                throw new IOException("Source artifact identity basename differs from its expected role/write UUID");
            WindowsMarkerIo.FileIdentity parent = readIdentity(input);
            WindowsMarkerIo.FileIdentity file = readIdentity(input);
            long length = input.readLong();
            if (length <= 0 || length > profile.maxNativeArtifactBytes())
                throw new IOException("Source artifact identity length is outside the active native artifact limit");
            byte[] sha = readExact(input, SHA256_BYTES);
            if (input.read() != -1) throw new IOException("Source artifact identity DATA has trailing bytes");
            DecodedArtifactIdentityData decoded;
            try { decoded = new DecodedArtifactIdentityData(expectedKind, expectedWrite, basename, parent, file, length, sha); }
            catch (IllegalArgumentException failure) { throw new IOException("Malformed source artifact identity DATA", failure); }
            IdentityCompareOutput comparison = new IdentityCompareOutput(frame);
            try (DataOutputStream canonical = new DataOutputStream(comparison)) {
                writeArtifactIdentityData(canonical, decoded);
                canonical.flush();
            }
            if (!comparison.exact()) throw new IOException("Source artifact identity DATA is not byte-canonical");
            return decoded;
        } catch (EOFException failure) {
            throw new IOException("Truncated complete source artifact identity DATA", failure);
        } catch (CharacterCodingException failure) {
            throw new IOException("Malformed UTF-8 in source artifact identity DATA", failure);
        } catch (IllegalArgumentException failure) {
            throw new IOException("Malformed source artifact identity DATA", failure);
        }
    }

    private static void writeArtifactIdentityData(DataOutputStream output, DecodedArtifactIdentityData data)
            throws IOException {
        writeText(output, ARTIFACT_ID_DOMAIN);
        writeText(output, data.basename);
        writeIdentity(output, data.parentDirectoryIdentity);
        writeIdentity(output, data.fileIdentity);
        output.writeLong(data.byteLength);
        output.write(data.contentSha256);
    }

    private static byte[] readExact(DataInputStream input, int length) throws IOException {
        if (length < 0 || length > ARTIFACT_ID_MAX_FRAME_BYTES || length > input.available())
            throw new EOFException("Truncated or oversized fixed source artifact identity field");
        byte[] value = input.readNBytes(length);
        if (value.length != length) throw new EOFException("Truncated source artifact identity field");
        return value;
    }

    private static String readText(DataInputStream input, int maxBytes) throws IOException, CharacterCodingException {
        int length = input.readInt();
        if (length < 1 || length > maxBytes || length > input.available())
            throw new IOException("Source artifact identity text field has an invalid length");
        byte[] bytes = readExact(input, length);
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(bytes)).toString();
    }

    private static String readAccountedText(DataInputStream input, int maxBytes,
            SourceForestAllocationBudget.Scope work, String context) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maxBytes || length > input.available())
            throw new IOException("Strict SOURCE text field has an invalid length: " + context);
        work.charge(Math.addExact((long) length, 128L), context + " strict UTF-8 buffer/String BEFORE allocation");
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) throw new EOFException("Truncated strict SOURCE text field: " + context);
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(bytes)).toString();
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        byte[] bytes;
        try {
            java.nio.ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value));
            bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
        } catch (CharacterCodingException failure) {
            throw new IOException("Source artifact identity contains invalid UTF-16", failure);
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void writeIdentity(DataOutputStream output, WindowsMarkerIo.FileIdentity identity) throws IOException {
        output.writeLong(identity.volumeSerialNumber());
        byte[] fileId = identity.fileId128();
        if (fileId.length != 16 || allZero(fileId))
            throw new IOException("Source artifact native FileIdentity must contain exactly 16 observable file-ID bytes");
        output.write(fileId);
    }

    private static void writeInlineArtifactIdentityData(DataOutputStream output,
            DecodedInlineArtifactIdentityData data) throws IOException {
        writeText(output, data.basename);
        writeIdentity(output, data.parentDirectoryIdentity);
        writeIdentity(output, data.fileIdentity);
        output.writeLong(data.byteLength);
        output.write(data.contentSha256);
    }

    private static DecodedInlineArtifactIdentityData readInlineArtifactIdentityData(DataInputStream input,
            InlineArtifactRole expectedRole, UUID expectedWrite, SourceLimitProfile profile) throws IOException {
        try {
            String basename = readText(input, 128);
            if (!inlineArtifactBasename(expectedRole, expectedWrite).equals(basename))
                throw new IOException("Inline artifact basename differs from its expected fixed role/write UUID");
            WindowsMarkerIo.FileIdentity parent = readIdentity(input);
            WindowsMarkerIo.FileIdentity file = readIdentity(input);
            long length = input.readLong();
            if (length <= 0L || length > profile.maxNativeArtifactBytes())
                throw new IOException("Inline artifact complete length is outside the active native artifact bound");
            byte[] sha = readExact(input, SHA256_BYTES);
            DecodedInlineArtifactIdentityData data = new DecodedInlineArtifactIdentityData(
                expectedRole, expectedWrite, basename, parent, file, length, sha);
            validateInlineArtifactData(data, profile);
            return data;
        } catch (EOFException failure) {
            throw new IOException("Truncated inline source artifact identity leaf", failure);
        } catch (CharacterCodingException failure) {
            throw new IOException("Malformed UTF-8 in inline source artifact identity leaf", failure);
        } catch (IllegalArgumentException failure) {
            throw new IOException("Malformed inline source artifact identity leaf", failure);
        }
    }

    private static void validateInlineArtifactData(DecodedInlineArtifactIdentityData data,
            SourceLimitProfile profile) throws IOException {
        if (!data.basename.equals(inlineArtifactBasename(data.role, data.writeUuid)))
            throw new IOException("Inline artifact basename is not fixed by the role/write UUID");
        if (data.byteLength <= 0L || data.byteLength > profile.maxNativeArtifactBytes())
            throw new IOException("Inline artifact length is outside the active native artifact bound");
        if (data.contentSha256.length != SHA256_BYTES)
            throw new IOException("Inline artifact SHA-256 must contain exactly 32 bytes");
        if (data.parentDirectoryIdentity.volumeSerialNumber() != data.fileIdentity.volumeSerialNumber()
            || allZero(data.fileIdentity.fileId128()))
            throw new IOException("Inline artifact file identity is not a same-volume observable native child");
    }

    private static String inlineArtifactBasename(InlineArtifactRole role, UUID write) {
        Objects.requireNonNull(role, "inline artifact role");
        requireUuid(write, "inline artifact write UUID");
        return switch (role) {
            case PAYLOAD -> sourceArtifactBasename(ArtifactKind.PAYLOAD, write);
            case UNDO -> sourceArtifactBasename(ArtifactKind.UNDO, write);
            case RESULT_CORE -> WindowsMarkerIo.sourceResultCanonicalBasename(write);
        };
    }

    private static boolean allZero(byte[] value) {
        int combined = 0;
        for (byte part : value) combined |= part;
        return combined == 0;
    }

    private static WindowsMarkerIo.FileIdentity readIdentity(DataInputStream input) throws IOException {
        long volume = input.readLong();
        byte[] fileId = readExact(input, 16);
        if (allZero(fileId)) throw new IOException("Source artifact DATA FileIdentity is unobservable");
        return new WindowsMarkerIo.FileIdentity(volume, fileId);
    }

    private static void validateArtifactData(DecodedArtifactIdentityData data, SourceLimitProfile profile) throws IOException {
        if (!data.basename.equals(sourceArtifactBasename(data.kind, data.writeUuid)))
            throw new IOException("Source artifact identity basename is not fixed by its role/write UUID");
        if (data.byteLength <= 0 || data.byteLength > profile.maxNativeArtifactBytes())
            throw new IOException("Source artifact identity length is outside the active native artifact limit");
        if (data.contentSha256.length != SHA256_BYTES) throw new IOException("Exact source artifact SHA-256 is required");
        if (data.parentDirectoryIdentity.volumeSerialNumber() != data.fileIdentity.volumeSerialNumber()
            || allZero(data.parentDirectoryIdentity.fileId128()) || allZero(data.fileIdentity.fileId128()))
            throw new IOException("Source artifact identity must name an observable same-volume native child");
    }

    private static void validateFrameBounds(int frameLength, SourceLimitProfile profile) throws IOException {
        if (frameLength < 1 || frameLength > ARTIFACT_ID_MAX_FRAME_BYTES
            || frameLength > profile.maxSqlPacketBytes() || frameLength > profile.maxRetainedBytes())
            throw new IOException("Source artifact identity frame is outside the active bounded DATA limits");
    }

    private static long identityWorkCharge(int sizeHint) {
        return Math.addExact(ARTIFACT_ID_FIXED_WORK_BYTES, Math.multiplyExact((long) sizeHint, 2L));
    }

    private static void requireAllocation(SourceLimitProfile profile, SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(profile, "exact source limit profile");
        Objects.requireNonNull(allocation, "original source allocation account");
        if (allocation.exactProfile() != profile) throw new IOException("Exact original SourceLimitProfile instance required");
    }

    static String sourceArtifactBasename(ArtifactKind kind, UUID writeUuid) {
        Objects.requireNonNull(kind, "source artifact role");
        requireUuid(writeUuid, "source artifact write UUID");
        return SOURCE_ARTIFACT_BASENAME_PREFIX + kind.basenamePart + "-" + compactUuid(writeUuid) + SOURCE_ARTIFACT_BASENAME_SUFFIX;
    }

    private static String encodeIdentityText(DecodedArtifactIdentityData data) {
        try {
            var bytes = new java.io.ByteArrayOutputStream(ARTIFACT_ID_MAX_FRAME_BYTES);
            try (var output = new DataOutputStream(bytes)) { writeArtifactIdentityData(output, data); }
            byte[] frame = bytes.toByteArray();
            if (frame.length > ARTIFACT_ID_MAX_FRAME_BYTES) throw new IllegalArgumentException("Artifact identity frame exceeded fixed bound");
            return ARTIFACT_ID_TEXT_PREFIX + HexFormat.of().formatHex(frame);
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory source artifact identity encoding failed", impossible);
        }
    }



    private static UUID requireUuid(UUID value, String name) {
        Objects.requireNonNull(value, name);
        if ((value.getMostSignificantBits() | value.getLeastSignificantBits()) == 0L)
            throw new IllegalArgumentException("Nonzero " + name + " required");
        return value;
    }

    private static String compactUuid(UUID value) { return value.toString().replace("-", ""); }

    private static byte[] exactSha256(byte[] value) {
        if (value == null || value.length != SHA256_BYTES) throw new IllegalArgumentException("Exact source artifact SHA-256 required");
        return value.clone();
    }

    private static int lowercaseHex(char value) {
        if (value >= '0' && value <= '9') return value - '0';
        if (value >= 'a' && value <= 'f') return value - 'a' + 10;
        return -1;
    }

    private static String requireWireText(String value, SourceLimitProfile profile, String field) throws IOException {
        Objects.requireNonNull(value, field);
        if (value.isEmpty()) throw new IOException("Non-empty " + field + " required by frozen source DATA");
        utf8LengthStrict(value, profile.maxSqlPacketBytes());
        return value;
    }

    private static int utf8LengthStrict(String value, long maximum) throws IOException {
        if (maximum <= 0L) throw new IOException("Positive UTF-8 DATA bound required");
        long bytes = 0L;
        for (int index = 0; index < value.length(); index++) {
            char unit = value.charAt(index);
            if (Character.isHighSurrogate(unit)) {
                if (++index >= value.length() || !Character.isLowSurrogate(value.charAt(index)))
                    throw new IOException("Malformed UTF-16 in strict source DATA text");
                bytes = Math.addExact(bytes, 4L);
            } else if (Character.isLowSurrogate(unit)) {
                throw new IOException("Malformed UTF-16 in strict source DATA text");
            } else if (unit <= 0x7f) bytes = Math.addExact(bytes, 1L);
            else if (unit <= 0x7ff) bytes = Math.addExact(bytes, 2L);
            else bytes = Math.addExact(bytes, 3L);
            if (bytes > maximum || bytes > Integer.MAX_VALUE)
                throw new IOException("Strict source DATA text exceeds its active byte bound");
        }
        return Math.toIntExact(bytes);
    }

    private static void writeUuid(DataOutputStream output, UUID value) throws IOException {
        requireUuid(value, "source DATA UUID");
        output.writeLong(value.getMostSignificantBits());
        output.writeLong(value.getLeastSignificantBits());
    }

    private static void releasePreserving(SourceForestAllocationBudget allocation, long bytes, String context,
            Throwable primary) throws IOException {
        try { allocation.releaseTemporary(bytes, context); }
        catch (Throwable cleanup) {
            if (primary != null) {
                if (cleanup != primary) primary.addSuppressed(cleanup);
                return;
            }
            rethrow(cleanup, context);
        }
    }

    private static void rethrow(Throwable failure, String context) throws IOException {
        if (failure == null) return;
        if (failure instanceof IOException io) throw io;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        throw new IOException(context, failure);
    }

    private static final class HeaderCaptureOutputStream extends OutputStream {
        private final byte[] header = new byte[8192];
        private int offset;
        @Override public void write(int value) throws IOException {
            if (offset >= header.length) throw new IOException("Original region header writer exceeded exactly 8192 bytes");
            header[offset++] = (byte) value;
        }
        @Override public void write(byte[] value, int start, int length) throws IOException {
            Objects.checkFromIndexSize(start, length, value.length);
            if (length > header.length - offset)
                throw new IOException("Original region header writer exceeded exactly 8192 bytes");
            System.arraycopy(value, start, header, offset, length);
            offset += length;
        }
        @Override public void close() { }
        private byte[] complete() throws IOException {
            if (offset != header.length)
                throw new IOException("Original same-handle region header writer returned " + offset + " bytes instead of exactly 8192");
            return header;
        }
    }

    private static final class BoundedCountingOutputStream extends OutputStream {
        private final OutputStream target;
        private final long expectedLength;
        private long written;
        private BoundedCountingOutputStream(OutputStream target, long expectedLength) {
            this.target = Objects.requireNonNull(target);
            this.expectedLength = expectedLength;
        }
        @Override public void write(int value) throws IOException {
            if (written >= expectedLength) throw new IOException("Canonical CarrierKey writer exceeded its exact frozen length");
            target.write(value);
            written = Math.addExact(written, 1L);
        }
        @Override public void write(byte[] value, int start, int length) throws IOException {
            Objects.checkFromIndexSize(start, length, value.length);
            if (length > expectedLength - written)
                throw new IOException("Canonical CarrierKey writer exceeded its exact frozen length");
            target.write(value, start, length);
            written = Math.addExact(written, length);
        }
        @Override public void flush() { }
        @Override public void close() { }
        private void requireComplete() throws IOException {
            if (written != expectedLength)
                throw new IOException("Canonical CarrierKey writer returned " + written + " bytes instead of " + expectedLength);
        }
    }

    private static final class ExactArrayOutputStream extends OutputStream {
        private final byte[] target;
        private int position;
        private ExactArrayOutputStream(byte[] target) { this.target = Objects.requireNonNull(target); }
        @Override public void write(int value) throws IOException {
            if (position >= target.length) throw new IOException("Strict publication encoder exceeded its exact precharged frame length");
            target[position++] = (byte) value;
        }
        @Override public void write(byte[] value, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, value.length);
            if (length > target.length - position)
                throw new IOException("Strict publication encoder exceeded its exact precharged frame length");
            System.arraycopy(value, offset, target, position, length);
            position = Math.addExact(position, length);
        }
        @Override public void flush() { }
        @Override public void close() { }
        private void requireComplete() throws IOException {
            if (position != target.length)
                throw new IOException("Strict publication encoder produced " + position + " bytes instead of exact frame length " + target.length);
        }
    }

    private static final class IdentityCompareOutput extends OutputStream {
        private final byte[] expected;
        private int offset;
        private boolean matches = true;
        private IdentityCompareOutput(byte[] expected) { this.expected = expected; }
        @Override public void write(int value) {
            if (offset >= expected.length || expected[offset] != (byte) value) matches = false;
            offset++;
        }
        @Override public void write(byte[] value, int start, int length) {
            Objects.checkFromIndexSize(start, length, value.length);
            if (offset > expected.length || length > expected.length - offset) matches = false;
            else for (int index = 0; index < length; index++)
                if (expected[offset + index] != value[start + index]) { matches = false; break; }
            offset = Math.addExact(offset, length);
        }
        private boolean exact() { return matches && offset == expected.length; }
    }

    private static final class ExactRangeOutputStream extends OutputStream {
        private final byte[] expected;
        private final int rangeStart;
        private final int rangeLength;
        private final String context;
        private int written;

        private ExactRangeOutputStream(byte[] expected, int rangeStart, int rangeLength, String context) throws IOException {
            this.expected = Objects.requireNonNull(expected);
            this.context = Objects.requireNonNull(context);
            if (rangeStart < 0 || rangeLength < 0 || rangeStart > expected.length - rangeLength)
                throw new IOException(context + " comparison range is outside its exact original frame");
            this.rangeStart = rangeStart;
            this.rangeLength = rangeLength;
        }

        @Override public void write(int value) throws IOException {
            if (written >= rangeLength) throw new IOException(context + " exceeded its exact original byte range");
            if (expected[rangeStart + written] != (byte) value)
                throw new IOException(context + " differs from its exact original byte at offset " + written);
            written = Math.addExact(written, 1);
        }

        @Override public void write(byte[] value, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, value.length);
            if (length > rangeLength - written) throw new IOException(context + " exceeded its exact original byte range");
            for (int index = 0; index < length; index++) {
                if (expected[rangeStart + written + index] != value[offset + index])
                    throw new IOException(context + " differs from its exact original byte at offset " + (written + index));
            }
            written = Math.addExact(written, length);
        }

        @Override public void flush() { }
        @Override public void close() { }
        private void requireComplete() throws IOException {
            if (written != rangeLength)
                throw new IOException(context + " emitted " + written + " bytes instead of exact range length " + rangeLength);
        }
    }

    private static final class BoundedExactCompareOutputStream extends OutputStream {
        private final OutputStream target;
        private final byte[] expected;
        private final int expectedOffset;
        private final int expectedLength;
        private final String context;
        private int written;

        private BoundedExactCompareOutputStream(OutputStream target, byte[] expected,
                int expectedOffset, int expectedLength, String context) throws IOException {
            this.target = Objects.requireNonNull(target);
            this.expected = Objects.requireNonNull(expected);
            this.context = Objects.requireNonNull(context);
            if (expectedOffset < 0 || expectedLength < 0 || expectedOffset > expected.length - expectedLength)
                throw new IOException(context + " expected range is outside its exact bytes");
            this.expectedOffset = expectedOffset;
            this.expectedLength = expectedLength;
        }

        @Override public void write(int value) throws IOException {
            if (written >= expectedLength) throw new IOException(context + " exceeded its exact original frame length");
            if (expected[expectedOffset + written] != (byte) value)
                throw new IOException(context + " differs from its exact original bytes");
            target.write(value);
            written = Math.addExact(written, 1);
        }

        @Override public void write(byte[] value, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, value.length);
            if (length > expectedLength - written) throw new IOException(context + " exceeded its exact original frame length");
            for (int index = 0; index < length; index++) {
                if (expected[expectedOffset + written + index] != value[offset + index])
                    throw new IOException(context + " differs from its exact original bytes");
            }
            target.write(value, offset, length);
            written = Math.addExact(written, length);
        }

        @Override public void flush() throws IOException { target.flush(); }
        @Override public void close() throws IOException { flush(); }
        private void requireComplete() throws IOException {
            if (written != expectedLength)
                throw new IOException(context + " emitted " + written + " bytes instead of exact length " + expectedLength);
        }
    }

    private static final class TeeOutputStream extends OutputStream {
        private final OutputStream first;
        private final OutputStream second;
        private TeeOutputStream(OutputStream first, OutputStream second) {
            this.first = Objects.requireNonNull(first);
            this.second = Objects.requireNonNull(second);
        }
        @Override public void write(int value) throws IOException { first.write(value); second.write(value); }
        @Override public void write(byte[] value, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, value.length);
            first.write(value, offset, length);
            second.write(value, offset, length);
        }
        @Override public void flush() throws IOException { first.flush(); second.flush(); }
        @Override public void close() throws IOException { flush(); }
    }

    private static final class ReceiptCoreDigest {
        private final byte[] expected;
        private final java.security.MessageDigest digest;
        private final DigestRangeCompareOutputStream prefix;
        private final DigestRangeCompareOutputStream diagnostic;
        private boolean finished;

        private ReceiptCoreDigest(byte[] expected, int prefixLength, int diagnosticOffset,
                int diagnosticLength, SourceForestAllocationBudget.Scope work) throws IOException {
            if (prefixLength < 0 || prefixLength > expected.length || diagnosticOffset < 0
                || diagnosticLength < 0 || diagnosticOffset > expected.length - diagnosticLength)
                throw new IOException("Typed receipt core digest ranges exceed the original wire frame");
            this.expected = expected;
            work.charge(256L + 192L, "typed receipt core SHA-256 and two bounded range comparators BEFORE allocation");
            try { digest = java.security.MessageDigest.getInstance("SHA-256"); }
            catch (java.security.NoSuchAlgorithmException impossible) {
                throw new IOException("SHA-256 provider unavailable for typed receipt core reconstruction", impossible);
            }
            prefix = new DigestRangeCompareOutputStream(this, 0, prefixLength, "typed receipt core prefix");
            diagnostic = new DigestRangeCompareOutputStream(this, diagnosticOffset, diagnosticLength,
                "typed receipt core original diagnostic option");
        }

        private OutputStream prefixSink() { return prefix; }
        private OutputStream diagnosticSink() { return diagnostic; }
        private void update(byte[] bytes, int offset, int length) { digest.update(bytes, offset, length); }
        private void update(int value) { digest.update((byte) value); }
        private long byteLength() { return Math.addExact((long) prefix.written, diagnostic.written); }
        private void requireComplete() throws IOException {
            if (finished) throw new IOException("Typed receipt core digest was already finalized");
            prefix.requireComplete();
            diagnostic.requireComplete();
        }
        private byte[] finishSha256() throws IOException {
            requireComplete();
            finished = true;
            return digest.digest();
        }
    }

    private static final class DigestRangeCompareOutputStream extends OutputStream {
        private final ReceiptCoreDigest owner;
        private final int start;
        private final int length;
        private final String context;
        private int written;

        private DigestRangeCompareOutputStream(ReceiptCoreDigest owner, int start, int length, String context) {
            this.owner = owner; this.start = start; this.length = length; this.context = context;
        }
        @Override public void write(int value) throws IOException {
            if (written >= length) throw new IOException(context + " exceeded its exact original byte range");
            if (owner.expected[start + written] != (byte) value)
                throw new IOException(context + " differs from the original wire bytes");
            owner.update(value);
            written = Math.addExact(written, 1);
        }
        @Override public void write(byte[] value, int offset, int count) throws IOException {
            Objects.checkFromIndexSize(offset, count, value.length);
            if (count > length - written) throw new IOException(context + " exceeded its exact original byte range");
            for (int index = 0; index < count; index++) {
                if (owner.expected[start + written + index] != value[offset + index])
                    throw new IOException(context + " differs from the original wire bytes");
            }
            owner.update(value, offset, count);
            written = Math.addExact(written, count);
        }
        @Override public void flush() { }
        @Override public void close() { }
        private void requireComplete() throws IOException {
            if (written != length)
                throw new IOException(context + " emitted " + written + " bytes instead of exact length " + length);
        }
    }
}

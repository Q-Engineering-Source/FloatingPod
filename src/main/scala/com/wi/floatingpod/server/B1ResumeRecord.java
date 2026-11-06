package com.wi.floatingpod.server;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** 完整磁盘快照及 caller assertions；这些数据本身不签发 SQL、退休或 Ready 权限。 */
public record B1ResumeRecord(long sequence, byte[] previousRecordSha256, Kind kind,
        byte[] sourceAnchorSha256, byte[] originalReceipt, String barrierNamespace,
        FullStateAssertions assertions) {
    public enum Kind { GENESIS, PREPARED_CONFIRMED, HANDOFF_PREPARED, DB_ACQUIRED,
        NATIVE_ACK_CERTIFY_INTENT, CERTIFIED, ATTEMPT_CANCELLED, PREPARE_SUCCESSOR_INTENT }
    public enum Phase { PREPARE, CERTIFY, HANDOFF }
    public enum SettlementKind { CONFIRMED, CANCELLED }
    public B1ResumeRecord {
        if (sequence < 0) throw new IllegalArgumentException("Nonnegative bounded sequence required");
        previousRecordSha256 = hash(previousRecordSha256); sourceAnchorSha256 = hash(sourceAnchorSha256);
        originalReceipt = bounded(originalReceipt); Objects.requireNonNull(kind); Objects.requireNonNull(assertions);
        if (barrierNamespace == null || !barrierNamespace.matches("fpb1:[A-Za-z0-9_-]{43}"))
            throw new IllegalArgumentException("Full shared barrier namespace required");
        if (sequence == 0 && (kind != Kind.GENESIS || !allZero(previousRecordSha256)) || sequence > 0 && kind == Kind.GENESIS)
            throw new IllegalArgumentException("Genesis is sequence zero with zero predecessor only");
    }
    @Override public byte[] previousRecordSha256() { return previousRecordSha256.clone(); }
    @Override public byte[] sourceAnchorSha256() { return sourceAnchorSha256.clone(); }
    @Override public byte[] originalReceipt() { return originalReceipt.clone(); }
    @Override public String toString() { return "B1ResumeRecord[caller assertions; no runtime authority]"; }
    public record EffectAssertions(String targetIdentity, long expectedVersion, byte[] desiredFingerprint) {
        public EffectAssertions { printable(targetIdentity); if (expectedVersion < 0) throw new IllegalArgumentException("Effect version must be nonnegative"); desiredFingerprint = hash(desiredFingerprint); }
        @Override public byte[] desiredFingerprint() { return desiredFingerprint.clone(); }
    }
    public record OperationAssertions(UUID boot, long writerEpoch, UUID binding, UUID operation,
            List<EffectAssertions> effects, byte[] originalOperationFingerprint) {
        public OperationAssertions {
            Objects.requireNonNull(boot); Objects.requireNonNull(binding); Objects.requireNonNull(operation);
            if (writerEpoch < 0) throw new IllegalArgumentException("Nonnegative writer epoch required");
            effects = List.copyOf(effects); if (effects.isEmpty() || effects.size() > 256) throw new IllegalArgumentException("Complete bounded effects required");
            String previous = null;
            for (var effect : effects) { if (previous != null && previous.compareTo(effect.targetIdentity()) >= 0)
                throw new IllegalArgumentException("Effects must be uniquely canonical ordered"); previous = effect.targetIdentity(); }
            originalOperationFingerprint = hash(originalOperationFingerprint);
        }
        @Override public byte[] originalOperationFingerprint() { return originalOperationFingerprint.clone(); }
    }
    public record ReceiptSlotAssertions(UUID boot, long writerEpoch, UUID binding, UUID operation, byte[] fingerprint) {
        public ReceiptSlotAssertions { Objects.requireNonNull(boot); Objects.requireNonNull(binding); Objects.requireNonNull(operation);
            if (writerEpoch < 0) throw new IllegalArgumentException("Nonnegative receipt epoch required"); fingerprint = hash(fingerprint); }
        @Override public byte[] fingerprint() { return fingerprint.clone(); }
    }
    public record WriterAssertions(UUID binding, long epoch, UUID ownerBoot, LocalDateTime leaseUntil,
            ReceiptSlotAssertions lastSlot) {
        public WriterAssertions { Objects.requireNonNull(binding); if (epoch < 0) throw new IllegalArgumentException("Nonnegative W epoch required");
            Objects.requireNonNull(leaseUntil); if (leaseUntil.getNano() % 1000 != 0) throw new IllegalArgumentException("Actual database microsecond precision required"); }
    }
    /** 三个完整 canonical 图组件，不以摘要代替图；语义与 actual SQL decoder 由 coordinator 核验。 */
    public record CommonGraphAssertions(String manifestEncoding, byte[] manifestBytes,
            String bindingEncoding, byte[] bindingBytes, String repositoryEncoding, byte[] repositoryBytes) {
        public CommonGraphAssertions { printable(manifestEncoding); printable(bindingEncoding); printable(repositoryEncoding);
            manifestBytes = bounded(manifestBytes); bindingBytes = bounded(bindingBytes); repositoryBytes = bounded(repositoryBytes); }
        @Override public byte[] manifestBytes() { return manifestBytes.clone(); }
        @Override public byte[] bindingBytes() { return bindingBytes.clone(); }
        @Override public byte[] repositoryBytes() { return repositoryBytes.clone(); }
    }
    public record NativeOwnerAssertions(long pid, long fileTime, UUID boot, long generation) {
        public NativeOwnerAssertions { WindowsMarkerIo.validateProcessIdentity(pid, fileTime); Objects.requireNonNull(boot);
            if (generation <= 0) throw new IllegalArgumentException("Positive custody generation required"); }
    }
    public record RetirementAssertions(String protocol, byte[] originalEvidence) {
        public RetirementAssertions { printable(protocol); originalEvidence = bounded(originalEvidence); }
        @Override public byte[] originalEvidence() { return originalEvidence.clone(); }
    }
    public record AttemptAssertions(UUID attemptId, Phase phase, UUID logicalWriteUuid, long bootGeneration,
            UUID connectionGeneration, long connectionId, String namespace, OperationAssertions context,
            WriterAssertions beforeWriter, WriterAssertions targetWriter,
            CommonGraphAssertions beforeGraph, CommonGraphAssertions targetGraph) {
        public AttemptAssertions { Objects.requireNonNull(attemptId); Objects.requireNonNull(phase); Objects.requireNonNull(logicalWriteUuid);
            Objects.requireNonNull(connectionGeneration); Objects.requireNonNull(context); Objects.requireNonNull(beforeWriter); Objects.requireNonNull(targetWriter);
            Objects.requireNonNull(beforeGraph); Objects.requireNonNull(targetGraph);
            if (bootGeneration <= 0 || connectionId <= 0 || namespace == null || !namespace.matches("fpb1:[A-Za-z0-9_-]{43}"))
                throw new IllegalArgumentException("Complete attempt boot/session assertions required");
            if (!logicalWriteUuid.equals(context.operation())) throw new IllegalArgumentException("Attempt operation context differs from logical write"); }
    }
    public record SettlementAssertions(SettlementKind kind, AttemptAssertions settledAttempt, String proofProtocol,
            byte[] fullOriginalEvidence, byte[] originalUnknownDiagnosticIdentity) {
        public SettlementAssertions { Objects.requireNonNull(kind); Objects.requireNonNull(settledAttempt); printable(proofProtocol);
            fullOriginalEvidence = bounded(fullOriginalEvidence); originalUnknownDiagnosticIdentity = hash(originalUnknownDiagnosticIdentity); }
        @Override public byte[] fullOriginalEvidence() { return fullOriginalEvidence.clone(); }
        @Override public byte[] originalUnknownDiagnosticIdentity() { return originalUnknownDiagnosticIdentity.clone(); }
    }
    public record MarkerAssertions(byte[] fullMarkerBytes, String fileIdentity, String parentIdentity,
            String nativeAckProtocol, long custodyGeneration, byte[] originalNativeAckEvidence) {
        public MarkerAssertions { fullMarkerBytes = bounded(fullMarkerBytes); originalNativeAckEvidence = bounded(originalNativeAckEvidence);
            printable(nativeAckProtocol); Objects.requireNonNull(fileIdentity); Objects.requireNonNull(parentIdentity);
            if (custodyGeneration <= 0) throw new IllegalArgumentException("Positive marker custody generation required"); }
        @Override public byte[] fullMarkerBytes() { return fullMarkerBytes.clone(); }
        @Override public byte[] originalNativeAckEvidence() { return originalNativeAckEvidence.clone(); }
    }
    public record FullStateAssertions(NativeOwnerAssertions currentOwner, NativeOwnerAssertions previousOwner,
            RetirementAssertions retirement, AttemptAssertions originalPrepareWitness,
            WriterAssertions currentWriter, WriterAssertions targetWriter, AttemptAssertions pendingAttempt,
            SettlementAssertions latestSettlement, MarkerAssertions marker, AttemptAssertions acknowledgedCertifyWitness) {
        public FullStateAssertions { Objects.requireNonNull(currentOwner); Objects.requireNonNull(currentWriter); Objects.requireNonNull(targetWriter); }
    }
    static byte[] hash(byte[] input) { if (input == null || input.length != 32) throw new IllegalArgumentException("Exact SHA256 required"); return input.clone(); }
    static byte[] bounded(byte[] input) { if (input == null || input.length == 0 || input.length > 65536) throw new IllegalArgumentException("Complete bounded bytes required"); return input.clone(); }
    static boolean allZero(byte[] bytes) { for (byte value : bytes) if (value != 0) return false; return true; }
    static void printable(String value) { if (value == null || value.isEmpty() || value.length() > 4096 || !value.chars().allMatch(c -> c >= '!' && c <= '~'))
        throw new IllegalArgumentException("Bounded printable protocol/identity required"); }
}

package com.wi.floatingpod.deployment.command;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.UUID;

/**
 * Fixed-width proposal codec for the DEPLOYMENT command family.
 *
 * <p>This class only translates bounded wire frames to immutable wire-model
 * records. It has no actor, operation-store, capture, world, persistence,
 * cleanup, readiness, snapshot, or client-sink authority.</p>
 */
public final class DeploymentCommandWireCodec {
    private static final byte[] MAGIC = { (byte) 'F', (byte) 'P', (byte) 'D', (byte) 'C' };
    private static final int SCHEMA_VERSION = 1;
    private static final int COMMON_PREFIX_LENGTH = 6;
    private static final int REQUEST_PREFIX_LENGTH = 22;
    private static final int EXPECTED_VERSIONS_LENGTH = 72;
    private static final int MUTATION_REQUEST_LENGTH = 134;
    private static final int QUERY_REQUEST_LENGTH = 22;
    private static final int RECEIPT_LENGTH = 142;
    private static final int QUERY_NOT_FOUND_LENGTH = 23;
    private static final int QUERY_FOUND_LENGTH = 143;
    private static final int PROTOCOL_FAILURE_LENGTH = 23;
    private static final int COMMAND_NOTICE_LENGTH = 24;
    private static final int MAX_FRAME_LENGTH = 143;
    private static final int RECEIPT_TAIL_LENGTH = 120;

    private DeploymentCommandWireCodec() {
        throw new AssertionError("no instances");
    }

    public static byte[] encodeRequest(Request request) {
        requireArgument(request, "request");
        if (request instanceof DeployRequest deploy) {
            ByteBuffer out = newOutput(MUTATION_REQUEST_LENGTH);
            putRequestPrefix(out, MutationKind.DEPLOY.wireCode(), deploy.operationId());
            putMutationBody(out, deploy.podUuid(), deploy.moduleInstanceId(),
                    deploy.moduleGeneration(), deploy.expected());
            return out.array();
        }
        if (request instanceof RetractRequest retract) {
            ByteBuffer out = newOutput(MUTATION_REQUEST_LENGTH);
            putRequestPrefix(out, MutationKind.RETRACT.wireCode(), retract.operationId());
            putMutationBody(out, retract.podUuid(), retract.moduleInstanceId(),
                    retract.moduleGeneration(), retract.expected());
            return out.array();
        }
        if (request instanceof QueryOriginalRequest query) {
            ByteBuffer out = newOutput(QUERY_REQUEST_LENGTH);
            putRequestPrefix(out, 0x03, query.originalOperationId());
            return out.array();
        }
        throw new IllegalArgumentException("unsupported request type");
    }

    public static DecodeResult<Request> decodeRequest(byte[] frame) {
        requireArgument(frame, "frame");
        Optional<UntrustedCorrelation> correlation = correlation(frame);
        if (frame.length < COMMON_PREFIX_LENGTH || frame.length > MAX_FRAME_LENGTH) {
            return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                    DecodeStage.FRAME_LENGTH);
        }
        StructuralFailure<Request> prefixFailure = checkPrefix(frame, correlation);
        if (prefixFailure != null) {
            return prefixFailure;
        }

        int kindCode = Byte.toUnsignedInt(frame[5]);
        if (kindCode != 0x01 && kindCode != 0x02 && kindCode != 0x03) {
            return structuralFailure(StructuralCode.UNKNOWN_KIND_OR_CODE, correlation,
                    DecodeStage.MESSAGE_KIND);
        }
        int expectedLength = kindCode == 0x03 ? QUERY_REQUEST_LENGTH : MUTATION_REQUEST_LENGTH;
        if (frame.length != expectedLength) {
            return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                    DecodeStage.FRAME_LENGTH);
        }

        UUID operationId = readUuid(frame, 6);
        if (kindCode == 0x03) {
            return new Decoded<Request>(new QueryOriginalRequest(operationId));
        }

        long moduleGeneration = readLong(frame, 54);
        if (moduleGeneration < 0 || !hasNonNegativeVersionsOnWire(frame, 62)) {
            return structuralFailure(StructuralCode.INVALID_RANGE_OR_BODY, correlation,
                    DecodeStage.REQUEST_BODY);
        }
        UUID podUuid = readUuid(frame, 22);
        UUID moduleInstanceId = readUuid(frame, 38);
        ExpectedCommandVersions expected = readExpectedVersions(frame, 62);
        if (kindCode == 0x01) {
            return new Decoded<Request>(
                    new DeployRequest(operationId, podUuid, moduleInstanceId, moduleGeneration, expected));
        }
        return new Decoded<Request>(
                new RetractRequest(operationId, podUuid, moduleInstanceId, moduleGeneration, expected));
    }

    public static byte[] encodeResponse(Response response) {
        requireArgument(response, "response");
        if (response instanceof MutationReceipt receipt) {
            requireLegalReceipt(receipt);
            ByteBuffer out = newOutput(RECEIPT_LENGTH);
            putResponsePrefix(out, 0x81, receipt.operationId());
            putReceiptTail(out, receipt);
            return out.array();
        }
        if (response instanceof QueryOriginalResult query) {
            if (query.status() == LookupStatus.NOT_FOUND) {
                ByteBuffer out = newOutput(QUERY_NOT_FOUND_LENGTH);
                putResponsePrefix(out, 0x82, query.originalOperationId());
                out.put((byte) LookupStatus.NOT_FOUND.wireCode());
                return out.array();
            }
            MutationReceipt receipt = query.originalReceipt().orElseThrow(
                    () -> new IllegalArgumentException("FOUND requires original receipt"));
            if (!receipt.operationId().equals(query.originalOperationId())) {
                throw new IllegalArgumentException("FOUND receipt must match queried operationId");
            }
            requireLegalReceipt(receipt);
            ByteBuffer out = newOutput(QUERY_FOUND_LENGTH);
            putResponsePrefix(out, 0x82, query.originalOperationId());
            out.put((byte) LookupStatus.FOUND.wireCode());
            putReceiptTail(out, receipt);
            return out.array();
        }
        if (response instanceof ProtocolFailure failure) {
            UntrustedCorrelation correlation = failure.correlation().orElseThrow(
                    () -> new IllegalArgumentException("ProtocolFailure requires full correlation to encode"));
            ByteBuffer out = newOutput(PROTOCOL_FAILURE_LENGTH);
            putResponsePrefix(out, 0x83, correlation.rawOperationId());
            out.put((byte) failure.code().wireCode());
            return out.array();
        }
        if (response instanceof CommandNotice notice) {
            ByteBuffer out = newOutput(COMMAND_NOTICE_LENGTH);
            putResponsePrefix(out, 0x84, notice.operationId());
            out.putShort((short) notice.reason().wireCode());
            return out.array();
        }
        throw new IllegalArgumentException("unsupported response type");
    }

    public static DecodeResult<Response> decodeResponse(byte[] frame) {
        requireArgument(frame, "frame");
        Optional<UntrustedCorrelation> correlation = correlation(frame);
        if (frame.length < COMMON_PREFIX_LENGTH || frame.length > MAX_FRAME_LENGTH) {
            return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                    DecodeStage.FRAME_LENGTH);
        }
        StructuralFailure<Response> prefixFailure = checkPrefix(frame, correlation);
        if (prefixFailure != null) {
            return prefixFailure;
        }

        int kindCode = Byte.toUnsignedInt(frame[5]);
        if (kindCode < 0x81 || kindCode > 0x84) {
            return structuralFailure(StructuralCode.UNKNOWN_KIND_OR_CODE, correlation,
                    DecodeStage.MESSAGE_KIND);
        }

        if (kindCode == 0x81) {
            if (frame.length != RECEIPT_LENGTH) {
                return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                        DecodeStage.FRAME_LENGTH);
            }
            UUID operationId = readUuid(frame, 6);
            DecodeResult<MutationReceipt> receiptResult =
                    decodeReceiptTail(frame, 22, operationId);
            if (receiptResult instanceof StructuralFailure<MutationReceipt> failure) {
                return new StructuralFailure<Response>(failure.code(), failure.correlation(), failure.stage());
            }
            if (receiptResult instanceof Decoded<MutationReceipt> decoded) {
                return new Decoded<Response>(decoded.value());
            }
            throw new IllegalStateException("sealed receipt decoder returned an unknown result subtype");
        }

        if (kindCode == 0x82) {
            if (frame.length < QUERY_NOT_FOUND_LENGTH) {
                return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                        DecodeStage.FRAME_LENGTH);
            }
            int lookupCode = Byte.toUnsignedInt(frame[22]);
            LookupStatus status = LookupStatus.fromWireCode(lookupCode);
            if (status == null) {
                return structuralFailure(StructuralCode.UNKNOWN_KIND_OR_CODE, correlation,
                        DecodeStage.RESPONSE_BODY);
            }
            UUID operationId = readUuid(frame, 6);
            if (status == LookupStatus.NOT_FOUND) {
                if (frame.length != QUERY_NOT_FOUND_LENGTH) {
                    return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                            DecodeStage.FRAME_LENGTH);
                }
                return new Decoded<Response>(
                        new QueryOriginalResult(operationId, LookupStatus.NOT_FOUND, Optional.empty()));
            }
            if (frame.length != QUERY_FOUND_LENGTH) {
                return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                        DecodeStage.FRAME_LENGTH);
            }
            DecodeResult<MutationReceipt> receiptResult =
                    decodeReceiptTail(frame, 23, operationId);
            if (receiptResult instanceof StructuralFailure<MutationReceipt> failure) {
                return new StructuralFailure<Response>(failure.code(), failure.correlation(), failure.stage());
            }
            if (receiptResult instanceof Decoded<MutationReceipt> decoded) {
                return new Decoded<Response>(
                        new QueryOriginalResult(operationId, LookupStatus.FOUND, Optional.of(decoded.value())));
            }
            throw new IllegalStateException("sealed receipt decoder returned an unknown result subtype");
        }

        if (kindCode == 0x83) {
            if (frame.length != PROTOCOL_FAILURE_LENGTH) {
                return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                        DecodeStage.FRAME_LENGTH);
            }
            StructuralCode code = StructuralCode.fromWireCode(Byte.toUnsignedInt(frame[22]));
            if (code == null) {
                return structuralFailure(StructuralCode.UNKNOWN_KIND_OR_CODE, correlation,
                        DecodeStage.RESPONSE_BODY);
            }
            UUID rawOperationId = readUuid(frame, 6);
            return new Decoded<Response>(
                    new ProtocolFailure(Optional.of(new UntrustedCorrelation(rawOperationId)), code));
        }

        if (frame.length != COMMAND_NOTICE_LENGTH) {
            return structuralFailure(StructuralCode.TRUNCATED_OR_WRONG_LENGTH, correlation,
                    DecodeStage.FRAME_LENGTH);
        }
        int noticeCode = Short.toUnsignedInt(readShort(frame, 22));
        NoticeReason reason = NoticeReason.fromWireCode(noticeCode);
        if (reason == null) {
            return structuralFailure(StructuralCode.UNKNOWN_KIND_OR_CODE, correlation,
                    DecodeStage.RESPONSE_BODY);
        }
        UUID operationId = readUuid(frame, 6);
        return new Decoded<Response>(new CommandNotice(operationId, reason));
    }

    private static void putMutationBody(
            ByteBuffer out,
            UUID podUuid,
            UUID moduleInstanceId,
            long moduleGeneration,
            ExpectedCommandVersions expected) {
        putUuid(out, podUuid);
        putUuid(out, moduleInstanceId);
        out.putLong(moduleGeneration);
        putExpectedVersions(out, expected);
    }

    private static void putReceiptTail(ByteBuffer out, MutationReceipt receipt) {
        putUuid(out, receipt.podUuid());
        putUuid(out, receipt.moduleInstanceId());
        out.putLong(receipt.moduleGeneration());
        out.put((byte) receipt.commandKind().wireCode());
        putExpectedVersions(out, receipt.expected());
        out.put((byte) receipt.phase().wireCode());
        out.putShort((short) receipt.reason().wireCode());
        out.put((byte) receipt.actualEffect().wireCode());
        out.put((byte) receipt.criticalCommit().wireCode());
        out.put((byte) receipt.readiness().wireCode());
        out.put((byte) receipt.cleanup().wireCode());
    }

    private static void putExpectedVersions(ByteBuffer out, ExpectedCommandVersions expected) {
        putUuid(out, expected.bootUuid());
        out.putLong(expected.bootGeneration());
        putUuid(out, expected.worldBindingId());
        out.putLong(expected.entityGeneration());
        out.putLong(expected.assemblyVersion());
        out.putLong(expected.controlVersion());
        out.putLong(expected.deploymentSequence());
    }

    private static DecodeResult<MutationReceipt> decodeReceiptTail(
            byte[] frame, int tailOffset, UUID operationId) {
        long moduleGeneration = readLong(frame, tailOffset + 32);
        if (moduleGeneration < 0 || !hasNonNegativeVersionsOnWire(frame, tailOffset + 41)) {
            return structuralFailure(StructuralCode.INVALID_RANGE_OR_BODY,
                    Optional.of(new UntrustedCorrelation(operationId)), DecodeStage.RESPONSE_BODY);
        }

        UUID podUuid = readUuid(frame, tailOffset);
        UUID moduleInstanceId = readUuid(frame, tailOffset + 16);
        MutationKind commandKind = MutationKind.fromWireCode(Byte.toUnsignedInt(frame[tailOffset + 40]));
        ExpectedCommandVersions expected = readExpectedVersions(frame, tailOffset + 41);
        OperationPhase phase = OperationPhase.fromWireCode(Byte.toUnsignedInt(frame[tailOffset + 113]));
        ReceiptReason reason = ReceiptReason.fromWireCode(Short.toUnsignedInt(readShort(frame, tailOffset + 114)));
        ActualEffectStatus actualEffect =
                ActualEffectStatus.fromWireCode(Byte.toUnsignedInt(frame[tailOffset + 116]));
        CriticalCommitStatus criticalCommit =
                CriticalCommitStatus.fromWireCode(Byte.toUnsignedInt(frame[tailOffset + 117]));
        ReadinessStatus readiness =
                ReadinessStatus.fromWireCode(Byte.toUnsignedInt(frame[tailOffset + 118]));
        CleanupStatus cleanup = CleanupStatus.fromWireCode(Byte.toUnsignedInt(frame[tailOffset + 119]));

        if (commandKind == null || phase == null || reason == null || actualEffect == null
                || criticalCommit == null || readiness == null || cleanup == null) {
            return structuralFailure(StructuralCode.UNKNOWN_KIND_OR_CODE,
                    Optional.of(new UntrustedCorrelation(operationId)), DecodeStage.RESPONSE_BODY);
        }
        if (!isLegalReceipt(phase, reason, actualEffect, criticalCommit, readiness, cleanup)) {
            return structuralFailure(StructuralCode.INVALID_RANGE_OR_BODY,
                    Optional.of(new UntrustedCorrelation(operationId)), DecodeStage.RECEIPT_INVARIANTS);
        }
        return new Decoded<MutationReceipt>(new MutationReceipt(operationId, podUuid, moduleInstanceId,
                moduleGeneration, commandKind, expected, phase, reason, actualEffect,
                criticalCommit, readiness, cleanup));
    }

    private static ExpectedCommandVersions readExpectedVersions(byte[] frame, int offset) {
        return new ExpectedCommandVersions(
                readUuid(frame, offset),
                readLong(frame, offset + 16),
                readUuid(frame, offset + 24),
                readLong(frame, offset + 40),
                readLong(frame, offset + 48),
                readLong(frame, offset + 56),
                readLong(frame, offset + 64));
    }

    private static boolean hasNonNegativeVersionsOnWire(byte[] frame, int offset) {
        return readLong(frame, offset + 16) >= 0
                && readLong(frame, offset + 40) >= 0
                && readLong(frame, offset + 48) >= 0
                && readLong(frame, offset + 56) >= 0
                && readLong(frame, offset + 64) >= 0;
    }

    private static void putRequestPrefix(ByteBuffer out, int kindCode, UUID operationId) {
        putCommonPrefix(out, kindCode);
        putUuid(out, operationId);
    }

    private static void putResponsePrefix(ByteBuffer out, int kindCode, UUID operationId) {
        putCommonPrefix(out, kindCode);
        putUuid(out, operationId);
    }

    private static void putCommonPrefix(ByteBuffer out, int kindCode) {
        out.put(MAGIC);
        out.put((byte) SCHEMA_VERSION);
        out.put((byte) kindCode);
    }

    private static void putUuid(ByteBuffer out, UUID value) {
        out.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits());
    }

    private static UUID readUuid(byte[] frame, int offset) {
        ByteBuffer view = ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN);
        return new UUID(view.getLong(offset), view.getLong(offset + Long.BYTES));
    }

    private static long readLong(byte[] frame, int offset) {
        return ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN).getLong(offset);
    }

    private static short readShort(byte[] frame, int offset) {
        return ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN).getShort(offset);
    }

    private static ByteBuffer newOutput(int length) {
        return ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
    }

    private static Optional<UntrustedCorrelation> correlation(byte[] frame) {
        if (frame.length < REQUEST_PREFIX_LENGTH) {
            return Optional.empty();
        }
        return Optional.of(new UntrustedCorrelation(readUuid(frame, 6)));
    }

    private static <T> StructuralFailure<T> checkPrefix(
            byte[] frame, Optional<UntrustedCorrelation> correlation) {
        for (int index = 0; index < MAGIC.length; index++) {
            if (frame[index] != MAGIC[index]) {
                return new StructuralFailure<T>(
                        StructuralCode.BAD_MAGIC, correlation, DecodeStage.PREFIX);
            }
        }
        if (Byte.toUnsignedInt(frame[4]) != SCHEMA_VERSION) {
            return new StructuralFailure<T>(
                    StructuralCode.UNSUPPORTED_SCHEMA, correlation, DecodeStage.PREFIX);
        }
        return null;
    }

    private static <T> StructuralFailure<T> structuralFailure(
            StructuralCode code,
            Optional<UntrustedCorrelation> correlation,
            DecodeStage stage) {
        return new StructuralFailure<T>(code, correlation, stage);
    }

    private static void requireLegalReceipt(MutationReceipt receipt) {
        if (!isLegalReceipt(receipt.phase(), receipt.reason(), receipt.actualEffect(),
                receipt.criticalCommit(), receipt.readiness(), receipt.cleanup())) {
            throw new IllegalArgumentException("receipt phase/reason/axes are inconsistent");
        }
    }

    private static boolean isLegalReceipt(
            OperationPhase phase,
            ReceiptReason reason,
            ActualEffectStatus effect,
            CriticalCommitStatus commit,
            ReadinessStatus readiness,
            CleanupStatus cleanup) {
        if (phase == null || reason == null || effect == null || commit == null
                || readiness == null || cleanup == null) {
            return false;
        }
        return switch (phase) {
            case QUEUED -> reason == ReceiptReason.NONE
                    && effect == ActualEffectStatus.NOT_STARTED
                    && commit == CriticalCommitStatus.NOT_ATTEMPTED
                    && readiness == ReadinessStatus.NOT_EVALUATED
                    && cleanup == CleanupStatus.NOT_REQUIRED;
            case EXECUTING -> reason == ReceiptReason.NONE
                    && effect != ActualEffectStatus.UNKNOWN
                    && (commit == CriticalCommitStatus.NOT_ATTEMPTED
                    || commit == CriticalCommitStatus.COMMITTED)
                    && cleanup != CleanupStatus.FAILED
                    && cleanup != CleanupStatus.UNKNOWN;
            case APPLIED -> reason == ReceiptReason.NONE
                    && effect == ActualEffectStatus.APPLIED_CONFIRMED
                    && commit == CriticalCommitStatus.COMMITTED
                    && cleanup != CleanupStatus.FAILED
                    && cleanup != CleanupStatus.UNKNOWN;
            case REJECTED -> isLegalRejection(reason, effect, commit, readiness, cleanup);
            case FAILED -> isLegalFailure(reason, effect, commit, readiness, cleanup);
            case UNKNOWN -> isLegalUnknown(reason, effect, commit, readiness, cleanup);
        };
    }

    private static boolean isLegalRejection(
            ReceiptReason reason,
            ActualEffectStatus effect,
            CriticalCommitStatus commit,
            ReadinessStatus readiness,
            CleanupStatus cleanup) {
        boolean legalReason = reason == ReceiptReason.AUTHORIZATION_DENIED
                || reason == ReceiptReason.EXPECTED_VERSIONS_STALE
                || reason == ReceiptReason.TARGET_BINDING_INVALID
                || reason == ReceiptReason.MODULE_GENERATION_STALE
                || reason == ReceiptReason.QUEUE_EXPIRED
                || reason == ReceiptReason.SEQUENCE_EXHAUSTED;
        return legalReason
                && effect == ActualEffectStatus.NONE_CONFIRMED
                && commit == CriticalCommitStatus.NOT_ATTEMPTED
                && cleanup == CleanupStatus.NOT_REQUIRED
                && (readiness == ReadinessStatus.NOT_EVALUATED
                    || readiness == ReadinessStatus.INVALIDATED);
    }

    private static boolean isLegalFailure(
            ReceiptReason reason,
            ActualEffectStatus effect,
            CriticalCommitStatus commit,
            ReadinessStatus readiness,
            CleanupStatus cleanup) {
        if (effect == ActualEffectStatus.UNKNOWN
                || commit == CriticalCommitStatus.UNKNOWN
                || cleanup == CleanupStatus.UNKNOWN) {
            return false;
        }
        if (cleanup == CleanupStatus.PENDING || cleanup == CleanupStatus.FAILED) {
            return reason == ReceiptReason.CLEANUP_DEBT;
        }
        if (reason == ReceiptReason.CLEANUP_DEBT) {
            return false;
        }
        return switch (reason) {
            case EFFECT_FAILURE -> effect == ActualEffectStatus.NONE_CONFIRMED
                    || effect == ActualEffectStatus.PARTIAL_CONFIRMED;
            case CRITICAL_COMMIT_FAILURE -> commit == CriticalCommitStatus.FAILED;
            case INTERNAL_FAILURE -> isLegalInternalFailure(effect, commit, readiness, cleanup);
            default -> false;
        };
    }

    private static boolean isLegalInternalFailure(
            ActualEffectStatus effect,
            CriticalCommitStatus commit,
            ReadinessStatus readiness,
            CleanupStatus cleanup) {
        boolean effectAllowed = effect == ActualEffectStatus.NOT_STARTED
                || effect == ActualEffectStatus.NONE_CONFIRMED
                || effect == ActualEffectStatus.PARTIAL_CONFIRMED
                || effect == ActualEffectStatus.APPLIED_CONFIRMED;
        boolean commitAllowed = commit == CriticalCommitStatus.NOT_ATTEMPTED
                || commit == CriticalCommitStatus.COMMITTED
                || commit == CriticalCommitStatus.FAILED;
        boolean readinessAllowed = readiness == ReadinessStatus.NOT_EVALUATED
                || readiness == ReadinessStatus.NOT_READY
                || readiness == ReadinessStatus.QUALIFIED
                || readiness == ReadinessStatus.INVALIDATED;
        boolean cleanupAllowed = cleanup == CleanupStatus.NOT_REQUIRED
                || cleanup == CleanupStatus.COMPLETE;
        return effectAllowed && commitAllowed && readinessAllowed && cleanupAllowed
                && !(effect == ActualEffectStatus.APPLIED_CONFIRMED
                    && commit == CriticalCommitStatus.COMMITTED);
    }

    private static boolean isLegalUnknown(
            ReceiptReason reason,
            ActualEffectStatus effect,
            CriticalCommitStatus commit,
            ReadinessStatus readiness,
            CleanupStatus cleanup) {
        boolean unknown = effect == ActualEffectStatus.UNKNOWN
                || commit == CriticalCommitStatus.UNKNOWN
                || cleanup == CleanupStatus.UNKNOWN;
        if (!unknown || readiness == ReadinessStatus.QUALIFIED) {
            return false;
        }
        if (cleanup == CleanupStatus.PENDING || cleanup == CleanupStatus.FAILED
                || cleanup == CleanupStatus.UNKNOWN) {
            return reason == ReceiptReason.CLEANUP_DEBT;
        }
        return reason == ReceiptReason.OUTCOME_UNKNOWN;
    }

    private static void requireNonNegative(long value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be nonnegative");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    public sealed interface Request permits DeployRequest, RetractRequest, QueryOriginalRequest {
    }

    public record ExpectedCommandVersions(
            UUID bootUuid,
            long bootGeneration,
            UUID worldBindingId,
            long entityGeneration,
            long assemblyVersion,
            long controlVersion,
            long deploymentSequence) {
        public ExpectedCommandVersions {
            requireArgument(bootUuid, "bootUuid");
            requireArgument(worldBindingId, "worldBindingId");
            requireNonNegative(bootGeneration, "bootGeneration");
            requireNonNegative(entityGeneration, "entityGeneration");
            requireNonNegative(assemblyVersion, "assemblyVersion");
            requireNonNegative(controlVersion, "controlVersion");
            requireNonNegative(deploymentSequence, "deploymentSequence");
        }
    }

    public record DeployRequest(
            UUID operationId,
            UUID podUuid,
            UUID moduleInstanceId,
            long moduleGeneration,
            ExpectedCommandVersions expected) implements Request {
        public DeployRequest {
            requireArgument(operationId, "operationId");
            requireArgument(podUuid, "podUuid");
            requireArgument(moduleInstanceId, "moduleInstanceId");
            requireArgument(expected, "expected");
            requireNonNegative(moduleGeneration, "moduleGeneration");
        }
    }

    public record RetractRequest(
            UUID operationId,
            UUID podUuid,
            UUID moduleInstanceId,
            long moduleGeneration,
            ExpectedCommandVersions expected) implements Request {
        public RetractRequest {
            requireArgument(operationId, "operationId");
            requireArgument(podUuid, "podUuid");
            requireArgument(moduleInstanceId, "moduleInstanceId");
            requireArgument(expected, "expected");
            requireNonNegative(moduleGeneration, "moduleGeneration");
        }
    }

    public record QueryOriginalRequest(UUID originalOperationId) implements Request {
        public QueryOriginalRequest {
            requireArgument(originalOperationId, "originalOperationId");
        }
    }

    public sealed interface Response permits MutationReceipt, QueryOriginalResult, ProtocolFailure, CommandNotice {
    }

    public record MutationReceipt(
            UUID operationId,
            UUID podUuid,
            UUID moduleInstanceId,
            long moduleGeneration,
            MutationKind commandKind,
            ExpectedCommandVersions expected,
            OperationPhase phase,
            ReceiptReason reason,
            ActualEffectStatus actualEffect,
            CriticalCommitStatus criticalCommit,
            ReadinessStatus readiness,
            CleanupStatus cleanup) implements Response {
        public MutationReceipt {
            requireArgument(operationId, "operationId");
            requireArgument(podUuid, "podUuid");
            requireArgument(moduleInstanceId, "moduleInstanceId");
            requireArgument(commandKind, "commandKind");
            requireArgument(expected, "expected");
            requireArgument(phase, "phase");
            requireArgument(reason, "reason");
            requireArgument(actualEffect, "actualEffect");
            requireArgument(criticalCommit, "criticalCommit");
            requireArgument(readiness, "readiness");
            requireArgument(cleanup, "cleanup");
            requireNonNegative(moduleGeneration, "moduleGeneration");
            if (!isLegalReceipt(phase, reason, actualEffect, criticalCommit, readiness, cleanup)) {
                throw new IllegalArgumentException("receipt phase/reason/axes are inconsistent");
            }
        }
    }

    public record QueryOriginalResult(
            UUID originalOperationId,
            LookupStatus status,
            Optional<MutationReceipt> originalReceipt) implements Response {
        public QueryOriginalResult {
            requireArgument(originalOperationId, "originalOperationId");
            requireArgument(status, "status");
            requireArgument(originalReceipt, "originalReceipt");
            if (status == LookupStatus.FOUND) {
                if (originalReceipt.isEmpty()
                        || !originalReceipt.orElseThrow().operationId().equals(originalOperationId)) {
                    throw new IllegalArgumentException("FOUND requires matching original receipt");
                }
            } else if (originalReceipt.isPresent()) {
                throw new IllegalArgumentException("NOT_FOUND cannot contain a receipt");
            }
        }
    }

    public record ProtocolFailure(
            Optional<UntrustedCorrelation> correlation,
            StructuralCode code) implements Response {
        public ProtocolFailure {
            requireArgument(correlation, "correlation");
            requireArgument(code, "code");
        }
    }

    public record CommandNotice(UUID operationId, NoticeReason reason) implements Response {
        public CommandNotice {
            requireArgument(operationId, "operationId");
            requireArgument(reason, "reason");
        }
    }

    public sealed interface DecodeResult<T> permits Decoded, StructuralFailure {
    }

    public record Decoded<T>(T value) implements DecodeResult<T> {
        public Decoded {
            requireArgument(value, "value");
        }
    }

    public record StructuralFailure<T>(
            StructuralCode code,
            Optional<UntrustedCorrelation> correlation,
            DecodeStage stage) implements DecodeResult<T> {
        public StructuralFailure {
            requireArgument(code, "code");
            requireArgument(correlation, "correlation");
            requireArgument(stage, "stage");
        }
    }

    public record UntrustedCorrelation(UUID rawOperationId) {
        public UntrustedCorrelation {
            requireArgument(rawOperationId, "rawOperationId");
        }
    }

    public enum DecodeStage {
        FRAME_LENGTH,
        PREFIX,
        MESSAGE_KIND,
        OPERATION_ID,
        REQUEST_BODY,
        RESPONSE_BODY,
        RECEIPT_INVARIANTS
    }

    public enum MutationKind {
        DEPLOY(0x01),
        RETRACT(0x02);

        private final int wireCode;

        MutationKind(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static MutationKind fromWireCode(int code) {
            for (MutationKind value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum LookupStatus {
        FOUND(0x01),
        NOT_FOUND(0x02);

        private final int wireCode;

        LookupStatus(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static LookupStatus fromWireCode(int code) {
            for (LookupStatus value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum OperationPhase {
        QUEUED(0x01),
        EXECUTING(0x02),
        APPLIED(0x03),
        REJECTED(0x04),
        FAILED(0x05),
        UNKNOWN(0x06);

        private final int wireCode;

        OperationPhase(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static OperationPhase fromWireCode(int code) {
            for (OperationPhase value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum ReceiptReason {
        NONE(0),
        AUTHORIZATION_DENIED(1),
        EXPECTED_VERSIONS_STALE(2),
        TARGET_BINDING_INVALID(3),
        MODULE_GENERATION_STALE(4),
        QUEUE_EXPIRED(5),
        QUEUE_POLICY_UNAVAILABLE(6),
        QUEUE_CAPACITY_REACHED(7),
        OPERATION_ID_CONFLICT(8),
        EFFECT_FAILURE(9),
        CRITICAL_COMMIT_FAILURE(10),
        CLEANUP_DEBT(11),
        OUTCOME_UNKNOWN(12),
        SEQUENCE_EXHAUSTED(13),
        INTERNAL_FAILURE(14);

        private final int wireCode;

        ReceiptReason(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static ReceiptReason fromWireCode(int code) {
            for (ReceiptReason value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum NoticeReason {
        AUTHORIZATION_DENIED(1),
        QUEUE_POLICY_UNAVAILABLE(6),
        QUEUE_CAPACITY_REACHED(7),
        OPERATION_ID_CONFLICT(8);

        private final int wireCode;

        NoticeReason(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static NoticeReason fromWireCode(int code) {
            for (NoticeReason value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum ActualEffectStatus {
        NOT_STARTED(0),
        NONE_CONFIRMED(1),
        PARTIAL_CONFIRMED(2),
        APPLIED_CONFIRMED(3),
        UNKNOWN(4);

        private final int wireCode;

        ActualEffectStatus(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static ActualEffectStatus fromWireCode(int code) {
            for (ActualEffectStatus value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum CriticalCommitStatus {
        NOT_ATTEMPTED(0),
        COMMITTED(1),
        FAILED(2),
        UNKNOWN(3);

        private final int wireCode;

        CriticalCommitStatus(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static CriticalCommitStatus fromWireCode(int code) {
            for (CriticalCommitStatus value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum ReadinessStatus {
        NOT_EVALUATED(0),
        NOT_READY(1),
        QUALIFIED(2),
        INVALIDATED(3);

        private final int wireCode;

        ReadinessStatus(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static ReadinessStatus fromWireCode(int code) {
            for (ReadinessStatus value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum CleanupStatus {
        NOT_REQUIRED(0),
        PENDING(1),
        COMPLETE(2),
        FAILED(3),
        UNKNOWN(4);

        private final int wireCode;

        CleanupStatus(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static CleanupStatus fromWireCode(int code) {
            for (CleanupStatus value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }

    public enum StructuralCode {
        TRUNCATED_OR_WRONG_LENGTH(1),
        BAD_MAGIC(2),
        UNSUPPORTED_SCHEMA(3),
        UNKNOWN_KIND_OR_CODE(4),
        INVALID_RANGE_OR_BODY(5);

        private final int wireCode;

        StructuralCode(int wireCode) {
            this.wireCode = wireCode;
        }

        public int wireCode() {
            return wireCode;
        }

        private static StructuralCode fromWireCode(int code) {
            for (StructuralCode value : values()) {
                if (value.wireCode == code) {
                    return value;
                }
            }
            return null;
        }
    }
}

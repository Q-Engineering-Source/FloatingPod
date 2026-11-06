package com.wi.floatingpod.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** schema5 专用双链容器 DATA；不验证 artifact 完整语义或签发 native/SQL 权限。 */
public final class B1ResumeRecordV6 {
    public static final String DOMAIN = "FP_B1_RESUME_BINARY_BE_V6";
    public static final int MAX_RECORD_BYTES = 65536;
    public static final String SLOT_0 = ".floatingpod-b1-resume-v6.0";
    public static final String SLOT_1 = ".floatingpod-b1-resume-v6.1";
    public static final String TEMPORARY = ".floatingpod-b1-resume-v6.tmp";
    public record References(B1Full26WitnessReference originalPrepare, B1Full26WitnessReference pending,
            B1Full26WitnessReference settled, B1Full26WitnessReference acknowledgedCertify) { }
    private final B1ResumeRecord transition;
    private final byte[] previousContainerSha256;
    private final References references;

    public B1ResumeRecordV6(B1ResumeRecord transition, byte[] previousContainerSha256, References references) throws IOException {
        this.transition = Objects.requireNonNull(transition);
        this.previousContainerSha256 = B1Full26WitnessReference.hash(previousContainerSha256);
        this.references = Objects.requireNonNull(references);
        B1ResumeRecordCodec.validateSchema5Data(transition);
        boolean innerZero = B1ResumeRecord.allZero(transition.previousRecordSha256());
        boolean outerZero = B1ResumeRecord.allZero(this.previousContainerSha256);
        if (transition.sequence() == 0 ? transition.kind() != B1ResumeRecord.Kind.GENESIS || !innerZero || !outerZero
                : transition.kind() == B1ResumeRecord.Kind.GENESIS || innerZero || outerZero)
            throw new IOException("V6 requires genuine genesis or both nonzero predecessor chains");
        validateReferences();
    }
    public B1ResumeRecord transition() { return transition; }
    public byte[] previousContainerSha256() { return previousContainerSha256.clone(); }
    public References references() { return references; }
    public List<B1Full26WitnessReference> allReferences() {
        var result = new ArrayList<B1Full26WitnessReference>(4);
        for (var ref : new B1Full26WitnessReference[]{references.originalPrepare(), references.pending(),
                references.settled(), references.acknowledgedCertify()}) if (ref != null) result.add(ref);
        return List.copyOf(result);
    }
    private void validateReferences() throws IOException {
        var s = transition.assertions();
        byte[] ddl = HexFormat.of().parseHex(WindowsWorldProvisionReceiptDataCodec.decode(transition.originalReceipt()).plan().ddlSha256());
        requireRef(s.originalPrepareWitness(), references.originalPrepare(), ddl);
        requireRef(s.pendingAttempt(), references.pending(), ddl);
        requireRef(s.latestSettlement() == null ? null : s.latestSettlement().settledAttempt(), references.settled(), ddl);
        requireRef(s.acknowledgedCertifyWitness(), references.acknowledgedCertify(), ddl);
        var identities = new HashMap<UUID, B1Full26WitnessReference>();
        B1Full26WitnessReference first = null;
        for (var ref : allReferences()) {
            var prior = identities.putIfAbsent(ref.attemptId(), ref);
            if (prior != null && !prior.exactlyEquals(ref)) throw new IOException("Same attempt has conflicting Full26 references");
            if (first != null && (!Arrays.equals(first.configurationSha256(), ref.configurationSha256())
                    || !Arrays.equals(first.ddlSha256(), ref.ddlSha256())
                    || !Arrays.equals(first.sourceLimitProfileSha256(), ref.sourceLimitProfileSha256())))
                throw new IOException("Full26 C/D/S declarations differ across retained references");
            first = ref;
        }
    }
    private void requireRef(B1ResumeRecord.AttemptAssertions attempt, B1Full26WitnessReference ref, byte[] ddl) throws IOException {
        if ((attempt == null) != (ref == null)) throw new IOException("Every retained attempt requires exactly its Full26 reference");
        if (ref == null) return;
        if (!attempt.attemptId().equals(ref.attemptId()) || attempt.phase() != ref.phase()
                || !attempt.logicalWriteUuid().equals(ref.logicalOperation())
                || !Arrays.equals(transition.sourceAnchorSha256(), ref.sourceAnchorSha256())
                || !Arrays.equals(WindowsWorldProvisioner.digest(transition.originalReceipt()), ref.originalReceiptSha256())
                || !Arrays.equals(ddl, ref.ddlSha256()))
            throw new IOException("Full26 reference differs from original attempt/source/receipt/DDL");
    }
    public static byte[] encodeData(B1ResumeRecordV6 value) throws IOException {
        Objects.requireNonNull(value);
        byte[] inner = B1ResumeRecordCodec.encodeSchema5Data(value.transition);
        var sink = new Bounded();
        try (var out = new DataOutputStream(sink)) {
            text(out, DOMAIN); out.write(value.previousContainerSha256); out.writeInt(inner.length); out.write(inner);
            reference(out, value.references.originalPrepare()); reference(out, value.references.pending());
            reference(out, value.references.settled()); reference(out, value.references.acknowledgedCertify());
        }
        return sink.bytes.toByteArray();
    }
    public static B1ResumeRecordV6 decodeData(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_RECORD_BYTES) throw new IOException("V6 record bound");
        byte[] input = bytes.clone();
        try (var in = new DataInputStream(new ByteArrayInputStream(input))) {
            if (!DOMAIN.equals(text(in, 128))) throw new IOException("Unknown V6 domain");
            byte[] previous = read(in, 32); int size = in.readInt();
            if (size <= 0 || size > MAX_RECORD_BYTES || size > in.available()) throw new IOException("V6 inner transition bound");
            var transition = B1ResumeRecordCodec.decodeSchema5Data(read(in, size));
            var refs = new References(reference(in), reference(in), reference(in), reference(in));
            if (in.read() != -1) throw new IOException("V6 trailing bytes");
            var result = new B1ResumeRecordV6(transition, previous, refs);
            B1ResumeRecordCodec.exact(input, encodeData(result), "Noncanonical V6 DATA");
            return result;
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IOException("Invalid V6 DATA identity", failure);
        }
    }
    public static void consecutiveData(B1ResumeRecordV6 older, byte[] olderBytes, B1ResumeRecordV6 newer) throws IOException {
        B1ResumeRecordCodec.exact(encodeData(older), olderBytes, "V6 predecessor bytes differ from actual older DATA");
        B1ResumeRecordCodec.exact(WindowsWorldProvisioner.digest(olderBytes), newer.previousContainerSha256, "V6 outer predecessor changed");
        B1ResumeRecordCodec.consecutiveSchema5Data(older.transition, B1ResumeRecordCodec.encodeSchema5Data(older.transition), newer.transition);
        preserve(older.references.originalPrepare(), newer.references.originalPrepare());
        preserve(older.references.acknowledgedCertify(), newer.references.acknowledgedCertify());
        if(older.references.originalPrepare()==null && newer.references.originalPrepare()!=null &&
                (newer.transition.kind()!=B1ResumeRecord.Kind.PREPARED_CONFIRMED ||
                 older.transition.kind()!=B1ResumeRecord.Kind.GENESIS && older.transition.kind()!=B1ResumeRecord.Kind.PREPARE_SUCCESSOR_INTENT))
            throw new IOException("Original PREPARE witness/reference can initialize only at its first exact confirmation");
        for (var old : older.allReferences()) for (var next : newer.allReferences())
            if (old.attemptId().equals(next.attemptId()) && !old.exactlyEquals(next))
                throw new IOException("Shared retained Full26 attempt reference changed");
        var pending = older.references.pending();
        if (pending != null && newer.allReferences().stream().noneMatch(pending::exactlyEquals))
            throw new IOException("Unresolved older pending Full26 attempt disappeared");
        if(older.transition.kind()==B1ResumeRecord.Kind.PREPARE_SUCCESSOR_INTENT &&
                newer.transition.kind()==B1ResumeRecord.Kind.PREPARED_CONFIRMED){
            if(pending==null || newer.references.settled()==null || !pending.exactlyEquals(newer.references.settled()))
                throw new IOException("Successor confirmation must settle exact original pending Full26 reference");
            if(older.references.originalPrepare()==null && (newer.references.originalPrepare()==null ||
                    !pending.exactlyEquals(newer.references.originalPrepare())))
                throw new IOException("First confirmation must initialize exact successor original reference");
        }
    }
    private static void preserve(B1Full26WitnessReference old, B1Full26WitnessReference next) throws IOException {
        if (old != null && !old.exactlyEquals(next)) throw new IOException("Original prepare/ACK Full26 reference removed or changed");
    }
    /** 完整连续历史的 DATA 验证；返回唯一实际确认的 PREPARE，不签发恢复或安装权限。 */
    public static B1Full26WitnessReference validateHistoryData(List<B1ResumeRecordV6> history) throws IOException {
        Objects.requireNonNull(history);
        if(history.isEmpty() || history.getFirst().transition.sequence()!=0)
            throw new IOException("Complete V6 history must begin at actual genesis");
        var attempts=new HashMap<UUID,byte[]>();
        var references=new HashMap<UUID,B1Full26WitnessReference>();
        var executors=new HashMap<UUID,B1ResumeRecord.NativeOwnerAssertions>();
        for(var record:history){
            Objects.requireNonNull(record);
            var state=record.transition.assertions();
            for(var owner:new B1ResumeRecord.NativeOwnerAssertions[]{state.currentOwner(),state.previousOwner()}){
                if(owner==null)continue;
                var prior=executors.putIfAbsent(owner.boot(),owner);
                if(prior!=null&&!prior.equals(owner))throw new IOException("Historical native boot has conflicting complete process identity");
            }
        }
        B1Full26WitnessReference committed=null;
        B1ResumeRecordV6 previous=null;
        for(var record:history){
            Objects.requireNonNull(record);
            if(previous!=null)consecutiveData(previous,encodeData(previous),record);
            var state=record.transition.assertions();
            var entering=state.pendingAttempt();
            requireWriterExecutor(state.currentWriter(),executors);
            requireWriterExecutor(state.targetWriter(),executors);
            if(entering!=null && attempts.containsKey(entering.attemptId()) &&
                    (previous==null || previous.assertionsPending()==null ||
                     !previous.assertionsPending().attemptId().equals(entering.attemptId())))
                throw new IOException("A settled historical execution attempt cannot be reopened as a new pending execution");
            if(record.transition.kind()==B1ResumeRecord.Kind.PREPARE_SUCCESSOR_INTENT &&
                    state.originalPrepareWitness()==null && committed!=null)
                throw new IOException("Successor cannot claim first confirmation after historical committed PREPARE");
            for(var attempt:new B1ResumeRecord.AttemptAssertions[]{state.originalPrepareWitness(),state.pendingAttempt(),
                    state.latestSettlement()==null?null:state.latestSettlement().settledAttempt(),state.acknowledgedCertifyWitness()}){
                if(attempt==null)continue;
                var executor=executors.get(attempt.context().boot());
                if(executor==null||executor.generation()!=attempt.bootGeneration())
                    throw new IOException("Historical attempt executor lacks its complete native process identity");
                requireWriterExecutor(attempt.beforeWriter(),executors);
                requireWriterExecutor(attempt.targetWriter(),executors);
                var bytes=B1ResumeRecordCodec.attemptBytes(attempt);
                var prior=attempts.putIfAbsent(attempt.attemptId(),bytes);
                if(prior!=null)B1ResumeRecordCodec.exact(prior,bytes,"Historical execution attempt changed complete scope/body assertions");
            }
            for(var ref:record.allReferences()){
                var prior=references.putIfAbsent(ref.attemptId(),ref);
                if(prior!=null && !prior.exactlyEquals(ref))throw new IOException("Historical Full26 reference changed");
            }
            if(record.transition.kind()==B1ResumeRecord.Kind.PREPARED_CONFIRMED){
                var settlement=state.latestSettlement();
                if(previous==null || previous.assertionsPending()==null || settlement==null ||
                        settlement.kind()!=B1ResumeRecord.SettlementKind.CONFIRMED ||
                        settlement.settledAttempt().phase()!=B1ResumeRecord.Phase.PREPARE || state.pendingAttempt()!=null)
                    throw new IOException("PREPARED confirmation requires actual preceding pending PREPARE settlement");
                B1ResumeRecordCodec.exact(B1ResumeRecordCodec.attemptBytes(previous.assertionsPending()),
                    B1ResumeRecordCodec.attemptBytes(settlement.settledAttempt()),"Confirmed PREPARE differs from actual preceding execution");
                B1ResumeRecordCodec.requireConfirmedPrepareState(previous.assertionsPending(),state);
                var ref=record.references.settled();
                if(ref==null || previous.references.pending()==null || !ref.exactlyEquals(previous.references.pending()))
                    throw new IOException("Confirmed PREPARE lacks original pending Full26 reference");
                if(committed!=null && !committed.exactlyEquals(ref))throw new IOException("Different PREPARE executions both claim committed confirmation");
                committed=ref;
            }
            previous=record;
        }
        return committed;
    }
    private static void requireWriterExecutor(B1ResumeRecord.WriterAssertions writer,
            HashMap<UUID,B1ResumeRecord.NativeOwnerAssertions> executors)throws IOException {
        if(writer.ownerBoot()!=null&&!executors.containsKey(writer.ownerBoot()))
            throw new IOException("Historical SQL W owner lacks complete native process identity");
        if(writer.lastSlot()!=null&&!executors.containsKey(writer.lastSlot().boot()))
            throw new IOException("Historical logical receipt executor lacks complete native process identity");
    }
    private B1ResumeRecord.AttemptAssertions assertionsPending(){return transition.assertions().pendingAttempt();}
    private static void reference(DataOutputStream out, B1Full26WitnessReference ref) throws IOException {
        out.writeBoolean(ref != null); if (ref == null) return;
        text(out, ref.basename()); out.writeLong(ref.fileIdentity().volumeSerialNumber()); out.write(ref.fileIdentity().fileId128());
        out.writeLong(ref.fullLength()); out.write(ref.fullSha256()); out.write(ref.sourceAnchorSha256());
        out.write(ref.originalReceiptSha256()); out.write(ref.sourceLimitProfileSha256()); out.write(ref.configurationSha256()); out.write(ref.ddlSha256());
        uuid(out, ref.attemptId()); out.writeInt(ref.phase().ordinal()); uuid(out, ref.logicalOperation());
    }
    private static B1Full26WitnessReference reference(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte(); if (tag == 0) return null; if (tag != 1) throw new IOException("V6 optional reference tag");
        String name = text(in, 256); var id = new WindowsMarkerIo.FileIdentity(in.readLong(), read(in, 16));
        long length = in.readLong(); byte[] full = read(in, 32), source = read(in, 32), receipt = read(in, 32);
        byte[] profile = read(in, 32), configuration = read(in, 32), ddl = read(in, 32);
        UUID attempt = uuid(in); int phase = in.readInt();
        if (phase < 0 || phase >= B1ResumeRecord.Phase.values().length) throw new IOException("V6 phase ordinal");
        return new B1Full26WitnessReference(name, id, length, full, source, receipt, profile, configuration, ddl,
            attempt, B1ResumeRecord.Phase.values()[phase], uuid(in));
    }
    private static void uuid(DataOutputStream out, UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }
    private static byte[] read(DataInputStream in, int length) throws IOException {
        byte[] bytes = in.readNBytes(length); if (bytes.length != length) throw new EOFException("Truncated V6 DATA"); return bytes;
    }
    private static void text(DataOutputStream out, String value) throws IOException {
        var encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
        if (encoded.remaining() <= 0 || encoded.remaining() > 256) throw new IOException("V6 text bound");
        out.writeInt(encoded.remaining()); byte[] bytes = new byte[encoded.remaining()]; encoded.get(bytes); out.write(bytes);
    }
    private static String text(DataInputStream in, int limit) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > limit || length > in.available()) throw new IOException("V6 text bound");
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(read(in, length))).toString();
    }
    private static final class Bounded extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        @Override public void write(int value) throws IOException { require(1); bytes.write(value); }
        @Override public void write(byte[] value, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, value.length); require(length); bytes.write(value, offset, length);
        }
        private void require(int length) throws IOException {
            if (length > MAX_RECORD_BYTES - bytes.size()) throw new IOException("V6 record exceeds 65536 bytes");
        }
    }
}

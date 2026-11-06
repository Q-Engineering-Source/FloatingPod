package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static com.wi.floatingpod.server.B1Full26WitnessReferenceTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 结构图仅为 DATA fixture；这里不声称完整 Full26 payload 或文件/native ACK。 */
class B1ResumeRecordV6Test {
    @TempDir Path directory;
    private static B1ResumeRecordV6 genesis() throws Exception {
        var r = B1ResumeRecordCodecTest.schema5DataRecord();
        return wrap(r, new byte[32]);
    }
    private static B1ResumeRecordV6 wrap(B1ResumeRecord r, byte[] previous) throws Exception {
        var s = r.assertions();
        return new B1ResumeRecordV6(r, previous, new B1ResumeRecordV6.References(
            s.originalPrepareWitness() == null ? null : reference(r, s.originalPrepareWitness()),
            s.pendingAttempt() == null ? null : reference(r, s.pendingAttempt()),
            s.latestSettlement() == null ? null : reference(r, s.latestSettlement().settledAttempt()),
            s.acknowledgedCertifyWitness() == null ? null : reference(r, s.acknowledgedCertifyWitness())));
    }
    private static B1ResumeRecord record(B1ResumeRecord basis, long seq, byte[] previous, Kind kind, FullStateAssertions state) {
        return new B1ResumeRecord(seq, previous, kind, basis.sourceAnchorSha256(), basis.originalReceipt(), basis.barrierNamespace(), state);
    }
    private static B1ResumeRecordV6 prepared(B1ResumeRecordV6 old) throws Exception {
        var r = old.transition();
        var s=B1TwoSlotFixture.prepared(r.assertions());var attempt=r.assertions().pendingAttempt();
        var confirmed=new FullStateAssertions(s.currentOwner(),s.previousOwner(),s.retirement(),attempt,
            attempt.targetWriter(),attempt.targetWriter(),null,
            new SettlementAssertions(SettlementKind.CONFIRMED,attempt,"TEST_ONLY_EXACT_PREPARED_CONFIRMATION",new byte[]{1},new byte[32]),null,null);
        return wrap(record(r, r.sequence() + 1, WindowsWorldProvisioner.digest(B1ResumeRecordCodec.encodeSchema5Data(r)),
            Kind.PREPARED_CONFIRMED, confirmed), WindowsWorldProvisioner.digest(B1ResumeRecordV6.encodeData(old)));
    }
    private static AttemptAssertions other(AttemptAssertions a, Phase phase, UUID attempt, UUID operation) {
        var c = new OperationAssertions(a.context().boot(), a.context().writerEpoch(), a.context().binding(), operation,
            a.context().effects(), a.context().originalOperationFingerprint());
        var target = new WriterAssertions(c.binding(), c.writerEpoch(), c.boot(), a.targetWriter().leaseUntil(),
            new ReceiptSlotAssertions(c.boot(), c.writerEpoch(), c.binding(), c.operation(), c.originalOperationFingerprint()));
        return new AttemptAssertions(attempt, phase, operation, a.bootGeneration(), a.connectionGeneration(), a.connectionId(), a.namespace(),
            c, a.beforeWriter(), target, a.beforeGraph(), a.targetGraph());
    }
    private static B1ResumeRecordV6 fourSlots() throws Exception {
        var g = genesis(); var r = g.transition(); var s = r.assertions(); var a = s.pendingAttempt();
        var certify = other(a, Phase.CERTIFY, UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf3333"), WindowsWorldProvisionReceiptDataCodecTest.plan(5).certifyWriteUuid());
        var state = new FullStateAssertions(s.currentOwner(), null, null, a, s.currentWriter(), s.targetWriter(), a,
            new SettlementAssertions(SettlementKind.CONFIRMED, a, "TEST_ONLY_STRUCTURAL_SETTLEMENT", new byte[]{1}, new byte[32]), null, certify);
        return wrap(record(r, 1, hash(1), Kind.DB_ACQUIRED, state), hash(2));
    }
    private static B1Full26WitnessReference[] refs(B1ResumeRecordV6 v) {
        var r = v.references(); return new B1Full26WitnessReference[]{r.originalPrepare(), r.pending(), r.settled(), r.acknowledgedCertify()};
    }
    private static B1ResumeRecordV6 withRefs(B1ResumeRecordV6 v, B1Full26WitnessReference[] refs) throws IOException {
        return new B1ResumeRecordV6(v.transition(), v.previousContainerSha256(), new B1ResumeRecordV6.References(refs[0], refs[1], refs[2], refs[3]));
    }
    private static B1Full26WitnessReference changed(B1Full26WitnessReference ref, int field) {
        byte[][] hashes = {ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256()};
        if (field < 6) hashes[field][0] ^= 1;
        UUID attempt = field == 6 ? UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf9999") : ref.attemptId();
        return new B1Full26WitnessReference(B1Full26WitnessReference.basename(attempt, hashes[0]), ref.fileIdentity(), ref.fullLength(),
            hashes[0], hashes[1], hashes[2], hashes[3], hashes[4], hashes[5], attempt,
            field == 7 ? Phase.HANDOFF : ref.phase(), field == 8 ? UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf8888") : ref.logicalOperation());
    }
    @Test void independentFixedV6GoldenAndExactOuterFraming() throws Exception {
        var value = genesis(); byte[] actual = B1ResumeRecordV6.encodeData(value);
        // Fixed expected length/digest computed independently with Python struct/UUID/SHA-256,
        // including the complete literal structural fixture, not by this encoder or its output.
        assertEquals(2389, actual.length);
        assertEquals("F7197078FDA5D7436F8CAD03C12AC89922EEA70758D1EB9C8BABD518FD532574", HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(actual)));
        var outBytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(outBytes)) {
            independentText(out, "FP_B1_RESUME_BINARY_BE_V6"); out.write(new byte[32]);
            byte[] inner = B1ResumeRecordCodec.encodeSchema5Data(value.transition()); out.writeInt(inner.length); out.write(inner);
            out.writeByte(0); independentReference(out); out.writeByte(0); out.writeByte(0);
        }
        assertArrayEquals(outBytes.toByteArray(), actual);
    }
    @Test void completeHistoryRequiresGenesisAndExactConfirmedPrepareSettlement() throws Exception {
        var original=genesis();
        assertNull(B1ResumeRecordV6.validateHistoryData(java.util.List.of(original)));
        assertThrows(IOException.class,()->B1ResumeRecordV6.validateHistoryData(java.util.List.of()));
        var confirmed=prepared(original);
        assertThrows(IOException.class,()->B1ResumeRecordV6.validateHistoryData(java.util.List.of(confirmed)));
        var s=confirmed.transition().assertions();
        var attempt=original.transition().assertions().pendingAttempt();
        var state=new FullStateAssertions(s.currentOwner(),s.previousOwner(),s.retirement(),attempt,
            attempt.targetWriter(),attempt.targetWriter(),null,
            new SettlementAssertions(SettlementKind.CONFIRMED,attempt,"TEST_ONLY_COMPLETE_CONFIRMATION",new byte[]{1},new byte[32]),
            s.marker(),s.acknowledgedCertifyWitness());
        var complete=successor(original,state,Kind.PREPARED_CONFIRMED);
        assertTrue(original.references().pending().exactlyEquals(
            B1ResumeRecordV6.validateHistoryData(java.util.List.of(original,complete))));
        assertThrows(IOException.class,()->B1ResumeRecordV6.validateHistoryData(java.util.List.of(original,complete,complete)));
    }
    @Test void fullHistoryRequiresCompleteExecutorIdentitiesBeforeNativeRetirement() throws Exception {
        var g=genesis();var r=g.transition();var s=r.assertions();var a=s.pendingAttempt();
        assertNull(B1ResumeRecordV6.validateHistoryData(java.util.List.of(g)));
        var orphan=UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf9090");
        var c=new OperationAssertions(orphan,a.context().writerEpoch(),a.context().binding(),a.context().operation(),
            a.context().effects(),a.context().originalOperationFingerprint());
        var w=new WriterAssertions(a.targetWriter().binding(),a.targetWriter().epoch(),orphan,a.targetWriter().leaseUntil(),null);
        var missing=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),
            a.connectionId(),a.namespace(),c,a.beforeWriter(),w,a.beforeGraph(),a.targetGraph());
        var missingRecord=wrap(record(r,0,new byte[32],Kind.GENESIS,new FullStateAssertions(s.currentOwner(),null,null,null,
            missing.beforeWriter(),missing.targetWriter(),missing,null,null,null)),new byte[32]);
        assertThrows(IOException.class,()->B1ResumeRecordV6.validateHistoryData(java.util.List.of(missingRecord)));
        var wrongGeneration=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration()+1,
            a.connectionGeneration(),a.connectionId(),a.namespace(),a.context(),a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        var wrongRecord=wrap(record(r,0,new byte[32],Kind.GENESIS,new FullStateAssertions(s.currentOwner(),null,null,null,
            a.beforeWriter(),a.targetWriter(),wrongGeneration,null,null,null)),new byte[32]);
        assertThrows(IOException.class,()->B1ResumeRecordV6.validateHistoryData(java.util.List.of(wrongRecord)));
        var duplicate=new NativeOwnerAssertions(s.currentOwner().pid()+4,s.currentOwner().fileTime(),s.currentOwner().boot(),s.currentOwner().generation());
        var conflicting=wrap(record(r,0,new byte[32],Kind.GENESIS,new FullStateAssertions(s.currentOwner(),duplicate,
            new RetirementAssertions("TEST_ONLY_DATA_RETIREMENT",new byte[]{1}),null,a.beforeWriter(),a.targetWriter(),a,null,null,null)),new byte[32]);
        assertThrows(IOException.class,()->B1ResumeRecordV6.validateHistoryData(java.util.List.of(conflicting)));
    }
    @Test void successorPrepareUsesNewExecutionAndPreservesCancelledProofUntilExactConfirmation() throws Exception {
        var base=genesis();var r=base.transition();var s=r.assertions();var a=s.pendingAttempt();
        var seed=new WriterAssertions(a.beforeWriter().binding(),0,null,a.beforeWriter().leaseUntil(),null);
        var first=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),
            a.namespace(),a.context(),seed,a.targetWriter(),a.beforeGraph(),a.targetGraph());
        var g=wrap(record(r,0,new byte[32],Kind.GENESIS,new FullStateAssertions(s.currentOwner(),null,null,null,seed,
            first.targetWriter(),first,null,null,null)),new byte[32]);
        var cancelled=new SettlementAssertions(SettlementKind.CANCELLED,first,"TEST_ONLY_ORIGINAL_UNKNOWN_SETTLEMENT",new byte[]{1,2},hash(9));
        var old=successor(g,new FullStateAssertions(s.currentOwner(),null,null,null,seed,seed,null,cancelled,null,null),Kind.ATTEMPT_CANCELLED);
        assertNull(old.transition().assertions().originalPrepareWitness());
        assertNull(old.references().originalPrepare());
        assertNull(old.transition().assertions().pendingAttempt());
        assertNotNull(old.references().settled());
        // 首次确认之前只允许双null；单边reference不能从合法cancelled历史铸造原PREPARE。
        var oneSided=new B1ResumeRecordV6.References(old.references().settled(),null,old.references().settled(),null);
        assertThrows(IOException.class,()->new B1ResumeRecordV6(old.transition(),old.previousContainerSha256(),oneSided));
        var boot=UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf7777");
        var context=new OperationAssertions(boot,1,seed.binding(),first.logicalWriteUuid(),first.context().effects(),first.context().originalOperationFingerprint());
        var target=new WriterAssertions(seed.binding(),1,boot,first.targetWriter().leaseUntil(),
            new ReceiptSlotAssertions(boot,1,seed.binding(),context.operation(),context.originalOperationFingerprint()));
        var pending=new AttemptAssertions(UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf6666"),Phase.PREPARE,first.logicalWriteUuid(),
            first.bootGeneration()+1,UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf5555"),first.connectionId()+1,first.namespace(),context,
            seed,target,first.beforeGraph(),first.targetGraph());
        var owner=new NativeOwnerAssertions(s.currentOwner().pid(),s.currentOwner().fileTime(),boot,s.currentOwner().generation()+1);
        var retired=new RetirementAssertions("TEST_ONLY_DATA_RETIREMENT",new byte[]{3});
        var intent=successor(old,new FullStateAssertions(owner,s.currentOwner(),retired,null,seed,target,pending,cancelled,null,null),Kind.PREPARE_SUCCESSOR_INTENT);
        B1ResumeRecordV6.consecutiveData(old,B1ResumeRecordV6.encodeData(old),intent);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(intent.transition()));
        assertNull(B1ResumeRecordV6.validateHistoryData(java.util.List.of(g,old,intent)));
        assertThrows(IOException.class,()->B1ResumeRecordV6.validateHistoryData(java.util.List.of(old,intent)));
        assertNull(intent.transition().assertions().originalPrepareWitness());
        assertNull(intent.references().originalPrepare());
        var confirmed=successor(intent,new FullStateAssertions(owner,s.currentOwner(),retired,pending,target,target,null,
            new SettlementAssertions(SettlementKind.CONFIRMED,pending,"TEST_ONLY_CONFIRMED_SUCCESSOR",new byte[]{4},hash(9)),null,null),Kind.PREPARED_CONFIRMED);
        assertTrue(intent.references().pending().exactlyEquals(B1ResumeRecordV6.validateHistoryData(java.util.List.of(g,old,intent,confirmed))));
        var changed=new SettlementAssertions(SettlementKind.CANCELLED,first,cancelled.proofProtocol(),new byte[]{8},hash(9));
        var bad=successor(old,new FullStateAssertions(owner,s.currentOwner(),retired,null,seed,target,pending,changed,null,null),Kind.PREPARE_SUCCESSOR_INTENT);
        assertThrows(IOException.class,()->B1ResumeRecordV6.consecutiveData(old,B1ResumeRecordV6.encodeData(old),bad));
    }
    private static void independentText(DataOutputStream out, String value) throws IOException {
        byte[] b = value.getBytes(StandardCharsets.UTF_8); out.writeInt(b.length); out.write(b);
    }
    private static void independentReference(DataOutputStream out) throws IOException {
        out.writeByte(1); independentText(out, ".floatingpod-b1-full26-v1-21a0707c-cd88-4004-8c06-5d790eaf1111-" + "07".repeat(32) + ".bin"); out.writeLong(0x1234); out.write(new byte[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5}); out.writeLong(90000);
        out.write(hash(7)); out.write(new byte[32]); out.write(HexFormat.of().parseHex("8B442C529074376413AE520045C17846E168D8A4F4A299E941B7E8313C5508DF"));
        out.write(hash(11)); out.write(hash(12)); byte[] ddl = new byte[32]; Arrays.fill(ddl, (byte)0xab); out.write(ddl);
        out.writeLong(0x21a0707ccd884004L); out.writeLong(0x8c065d790eaf1111L); out.writeInt(0);
        out.writeLong(0x1be6d5976a894e86L); out.writeLong(0x8881247c17ad603dL);
    }
    @Test void genuineSchema5GenesisRoundtripAndTemporaryFileData() throws Exception {
        var value = genesis(); byte[] bytes = B1ResumeRecordV6.encodeData(value);
        var path = directory.resolve(B1ResumeRecordV6.SLOT_0); Files.write(path, bytes);
        var read = B1ResumeRecordV6.decodeData(Files.readAllBytes(path));
        assertArrayEquals(bytes, B1ResumeRecordV6.encodeData(read));
        assertEquals(5, WindowsWorldProvisionReceiptDataCodec.decode(read.transition().originalReceipt()).plan().schemaFormat());
        assertTrue(read.references().pending().exactlyEquals(value.references().pending()));
        byte[] previous = read.previousContainerSha256(); previous[0] = 1; assertArrayEquals(new byte[32], read.previousContainerSha256());
    }
    @Test void fourSlotsRequireExactAttemptSourceReceiptDdlAndCoherentConfigurationProfile() throws Exception {
        var value = fourSlots(); assertEquals(4, value.allReferences().size());
        assertArrayEquals(B1ResumeRecordV6.encodeData(value), B1ResumeRecordV6.encodeData(B1ResumeRecordV6.decodeData(B1ResumeRecordV6.encodeData(value))));
        for (int slot = 0; slot < 4; slot++) {
            var missing = refs(value); missing[slot] = null;
            assertThrows(IOException.class, () -> withRefs(value, missing));
            for (int field = 0; field < 9; field++) {
                // The ACK has a distinct attempt: artifact hash may independently differ there.
                if (slot == 3 && field == 0) continue;
                var wrong = refs(value); wrong[slot] = changed(wrong[slot], field);
                assertThrows(IOException.class, () -> withRefs(value, wrong));
            }
        }
        var g = genesis(); var extra = refs(g); extra[0] = extra[1]; assertThrows(IOException.class, () -> withRefs(g, extra));
        // C/S are declared DATA: one coherent replacement is permitted pending artifact semantic validation.
        var changedAll = refs(value);
        for (int i = 0; i < changedAll.length; i++) changedAll[i] = changed(changed(changedAll[i], 3), 4);
        withRefs(value, changedAll);
    }
    @Test void realGenesisAndNonzeroDualChainsRejectResetAndHalfChains() throws Exception {
        var g = genesis(); var p = prepared(g);
        assertThrows(IOException.class, () -> new B1ResumeRecordV6(g.transition(), hash(1), g.references()));
        assertThrows(IOException.class, () -> new B1ResumeRecordV6(p.transition(), new byte[32], p.references()));
        var zeroInner = record(p.transition(), 1, new byte[32], Kind.PREPARED_CONFIRMED, p.transition().assertions());
        assertThrows(IOException.class, () -> new B1ResumeRecordV6(zeroInner, hash(1), p.references()));
        assertThrows(IllegalArgumentException.class, () -> record(g.transition(), 1, hash(1), Kind.GENESIS, g.transition().assertions()));
        assertThrows(IllegalArgumentException.class, () -> record(g.transition(), 0, new byte[32], Kind.DB_ACQUIRED, g.transition().assertions()));
        B1ResumeRecordV6.consecutiveData(g, B1ResumeRecordV6.encodeData(g), p);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(p, B1ResumeRecordV6.encodeData(p), g));
    }
    @Test void consecutiveRequiresActualOlderBytesAndBothCanonicalPredecessors() throws Exception {
        var g = genesis(); byte[] old = B1ResumeRecordV6.encodeData(g); var p = prepared(g);
        B1ResumeRecordV6.consecutiveData(g, old, p);
        byte[] corrupt = old.clone(); corrupt[0] ^= 1;
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(g, corrupt, p));
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(g, Arrays.copyOf(old, old.length + 1), p));
        var outer = new B1ResumeRecordV6(p.transition(), hash(9), p.references());
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(g, old, outer));
        var inner = new B1ResumeRecordV6(record(p.transition(), 1, hash(9), p.transition().kind(), p.transition().assertions()), p.previousContainerSha256(), p.references());
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(g, old, inner));
        var gap = new B1ResumeRecordV6(record(p.transition(), 2, p.transition().previousRecordSha256(), p.transition().kind(), p.transition().assertions()), p.previousContainerSha256(), p.references());
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(g, old, gap));
    }
    private static B1ResumeRecordV6 successor(B1ResumeRecordV6 old, FullStateAssertions state, Kind kind) throws Exception {
        return wrap(record(old.transition(), old.transition().sequence() + 1, WindowsWorldProvisioner.digest(B1ResumeRecordCodec.encodeSchema5Data(old.transition())), kind, state), WindowsWorldProvisioner.digest(B1ResumeRecordV6.encodeData(old)));
    }
    @Test void consecutivePreservesOriginalAckSharedAttemptsAndUnresolvedPending() throws Exception {
        var old = fourSlots(); var s = old.transition().assertions(); var a = s.pendingAttempt();
        var same = successor(old, s, Kind.DB_ACQUIRED); B1ResumeRecordV6.consecutiveData(old, B1ResumeRecordV6.encodeData(old), same);
        var noAck = new FullStateAssertions(s.currentOwner(), null, null, a, s.currentWriter(), s.targetWriter(), a, s.latestSettlement(), null, null);
        var removed = successor(old, noAck, Kind.DB_ACQUIRED);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(old, B1ResumeRecordV6.encodeData(old), removed));
        var shared = refs(same); for (int i = 0; i < 3; i++) shared[i] = changed(shared[i], 0);
        var conflicting = withRefs(same, shared);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(old, B1ResumeRecordV6.encodeData(old), conflicting));
        var handoff = other(a, Phase.HANDOFF, UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf4444"), UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf5555"));
        var pendingState = new FullStateAssertions(s.currentOwner(), null, null, a, s.currentWriter(), s.targetWriter(), handoff, s.latestSettlement(), null, s.acknowledgedCertifyWitness());
        var pending = successor(old, pendingState, Kind.DB_ACQUIRED);
        var vanishedState = new FullStateAssertions(s.currentOwner(), null, null, a, s.currentWriter(), s.targetWriter(), null, s.latestSettlement(), null, s.acknowledgedCertifyWitness());
        var vanished = successor(pending, vanishedState, Kind.DB_ACQUIRED);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(pending, B1ResumeRecordV6.encodeData(pending), vanished));
        var settledState = new FullStateAssertions(s.currentOwner(), null, null, a, s.currentWriter(), s.targetWriter(), null,
            new SettlementAssertions(SettlementKind.CONFIRMED, handoff, "TEST_ONLY_SETTLED", new byte[]{1}, new byte[32]), null, s.acknowledgedCertifyWitness());
        B1ResumeRecordV6.consecutiveData(pending, B1ResumeRecordV6.encodeData(pending), successor(pending, settledState, Kind.DB_ACQUIRED));
        var noOriginal = new FullStateAssertions(s.currentOwner(), null, null, null, s.currentWriter(), s.targetWriter(), null,
            new SettlementAssertions(SettlementKind.CANCELLED, a, "TEST_ONLY_CANCELLED", new byte[]{1}, new byte[32]), null, s.acknowledgedCertifyWitness());
        var lostOriginal = successor(old, noOriginal, Kind.ATTEMPT_CANCELLED);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.consecutiveData(old, B1ResumeRecordV6.encodeData(old), lostOriginal));
    }
    @Test void rejectsLegacyContainersTransitionsAndFull20ReferenceNames() throws Exception {
        var g = genesis(); byte[] bytes = B1ResumeRecordV6.encodeData(g);
        assertThrows(IOException.class, () -> B1ResumeRecordCodec.decode(bytes));
        assertThrows(IOException.class, () -> B1ResumeRecordV5.decodeData(bytes));
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(B1ResumeRecordCodec.encodeSchema5Data(g.transition())));
        for (int format : new int[]{2, 3}) {
            var r = g.transition();
            var wrong = new B1ResumeRecord(0, new byte[32], Kind.GENESIS, r.sourceAnchorSha256(), WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodecTest.receipt(format)), r.barrierNamespace(), r.assertions());
            assertThrows(IOException.class, () -> new B1ResumeRecordV6(wrong, new byte[32], g.references()));
            byte[] legacy = B1ResumeRecordCodec.encode(wrong);
            assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(legacy));
            var a = wrong.assertions().pendingAttempt(); var s = wrong.assertions();
            var post = record(wrong, 1, new byte[32], Kind.DB_ACQUIRED,
                new FullStateAssertions(s.currentOwner(), null, null, a, s.currentWriter(), s.targetWriter(), a, null, null, null));
            var legacyRef = new B1Full20WitnessReference(B1Full20WitnessReference.basename(a.attemptId(), hash(7)), identity(5), 90000,
                hash(7), post.sourceAnchorSha256(), WindowsWorldProvisioner.digest(post.originalReceipt()), hash(11), a.attemptId(), a.phase(), a.logicalWriteUuid());
            var v5 = new B1ResumeRecordV5(post, 0, new byte[32], identity(6), new byte[32], new B1ResumeRecordV5.References(legacyRef, legacyRef, null, null));
            assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(B1ResumeRecordV5.encodeData(v5)));
        }
        byte[] foreign = bytes.clone(); byte[] prefix = "full26".getBytes(StandardCharsets.US_ASCII);
        int name = referenceStart(bytes) + 5;
        for (int i = name; i < foreign.length - prefix.length; i++) if (Arrays.equals(Arrays.copyOfRange(foreign, i, i + prefix.length), prefix)) { foreign[i + 5] = '0'; break; }
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(foreign));
    }
    private static int referenceStart(byte[] bytes) {
        ByteBuffer in = ByteBuffer.wrap(bytes); int domain = in.getInt(); in.position(4 + domain + 32); int inner = in.getInt(); return in.position() + inner;
    }
    @Test void malformedWireRejectsEofUtf8TagsPhaseAndBoundWithoutFallback() throws Exception {
        byte[] good = B1ResumeRecordV6.encodeData(genesis());
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(null));
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(new byte[0]));
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(new byte[65537]));
        for (int length : new int[]{1, 4, good.length - 1}) assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(Arrays.copyOf(good, length)));
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(Arrays.copyOf(good, good.length + 1)));
        byte[] utf8 = good.clone(); utf8[4] = (byte)0xc0;
        assertThrows(CharacterCodingException.class, () -> B1ResumeRecordV6.decodeData(utf8));
        int refs = referenceStart(good); byte[] tag = good.clone(); tag[refs] = 2;
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(tag));
        byte[] phase = good.clone(); int nameLength = ByteBuffer.wrap(good).getInt(refs + 2);
        int phaseOffset = refs + 2 + 4 + nameLength + 24 + 8 + 192 + 16; ByteBuffer.wrap(phase).putInt(phaseOffset, 3);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(phase));
        byte[] size = good.clone(); ByteBuffer.wrap(size).putInt(4 + B1ResumeRecordV6.DOMAIN.length() + 32, Integer.MAX_VALUE);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(size));
        byte[] nameUtf8 = good.clone(); nameUtf8[refs + 6] = (byte)0xc0;
        assertThrows(CharacterCodingException.class, () -> B1ResumeRecordV6.decodeData(nameUtf8));
        byte[] innerHash = good.clone(); innerHash[4 + B1ResumeRecordV6.DOMAIN.length() + 32 + 4 + 40] ^= 1;
        assertThrows(IOException.class, () -> B1ResumeRecordV6.decodeData(innerHash));
    }
    @Test void outputBoundAppliesToCompleteOuterNotOnlyInner() throws Exception {
        var g = genesis(); var r = g.transition(); var s = r.assertions(); var a = s.pendingAttempt();
        var large = new CommonGraphAssertions(a.beforeGraph().manifestEncoding(), new byte[60000], a.beforeGraph().bindingEncoding(), new byte[]{1}, a.beforeGraph().repositoryEncoding(), new byte[]{1});
        var attempt = new AttemptAssertions(a.attemptId(), a.phase(), a.logicalWriteUuid(), a.bootGeneration(), a.connectionGeneration(), a.connectionId(), a.namespace(), a.context(), a.beforeWriter(), a.targetWriter(), large, a.targetGraph());
        var state = new FullStateAssertions(s.currentOwner(), null, null, null, s.currentWriter(), s.targetWriter(), attempt, null, null, null);
        var largeRecord = record(r, 0, new byte[32], Kind.GENESIS, state);
        // Derive the remaining space from the actual dedicated inner bound, then make only the outer overflow.
        byte[] first = B1ResumeRecordCodec.encodeSchema5Data(largeRecord);
        int extra = 65536 - first.length - 100;
        var graph = new CommonGraphAssertions(large.manifestEncoding(), new byte[60000 + extra], large.bindingEncoding(), large.bindingBytes(), large.repositoryEncoding(), large.repositoryBytes());
        var bigger = new AttemptAssertions(a.attemptId(), a.phase(), a.logicalWriteUuid(), a.bootGeneration(), a.connectionGeneration(), a.connectionId(), a.namespace(), a.context(), a.beforeWriter(), a.targetWriter(), graph, a.targetGraph());
        var finalState = new FullStateAssertions(s.currentOwner(), null, null, null, s.currentWriter(), s.targetWriter(), bigger, null, null, null);
        var value = wrap(record(r, 0, new byte[32], Kind.GENESIS, finalState), new byte[32]);
        assertTrue(B1ResumeRecordCodec.encodeSchema5Data(value.transition()).length < 65536);
        assertThrows(IOException.class, () -> B1ResumeRecordV6.encodeData(value));
    }
}

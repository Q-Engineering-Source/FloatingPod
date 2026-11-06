package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class B1Full26WitnessReferenceTest {
    static byte[] hash(int n) { byte[] value = new byte[32]; Arrays.fill(value, (byte)n); return value; }
    static WindowsMarkerIo.FileIdentity identity(int n) { byte[] id = new byte[16]; id[15] = (byte)n; return new WindowsMarkerIo.FileIdentity(0x1234, id); }
    static B1Full26WitnessReference reference(B1ResumeRecord r, B1ResumeRecord.AttemptAssertions a) throws Exception {
        return new B1Full26WitnessReference(B1Full26WitnessReference.basename(a.attemptId(), hash(7)), identity(5), 90000,
            hash(7), r.sourceAnchorSha256(), WindowsWorldProvisioner.digest(r.originalReceipt()), hash(11), hash(12),
            HexFormat.of().parseHex(WindowsWorldProvisionReceiptDataCodec.decode(r.originalReceipt()).plan().ddlSha256()),
            a.attemptId(), a.phase(), a.logicalWriteUuid());
    }
    @Test void immutableSixHashesAndCompleteIdentityEquality() throws Exception {
        var r = B1ResumeRecordCodecTest.schema5DataRecord(); var a = r.assertions().pendingAttempt(); var ref = reference(r, a);
        assertTrue(ref.exactlyEquals(reference(r, a)));
        byte[][] inputs = {ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256()};
        var copy = new B1Full26WitnessReference(ref.basename(), ref.fileIdentity(), ref.fullLength(), inputs[0], inputs[1], inputs[2], inputs[3], inputs[4], inputs[5], ref.attemptId(), ref.phase(), ref.logicalOperation());
        for (var input : inputs) input[0] ^= 1;
        assertTrue(ref.exactlyEquals(copy));
        for (int field = 0; field < 6; field++) {
            byte[][] hashes = {ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256()};
            hashes[field][0] ^= 1;
            var changed = new B1Full26WitnessReference(B1Full26WitnessReference.basename(ref.attemptId(), hashes[0]), ref.fileIdentity(), ref.fullLength(),
                hashes[0], hashes[1], hashes[2], hashes[3], hashes[4], hashes[5], ref.attemptId(), ref.phase(), ref.logicalOperation());
            assertFalse(ref.exactlyEquals(changed));
        }
        assertFalse(ref.exactlyEquals(null));
        assertFalse(ref.exactlyEquals(new B1Full26WitnessReference(ref.basename(), identity(6), 90000, ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256(), ref.attemptId(), ref.phase(), ref.logicalOperation())));
        assertFalse(ref.exactlyEquals(new B1Full26WitnessReference(ref.basename(), ref.fileIdentity(), 90001, ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256(), ref.attemptId(), ref.phase(), ref.logicalOperation())));
    }
    @Test void fixedFull26BasenameRejectsPathsForeignFamilyAndNoncanonicalForms() throws Exception {
        var r = B1ResumeRecordCodecTest.schema5DataRecord(); var ref = reference(r, r.assertions().pendingAttempt());
        assertEquals(".floatingpod-b1-full26-v1-21a0707c-cd88-4004-8c06-5d790eaf1111-" + "07".repeat(32) + ".bin", ref.basename());
        for (String name : new String[]{"../" + ref.basename(), "C:/" + ref.basename(), ref.basename().replace("full26", "full20"), ref.basename().toUpperCase(), ref.basename() + ".tmp"})
            assertThrows(IllegalArgumentException.class, () -> new B1Full26WitnessReference(name, ref.fileIdentity(), ref.fullLength(), ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256(), ref.attemptId(), ref.phase(), ref.logicalOperation()));
    }
    @Test void referenceRejectsMissingHashesLengthAndZeroIds() throws Exception {
        var r = B1ResumeRecordCodecTest.schema5DataRecord(); var ref = reference(r, r.assertions().pendingAttempt());
        for (int field = 0; field < 6; field++) for (int length : new int[]{0, 31, 33}) {
            byte[][] hashes = {ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256()}; hashes[field] = new byte[length];
            assertThrows(IllegalArgumentException.class, () -> new B1Full26WitnessReference(ref.basename(), ref.fileIdentity(), ref.fullLength(), hashes[0], hashes[1], hashes[2], hashes[3], hashes[4], hashes[5], ref.attemptId(), ref.phase(), ref.logicalOperation()));
        }
        for (long length : new long[]{0, -1}) assertThrows(IllegalArgumentException.class, () -> new B1Full26WitnessReference(ref.basename(), ref.fileIdentity(), length, ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256(), ref.attemptId(), ref.phase(), ref.logicalOperation()));
        assertThrows(IllegalArgumentException.class, () -> B1Full26WitnessReference.basename(new UUID(0, 0), ref.fullSha256()));
        assertThrows(IllegalArgumentException.class, () -> new B1Full26WitnessReference(ref.basename(), ref.fileIdentity(), ref.fullLength(), ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256(), ref.attemptId(), ref.phase(), new UUID(0, 0)));
        assertThrows(NullPointerException.class, () -> new B1Full26WitnessReference(ref.basename(), null, ref.fullLength(), ref.fullSha256(), ref.sourceAnchorSha256(), ref.originalReceiptSha256(), ref.sourceLimitProfileSha256(), ref.configurationSha256(), ref.ddlSha256(), ref.attemptId(), ref.phase(), ref.logicalOperation()));
    }
}

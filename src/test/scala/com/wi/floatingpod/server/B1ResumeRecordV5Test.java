package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

/** 实际 DATA framing。合成 V4/identity 仅数据，不构造 private native ACK。 */
class B1ResumeRecordV5Test {
    private static B1ResumeRecordV5 value() throws Exception {
        var original = B1FreshPrepareRecordFixture.validRecord();
        var a = original.assertions();
        var state = new B1ResumeRecord.FullStateAssertions(a.currentOwner(),a.previousOwner(),a.retirement(),
                a.pendingAttempt(),a.currentWriter(),a.targetWriter(),a.pendingAttempt(),a.latestSettlement(),a.marker(),null);
        var record = new B1ResumeRecord(1, new byte[32], B1ResumeRecord.Kind.DB_ACQUIRED, original.sourceAnchorSha256(),
                original.originalReceipt(), original.barrierNamespace(), state);
        var attempt = record.assertions().pendingAttempt();
        byte[] hash = new byte[32]; hash[31] = 7;
        var reference = new B1Full20WitnessReference(B1Full20WitnessReference.basename(attempt.attemptId(), hash),
                new WindowsMarkerIo.FileIdentity(5, new byte[16]), 90000, hash, record.sourceAnchorSha256(),
                WindowsWorldProvisioner.digest(record.originalReceipt()), new byte[32], attempt.attemptId(),
                attempt.phase(), attempt.logicalWriteUuid());
        return new B1ResumeRecordV5(record, 0, new byte[32], new WindowsMarkerIo.FileIdentity(6,new byte[16]),
                new byte[32], new B1ResumeRecordV5.References(reference, reference, null, null));
    }
    @Test void completeLargeArtifactReferenceRoundtripsWithoutChangingV4Bytes() throws Exception {
        var original = value(); byte[] v4 = B1ResumeRecordCanonical.encode(original.transition());
        byte[] bytes = B1ResumeRecordV5.encodeData(original);
        var decoded = B1ResumeRecordV5.decodeData(bytes);
        assertArrayEquals(bytes, B1ResumeRecordV5.encodeData(decoded));
        assertArrayEquals(v4, B1ResumeRecordCanonical.encode(decoded.transition()));
        assertEquals(90000, decoded.references().pending().fullLength());
        assertTrue(original.references().pending().exactlyEquals(decoded.references().pending()));
        byte[] defensive = decoded.bridgeSha256(); defensive[0] = 9;
        assertEquals(0, decoded.bridgeSha256()[0]);
    }
    @Test void missingRequiredReferenceAndForeignSourceCannotBecomeV5Data() throws Exception {
        var original = value();
        assertThrows(IllegalArgumentException.class, () -> new B1ResumeRecordV5(original.transition(), 0,
                new byte[32], original.bridgeIdentity(), new byte[32], new B1ResumeRecordV5.References(null,null,null,null)));
        var r = original.references().pending(); byte[] wrong = r.sourceAnchorSha256(); wrong[0] = 1;
        var foreign = new B1Full20WitnessReference(r.basename(), r.fileIdentity(), r.fullLength(), r.fullSha256(),
                wrong, r.originalReceiptSha256(), r.profileSha256(), r.attemptId(), r.phase(), r.logicalOperation());
        assertThrows(IllegalArgumentException.class, () -> new B1ResumeRecordV5(original.transition(),0,
                new byte[32],original.bridgeIdentity(),new byte[32],new B1ResumeRecordV5.References(original.references().originalPrepare(),foreign,null,null)));
    }
    @Test void truncatedTrailingAndUnknownDomainAreExplicitFailures() throws Exception {
        byte[] bytes = B1ResumeRecordV5.encodeData(value());
        assertThrows(IOException.class, () -> B1ResumeRecordV5.decodeData(Arrays.copyOf(bytes,bytes.length-1)));
        assertThrows(IOException.class, () -> B1ResumeRecordV5.decodeData(Arrays.copyOf(bytes,bytes.length+1)));
        bytes[4] ^= 1;
        assertThrows(IOException.class, () -> B1ResumeRecordV5.decodeData(bytes));
    }
    @Test void consecutiveV5UsesBothCompleteTransitionAndContainerPredecessors() throws Exception {
        var first = value();byte[] firstBytes=B1ResumeRecordV5.encodeData(first);
        var old=first.transition();
        var nextTransition=new B1ResumeRecord(2,WindowsWorldProvisioner.digest(B1ResumeRecordCanonical.encode(old)),
                old.kind(),old.sourceAnchorSha256(),old.originalReceipt(),old.barrierNamespace(),old.assertions());
        var next=new B1ResumeRecordV5(nextTransition,first.bridgeSequence(),first.bridgeSha256(),first.bridgeIdentity(),
                WindowsWorldProvisioner.digest(firstBytes),first.references());
        B1ResumeRecordV5.consecutive(first,firstBytes,next);
        var foreign=new B1ResumeRecordV5(nextTransition,first.bridgeSequence(),first.bridgeSha256(),
                new WindowsMarkerIo.FileIdentity(999,new byte[16]),WindowsWorldProvisioner.digest(firstBytes),first.references());
        assertThrows(IOException.class, () -> B1ResumeRecordV5.consecutive(first,firstBytes,foreign));
        var wrongPrevious=new B1ResumeRecordV5(nextTransition,first.bridgeSequence(),first.bridgeSha256(),first.bridgeIdentity(),
                new byte[32],first.references());
        assertThrows(IOException.class, () -> B1ResumeRecordV5.consecutive(first,firstBytes,wrongPrevious));
    }

    private static B1ResumeRecordV5 withReceiptSchema(int format)throws Exception {
        var old=value();var t=old.transition();
        var receipt=WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodecTest.receipt(format));
        var changed=new B1ResumeRecord(t.sequence(),t.previousRecordSha256(),t.kind(),t.sourceAnchorSha256(),receipt,t.barrierNamespace(),t.assertions());
        var r=old.references().pending();
        var ref=new B1Full20WitnessReference(r.basename(),r.fileIdentity(),r.fullLength(),r.fullSha256(),r.sourceAnchorSha256(),
            WindowsWorldProvisioner.digest(receipt),r.profileSha256(),r.attemptId(),r.phase(),r.logicalOperation());
        return new B1ResumeRecordV5(changed,old.bridgeSequence(),old.bridgeSha256(),old.bridgeIdentity(),old.previousContainerSha256(),
            new B1ResumeRecordV5.References(ref,ref,null,null));
    }
    @Test void independentLegacy2And3V5ByteGoldensAndSchema5RefusalRemainFixed()throws Exception {
        for(int format:new int[]{2,3}) {
            byte[] bytes=B1ResumeRecordV5.encodeData(withReceiptSchema(format));
            assertEquals(3354,bytes.length);
            assertEquals(format==2?"FFD69CE1A3084A17611DA6D29B0848C8CB807EAC873AA051314D48D9CE367CDF":"32309396CE7C2209FB06BBD12186E2BED29DFD748FCC95938CAE490355A44681",WindowsWorldProvisioner.sha256(bytes));
            assertArrayEquals(bytes,B1ResumeRecordV5.encodeData(B1ResumeRecordV5.decodeData(bytes)));
        }
        assertThrows(IOException.class,()->B1ResumeRecordV5.encodeData(withReceiptSchema(5)));
    }

    @Test void existingV5DecoderCannotAdmitDedicatedSchema5TransitionBytes()throws Exception {
        var genesis=B1ResumeRecordCodecTest.schema5DataRecord();var a=genesis.assertions();
        var state=new B1ResumeRecord.FullStateAssertions(a.currentOwner(),null,null,a.pendingAttempt(),
            a.currentWriter(),a.targetWriter(),a.pendingAttempt(),null,null,null);
        var transition=new B1ResumeRecord(1,new byte[32],B1ResumeRecord.Kind.DB_ACQUIRED,genesis.sourceAnchorSha256(),
            genesis.originalReceipt(),genesis.barrierNamespace(),state);
        byte[] schema5=B1ResumeRecordCanonical.encodeSchema5Data(transition);
        byte[] legacy=B1ResumeRecordV5.encodeData(value());
        var offsets=ByteBuffer.wrap(legacy);
        int domainLength=offsets.getInt();int position=4+domainLength+8+32+24+32;
        int oldLength=offsets.getInt(position);
        var sink=new ByteArrayOutputStream();
        try(var out=new DataOutputStream(sink)) {
            out.write(legacy,0,position);out.writeInt(schema5.length);out.write(schema5);
            out.write(legacy,position+4+oldLength,legacy.length-position-4-oldLength);
        }
        // Decoder must reject at its legacy receipt-family gate, before historical full20 references are consumed.
        var failure=assertThrows(IOException.class,()->B1ResumeRecordV5.decodeData(sink.toByteArray()));
        assertTrue(failure.getMessage().contains("receipt schema family"),failure.getMessage());
    }
}

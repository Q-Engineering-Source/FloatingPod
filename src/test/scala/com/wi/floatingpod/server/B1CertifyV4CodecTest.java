package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** Direct V4 codec/consecutive tests; facts are not SQL acknowledgement or recovery settlement proof. */
@Tag("b1-certify-pure")
class B1CertifyV4CodecTest {
    private B1ResumeRecord intent()throws Exception {
        var g=B1TwoSlotFixture.record(0,new byte[32],Kind.GENESIS,B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner()));
        var p=B1NormalCertifyFixture.successor(g,Kind.PREPARED_CONFIRMED,B1NormalCertifyFixture.prepared(g.assertions()));
        return B1NormalCertifyFixture.successor(p,Kind.NATIVE_ACK_CERTIFY_INTENT,B1NormalCertifyFixture.intent(p.assertions(),B1NormalCertifyFixture.marker()));
    }
    @Test void v4FullDoubleWitnessRoundtripRetainsCertifyScopeGraphsAndPrepare()throws Exception {
        var i=intent();var c=B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,B1NormalCertifyFixture.certified(i.assertions()));
        byte[] bytes=B1ResumeRecordCodec.encode(c);var d=B1ResumeRecordCodec.decode(bytes);var a=d.assertions().acknowledgedCertifyWitness();
        assertEquals(0x4650423152535634L,ByteBuffer.wrap(bytes).getLong());assertEquals(4,ByteBuffer.wrap(bytes).getInt(8));
        assertArrayEquals(bytes,B1ResumeRecordCodec.encode(d));assertNull(d.assertions().pendingAttempt());assertNull(d.assertions().latestSettlement());
        assertEquals(i.assertions().pendingAttempt().connectionGeneration(),a.connectionGeneration());assertEquals(i.assertions().pendingAttempt().connectionId(),a.connectionId());
        assertArrayEquals(i.assertions().pendingAttempt().beforeGraph().repositoryBytes(),a.beforeGraph().repositoryBytes());
        assertArrayEquals(i.assertions().pendingAttempt().targetGraph().manifestBytes(),a.targetGraph().manifestBytes());
        assertEquals(B1SourceAnchorFixture.plan().certifyWriteUuid(),a.context().operation());
        assertEquals(B1SourceAnchorFixture.plan().prepareWriteUuid(),d.assertions().originalPrepareWitness().context().operation());
        B1ResumeRecordCodec.consecutive(i,B1ResumeRecordCodec.encode(i),c);
    }
    @Test void v3EnvelopeAndMagicUnderV4NameAreRejected()throws Exception {
        byte[] bytes=B1ResumeRecordCodec.encode(intent());byte[] magic=bytes.clone();ByteBuffer.wrap(magic).putLong(0x4650423152535633L);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(magic));byte[] version=bytes.clone();ByteBuffer.wrap(version).putInt(8,3);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(version));
    }
    @Test void certifiedRequiresAcknowledgedCertifyWitnessAndNoPending()throws Exception {
        var i=intent();var s=B1NormalCertifyFixture.certified(i.assertions());
        assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,B1NormalCertifyFixture.withAcknowledged(s,null))));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,B1NormalCertifyFixture.withAcknowledged(s,s.originalPrepareWitness()))));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,i.assertions())));
    }
    @Test void fullCertifyWitnessCannotChangeAcrossIntentConfirmation()throws Exception {
        var i=intent();var s=B1NormalCertifyFixture.certified(i.assertions());var a=s.acknowledgedCertifyWitness();
        var changed=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId()+1,
                a.namespace(),a.context(),a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        var c=B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,B1NormalCertifyFixture.withAcknowledged(s,changed));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.consecutive(i,B1ResumeRecordCodec.encode(i),c));
    }
    @Test void acknowledgedCertifyCannotDisappearOrBeReplacedLater()throws Exception {
        var i=intent();var c=B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,B1NormalCertifyFixture.certified(i.assertions()));var a=c.assertions().acknowledgedCertifyWitness();
        var dropped=B1NormalCertifyFixture.successor(c,Kind.DB_ACQUIRED,B1NormalCertifyFixture.withAcknowledged(c.assertions(),null));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.consecutive(c,B1ResumeRecordCodec.encode(c),dropped));
        var changed=new AttemptAssertions(java.util.UUID.randomUUID(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),
            a.namespace(),a.context(),a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        var replaced=B1NormalCertifyFixture.successor(c,Kind.DB_ACQUIRED,B1NormalCertifyFixture.withAcknowledged(c.assertions(),changed));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.consecutive(c,B1ResumeRecordCodec.encode(c),replaced));
    }
    @Test void certifyFixedUuidNamespaceBindingAndTargetReceiptRemainExact()throws Exception {
        var i=intent();var s=B1NormalCertifyFixture.certified(i.assertions());var a=s.acknowledgedCertifyWitness();var c=a.context();
        var other=java.util.UUID.randomUUID();
        var wrongOperation=new OperationAssertions(c.boot(),c.writerEpoch(),c.binding(),other,c.effects(),c.originalOperationFingerprint());
        var operation=new AttemptAssertions(a.attemptId(),a.phase(),other,a.bootGeneration(),a.connectionGeneration(),a.connectionId(),a.namespace(),wrongOperation,
            a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        var namespace=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),"fpb1:"+"a".repeat(43),c,
            a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        var wrongBinding=new OperationAssertions(c.boot(),c.writerEpoch(),other,c.operation(),c.effects(),c.originalOperationFingerprint());
        var binding=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),a.namespace(),wrongBinding,
            a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        byte[] fingerprint=c.originalOperationFingerprint();fingerprint[2]^=1;
        var wrongContext=new OperationAssertions(c.boot(),c.writerEpoch(),c.binding(),c.operation(),c.effects(),fingerprint);
        var receipt=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),a.namespace(),wrongContext,
            a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        for(var bad:new AttemptAssertions[]{operation,namespace,binding,receipt})
            assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,B1NormalCertifyFixture.withAcknowledged(s,bad))));
    }
    @Test void originalPrepareWitnessCannotChangeDuringNormalCertification()throws Exception {
        var i=intent();var s=B1NormalCertifyFixture.certified(i.assertions());var a=s.originalPrepareWitness();
        var changed=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId()+1,a.namespace(),
            a.context(),a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
        var state=new FullStateAssertions(s.currentOwner(),s.previousOwner(),s.retirement(),changed,s.currentWriter(),s.targetWriter(),s.pendingAttempt(),s.latestSettlement(),s.marker(),s.acknowledgedCertifyWitness());
        var c=B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,state);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.consecutive(i,B1ResumeRecordCodec.encode(i),c));
    }
    @Test void fullDoubleWitnessCapacityFailsWithoutDroppingPrepare()throws Exception {
        var i=intent();var s=B1NormalCertifyFixture.certified(i.assertions());var a=s.acknowledgedCertifyWitness();
        var huge=new CommonGraphAssertions("TEST_FULL_M",new byte[16000],"TEST_FULL_B",new byte[16000],"TEST_FULL_R",new byte[16000]);
        var enlarged=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),a.namespace(),
            a.context(),a.beforeWriter(),a.targetWriter(),huge,huge);
        var c=B1NormalCertifyFixture.successor(i,Kind.CERTIFIED,B1NormalCertifyFixture.withAcknowledged(s,enlarged));
        var failure=assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(c));assertEquals("Full B1 state exceeds 65536 byte bound",failure.getMessage());
        assertSame(s.originalPrepareWitness(),c.assertions().originalPrepareWitness());
    }
}

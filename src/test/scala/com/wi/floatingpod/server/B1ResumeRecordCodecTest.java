package com.wi.floatingpod.server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static com.wi.floatingpod.server.B1ResumeRecord.*;

@Tag("b1-two-slot-pure")
class B1ResumeRecordCodecTest {
    @Test void fullGenesisRoundTripPreservesCompleteBeforeTargetContextSessionAndPrecision()throws Exception {
        var state=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());var r=B1TwoSlotFixture.record(0,new byte[32],Kind.GENESIS,state);
        byte[] bytes=B1ResumeRecordCodec.encode(r);var read=B1ResumeRecordCodec.decode(bytes);assertArrayEquals(bytes,B1ResumeRecordCodec.encode(read));
        assertEquals(state.currentOwner(),read.assertions().currentOwner());assertEquals(state.currentWriter(),read.assertions().currentWriter());
        assertEquals(123456000,read.assertions().currentWriter().leaseUntil().getNano());
        assertEquals(state.pendingAttempt().attemptId(),read.assertions().pendingAttempt().attemptId());
        assertEquals(state.pendingAttempt().connectionId(),read.assertions().pendingAttempt().connectionId());
        assertEquals(state.pendingAttempt().connectionGeneration(),read.assertions().pendingAttempt().connectionGeneration());
        assertArrayEquals(state.pendingAttempt().beforeGraph().repositoryBytes(),read.assertions().pendingAttempt().beforeGraph().repositoryBytes());
        assertArrayEquals(state.pendingAttempt().targetGraph().manifestBytes(),read.assertions().pendingAttempt().targetGraph().manifestBytes());
        assertEquals(state.pendingAttempt().context().operation(),read.assertions().pendingAttempt().context().operation());
        assertArrayEquals(r.originalReceipt(),read.originalReceipt());assertEquals(r.barrierNamespace(),read.barrierNamespace());
    }
    @Test void consecutiveRecordsRequireFullSourceAndImmediateHashPredecessor()throws Exception {
        var genesis=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());var older=B1TwoSlotFixture.record(0,new byte[32],Kind.GENESIS,genesis);
        byte[] original=B1ResumeRecordCodec.encode(older);var newer=B1TwoSlotFixture.record(1,WindowsWorldProvisioner.digest(original),Kind.PREPARED_CONFIRMED,B1TwoSlotFixture.prepared(genesis));
        B1ResumeRecordCodec.consecutive(older,original,newer);assertArrayEquals(B1ResumeRecordCodec.encode(newer),B1ResumeRecordCodec.encode(B1ResumeRecordCodec.decode(B1ResumeRecordCodec.encode(newer))));
        var skipped=B1TwoSlotFixture.record(3,WindowsWorldProvisioner.digest(original),Kind.PREPARED_CONFIRMED,B1TwoSlotFixture.prepared(genesis));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.consecutive(older,original,skipped));
        var wrongHash=B1TwoSlotFixture.record(1,new byte[32],Kind.PREPARED_CONFIRMED,B1TwoSlotFixture.prepared(genesis));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.consecutive(older,original,wrongHash));
    }
    @Test void corruptTruncatedTrailingAndUnknownVersionFail()throws Exception {
        byte[] good=B1ResumeRecordCodec.encode(B1TwoSlotFixture.record(0,new byte[32],Kind.GENESIS,B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner())));
        byte[] corrupt=good.clone();corrupt[40]^=1;assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(corrupt));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(Arrays.copyOf(good,good.length-1)));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(Arrays.copyOf(good,good.length+1)));
        byte[] version=good.clone();version[11]=3;assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(version));
        assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(new byte[65537]));
    }
    @Test void malformedUtf8AndNullableTagsFailAtRealDecoder()throws Exception {
        byte[] good=B1ResumeRecordCodec.encode(B1TwoSlotFixture.record(0,new byte[32],Kind.GENESIS,B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner())));
        byte[] bad=good.clone();bad[20]=(byte)0xc0;reseal(bad);assertThrows(java.nio.charset.CharacterCodingException.class,()->B1ResumeRecordCodec.decode(bad));
        ByteBuffer offsets=ByteBuffer.wrap(good);offsets.position(16);skipBlob(offsets);offsets.position(offsets.position()+8+32);
        skipBlob(offsets);offsets.position(offsets.position()+32);skipBlob(offsets);offsets.position(offsets.position()+32);skipBlob(offsets);
        offsets.position(offsets.position()+40);byte[] badTag=good.clone();badTag[offsets.position()]=2;reseal(badTag);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.decode(badTag));
    }
    @Test void completeStateOverBoundFailsBeforePublication()throws Exception {
        var state=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());var a=state.pendingAttempt();
        var huge=new CommonGraphAssertions("TEST_MANIFEST",new byte[32000],"TEST_BINDING",new byte[32000],"TEST_REPOSITORY",new byte[32000]);
        var enlarged=new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),a.namespace(),a.context(),a.beforeWriter(),a.targetWriter(),huge,huge);
        var full=new FullStateAssertions(state.currentOwner(),null,null,null,state.currentWriter(),state.targetWriter(),enlarged,null,null, null);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(B1TwoSlotFixture.record(0,new byte[32],Kind.GENESIS,full)));
    }
    @Test void originalPrepareScopeCannotBeDroppedOrReplacedAcrossSuccessors()throws Exception {
        var initial=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());var state=B1TwoSlotFixture.prepared(initial);
        var old=B1TwoSlotFixture.record(1,new byte[32],Kind.PREPARED_CONFIRMED,state);byte[] oldBytes=B1ResumeRecordCodec.encode(old);
        var dropped=new FullStateAssertions(state.currentOwner(),null,null,null,state.currentWriter(),state.targetWriter(),null,null,null, null);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.encode(B1TwoSlotFixture.record(2,WindowsWorldProvisioner.digest(oldBytes),Kind.DB_ACQUIRED,dropped)));
    }
    @Test void cancellationPreservesFullOriginalPendingAttemptRatherThanOnlyId()throws Exception {
        var initial=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());var old=B1TwoSlotFixture.record(0,new byte[32],Kind.GENESIS,initial);byte[] oldBytes=B1ResumeRecordCodec.encode(old);
        var settlement=new SettlementAssertions(SettlementKind.CANCELLED,initial.pendingAttempt(),"TEST_ONLY_CANCELLED",new byte[]{7,8},new byte[32]);
        var state=new FullStateAssertions(initial.currentOwner(),null,null,null,initial.currentWriter(),initial.targetWriter(),null,settlement,null, null);
        var cancelled=B1TwoSlotFixture.record(1,WindowsWorldProvisioner.digest(oldBytes),Kind.ATTEMPT_CANCELLED,state);
        B1ResumeRecordCodec.consecutive(old,oldBytes,cancelled);var decoded=B1ResumeRecordCodec.decode(B1ResumeRecordCodec.encode(cancelled));
        assertArrayEquals(initial.pendingAttempt().beforeGraph().manifestBytes(),decoded.assertions().latestSettlement().settledAttempt().beforeGraph().manifestBytes());
    }
    @Test void actualDatabasePrecisionAndCanonicalEffectsAreMandatory()throws Exception {
        var state=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());var w=state.currentWriter();
        assertThrows(IllegalArgumentException.class,()->new WriterAssertions(w.binding(),w.epoch(),w.ownerBoot(),w.leaseUntil().withNano(1),w.lastSlot()));
        byte[] hash=new byte[32];var e=new EffectAssertions("target",0,hash);hash[0]=1;assertEquals(0,e.desiredFingerprint()[0]);
        assertThrows(IllegalArgumentException.class,()->new B1ResumeRecord(0,new byte[32],Kind.CERTIFIED,new byte[32],new byte[]{1},B1TwoSlotFixture.namespace(),state));
    }
    private static void skipBlob(ByteBuffer bytes){int length=bytes.getInt();bytes.position(bytes.position()+length);}
    private static void reseal(byte[] bytes){int size=ByteBuffer.wrap(bytes).getInt(12);System.arraycopy(WindowsWorldProvisioner.digest(Arrays.copyOfRange(bytes,16,16+size)),0,bytes,bytes.length-32,32);}

    @Test void independentLegacy2And3V4ByteGoldensRemainFixed()throws Exception {
        for(int format:new int[]{2,3}) {
            var state=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());
            var record=new B1ResumeRecord(0,new byte[32],Kind.GENESIS,new byte[32],
                WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodecTest.receipt(format)),B1TwoSlotFixture.namespace(),state);
            byte[] bytes=B1ResumeRecordCodec.encode(record);
            assertEquals(1685,bytes.length);
            assertEquals(format==2?"9FC18FE9503E94B5FF212DC039262E93A2B9CC37058E82E40EF271255167DDDB":"69CC4B9BC30D585E2F1B18DB040AC0322E488C59DC9D63D49D7100BFA12C6D88",WindowsWorldProvisioner.sha256(bytes));
            assertArrayEquals(bytes,B1ResumeRecordCanonical.encode(B1ResumeRecordCanonical.decodeData(bytes)));
            assertThrows(IOException.class,()->B1ResumeRecordCanonical.decodeSchema5Data(bytes));
        }
    }

    private static final String FULL26="floatingpod.mysql.script-publication1-deployment-receipt1-repository-commitment/v1";
    private static CommonGraphAssertions schema5Graph(String side,String repositoryEncoding) {
        // New structural fixture bytes, never a relabeled Full20 payload or semantic Full26 validation.
        return new CommonGraphAssertions("floatingpod.mysql.b1-manifest/v1",("STRUCTURAL_SCHEMA5_MANIFEST_"+side).getBytes(StandardCharsets.US_ASCII),
            "floatingpod.mysql.b1-binding/v1",("STRUCTURAL_SCHEMA5_BINDING_"+side).getBytes(StandardCharsets.US_ASCII),
            repositoryEncoding,("STRUCTURAL_SCHEMA5_REPOSITORY_"+side).getBytes(StandardCharsets.US_ASCII));
    }
    private static AttemptAssertions withGraphs(AttemptAssertions a,CommonGraphAssertions before,CommonGraphAssertions target) {
        return new AttemptAssertions(a.attemptId(),a.phase(),a.logicalWriteUuid(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),
            a.namespace(),a.context(),a.beforeWriter(),a.targetWriter(),before,target);
    }
    static B1ResumeRecord schema5DataRecord()throws Exception {
        var old=B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());
        var a=withGraphs(old.pendingAttempt(),schema5Graph("before",FULL26),schema5Graph("target",FULL26));
        var state=new FullStateAssertions(old.currentOwner(),null,null,null,old.currentWriter(),old.targetWriter(),a,null,null,null);
        return new B1ResumeRecord(0,new byte[32],Kind.GENESIS,new byte[32],
            WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodecTest.receipt(5)),B1TwoSlotFixture.namespace(),state);
    }
    private static B1ResumeRecord withState(B1ResumeRecord r,FullStateAssertions state) {
        return new B1ResumeRecord(r.sequence(),r.previousRecordSha256(),r.kind(),r.sourceAnchorSha256(),r.originalReceipt(),r.barrierNamespace(),state);
    }
    @Test void realSchema5ReceiptDedicatedDataRoundtripAndLegacyCrossEntryRefusal()throws Exception {
        var r=schema5DataRecord();byte[] bytes=B1ResumeRecordCanonical.encodeSchema5Data(r);
        var read=B1ResumeRecordCanonical.decodeSchema5Data(bytes);
        assertEquals(5,WindowsWorldProvisionReceiptDataCodec.decode(read.originalReceipt()).plan().schemaFormat());
        assertArrayEquals(bytes,B1ResumeRecordCanonical.encodeSchema5Data(read));
        assertArrayEquals(r.originalReceipt(),read.originalReceipt());
        assertThrows(IOException.class,()->B1ResumeRecordCanonical.encode(r));
        assertThrows(IOException.class,()->B1ResumeRecordCanonical.decodeData(bytes));
        for(int format:new int[]{2,3}) {
            var wrong=new B1ResumeRecord(r.sequence(),r.previousRecordSha256(),r.kind(),r.sourceAnchorSha256(),
                WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodecTest.receipt(format)),r.barrierNamespace(),r.assertions());
            assertThrows(IOException.class,()->B1ResumeRecordCanonical.encodeSchema5Data(wrong));
        }
    }
    @Test void schema5ChecksBothGraphsOfEveryRetainedAttemptRatherThanOnlyPending()throws Exception {
        var genesis=schema5DataRecord();var s=genesis.assertions();var a=s.pendingAttempt();var p=WindowsWorldProvisionReceiptDataCodecTest.plan(5);
        var context=new OperationAssertions(a.context().boot(),a.context().writerEpoch(),p.bindingUuid(),p.certifyWriteUuid(),a.context().effects(),a.context().originalOperationFingerprint());
        var target=new WriterAssertions(p.bindingUuid(),context.writerEpoch(),context.boot(),a.targetWriter().leaseUntil(),
            new ReceiptSlotAssertions(context.boot(),context.writerEpoch(),p.bindingUuid(),context.operation(),context.originalOperationFingerprint()));
        var certify=new AttemptAssertions(UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf3333"),Phase.CERTIFY,p.certifyWriteUuid(),a.bootGeneration(),
            a.connectionGeneration(),a.connectionId(),a.namespace(),context,a.beforeWriter(),target,a.beforeGraph(),a.targetGraph());
        var healthy=new FullStateAssertions(s.currentOwner(),null,null,a,s.currentWriter(),s.targetWriter(),a,
            new SettlementAssertions(SettlementKind.CONFIRMED,a,"TEST_ONLY_STRUCTURAL_SETTLEMENT",new byte[]{1},new byte[32]),null,certify);
        var r=new B1ResumeRecord(1,new byte[32],Kind.DB_ACQUIRED,genesis.sourceAnchorSha256(),genesis.originalReceipt(),genesis.barrierNamespace(),healthy);
        B1ResumeRecordCodec.validateSchema5Data(r);
        for(int slot=0;slot<4;slot++)for(boolean before:new boolean[]{true,false})for(String family:new String[]{
                "floatingpod.mysql.format3-full20-repository-commitment/v1","floatingpod.mysql.full-repository-snapshot/v1","floatingpod.mysql.script-publication1-full24/v1"}) {
            var old=slot==3?certify:a;var foreign=schema5Graph("foreign",family);
            var wrong=withGraphs(old,before?foreign:old.beforeGraph(),before?old.targetGraph():foreign);
            var changed=new FullStateAssertions(s.currentOwner(),null,null,slot==0?wrong:a,s.currentWriter(),s.targetWriter(),slot==1?wrong:a,
                new SettlementAssertions(SettlementKind.CONFIRMED,slot==2?wrong:a,"TEST_ONLY_STRUCTURAL_SETTLEMENT",new byte[]{1},new byte[32]),null,slot==3?wrong:certify);
            var failure=assertThrows(IOException.class,()->B1ResumeRecordCodec.validateSchema5Data(withState(r,changed)));
            assertTrue(failure.getMessage().contains("Full26"),failure.getMessage());
        }
        var foreign=new CommonGraphAssertions("OTHER_MANIFEST",a.beforeGraph().manifestBytes(),a.beforeGraph().bindingEncoding(),a.beforeGraph().bindingBytes(),FULL26,a.beforeGraph().repositoryBytes());
        var wrong=withGraphs(a,foreign,a.targetGraph());
        assertThrows(IOException.class,()->B1ResumeRecordCodec.validateSchema5Data(withState(genesis,new FullStateAssertions(s.currentOwner(),null,null,null,s.currentWriter(),s.targetWriter(),wrong,null,null,null))));
    }
    @Test void schema5DedicatedDataKeepsNamespaceBindingAndFixedPhaseWriteChecks()throws Exception {
        var r=schema5DataRecord();var s=r.assertions();var a=s.pendingAttempt();var c=a.context();
        String other=com.wi.floatingpod.storage.mysql.MySqlSessionBarrierNamespace.forIdentity(
            WindowsWorldProvisionReceiptDataCodecTest.plan(5).mysqlServerUuid(),"other_schema",
            WindowsWorldProvisionReceiptDataCodecTest.plan(5).provisioningUuid(),c.binding()).lockName();
        var namespace=new B1ResumeRecord(r.sequence(),r.previousRecordSha256(),r.kind(),r.sourceAnchorSha256(),r.originalReceipt(),other,s);
        assertThrows(IOException.class,()->B1ResumeRecordCodec.validateSchema5Data(namespace));
        for(boolean binding:new boolean[]{true,false}) {
            var id=UUID.fromString("00000000-0000-4000-8000-000000000099");
            var changed=new OperationAssertions(c.boot(),c.writerEpoch(),binding?id:c.binding(),binding?c.operation():id,c.effects(),c.originalOperationFingerprint());
            var wrong=new AttemptAssertions(a.attemptId(),a.phase(),changed.operation(),a.bootGeneration(),a.connectionGeneration(),a.connectionId(),a.namespace(),changed,
                a.beforeWriter(),a.targetWriter(),a.beforeGraph(),a.targetGraph());
            var state=new FullStateAssertions(s.currentOwner(),null,null,null,s.currentWriter(),s.targetWriter(),wrong,null,null,null);
            assertThrows(IOException.class,()->B1ResumeRecordCodec.validateSchema5Data(withState(r,state)));
        }
    }
    @Test void schema5DedicatedDecoderRetainsHashTrailingCanonicalTagUtf8AndSizeFailures()throws Exception {
        byte[] good=B1ResumeRecordCanonical.encodeSchema5Data(schema5DataRecord());
        byte[] corrupt=good.clone();corrupt[40]^=1;
        assertThrows(IOException.class,()->B1ResumeRecordCanonical.decodeSchema5Data(corrupt));
        assertThrows(IOException.class,()->B1ResumeRecordCanonical.decodeSchema5Data(Arrays.copyOf(good,good.length-1)));
        assertThrows(IOException.class,()->B1ResumeRecordCanonical.decodeSchema5Data(Arrays.copyOf(good,good.length+1)));
        assertThrows(IOException.class,()->B1ResumeRecordCanonical.decodeSchema5Data(new byte[65537]));
        byte[] utf8=good.clone();utf8[20]=(byte)0xc0;reseal(utf8);
        assertThrows(java.nio.charset.CharacterCodingException.class,()->B1ResumeRecordCanonical.decodeSchema5Data(utf8));
        ByteBuffer offsets=ByteBuffer.wrap(good);offsets.position(16);skipBlob(offsets);offsets.position(offsets.position()+8+32);
        skipBlob(offsets);offsets.position(offsets.position()+32);skipBlob(offsets);offsets.position(offsets.position()+32);skipBlob(offsets);
        offsets.position(offsets.position()+40);byte[] tag=good.clone();tag[offsets.position()]=2;reseal(tag);
        assertThrows(IOException.class,()->B1ResumeRecordCanonical.decodeSchema5Data(tag));
    }
}

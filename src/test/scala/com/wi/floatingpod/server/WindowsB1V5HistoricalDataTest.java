package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** Production DATA checks only; synthetic native IDs never construct an original lookup or ACK. */
final class WindowsB1V5HistoricalDataTest {
    private record Fixture(B1ResumeRecord v4,byte[] bytes,WindowsMarkerIo.FileIdentity bridge,B1ResumeRecordV5 first){}
    private Fixture fixture()throws Exception {
        var base=B1FreshPrepareRecordFixture.validRecord();var old=base.assertions();var prepare=old.pendingAttempt();
        var nextOwner=new NativeOwnerAssertions(4567,133000000000000002L,new UUID(0,500),2);
        UUID op=new UUID(0,501);
        var context=new OperationAssertions(nextOwner.boot(),prepare.context().writerEpoch(),prepare.context().binding(),op,
            prepare.context().effects(),prepare.context().originalOperationFingerprint());
        var handoff=new AttemptAssertions(new UUID(0,502),Phase.HANDOFF,op,2,prepare.connectionGeneration(),
            prepare.connectionId(),prepare.namespace(),context,prepare.beforeWriter(),prepare.targetWriter(),prepare.beforeGraph(),prepare.targetGraph());
        var state=new FullStateAssertions(nextOwner,old.currentOwner(),null,prepare,old.currentWriter(),old.targetWriter(),handoff,null,null,null);
        var v4=new B1ResumeRecord(2,new byte[32],Kind.HANDOFF_PREPARED,base.sourceAnchorSha256(),base.originalReceipt(),base.barrierNamespace(),state);
        byte[] bytes=B1ResumeRecordCanonical.encode(v4);var bridge=new WindowsMarkerIo.FileIdentity(17,new byte[16]);
        var first=new B1ResumeRecord(3,WindowsWorldProvisioner.digest(bytes),Kind.DB_ACQUIRED,v4.sourceAnchorSha256(),v4.originalReceipt(),v4.barrierNamespace(),state);
        var refs=new B1ResumeRecordV5.References(reference(prepare,first),reference(handoff,first),null,null);
        return new Fixture(v4,bytes,bridge,new B1ResumeRecordV5(first,2,WindowsWorldProvisioner.digest(bytes),bridge,
            WindowsWorldProvisioner.digest(bytes),refs));
    }
    private B1Full20WitnessReference reference(AttemptAssertions attempt,B1ResumeRecord record){
        byte[] hash=new byte[32];hash[31]=7;
        return new B1Full20WitnessReference(B1Full20WitnessReference.basename(attempt.attemptId(),hash),
            new WindowsMarkerIo.FileIdentity(19,new byte[16]),90000,hash,record.sourceAnchorSha256(),
            WindowsWorldProvisioner.digest(record.originalReceipt()),new byte[32],attempt.attemptId(),attempt.phase(),attempt.logicalWriteUuid());
    }
    private B1ResumeRecordV5 next(B1ResumeRecordV5 previous)throws Exception {
        var old=previous.transition();
        var transition=new B1ResumeRecord(Math.incrementExact(old.sequence()),WindowsWorldProvisioner.digest(B1ResumeRecordCanonical.encode(old)),
            old.kind(),old.sourceAnchorSha256(),old.originalReceipt(),old.barrierNamespace(),old.assertions());
        return new B1ResumeRecordV5(transition,previous.bridgeSequence(),previous.bridgeSha256(),previous.bridgeIdentity(),
            WindowsWorldProvisioner.digest(B1ResumeRecordV5.encodeData(previous)),previous.references());
    }
    @Test void completeFirstAndMultipleContainerLinksUseActualProductionDataChecks()throws Exception {
        var f=fixture();WindowsB1V5HistoricalLookup.requireFirstData(f.first,f.v4,f.bytes,f.bridge.versionedIdentity());
        var current=f.first;
        for(int i=0;i<8;i++) {
            var successor=next(current);
            WindowsB1V5HistoricalLookup.requirePreviousData(current,B1ResumeRecordV5.encodeData(current),successor);
            WindowsB1V5HistoricalLookup.requireSourceData(successor,f.v4,f.bytes,f.bridge.versionedIdentity());current=successor;
        }
        var last=current;
        assertThrows(IOException.class,()->WindowsB1V5HistoricalLookup.requireFirstData(last,f.v4,f.bytes,f.bridge.versionedIdentity()));
    }
    @Test void wrongBridgeAndBothPredecessorLayersFail()throws Exception {
        var f=fixture();var successor=next(f.first);byte[] bytes=B1ResumeRecordV5.encodeData(f.first);
        var bad=new B1ResumeRecordV5(successor.transition(),successor.bridgeSequence(),successor.bridgeSha256(),successor.bridgeIdentity(),
            new byte[32],successor.references());
        assertThrows(IOException.class,()->WindowsB1V5HistoricalLookup.requirePreviousData(f.first,bytes,bad));
        byte[] changed=bytes.clone();changed[changed.length-1]^=1;
        assertThrows(IOException.class,()->WindowsB1V5HistoricalLookup.requirePreviousData(f.first,changed,successor));
        assertThrows(IOException.class,()->WindowsB1V5HistoricalLookup.requireFirstData(f.first,f.v4,f.bytes,
            new WindowsMarkerIo.FileIdentity(18,new byte[16]).versionedIdentity()));
        var wrongKind=new B1ResumeRecord(f.v4.sequence(),f.v4.previousRecordSha256(),Kind.DB_ACQUIRED,
            f.v4.sourceAnchorSha256(),f.v4.originalReceipt(),f.v4.barrierNamespace(),f.v4.assertions());
        assertThrows(IOException.class,()->WindowsB1V5HistoricalLookup.requireFirstData(f.first,wrongKind,f.bytes,f.bridge.versionedIdentity()));
    }
    private byte[] idData(B1ResumeRecordV5 value,byte[] bytes,WindowsMarkerIo.FileIdentity id)throws Exception {
        var buffer=new ByteArrayOutputStream();var out=new DataOutputStream(buffer);
        byte[] domain="FP_V5_RETIRED_ID_DATA_V1".getBytes(StandardCharsets.US_ASCII);
        out.writeInt(domain.length);out.write(domain);out.writeInt(1);out.writeLong(value.transition().sequence());
        out.writeInt(bytes.length);out.write(WindowsWorldProvisioner.digest(bytes));identity(out,id);
        out.write(value.transition().sourceAnchorSha256());out.write(WindowsWorldProvisioner.digest(value.transition().originalReceipt()));
        out.write(WindowsWorldProvisioner.digest(value.transition().barrierNamespace().getBytes(StandardCharsets.US_ASCII)));
        out.writeLong(value.bridgeSequence());out.write(value.bridgeSha256());identity(out,value.bridgeIdentity());return buffer.toByteArray();
    }
    private void identity(DataOutputStream out,WindowsMarkerIo.FileIdentity id)throws Exception {
        out.writeLong(id.volumeSerialNumber());out.write(id.fileId128());
    }
    @Test void retiredIdentityChecksCompletePayloadSourceAndNativeIdentity()throws Exception {
        var f=fixture();byte[] bytes=B1ResumeRecordV5.encodeData(f.first);var id=new WindowsMarkerIo.FileIdentity(22,new byte[16]);
        byte[] encoded=idData(f.first,bytes,id);
        assertEquals(WindowsB1V5WitnessRecordStore.retiredIdentityLength(),encoded.length);
        var decoded=WindowsB1V5WitnessRecordStore.decodeRetiredIdentity(encoded);
        WindowsB1V5WitnessRecordStore.requireIdentityMatches(decoded,f.first,bytes,id);
        assertThrows(IOException.class,()->WindowsB1V5WitnessRecordStore.requireIdentityMatches(decoded,f.first,bytes,
            new WindowsMarkerIo.FileIdentity(23,new byte[16])));
        for(int i=0;i<encoded.length;i++) {
            byte[] changed=encoded.clone();changed[i]^=1;
            assertThrows(IOException.class,()-> {
                var altered=WindowsB1V5WitnessRecordStore.decodeRetiredIdentity(changed);
                WindowsB1V5WitnessRecordStore.requireIdentityMatches(altered,f.first,bytes,id);
            });
        }
        assertThrows(IOException.class,()->WindowsB1V5WitnessRecordStore.decodeRetiredIdentity(Arrays.copyOf(encoded,encoded.length-1)));
        assertThrows(IOException.class,()->WindowsB1V5WitnessRecordStore.decodeRetiredIdentity(Arrays.copyOf(encoded,encoded.length+1)));
        byte[] changed=bytes.clone();changed[changed.length-1]^=1;
        assertThrows(IOException.class,()->WindowsB1V5WitnessRecordStore.requireIdentityMatches(decoded,f.first,changed,id));
    }
    @Test void historicalNamesRequireExactSequenceAndDigest()throws Exception {
        byte[] hash=new byte[32];hash[31]=1;
        String name=WindowsB1V5WitnessRecordStore.historyBasename(3,hash);
        assertTrue(name.endsWith("3."+java.util.HexFormat.of().formatHex(hash)));
        assertFalse(name.contains("/"));assertFalse(name.contains("\\"));
        assertThrows(IOException.class,()->WindowsB1V5WitnessRecordStore.historyBasename(0,hash));
        assertThrows(IOException.class,()->WindowsB1V5WitnessRecordStore.historyIdentityBasename(3,new byte[31]));
    }
}

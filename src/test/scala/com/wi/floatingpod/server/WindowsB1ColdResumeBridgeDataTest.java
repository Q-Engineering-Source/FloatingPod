package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.UUID;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import static org.junit.jupiter.api.Assertions.*;

/** 直接生产 DATA 比较；所有合成 identity/SQL 字段均不是私有 native ACK。 */
final class WindowsB1ColdResumeBridgeDataTest {
    private record Fixture(B1ResumeRecord v4,byte[] bytes,WindowsMarkerIo.FileIdentity identity,B1ResumeRecordV5 v5){}
    private Fixture fixture()throws Exception {
        var base=B1FreshPrepareRecordFixture.validRecord();var old=base.assertions();var prepare=old.pendingAttempt();
        var nextOwner=new NativeOwnerAssertions(4567,133000000000000002L,UUID.fromString("570ec9f4-231e-4f30-9b22-8c8fd78cd40c"),2);
        UUID op=UUID.fromString("ddd3709c-a053-41d7-b2cd-aafeea52b865");
        var context=new OperationAssertions(nextOwner.boot(),prepare.context().writerEpoch(),prepare.context().binding(),op,
            prepare.context().effects(),prepare.context().originalOperationFingerprint());
        var handoff=new AttemptAssertions(UUID.fromString("511f9e7e-76ba-4bc3-a328-91a4c5765c41"),Phase.HANDOFF,op,2,
            prepare.connectionGeneration(),prepare.connectionId(),prepare.namespace(),context,
            prepare.beforeWriter(),prepare.targetWriter(),prepare.beforeGraph(),prepare.targetGraph());
        var state=new FullStateAssertions(nextOwner,old.currentOwner(),null,prepare,old.currentWriter(),old.targetWriter(),handoff,null,null,null);
        var v4=new B1ResumeRecord(2,new byte[32],Kind.HANDOFF_PREPARED,base.sourceAnchorSha256(),base.originalReceipt(),base.barrierNamespace(),state);
        byte[] bytes=B1ResumeRecordCanonical.encode(v4);var id=new WindowsMarkerIo.FileIdentity(17,new byte[16]);
        var next=new B1ResumeRecord(3,WindowsWorldProvisioner.digest(bytes),Kind.DB_ACQUIRED,v4.sourceAnchorSha256(),v4.originalReceipt(),v4.barrierNamespace(),state);
        var refs=new B1ResumeRecordV5.References(reference(prepare,next),reference(handoff,next),null,null);
        var v5=new B1ResumeRecordV5(next,2,WindowsWorldProvisioner.digest(bytes),id,WindowsWorldProvisioner.digest(bytes),refs);
        return new Fixture(v4,bytes,id,v5);
    }
    private B1Full20WitnessReference reference(AttemptAssertions attempt,B1ResumeRecord record){
        byte[] sha=new byte[32];sha[31]=7;
        return new B1Full20WitnessReference(B1Full20WitnessReference.basename(attempt.attemptId(),sha),
            new WindowsMarkerIo.FileIdentity(19,new byte[16]),90000,sha,record.sourceAnchorSha256(),
            WindowsWorldProvisioner.digest(record.originalReceipt()),new byte[32],attempt.attemptId(),attempt.phase(),attempt.logicalWriteUuid());
    }
    private void verify(Fixture f,B1ResumeRecordV5 v5,B1ResumeRecord v4,byte[] bytes,String identity,NativeOwnerAssertions previous)throws Exception {
        WindowsB1ColdResumeLookup.requireBridgeData(v5,v4,bytes,identity,previous);
    }
    @Test void completeBridgeUsesCanonicalV4AndExactActualPreviousOwner()throws Exception {
        var f=fixture();byte[] all=B1ResumeRecordV5.encodeData(f.v5());var decoded=B1ResumeRecordV5.decodeData(all);
        verify(f,decoded,f.v4(),f.bytes(),f.identity().versionedIdentity(),f.v4().assertions().currentOwner());
        assertArrayEquals(all,B1ResumeRecordV5.encodeData(decoded));
    }
    @Test void changedCompleteV4BytesOrNativeIdentityCannotMatchBridge()throws Exception {
        var f=fixture();byte[] changed=f.bytes().clone();changed[changed.length-1]^=1;
        assertThrows(IOException.class,()->verify(f,f.v5(),f.v4(),changed,f.identity().versionedIdentity(),f.v4().assertions().currentOwner()));
        assertThrows(IOException.class,()->verify(f,f.v5(),f.v4(),f.bytes(),new WindowsMarkerIo.FileIdentity(18,new byte[16]).versionedIdentity(),f.v4().assertions().currentOwner()));
    }
    @Test void oldOwnerAndNonHandoffV4CannotBecomeColdBridge()throws Exception {
        var f=fixture();assertThrows(IOException.class,()->verify(f,f.v5(),f.v4(),f.bytes(),f.identity().versionedIdentity(),f.v4().assertions().previousOwner()));
        var plain=new B1ResumeRecord(f.v4().sequence(),f.v4().previousRecordSha256(),Kind.DB_ACQUIRED,f.v4().sourceAnchorSha256(),f.v4().originalReceipt(),f.v4().barrierNamespace(),f.v4().assertions());
        assertThrows(IOException.class,()->verify(f,f.v5(),plain,f.bytes(),f.identity().versionedIdentity(),f.v4().assertions().currentOwner()));
    }
    @Test void wrongSourceAnchorAndNamespaceCannotBeHiddenByValidReferenceHashes()throws Exception {
        var f=fixture();byte[] anchor=f.v4().sourceAnchorSha256();anchor[0]^=1;
        var wrong=new B1ResumeRecord(f.v4().sequence(),f.v4().previousRecordSha256(),Kind.HANDOFF_PREPARED,anchor,f.v4().originalReceipt(),f.v4().barrierNamespace(),f.v4().assertions());
        assertThrows(IOException.class,()->verify(f,f.v5(),wrong,f.bytes(),f.identity().versionedIdentity(),f.v4().assertions().currentOwner()));
        String differentNamespace="fpb1:"+(f.v4().barrierNamespace().charAt(5)=='A'?"B":"A")+f.v4().barrierNamespace().substring(6);
        var namespace=new B1ResumeRecord(f.v4().sequence(),f.v4().previousRecordSha256(),Kind.HANDOFF_PREPARED,f.v4().sourceAnchorSha256(),f.v4().originalReceipt(),differentNamespace,f.v4().assertions());
        assertThrows(IOException.class,()->verify(f,f.v5(),namespace,f.bytes(),f.identity().versionedIdentity(),f.v4().assertions().currentOwner()));
    }
}

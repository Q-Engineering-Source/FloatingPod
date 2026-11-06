package com.wi.floatingpod.server;

import java.util.List;
import java.util.UUID;
import static com.wi.floatingpod.server.B1ResumeRecord.*;

/** SQL components are explicitly synthetic assertions; never issue SQL/marker/custody capabilities. */
final class B1NormalCertifyFixture {
    static FullStateAssertions prepared(FullStateAssertions genesis) {
        return new FullStateAssertions(genesis.currentOwner(),null,null,genesis.pendingAttempt(),
                genesis.targetWriter(),genesis.targetWriter(),null,null,null,null);
    }
    static MarkerAssertions marker() {
        return new MarkerAssertions(WindowsMarkerIo.markerBytes(B1SourceAnchorFixture.plan().bindingUuid(),2),
            B1SourceAnchorFixture.identity(8),B1SourceAnchorFixture.identity(9),"TEST_ONLY_MARKER_FACTS_NOT_ACK",1,new byte[]{1,2,3});
    }
    static FullStateAssertions intent(FullStateAssertions prepared,MarkerAssertions marker) {
        var p=B1SourceAnchorFixture.plan();var before=prepared.currentWriter();var boot=prepared.currentOwner().boot();
        byte[] effect= new byte[32];effect[0]=7;
        var context=new OperationAssertions(boot,before.epoch(),p.bindingUuid(),p.certifyWriteUuid(),
            List.of(new EffectAssertions("bootstrap/full-graph",0,effect)),effect);
        var slot=new ReceiptSlotAssertions(boot,before.epoch(),p.bindingUuid(),p.certifyWriteUuid(),effect);
        var target=new WriterAssertions(p.bindingUuid(),before.epoch(),before.ownerBoot(),before.leaseUntil(),slot);
        var a=new AttemptAssertions(UUID.randomUUID(),Phase.CERTIFY,p.certifyWriteUuid(),prepared.currentOwner().generation(),
            UUID.randomUUID(),123,B1TwoSlotFixture.namespace(),context,before,target,B1TwoSlotFixture.graph("prepared"),B1TwoSlotFixture.graph("certified"));
        return new FullStateAssertions(prepared.currentOwner(),null,null,prepared.originalPrepareWitness(),before,target,a,null,marker,null);
    }
    static FullStateAssertions certified(FullStateAssertions intent) {
        return new FullStateAssertions(intent.currentOwner(),null,null,intent.originalPrepareWitness(),
            intent.targetWriter(),intent.targetWriter(),null,null,intent.marker(),intent.pendingAttempt());
    }
    static B1ResumeRecord successor(B1ResumeRecord previous,Kind kind,FullStateAssertions state)throws Exception {
        return B1TwoSlotFixture.record(previous.sequence()+1,WindowsWorldProvisioner.digest(B1ResumeRecordCodec.encode(previous)),kind,state);
    }
    static FullStateAssertions withAcknowledged(FullStateAssertions s,AttemptAssertions witness) {
        return new FullStateAssertions(s.currentOwner(),s.previousOwner(),s.retirement(),s.originalPrepareWitness(),s.currentWriter(),s.targetWriter(),
            s.pendingAttempt(),s.latestSettlement(),s.marker(),witness);
    }
}

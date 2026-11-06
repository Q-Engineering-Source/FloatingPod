package com.wi.floatingpod.server;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import com.wi.floatingpod.storage.mysql.MySqlSessionBarrierNamespace;
import static com.wi.floatingpod.server.B1ResumeRecord.*;

/** 全部 SQL 内容是明确 test-only caller assertions，不是实际 SQL/commit/session proof。 */
final class B1TwoSlotFixture {
    static String namespace(){var p=B1SourceAnchorFixture.plan();return MySqlSessionBarrierNamespace.forIdentity(p.mysqlServerUuid(),p.schema(),p.provisioningUuid(),p.bindingUuid()).lockName();}
    static NativeOwnerAssertions pureOwner(){return new NativeOwnerAssertions(1234,B1SourceAnchorFixture.FILETIME,
        B1SourceAnchorFixture.receiptData().creationBoot().bootId(),1);}
    static CommonGraphAssertions graph(String value){return new CommonGraphAssertions("TEST_ONLY_COMPLETE_MANIFEST",("manifest:"+value).getBytes(java.nio.charset.StandardCharsets.UTF_8),
        "TEST_ONLY_COMPLETE_BINDING",("binding:"+value).getBytes(java.nio.charset.StandardCharsets.UTF_8),
        "TEST_ONLY_COMPLETE_REPOSITORY",("repository:"+value).getBytes(java.nio.charset.StandardCharsets.UTF_8));}
    static FullStateAssertions genesis(NativeOwnerAssertions owner){var p=B1SourceAnchorFixture.plan();
        var before=new WriterAssertions(p.bindingUuid(),0,null,LocalDateTime.of(2026,10,7,1,2,3,123456000),null);
        var target=new WriterAssertions(p.bindingUuid(),1,owner.boot(),LocalDateTime.of(2026,10,7,1,3,3,987654000),null);
        var context=new OperationAssertions(owner.boot(),1,p.bindingUuid(),p.prepareWriteUuid(),
            List.of(new EffectAssertions("bootstrap/prepare",0,new byte[32])),new byte[32]);
        var attempt=new AttemptAssertions(UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf1111"),Phase.PREPARE,p.prepareWriteUuid(),owner.generation(),
            UUID.fromString("21a0707c-cd88-4004-8c06-5d790eaf2222"),123,namespace(),context,before,target,graph("before"),graph("target"));
        return new FullStateAssertions(owner,null,null,null,before,target,attempt,null,null, null);
    }
    static FullStateAssertions prepared(FullStateAssertions original){return new FullStateAssertions(original.currentOwner(),original.previousOwner(),original.retirement(),
        original.pendingAttempt(),original.currentWriter(),original.targetWriter(),null,
        new SettlementAssertions(SettlementKind.CONFIRMED,original.pendingAttempt(),"TEST_ONLY_ASSERTED_CONFIRMATION",new byte[]{1,2,3},new byte[32]),null, null);}
    static B1ResumeRecord record(long sequence,byte[] predecessor,Kind kind,FullStateAssertions state)throws Exception {
        return new B1ResumeRecord(sequence,predecessor,kind,new byte[32],WindowsWorldProvisioner.encodeReceipt(B1SourceAnchorFixture.receiptData()),namespace(),state);
    }
}

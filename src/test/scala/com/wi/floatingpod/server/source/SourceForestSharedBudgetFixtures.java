package com.wi.floatingpod.server.source;

import java.io.IOException;
import java.util.*;
import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestCodec.*;

/** 只复用现有真实NBT fixture，生产decoder和membership算法不复制。 */
public final class SourceForestSharedBudgetFixtures {
    private SourceForestSharedBudgetFixtures(){}
    public static SourceLimitProfile profile(){return SourceForestFixtures.LIMITS;}
    public static Forest forest(long sqlIdentity,boolean other,boolean unresolved,boolean retired)throws IOException {
        Forest original=SourceForestFixtures.forest(other,unresolved);
        Identity old=original.identity();Identity identity=new Identity(new UUID(0,sqlIdentity),old.unitId(),old.forestId(),old.sourceEpoch(),old.createdOperationId(),old.worldIdentity(),old.saveIdentity(),old.loaderProfile(),old.catalogGeneration(),old.readGeneration());
        List<Member> members=original.members();
        if(retired) {
            List<Member> removed=new ArrayList<>();
            for(Member member:members) {
                List<Stage> stages=new ArrayList<>();
                for(Stage stage:member.stages()) {
                    stages.add(stage.kind()!=StageKind.ACTUAL?stage:new Stage(stage.kind(),stage.captureId(),stage.carrierId(),stage.parentMemberId(),stage.entityPath(),stage.classification(),stage.entityNbt(),stage.factory(),stage.factoryCalls(),stage.read(),stage.readCalls(),stage.caps(),stage.capsCalls(),stage.placement(),stage.placementCalls(),Observation.NOT_OBSERVED,0,"",null));
                }
                removed.add(new Member(member.id(),Disposition.LEGITIMATELY_REMOVED,member.dispositionObservation(),Observation.RETURNED,1,stages.getLast().captureId(),stages,member.lineage()));
            }
            members=removed;
        }
        return new Forest(identity,original.coverage(),original.sourceObservation(),original.rawRoots(),original.fixedRoots(),original.actualRoots(),original.carriers(),members);
    }
}

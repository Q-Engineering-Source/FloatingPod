package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.server.B1ResumeRecord;
import com.wi.floatingpod.server.WindowsB1SourceAnchorStore;
import com.wi.floatingpod.server.WindowsB1TwoSlotStore;
import com.wi.floatingpod.server.WindowsWorldProvisioner;
import java.util.UUID;

/** 独立 Normal readonly plan；仅同 actor 完整 cleanup 后的 package issuer 可发行。 */
public final class MySqlNormalPreGenesisCapabilities {
    private MySqlNormalPreGenesisCapabilities() { }
    static final class Source {
        final Object actor; final UUID nonce; final VerifiedMySqlConnection verified;
        final MySqlAuditBootScope scope; final HeldSessionBarrier held;
        final WindowsWorldProvisioner.ResumeInspection inspection;
        final WindowsB1TwoSlotStore slots; final WindowsB1TwoSlotStore.ResumeSourceScope nativeScope;
        final WindowsB1SourceAnchorStore anchorStore;
        final WindowsB1SourceAnchorStore.ControlledAnchorLocation location;
        final WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor;
        Source(Object actor, UUID nonce, VerifiedMySqlConnection verified, MySqlAuditBootScope scope,
                HeldSessionBarrier held, WindowsWorldProvisioner.ResumeInspection inspection,
                WindowsB1TwoSlotStore slots, WindowsB1TwoSlotStore.ResumeSourceScope nativeScope,
                WindowsB1SourceAnchorStore anchorStore, WindowsB1SourceAnchorStore.ControlledAnchorLocation location,
                WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor) {
            this.actor=actor;this.nonce=nonce;this.verified=verified;this.scope=scope;this.held=held;
            this.inspection=inspection;this.slots=slots;this.nativeScope=nativeScope;
            this.anchorStore=anchorStore;this.location=location;this.anchor=anchor;
        }
    }
    public static final class PlannedNormalPreGenesis {
        private final Source source; private final UUID attempt;
        private final MySqlBootstrapPlanIdentity bootstrap; private final MySqlOperationContext context;
        private final MySqlB1CommonGraph before,target; private final MySqlBusinessWriterRow writer;
        private final MySqlFreshPrepareCapabilities.WriterTuple beforeWriter,targetWriter;
        private final B1ResumeRecord genesis,confirmation; private final byte[] genesisBytes,confirmationBytes;
        private PlannedNormalPreGenesis(Source source,UUID attempt,MySqlBootstrapPlanIdentity bootstrap,
                MySqlOperationContext context,MySqlB1CommonGraph before,MySqlB1CommonGraph target,
                MySqlBusinessWriterRow writer,MySqlFreshPrepareCapabilities.WriterTuple beforeWriter,
                MySqlFreshPrepareCapabilities.WriterTuple targetWriter,B1ResumeRecord genesis,byte[] genesisBytes,
                B1ResumeRecord confirmation,byte[] confirmationBytes) {
            this.source=source;this.attempt=attempt;this.bootstrap=bootstrap;this.context=context;
            this.before=before;this.target=target;this.writer=writer;this.beforeWriter=beforeWriter;this.targetWriter=targetWriter;
            this.genesis=genesis;this.confirmation=confirmation;this.genesisBytes=genesisBytes.clone();this.confirmationBytes=confirmationBytes.clone();
        }
        Source source(){return source;} MySqlBootstrapPlanIdentity bootstrap(){return bootstrap;}
        MySqlBusinessWriterRow initialWriter(){return writer;} B1ResumeRecord confirmation(){return confirmation;}
        byte[] confirmationBytes(){return confirmationBytes.clone();}
        public UUID attemptId(){return attempt;} public MySqlOperationContext context(){return context;}
        public MySqlB1CommonGraph beforeGraph(){return before;} public MySqlB1CommonGraph targetGraph(){return target;}
        public MySqlFreshPrepareCapabilities.WriterTuple beforeWriter(){return beforeWriter;}
        public MySqlFreshPrepareCapabilities.WriterTuple targetWriter(){return targetWriter;}
        public B1ResumeRecord genesisRecord(){return genesis;} public byte[] genesisCanonicalBytes(){return genesisBytes.clone();}
        @Override public String toString(){return "PlannedNormalPreGenesis[opaque; not Ready]";}
    }
    /** 真实 native ACK 与 installed custody 的强引用，不由 public DTO/boolean 开门。 */
    static final class InstalledOrigin {
        final PlannedNormalPreGenesis plan; final WindowsB1TwoSlotStore.DurablePreGenesisNativeAck ack;
        final WindowsWorldProvisioner.ProvisionedWorld world;
        private InstalledOrigin(PlannedNormalPreGenesis plan,WindowsB1TwoSlotStore.DurablePreGenesisNativeAck ack,
                WindowsWorldProvisioner.ProvisionedWorld world){this.plan=plan;this.ack=ack;this.world=world;}
    }
    static PlannedNormalPreGenesis issuePlan(Source source,UUID attempt,MySqlBootstrapPlanIdentity bootstrap,
            MySqlOperationContext context,MySqlB1CommonGraph before,MySqlB1CommonGraph target,
            MySqlBusinessWriterRow writer,MySqlFreshPrepareCapabilities.WriterTuple beforeWriter,
            MySqlFreshPrepareCapabilities.WriterTuple targetWriter,B1ResumeRecord genesis,byte[] genesisBytes,
            B1ResumeRecord confirmation,byte[] confirmationBytes){
        return new PlannedNormalPreGenesis(source,attempt,bootstrap,context,before,target,writer,beforeWriter,targetWriter,
                genesis,genesisBytes,confirmation,confirmationBytes);
    }
    static InstalledOrigin installed(PlannedNormalPreGenesis plan,WindowsB1TwoSlotStore.DurablePreGenesisNativeAck ack,
            WindowsWorldProvisioner.ProvisionedWorld world){return new InstalledOrigin(plan,ack,world);}
}

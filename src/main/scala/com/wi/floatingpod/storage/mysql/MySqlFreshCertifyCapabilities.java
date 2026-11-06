package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.server.B1ResumeRecord;
import com.wi.floatingpod.server.WindowsB1TwoSlotStore;
import com.wi.floatingpod.server.WindowsCanonicalMarkerPublicationBarrier;
import com.wi.floatingpod.server.WindowsWorldProvisioner;
import java.util.UUID;

/** 只能由同一个真实 Fresh actor 签发；private JVM constructors，不从磁盘 DTO 复活权限。 */
public final class MySqlFreshCertifyCapabilities {
    private MySqlFreshCertifyCapabilities() { }
    public static final class CurrentMarker {
        private final MySqlFreshPrepareCapabilities.PreparedConfirmed prepared;
        private final WindowsWorldProvisioner.CanonicalMarkerLocation location;
        private final WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck ack;
        private CurrentMarker(MySqlFreshPrepareCapabilities.PreparedConfirmed p,
                WindowsWorldProvisioner.CanonicalMarkerLocation l,
                WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck a) { prepared=p; location=l; ack=a; }
        MySqlFreshPrepareCapabilities.PreparedConfirmed prepared() { return prepared; }
        WindowsWorldProvisioner.CanonicalMarkerLocation location() { return location; }
        WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck ack() { return ack; }
        @Override public String toString() { return "CurrentMarker[actual prepared source/native ACK; not Ready]"; }
    }
    public static final class PlannedFreshCertify {
        private final CurrentMarker marker;
        private final UUID attempt;
        private final MySqlOperationContext context;
        private final MySqlB1CommonGraph before, target;
        private final MySqlFreshPrepareCapabilities.WriterTuple beforeWriter, targetWriter;
        private final B1ResumeRecord intent, confirmation;
        private final byte[] intentBytes, confirmationBytes;
        private PlannedFreshCertify(CurrentMarker marker, UUID attempt, MySqlOperationContext context,
                MySqlB1CommonGraph before, MySqlB1CommonGraph target,
                MySqlFreshPrepareCapabilities.WriterTuple beforeWriter, MySqlFreshPrepareCapabilities.WriterTuple targetWriter,
                B1ResumeRecord intent, byte[] intentBytes, B1ResumeRecord confirmation, byte[] confirmationBytes) {
            this.marker=marker; this.attempt=attempt; this.context=context; this.before=before; this.target=target;
            this.beforeWriter=beforeWriter; this.targetWriter=targetWriter; this.intent=intent; this.confirmation=confirmation;
            this.intentBytes=intentBytes.clone(); this.confirmationBytes=confirmationBytes.clone();
        }
        CurrentMarker marker() { return marker; }
        B1ResumeRecord confirmation() { return confirmation; }
        byte[] confirmationBytes() { return confirmationBytes.clone(); }
        public UUID attemptId() { return attempt; }
        public MySqlOperationContext context() { return context; }
        public MySqlB1CommonGraph beforeGraph() { return before; }
        public MySqlB1CommonGraph targetGraph() { return target; }
        public MySqlFreshPrepareCapabilities.WriterTuple beforeWriter() { return beforeWriter; }
        public MySqlFreshPrepareCapabilities.WriterTuple targetWriter() { return targetWriter; }
        public B1ResumeRecord intentRecord() { return intent; }
        int intentCanonicalByteLength() { return intentBytes.length; }
        public byte[] intentCanonicalBytes() { return intentBytes.clone(); }
        @Override public String toString() { return "PlannedFreshCertify[fixed distinct attempt; not SQL/Ready]"; }
    }
    public static final class DurableCertifyIntent {
        private final PlannedFreshCertify plan;
        private final WindowsB1TwoSlotStore.NativeRecordDurability ack;
        private DurableCertifyIntent(PlannedFreshCertify p, WindowsB1TwoSlotStore.NativeRecordDurability a) { plan=p; ack=a; }
        WindowsB1TwoSlotStore.NativeRecordDurability ack() { return ack; }
        public PlannedFreshCertify originalPlan() { return plan; }
        @Override public String toString() { return "DurableCertifyIntent[native pending retained; not Ready]"; }
    }
    public static final class CertifiedConfirmed {
        private final DurableCertifyIntent intent;
        private final WindowsB1TwoSlotStore.NativeRecordDurability ack;
        private CertifiedConfirmed(DurableCertifyIntent i, WindowsB1TwoSlotStore.NativeRecordDurability a) { intent=i; ack=a; }
        WindowsB1TwoSlotStore.NativeRecordDurability ack() { return ack; }
        public PlannedFreshCertify originalPlan() { return intent.originalPlan(); }
        public B1ResumeRecord confirmedRecord() { return ack.facts().record(); }
        @Override public String toString() { return "CertifiedConfirmed[SQL/full readback/native record; fresh audit still required]"; }
    }
    public static final class CompletionProof {
        private final MySqlFreshWorldBootstrap owner;
        private final CertifiedConfirmed certified;
        private final MySqlWriterFenceCoordinator coordinator;
        private final VerifiedMySqlConnection verified;
        private final MySqlAuditBootScope scope;
        private final MySqlBusinessWriterRow writer;
        private final MySqlWriterFenceToken token;
        private final RuntimeSchemaAuditReceipt receipt;
        private CompletionProof(MySqlFreshWorldBootstrap owner, CertifiedConfirmed certified,
                MySqlWriterFenceCoordinator coordinator, VerifiedMySqlConnection verified, MySqlAuditBootScope scope,
                MySqlBusinessWriterRow writer, MySqlWriterFenceToken token, RuntimeSchemaAuditReceipt receipt) {
            this.owner=owner; this.certified=certified; this.coordinator=coordinator; this.verified=verified; this.scope=scope;
            this.writer=writer; this.token=token; this.receipt=receipt;
        }
        MySqlFreshWorldBootstrap owner() { return owner; }
        CertifiedConfirmed certified() { return certified; }
        MySqlWriterFenceCoordinator coordinator() { return coordinator; }
        VerifiedMySqlConnection verified() { return verified; }
        MySqlAuditBootScope scope() { return scope; }
        MySqlBusinessWriterRow writer() { return writer; }
        MySqlWriterFenceToken token() { return token; }
        RuntimeSchemaAuditReceipt receipt() { return receipt; }
        @Override public String toString() { return "CompletionProof[opaque same actor/coordinator/current audit]"; }
    }
    static CurrentMarker issueMarker(MySqlFreshPrepareCapabilities.PreparedConfirmed p,
            WindowsWorldProvisioner.CanonicalMarkerLocation l, WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck a) {
        return new CurrentMarker(p,l,a);
    }
    static PlannedFreshCertify issuePlan(CurrentMarker m, UUID a, MySqlOperationContext c,
            MySqlB1CommonGraph b, MySqlB1CommonGraph t,
            MySqlFreshPrepareCapabilities.WriterTuple bw, MySqlFreshPrepareCapabilities.WriterTuple tw,
            B1ResumeRecord i, byte[] ib, B1ResumeRecord r, byte[] rb) { return new PlannedFreshCertify(m,a,c,b,t,bw,tw,i,ib,r,rb); }
    static DurableCertifyIntent issueIntent(PlannedFreshCertify p, WindowsB1TwoSlotStore.NativeRecordDurability a) { return new DurableCertifyIntent(p,a); }
    static CertifiedConfirmed issueCertified(DurableCertifyIntent i, WindowsB1TwoSlotStore.NativeRecordDurability a) { return new CertifiedConfirmed(i,a); }
    static CompletionProof issueCompletion(MySqlFreshWorldBootstrap o, CertifiedConfirmed c, MySqlWriterFenceCoordinator target,
            VerifiedMySqlConnection v, MySqlAuditBootScope s, MySqlBusinessWriterRow w, MySqlWriterFenceToken t, RuntimeSchemaAuditReceipt r) {
        return new CompletionProof(o,c,target,v,s,w,t,r);
    }
}

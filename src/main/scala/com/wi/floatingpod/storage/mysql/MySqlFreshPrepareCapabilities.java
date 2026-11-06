package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.server.B1ResumeRecord;
import com.wi.floatingpod.server.WindowsB1SourceAnchorStore;
import com.wi.floatingpod.server.WindowsB1TwoSlotStore;
import com.wi.floatingpod.server.WindowsMarkerIo;
import com.wi.floatingpod.server.WindowsWorldProvisioner;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** 真正 JVM private constructors；package issuer桥仅由actual actor使用，public不从DTO签发能力。 */
public final class MySqlFreshPrepareCapabilities {
    private MySqlFreshPrepareCapabilities() { }

    /** 六个实际stored W字段；databaseNow是观察时间，不冒充持久字段。 */
    public record WriterTuple(UUID binding, long epoch, UUID ownerBoot, LocalDateTime leaseUntil,
            UUID lastOperation, byte[] lastFingerprint) {
        public WriterTuple {
            Objects.requireNonNull(binding); Objects.requireNonNull(leaseUntil);
            if (epoch < 0 || leaseUntil.getNano() % 1000 != 0) throw new IllegalArgumentException("Exact stored W precision required");
            if ((lastOperation == null) != (lastFingerprint == null) || lastFingerprint != null && lastFingerprint.length != 32)
                throw new IllegalArgumentException("Complete actual W receipt pair required");
            lastFingerprint = lastFingerprint == null ? null : lastFingerprint.clone();
        }
        @Override public byte[] lastFingerprint() { return lastFingerprint == null ? null : lastFingerprint.clone(); }
        public boolean exactlyEquals(WriterTuple other) {
            return other != null && binding.equals(other.binding) && epoch == other.epoch
                    && Objects.equals(ownerBoot, other.ownerBoot) && leaseUntil.equals(other.leaseUntil)
                    && Objects.equals(lastOperation, other.lastOperation) && Arrays.equals(lastFingerprint, other.lastFingerprint);
        }
    }

    // Origin只是actor保存的原始actual资源引用，不是可由caller签发的scope或数据库能力。
    static final class Origin {
        final MySqlNormalPreGenesisCapabilities.InstalledOrigin normal;
        final Object issuer;
        final UUID nonce;
        final VerifiedMySqlConnection verified;
        final MySqlAuditBootScope scope;
        final HeldSessionBarrier held;
        final WindowsWorldProvisioner.ProvisionedWorld world;
        final WindowsWorldProvisioner.FreshProvisioningProof fresh;
        final WindowsWorldProvisioner.BootWorldCustodyProof custody;
        final WindowsB1TwoSlotStore slots;
        final WindowsB1TwoSlotStore.SourceScope nativeScope;
        final WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor;
        Origin(Object issuer, UUID nonce, VerifiedMySqlConnection verified, MySqlAuditBootScope scope,
                HeldSessionBarrier held, WindowsWorldProvisioner.ProvisionedWorld world,
                WindowsWorldProvisioner.FreshProvisioningProof fresh, WindowsWorldProvisioner.BootWorldCustodyProof custody,
                WindowsB1TwoSlotStore slots, WindowsB1TwoSlotStore.SourceScope nativeScope,
                WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor) {
            this(issuer,nonce,verified,scope,held,world,fresh,custody,slots,nativeScope,anchor,null);
        }
        Origin(Object issuer, UUID nonce, VerifiedMySqlConnection verified, MySqlAuditBootScope scope,
                HeldSessionBarrier held, WindowsWorldProvisioner.ProvisionedWorld world,
                WindowsWorldProvisioner.FreshProvisioningProof fresh, WindowsWorldProvisioner.BootWorldCustodyProof custody,
                WindowsB1TwoSlotStore slots, WindowsB1TwoSlotStore.SourceScope nativeScope,
                WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor,
                MySqlNormalPreGenesisCapabilities.InstalledOrigin normal) {
            this.normal = normal;
            this.issuer = Objects.requireNonNull(issuer); this.nonce = Objects.requireNonNull(nonce);
            this.verified = Objects.requireNonNull(verified); this.scope = Objects.requireNonNull(scope);
            this.held = Objects.requireNonNull(held); this.world = Objects.requireNonNull(world);
            this.fresh = Objects.requireNonNull(fresh); this.custody = Objects.requireNonNull(custody);
            this.slots = Objects.requireNonNull(slots); this.nativeScope = Objects.requireNonNull(nativeScope);
            this.anchor = Objects.requireNonNull(anchor);
        }
    }

    public static final class PlannedFreshPrepare {
        private final Origin origin;
        private final UUID attempt;
        private final MySqlBootstrapPlanIdentity bootstrap;
        private final MySqlOperationContext context;
        private final MySqlB1CommonGraph before;
        private final MySqlB1CommonGraph target;
        private final MySqlBusinessWriterRow initialWriter;
        private final WriterTuple beforeWriter;
        private final WriterTuple targetWriter;
        private final B1ResumeRecord genesis;
        private final byte[] genesisBytes;
        private final B1ResumeRecord confirmation;
        private final byte[] confirmationBytes;
        private PlannedFreshPrepare(Origin origin, UUID attempt, MySqlBootstrapPlanIdentity bootstrap,
                MySqlOperationContext context, MySqlB1CommonGraph before, MySqlB1CommonGraph target,
                MySqlBusinessWriterRow initialWriter, WriterTuple beforeWriter, WriterTuple targetWriter,
                B1ResumeRecord genesis, byte[] genesisBytes, B1ResumeRecord confirmation, byte[] confirmationBytes) {
            this.origin = origin; this.attempt = attempt; this.bootstrap = bootstrap; this.context = context;
            this.before = before; this.target = target; this.initialWriter = initialWriter;
            this.beforeWriter = beforeWriter; this.targetWriter = targetWriter;
            this.genesis = genesis; this.genesisBytes = genesisBytes.clone();
            this.confirmation = confirmation; this.confirmationBytes = confirmationBytes.clone();
        }
        Origin origin() { return origin; }
        MySqlBootstrapPlanIdentity bootstrap() { return bootstrap; }
        MySqlBusinessWriterRow initialWriter() { return initialWriter; }
        B1ResumeRecord confirmation() { return confirmation; }
        byte[] confirmationBytes() { return confirmationBytes.clone(); }
        public UUID attemptId() { return attempt; }
        public MySqlOperationContext context() { return context; }
        public MySqlB1CommonGraph beforeGraph() { return before; }
        public MySqlB1CommonGraph targetGraph() { return target; }
        public WriterTuple beforeWriter() { return beforeWriter; }
        public WriterTuple targetWriter() { return targetWriter; }
        public B1ResumeRecord genesisRecord() { return genesis; }
        int genesisCanonicalByteLength() { return genesisBytes.length; }
        public byte[] genesisCanonicalBytes() { return genesisBytes.clone(); }
        @Override public String toString() { return "PlannedFreshPrepare[opaque actual readonly plan; not Ready]"; }
    }
    public static final class DurableFreshGenesis {
        private final PlannedFreshPrepare plan;
        private final WindowsB1TwoSlotStore.NativeRecordDurability ack;
        private DurableFreshGenesis(PlannedFreshPrepare plan, WindowsB1TwoSlotStore.NativeRecordDurability ack) {
            this.plan = plan; this.ack = ack;
        }
        WindowsB1TwoSlotStore.NativeRecordDurability ack() { return ack; }
        public PlannedFreshPrepare originalPlan() { return plan; }
        public String nativeProtocol() { return ack.protocol(); }
        @Override public String toString() { return "DurableFreshGenesis[same plan native ACK; prepare only]"; }
    }
    public static final class PreparedConfirmed {
        private final DurableFreshGenesis genesis;
        private final WindowsB1TwoSlotStore.NativeRecordDurability ack;
        private PreparedConfirmed(DurableFreshGenesis genesis, WindowsB1TwoSlotStore.NativeRecordDurability ack) {
            this.genesis = genesis; this.ack = ack;
        }
        public PlannedFreshPrepare originalPlan() { return genesis.originalPlan(); }
        WindowsB1TwoSlotStore.NativeRecordDurability ack() { return ack; }
        public B1ResumeRecord confirmedRecord() { return ack.facts().record(); }
        public String nativeProtocol() { return ack.protocol(); }
        @Override public String toString() { return "PreparedConfirmed[SQL/full readback/native record; not certify/Ready]"; }
    }

    static PlannedFreshPrepare issuePlan(Origin origin, UUID attempt, MySqlBootstrapPlanIdentity bootstrap,
            MySqlOperationContext context, MySqlB1CommonGraph before, MySqlB1CommonGraph target,
            MySqlBusinessWriterRow initialWriter, WriterTuple beforeWriter, WriterTuple targetWriter,
            B1ResumeRecord genesis, byte[] genesisBytes, B1ResumeRecord confirmation, byte[] confirmationBytes) {
        return new PlannedFreshPrepare(origin, attempt, bootstrap, context, before, target, initialWriter,
                beforeWriter, targetWriter, genesis, genesisBytes, confirmation, confirmationBytes);
    }
    static DurableFreshGenesis issueGenesis(PlannedFreshPrepare plan, WindowsB1TwoSlotStore.NativeRecordDurability ack) {
        return new DurableFreshGenesis(plan, ack);
    }
    static PreparedConfirmed issueConfirmed(DurableFreshGenesis genesis, WindowsB1TwoSlotStore.NativeRecordDurability ack) {
        return new PreparedConfirmed(genesis, ack);
    }
}

package com.wi.floatingpod.server;

import com.wi.floatingpod.bootstrap.ProductionBootstrapControl;
import com.wi.floatingpod.storage.mysql.MySqlCertifiedFreshExitPermit;
import com.wi.floatingpod.storage.mysql.MySqlFormat3ReadBudget;
import com.wi.floatingpod.storage.mysql.MySqlDeploymentReceiptSession;
import com.wi.floatingpod.storage.mysql.MySqlDeploymentReceiptBootstrapActor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/**
 * Main 控制的 B1 source factory。必须在 first Anvil/world access 前调用。
 * 不触及 JDBC、fence、Ready 或真实世界 load；每个方法都在 WindowsMarkerIo owner thread。
 * 普通跨进程恢复必须同时持有原 source/guard/receipt 并取得绑定该 owner 的本机进程生命周期观察；
 * 同进程重启仍明确拒绝，PID 快照缺席与 PID 重用也不伪称精确进程退出 ACK。
 */
public final class WindowsWorldProvisioner {
    static final String RECEIPT_NAME = ".floatingpod-provision-v1.receipt";
    static final String GUARD_NAME = ".floatingpod-custody-v1.guard";
    static final long RECEIPT_MAGIC = 0x4650575243503031L, GUARD_MAGIC = 0x4650574353543031L;
    static final int OWNER_SIZE = 164;
    // 成功 custody 有意保留到实际 process 结束；不能由 GC 或普通 close 提前释放。
    private static final List<BootWorldCustodyProof> PROCESS_LIFETIME_CUSTODIES = new ArrayList<>();
    private static final List<DeploymentReceiptCustodyProof> DEPLOYMENT_RECEIPT_PROCESS_CUSTODIES = new ArrayList<>();
    private static final long DEPLOYMENT_RECEIPT_OWNER_MAGIC = 0x4650574353543035L;
    private final WindowsMarkerIo io;
    public WindowsWorldProvisioner(WindowsMarkerIo io) { this.io = Objects.requireNonNull(io); }

    /** 输入计划不含 caller 伪填的目录身份/receipt SHA；这两个实际值只能由 factory 封入 proof。 */
    public record Plan(UUID creationOperationUuid, UUID bootstrapOperationUuid, UUID prepareWriteUuid,
                       UUID certifyWriteUuid, UUID bindingUuid, int physicalFormat, UUID mysqlServerUuid,
                       String schema, int schemaFormat, String ddlSha256, UUID provisioningUuid) {
        public Plan {
            Objects.requireNonNull(creationOperationUuid); Objects.requireNonNull(bootstrapOperationUuid);
            Objects.requireNonNull(prepareWriteUuid); Objects.requireNonNull(certifyWriteUuid);
            Objects.requireNonNull(bindingUuid); Objects.requireNonNull(mysqlServerUuid); Objects.requireNonNull(provisioningUuid);
            if (prepareWriteUuid.equals(certifyWriteUuid)) throw new IllegalArgumentException("Two phase write UUIDs must be distinct");
            if (physicalFormat != 2 || (schemaFormat != 2 && schemaFormat != 3) || bindingUuid.version() != 4)
                throw new IllegalArgumentException("Fresh B1 requires schema2 or schema3/physical2/supplied UUIDv4 binding");
            if (schema == null || !schema.matches("[A-Za-z0-9_]{1,64}")) throw new IllegalArgumentException("Safe exact schema required");
            hashBytes(ddlSha256);
            ddlSha256 = ddlSha256.toUpperCase(Locale.ROOT);
        }
    }
    public record BootIdentity(UUID bootId, long generation) {
        public BootIdentity {
            Objects.requireNonNull(bootId);
            if (generation <= 0) throw new IllegalArgumentException("Positive boot generation required");
        }
    }
    /** 只能由实际 schema5 native creation 封入；没有 DATA-to-proof 构造入口。 */
    public static final class DeploymentReceiptCreationProof {
        private final WindowsWorldProvisionReceiptDataCodec.ReceiptData receipt;
        private final WindowsMarkerIo.FileIdentity sourceIdentity, guardIdentity, receiptIdentity;
        private final byte[] bytes;
        private DeploymentReceiptCreationProof(WindowsWorldProvisionReceiptDataCodec.ReceiptData receipt,
                WindowsMarkerIo.FileIdentity source, WindowsMarkerIo.FileIdentity guard, NativeReceiptAck ack) {
            this.receipt=receipt;sourceIdentity=source;guardIdentity=guard;receiptIdentity=ack.fileIdentity;bytes=ack.bytes.clone();
        }
        public WindowsWorldProvisionReceiptDataCodec.PlanData plan(){return receipt.plan();}
        public BootIdentity creationBoot(){return receipt.creationBoot();}
        public long creationProcessPid(){return receipt.creatorPid();}
        public long creationProcessFileTime(){return receipt.creatorCreationTime();}
        public String sourceWorldIdentity(){return sourceIdentity.versionedIdentity();}
        public String guardFileIdentity(){return guardIdentity.versionedIdentity();}
        public String receiptFileIdentity(){return receiptIdentity.versionedIdentity();}
        public byte[] receiptCanonicalBytes(){return bytes.clone();}
        public String receiptSha256(){return sha256(bytes);}
        @Override public String toString(){return "DeploymentReceiptCreationProof[actual native creation; not SQL/Ready]";}
    }
    public static final class DeploymentReceiptCustodyProof {
        private final WindowsWorldProvisioner provisioner;
        private final Thread thread=Thread.currentThread();
        private final Path directory;
        private final WindowsMarkerIo.Handle source,guard;
        private final DeploymentReceiptCreationProof creation;
        private final BootIdentity boot;
        private final WindowsMarkerIo.ProcessIdentity process;
        private final byte[] ownerBytes;
        private final String nativePath;
        private DeploymentReceiptCustodyProof(WindowsWorldProvisioner provisioner,Path directory,
                WindowsMarkerIo.Handle source,WindowsMarkerIo.Handle guard,DeploymentReceiptCreationProof creation,
                BootIdentity boot,WindowsMarkerIo.ProcessIdentity process,byte[] ownerBytes,String nativePath) {
            this.provisioner=provisioner;this.directory=directory;this.source=source;this.guard=guard;this.creation=creation;
            this.boot=boot;this.process=process;this.ownerBytes=ownerBytes.clone();this.nativePath=nativePath;
        }
        public Path directory(){return directory;}
        public DeploymentReceiptCreationProof creationProof(){return creation;}
        public UUID bootId(){return boot.bootId();}
        public long generation(){return boot.generation();}
        public void validateLive()throws IOException{provisioner.validateLive(this);}
        public String lifetime(){return "PROCESS_LIFETIME_NO_SCHEMA5_EARLY_RELEASE_V1";}
        @Override public String toString(){return "DeploymentReceiptCustodyProof[actual retained handles; no ordinary close]";}
    }
    public static final class DeploymentReceiptWorld {
        private final DeploymentReceiptCreationProof creation;
        private final DeploymentReceiptCustodyProof custody;
        private DeploymentReceiptWorld(DeploymentReceiptCreationProof creation,DeploymentReceiptCustodyProof custody){this.creation=creation;this.custody=custody;}
        public DeploymentReceiptCreationProof creationProof(){return creation;}
        public DeploymentReceiptCustodyProof custodyProof(){return custody;}
        @Override public String toString(){return "DeploymentReceiptWorld[actual native5 creation; not admission]";}
    }
    private static final List<DeploymentReceiptInspection> DEPLOYMENT_RECEIPT_INSPECTIONS = new ArrayList<>();

    /** inspect 的原句柄在成功、失败和 install 后均有实际持有者；不构成 SQL/admission。 */
    public static final class DeploymentReceiptInspection {
        private final WindowsWorldProvisioner issuer;
        private final Thread thread=Thread.currentThread();
        private final ProductionBootstrapControl.OriginalSourcePins pins;
        private final BootIdentity targetBoot;
        private WindowsMarkerIo.Handle source,guard;
        private DeploymentReceiptCreationProof creation;
        private PreviousOwner previous;
        private byte[] ownerBytes;
        private String nativePath;
        private WindowsMarkerIo.ProcessIdentity targetProcess;
        private DeploymentReceiptPreviousOwnerExit exit;
        private DeploymentReceiptWorld installed;
        private Throwable failure;
        private boolean installAttempted;
        private DeploymentReceiptInspection(WindowsWorldProvisioner issuer,ProductionBootstrapControl.OriginalSourcePins pins,BootIdentity targetBoot) {
            this.issuer=issuer;this.pins=pins;this.targetBoot=targetBoot;
        }
        public DeploymentReceiptCreationProof creationProof() throws IOException {issuer.validateDeploymentInspection(this);return creation;}
        public B1ResumeRecord.NativeOwnerAssertions previousOwner() throws IOException {
            issuer.validateDeploymentInspection(this);
            return new B1ResumeRecord.NativeOwnerAssertions(previous.pid,previous.creationTime,previous.boot.bootId(),previous.boot.generation());
        }
        public B1ResumeRecord.NativeOwnerAssertions targetOwner() throws IOException {
            issuer.validateDeploymentInspection(this);
            return new B1ResumeRecord.NativeOwnerAssertions(targetProcess.pid(),targetProcess.creationFileTime(),targetBoot.bootId(),targetBoot.generation());
        }
        public Throwable retainedFailure(){return failure;}
    }
    public static final class DeploymentReceiptPreviousOwnerExit {
        private final DeploymentReceiptInspection inspection;
        private final WindowsMarkerIo.PreviousProcessLifetimeObservation observation;
        private DeploymentReceiptPreviousOwnerExit(DeploymentReceiptInspection inspection,WindowsMarkerIo.PreviousProcessLifetimeObservation observation) {
            this.inspection=inspection;this.observation=observation;
        }
    }
    /** 只向固定 Full26/V6 native stores 开放的原 source 范围；无 Connection 或普通 close。 */
    public static final class DeploymentReceiptIoScope {
        private final WindowsWorldProvisioner issuer;
        private final DeploymentReceiptWorld fresh;
        private final DeploymentReceiptInspection inspection;
        private final WindowsB1SourceAnchorStore anchors;
        private final WindowsB1SourceAnchorStore.ControlledAnchorLocation location;
        private final WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor;
        private final List<DeploymentReceiptMarkerInspection> markerInspections=new ArrayList<>();
        private final List<DeploymentReceiptExecutorRetirement> executorRetirements=new ArrayList<>();
        private MySqlFormat3ReadBudget allocationRoot;
        private long executorRetirementFee;
        private boolean retirementAttempted,retired;
        private Throwable retirementFailure;
        private DeploymentReceiptIoScope(WindowsWorldProvisioner issuer,DeploymentReceiptWorld fresh,DeploymentReceiptInspection inspection,
                WindowsB1SourceAnchorStore anchors,WindowsB1SourceAnchorStore.ControlledAnchorLocation location,WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor) {
            this.issuer=issuer;this.fresh=fresh;this.inspection=inspection;this.anchors=anchors;this.location=location;this.anchor=anchor;
        }
        public void validateLive() throws IOException {
            if(retirementAttempted || retired)throw new IOException("schema5 original source native retirement already attempted",retirementFailure);
            if(fresh!=null)issuer.validateLive(fresh.custody);
            else {
                issuer.validateDeploymentInspection(inspection);
                issuer.requireDeploymentPreviousExit(inspection,inspection.exit);
            }
            var actual=anchors.rebarrierExisting(location,anchor.verifiedInput());
            requireBytes(hashBytes(anchor.anchorSha256()),hashBytes(actual.anchorSha256()),"schema5 original anchor changed");
            requireBytes(creation().bytes,actual.verifiedInput().originalReceiptCanonicalBytes(),"schema5 source receipt differs from native scope");
        }
        void bindAllocationRoot(MySqlFormat3ReadBudget root)throws IOException {
            validateLive();Objects.requireNonNull(root);
            if(root.isClosed() || allocationRoot!=null && allocationRoot!=root)throw new IOException("same original schema5 native allocation root required");
            allocationRoot=root;
        }
        WindowsMarkerIo io() throws IOException {validateLive();return issuer.io;}
        WindowsMarkerIo retirementIo(MySqlDeploymentReceiptSession.NativeRetirementPermit5 permit)throws IOException {
            Objects.requireNonNull(permit).requireNativeIdentity(this,issuer.io);
            if(retirementAttempted || retired || retirementFailure!=null)throw new IOException("schema5 source retirement is one-shot",retirementFailure);
            return issuer.io;
        }
        Path directory() throws IOException {validateLive();return fresh!=null?fresh.custody.directory:inspection.pins.worldPath();}
        String nativePath() throws IOException {validateLive();return fresh!=null?fresh.custody.nativePath:inspection.nativePath;}
        UUID operation(){return creation().plan().bootstrapOperationUuid();}
        DeploymentReceiptCreationProof creation(){return fresh!=null?fresh.creation:inspection.creation;}
        public byte[] originalReceipt(){return creation().receiptCanonicalBytes();}
        public int originalReceiptLength(){return creation().bytes.length;}
        public String sourceWorldIdentity(){return creation().receipt.sourceIdentity();}
        public String anchorSha256(){return anchor.anchorSha256();}
        public String namespace() throws IOException {validateLive();return anchor.verifiedInput().barrierNamespace();}
        public B1ResumeRecord.NativeOwnerAssertions currentOwner() throws IOException {
            validateLive();
            if(inspection!=null)return inspection.targetOwner();
            var p=fresh.custody;
            return new B1ResumeRecord.NativeOwnerAssertions(p.process.pid(),p.process.creationFileTime(),p.boot.bootId(),p.boot.generation());
        }
        public B1ResumeRecord.NativeOwnerAssertions previousOwner() throws IOException {
            validateLive();
            if(inspection==null)throw new IOException("original schema5 Ordinary inspection required");
            return inspection.previousOwner();
        }
        public B1ResumeRecord.RetirementAssertions previousOwnerRetirement() throws IOException {
            validateLive();
            if(inspection==null)throw new IOException("original schema5 previous-owner exit required");
            issuer.requireDeploymentPreviousExit(inspection,inspection.exit);
            return new B1ResumeRecord.RetirementAssertions("WIN32_PREVIOUS_PROCESS_EXIT_AND_SAME_NAMESPACE_SOURCE_BARRIER_V1",inspection.ownerBytes);
        }
        DeploymentReceiptExecutorRetirement verifyRelatedExecutorRetired(B1ResumeRecord.NativeOwnerAssertions executor) throws IOException {
            validateLive();Objects.requireNonNull(executor);
            if(inspection==null)throw new IOException("related executor retirement requires original cold source inspection");
            if(executor.equals(currentOwner()))throw new IOException("current target executor cannot certify its own retirement");
            for(var existing:executorRetirements)if(existing.executor.equals(executor)){
                existing.requireClaimed(this);return existing;
            }
            if(allocationRoot==null || allocationRoot.isClosed())throw new IOException("registered native allocation root required before related executor observation");
            allocationRoot.reserveRetainedMemory(512L,"schema5 related executor observation holder BEFORE native attempt");
            executorRetirementFee=Math.addExact(executorRetirementFee,512L);
            var pending=new DeploymentReceiptExecutorRetirement(this,executor);
            executorRetirements.add(pending); // 在实际进程观察之前保留失败/未完成槽。
            try {
                pending.observation=issuer.io.observePreviousProcessLifetime(executor.pid(),executor.fileTime(),operation());
                validateLive();
                issuer.io.claimPreviousProcessLifetimeObservation(pending.observation,executor.pid(),executor.fileTime(),operation());
                pending.claimed=true;pending.requireClaimed(this);return pending;
            }catch(Throwable cause){pending.failure=cause;throw propagate("schema5 related executor retirement uncertain",cause);}
        }
    }
    public void closeDeploymentReceiptCustody(DeploymentReceiptIoScope scope,DeploymentReceiptWorld world,
            MySqlDeploymentReceiptSession.NativeRetirementPermit5 permit)throws IOException {
        Objects.requireNonNull(scope);Objects.requireNonNull(world);
        var actualIo=scope.retirementIo(permit);
        if(scope.issuer!=this || actualIo!=io ||
                (scope.fresh!=null ? scope.fresh!=world : scope.inspection==null || scope.inspection.installed!=world))
            throw new IOException("exact original installed schema5 custody/source required");
        scope.validateLive();
        var proof=world.custody;
        if(proof.provisioner!=this)throw new IOException("original schema5 custody provisioner differs");
        scope.retirementAttempted=true;
        Throwable primary=null;
        try {
            for(var marker:scope.markerInspections){
                if(marker.canonical!=null){
                    try{io.closeChecked(marker.canonical);marker.canonical=null;}
                    catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
                }
                if(marker.parent!=null){
                    try{io.closeChecked(marker.parent);marker.parent=null;}
                    catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
                }
            }
            for(var handle:new WindowsMarkerIo.Handle[]{proof.guard,proof.source}){
                try{io.closeChecked(handle);}catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
            }
            if(primary!=null)throw propagate("schema5 custody actual native close",primary);
            synchronized(DEPLOYMENT_RECEIPT_PROCESS_CUSTODIES){
                if(!DEPLOYMENT_RECEIPT_PROCESS_CUSTODIES.remove(proof))throw new IOException("original schema5 custody lifetime registration missing");
            }
            if(scope.inspection!=null)synchronized(DEPLOYMENT_RECEIPT_INSPECTIONS){
                if(!DEPLOYMENT_RECEIPT_INSPECTIONS.remove(scope.inspection))throw new IOException("original schema5 inspection lifetime registration missing");
            }
            scope.markerInspections.clear();scope.executorRetirements.clear();
            if(scope.executorRetirementFee!=0L){
                scope.allocationRoot.releaseRetainedMemory(scope.executorRetirementFee,"schema5 actual related executor observation graph retired");
                scope.executorRetirementFee=0L;
            }
            scope.retired=true;
        }catch(Throwable cause){scope.retirementFailure=cause;throw propagate("schema5 native custody retirement incomplete",cause);}
    }
    private static final class DeploymentReceiptExecutorRetirement {
        private final DeploymentReceiptIoScope source;
        private final B1ResumeRecord.NativeOwnerAssertions executor;
        private WindowsMarkerIo.PreviousProcessLifetimeObservation observation;
        private boolean claimed;
        private Throwable failure;
        private DeploymentReceiptExecutorRetirement(DeploymentReceiptIoScope source,B1ResumeRecord.NativeOwnerAssertions executor){this.source=source;this.executor=executor;}
        private void requireClaimed(DeploymentReceiptIoScope actual)throws IOException {
            if(actual!=source || !claimed || failure!=null || observation==null)
                throw new IOException("exact original related executor exit observation required",failure);
            source.issuer.io.requireClaimedPreviousProcessLifetimeObservation(observation,executor.pid(),executor.fileTime(),source.operation());
        }
    }
    public DeploymentReceiptIoScope bindDeploymentReceiptSource(DeploymentReceiptWorld world,WindowsB1SourceAnchorStore anchors,
            WindowsB1SourceAnchorStore.ControlledAnchorLocation location,WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor) throws IOException {
        Objects.requireNonNull(world).custody.validateLive();
        if(world.custody.provisioner!=this)throw new IOException("schema5 native scope requires original provisioner");
        var scope=new DeploymentReceiptIoScope(this,world,null,Objects.requireNonNull(anchors),Objects.requireNonNull(location),Objects.requireNonNull(anchor));
        scope.validateLive();return scope;
    }
    public DeploymentReceiptInspection inspectOriginalSource(ProductionBootstrapControl.OriginalSourcePins pins,BootIdentity currentBoot) throws IOException {
        Objects.requireNonNull(pins);Objects.requireNonNull(currentBoot);
        if(pins.plan().schemaFormat()!=5||pins.plan().physicalFormat()!=2||currentBoot.bootId().equals(pins.creationBootUuid()))
            throw new IOException("schema5 original source and independent current boot required");
        var inspection=new DeploymentReceiptInspection(this,pins,currentBoot);
        synchronized(DEPLOYMENT_RECEIPT_INSPECTIONS){DEPLOYMENT_RECEIPT_INSPECTIONS.add(inspection);}
        try {
            var op=pins.plan().bootstrapOperationUuid();
            inspection.source=io.directory(pins.worldPath(),op);
            inspection.nativePath=io.finalPath(inspection.source);io.metadata(inspection.source,true);
            inspection.guard=io.open(pins.worldPath().resolve(GUARD_NAME),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,WindowsMarkerIo.OPEN_EXISTING,
                WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,op);
            io.metadata(inspection.guard,false,OWNER_SIZE);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(inspection.nativePath,io.finalPath(inspection.guard));
            var receipt=readReceipt(pins.worldPath(),inspection.nativePath,null,op);
            var data=WindowsWorldProvisionReceiptDataCodec.decode(receipt.bytes);
            if(!data.plan().equals(pins.plan())||!data.creationBoot().equals(new BootIdentity(pins.creationBootUuid(),pins.creationBootGeneration()))||
                    receipt.bytes.length!=pins.canonicalReceiptLength()||!sha256(receipt.bytes).equalsIgnoreCase(pins.canonicalReceiptSha256()))
                throw new IOException("schema5 complete original receipt/plan/creation boot differs from static pins");
            var sourceId=io.identity(inspection.source);var guardId=io.identity(inspection.guard);
            if(!sourceId.versionedIdentity().equals(pins.sourceWorldIdentity())||!guardId.versionedIdentity().equals(pins.guardFileIdentity())||
                    !receipt.fileIdentity.versionedIdentity().equals(pins.receiptFileIdentity())||!data.sourceIdentity().equals(pins.sourceWorldIdentity())||
                    !data.guardIdentity().equals(pins.guardFileIdentity()))throw new IOException("schema5 actual source/guard/receipt FileIds differ");
            inspection.creation=new DeploymentReceiptCreationProof(data,sourceId,guardId,receipt);
            inspection.ownerBytes=io.readPayloadExact(inspection.guard,OWNER_SIZE);
            inspection.previous=decodeDeploymentReceiptOwner(inspection.ownerBytes,inspection.creation);
            inspection.targetProcess=io.captureCurrentProcessIdentity(op);
            if(inspection.previous.pid==inspection.targetProcess.pid()&&inspection.previous.creationTime==inspection.targetProcess.creationFileTime())
                throw new IOException("schema5 same-process owner restart is forbidden");
            validateDeploymentInspection(inspection);return inspection;
        } catch(Throwable failure) {
            inspection.failure=failure;
            throw propagate("schema5 original source inspection retained; no install",failure);
        }
    }
    private void validateDeploymentInspection(DeploymentReceiptInspection inspection) throws IOException {
        if(inspection==null||inspection.issuer!=this||inspection.thread!=Thread.currentThread()||inspection.failure!=null||
                inspection.source==null||inspection.guard==null||inspection.creation==null||inspection.source.closeAttempted||inspection.guard.closeAttempted)
            throw new IOException("exact live schema5 original inspection required",inspection==null?null:inspection.failure);
        if(inspection.installed!=null){validateLive(inspection.installed.custody);return;}
        io.metadata(inspection.source,true);requireIdentity(inspection.creation.sourceIdentity,io.identity(inspection.source));
        if(!inspection.nativePath.equals(io.finalPath(inspection.source)))throw new IOException("schema5 inspected source moved");
        try(var reopened=io.directory(inspection.pins.worldPath(),inspection.creation.plan().bootstrapOperationUuid())){requireIdentity(inspection.creation.sourceIdentity,io.identity(reopened));}
        io.metadata(inspection.guard,false,OWNER_SIZE);requireIdentity(inspection.creation.guardIdentity,io.identity(inspection.guard));
        WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(inspection.nativePath,io.finalPath(inspection.guard));
        requireBytes(inspection.ownerBytes,io.readPayloadExact(inspection.guard,OWNER_SIZE),"schema5 inspected owner changed before install");
        var receipt=readReceipt(inspection.pins.worldPath(),inspection.nativePath,inspection.creation.receiptIdentity,inspection.creation.plan().bootstrapOperationUuid());
        requireBytes(inspection.creation.bytes,receipt.bytes,"schema5 inspected original receipt changed");
        var current=io.captureCurrentProcessIdentity(inspection.creation.plan().bootstrapOperationUuid());
        if(current.pid()!=inspection.targetProcess.pid()||current.creationFileTime()!=inspection.targetProcess.creationFileTime())throw new IOException("schema5 target process changed");
    }
    public DeploymentReceiptPreviousOwnerExit verifyPreviousOwnerExit(DeploymentReceiptInspection inspection) throws IOException {
        validateDeploymentInspection(inspection);
        if(inspection.exit!=null)throw new IOException("schema5 previous owner proof is single-use");
        var previous=inspection.previous;var op=inspection.creation.plan().bootstrapOperationUuid();
        var observed=io.observePreviousProcessLifetime(previous.pid,previous.creationTime,op);
        validateDeploymentInspection(inspection);
        io.claimPreviousProcessLifetimeObservation(observed,previous.pid,previous.creationTime,op);
        inspection.exit=new DeploymentReceiptPreviousOwnerExit(inspection,observed);return inspection.exit;
    }
    private void requireDeploymentPreviousExit(DeploymentReceiptInspection inspection,DeploymentReceiptPreviousOwnerExit proof) throws IOException {
        if(proof==null||proof!=inspection.exit||proof.inspection!=inspection)throw new IOException("actual schema5 previous owner exit proof required");
        io.requireClaimedPreviousProcessLifetimeObservation(proof.observation,inspection.previous.pid,inspection.previous.creationTime,inspection.creation.plan().bootstrapOperationUuid());
    }
    public DeploymentReceiptIoScope bindDeploymentReceiptInspection(DeploymentReceiptInspection inspection,DeploymentReceiptPreviousOwnerExit exit,
            WindowsB1SourceAnchorStore anchors,WindowsB1SourceAnchorStore.ControlledAnchorLocation location,WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor) throws IOException {
        validateDeploymentInspection(inspection);requireDeploymentPreviousExit(inspection,exit);
        if(!anchor.fileIdentity().equals(inspection.pins.anchorFileIdentity())||!anchor.anchorSha256().equalsIgnoreCase(inspection.pins.anchorSha256()))
            throw new IOException("schema5 original native anchor differs from pinned original source");
        var scope=new DeploymentReceiptIoScope(this,null,inspection,anchors,location,anchor);scope.validateLive();return scope;
    }
    public DeploymentReceiptWorld installCurrentOwner(DeploymentReceiptInspection inspection,DeploymentReceiptIoScope scope,
            WindowsB1V6WitnessRecordStore.DurableV6Ack handoff) throws IOException {
        validateDeploymentInspection(inspection);requireDeploymentPreviousExit(inspection,inspection.exit);
        if(scope==null||scope.inspection!=inspection||inspection.installAttempted)throw new IOException("exact uninstalled schema5 source required");
        Objects.requireNonNull(handoff).requireForInstallation(scope);
        return installValidatedDeploymentOwner(inspection,scope);
    }
    public DeploymentReceiptWorld installCurrentOwner(DeploymentReceiptInspection inspection,DeploymentReceiptIoScope scope,
            MySqlDeploymentReceiptBootstrapActor.SuccessorPrepareInstallPermit5 successor)throws IOException {
        validateDeploymentInspection(inspection);requireDeploymentPreviousExit(inspection,inspection.exit);
        if(scope==null||scope.inspection!=inspection||inspection.installAttempted)throw new IOException("exact uninstalled schema5 source required");
        Objects.requireNonNull(successor).requireForInstallation(scope);
        return installValidatedDeploymentOwner(inspection,scope);
    }
    private DeploymentReceiptWorld installValidatedDeploymentOwner(DeploymentReceiptInspection inspection,DeploymentReceiptIoScope scope)throws IOException {
        inspection.installAttempted=true;
        try {
            byte[] bytes=encodeDeploymentReceiptOwner(inspection.creation,inspection.targetBoot,inspection.targetProcess);
            persistOwner(inspection.guard,bytes);
            var custody=new DeploymentReceiptCustodyProof(this,inspection.pins.worldPath(),inspection.source,inspection.guard,inspection.creation,
                inspection.targetBoot,inspection.targetProcess,bytes,inspection.nativePath);
            inspection.installed=new DeploymentReceiptWorld(inspection.creation,custody);
            synchronized(DEPLOYMENT_RECEIPT_PROCESS_CUSTODIES){DEPLOYMENT_RECEIPT_PROCESS_CUSTODIES.add(custody);}
            validateLive(custody);return inspection.installed;
        } catch(Throwable failure){inspection.failure=failure;throw propagate("schema5 owner installation uncertain; no retry",failure);}
    }
    public static final class DeploymentCanonicalMarkerLocation {
        private final DeploymentReceiptIoScope scope;
        private final Path path;
        private final WindowsMarkerIo.FileIdentity parent;
        private DeploymentCanonicalMarkerLocation(DeploymentReceiptIoScope scope,Path path,WindowsMarkerIo.FileIdentity parent){this.scope=scope;this.path=path;this.parent=parent;}
        public WindowsCanonicalMarkerPublicationBarrier.Request barrierRequest() throws IOException {
            scope.validateLive();
            try(var handle=scope.io().directory(path.getParent(),scope.operation())){requireIdentity(parent,scope.io().identity(handle));
                WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(scope.nativePath(),scope.io().finalPath(handle));}
            var p=scope.creation().plan();return new WindowsCanonicalMarkerPublicationBarrier.Request(path,p.bindingUuid(),p.physicalFormat(),p.bootstrapOperationUuid(),parent);
        }
    }
    /** 原 marker 的只读身份核对加真实 writable flush；不创建或替换缺失文件。 */
    public static final class DeploymentReceiptMarkerInspection {
        private final DeploymentReceiptIoScope scope;
        private final B1ResumeRecord.MarkerAssertions original;
        private WindowsMarkerIo.Handle parent,canonical;
        private Throwable failure;
        private boolean complete;
        private DeploymentReceiptMarkerInspection(DeploymentReceiptIoScope scope,B1ResumeRecord.MarkerAssertions original){this.scope=scope;this.original=original;}
        public void requireCurrent() throws IOException {
            scope.validateLive();
            if(!complete||failure!=null||parent!=null||canonical!=null)throw new IOException("original marker rebarrier incomplete",failure);
        }
    }
    public DeploymentReceiptMarkerInspection rebarrierDeploymentReceiptMarker(DeploymentReceiptIoScope scope,
            B1ResumeRecord.MarkerAssertions original) throws IOException {
        Objects.requireNonNull(scope).validateLive();Objects.requireNonNull(original);
        if(scope.issuer!=this)throw new IOException("original schema5 native source required");
        var attempt=new DeploymentReceiptMarkerInspection(scope,original);
        scope.markerInspections.add(attempt);
        try {
            var p=scope.creation().plan();
            requireBytes(WindowsMarkerIo.markerBytes(p.bindingUuid(),p.physicalFormat()),original.fullMarkerBytes(),"original schema5 marker token differs");
            Path data=scope.directory().resolve("data");
            attempt.parent=io.directory(data,scope.operation());io.metadata(attempt.parent,true);
            if(!io.identity(attempt.parent).versionedIdentity().equals(original.parentIdentity()))throw new IOException("original marker parent FileId changed");
            String nativeData=io.finalPath(attempt.parent);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(scope.nativePath(),nativeData);
            attempt.canonical=io.open(data.resolve("floatingpod-world-binding.bin"),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,scope.operation());
            io.metadata(attempt.canonical,false);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(nativeData,io.finalPath(attempt.canonical));
            if(!io.identity(attempt.canonical).versionedIdentity().equals(original.fileIdentity()))throw new IOException("original marker FileId changed");
            requireBytes(original.fullMarkerBytes(),io.readBackExact(attempt.canonical),"original marker bytes changed");
            io.flushChecked(attempt.canonical);
            requireBytes(original.fullMarkerBytes(),io.readBackExact(attempt.canonical),"original marker post-flush bytes changed");
            io.closeChecked(attempt.canonical);attempt.canonical=null;
            if(!io.identity(attempt.parent).versionedIdentity().equals(original.parentIdentity()))throw new IOException("original marker parent changed during rebarrier");
            io.closeChecked(attempt.parent);attempt.parent=null;
            scope.validateLive();attempt.complete=true;return attempt;
        } catch(Throwable failure){attempt.failure=failure;throw propagate("schema5 original marker rebarrier failed; partial handles retained",failure);}
    }
    public DeploymentCanonicalMarkerLocation ensureDeploymentCanonicalMarkerLocation(DeploymentReceiptIoScope scope) throws IOException {
        Objects.requireNonNull(scope).validateLive();
        if(scope.issuer!=this||(scope.inspection!=null&&scope.inspection.installed==null))throw new IOException("actual schema5 current custody required for marker");
        Path data=scope.directory().resolve("data");
        try{io.createDirectoryNew(data,scope.operation());}
        catch(WindowsMarkerIo.NativeFailure existing){if(existing.win32Error()!=183)throw existing;}
        WindowsMarkerIo.FileIdentity identity;
        try(var handle=io.directory(data,scope.operation())){io.metadata(handle,true);identity=io.identity(handle);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(scope.nativePath(),io.finalPath(handle));}
        scope.validateLive();return new DeploymentCanonicalMarkerLocation(scope,data.resolve("floatingpod-world-binding.bin"),identity);
    }
    private static PreviousOwner decodeDeploymentReceiptOwner(byte[] bytes,DeploymentReceiptCreationProof proof) throws IOException {
        if(bytes.length!=OWNER_SIZE||!MessageDigest.isEqual(digest(Arrays.copyOf(bytes,OWNER_SIZE-32)),Arrays.copyOfRange(bytes,OWNER_SIZE-32,OWNER_SIZE)))
            throw new IOException("invalid schema5 owner body/digest");
        try {
            ByteBuffer in=ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            if(in.getLong()!=DEPLOYMENT_RECEIPT_OWNER_MAGIC||in.getInt()!=5)throw new IOException("schema5 owner version required");
            requireIdentity(proof.sourceIdentity,getIdentity(in));requireIdentity(proof.guardIdentity,getIdentity(in));
            byte[] hash=new byte[32];in.get(hash);requireBytes(digest(proof.bytes),hash,"schema5 owner receipt differs");
            long pid=in.getLong(),time=in.getLong();WindowsMarkerIo.validateProcessIdentity(pid,time);
            return new PreviousOwner(pid,time,new BootIdentity(new UUID(in.getLong(),in.getLong()),in.getLong()));
        } catch(IllegalArgumentException failure){throw new IOException("invalid schema5 owner identity",failure);}
    }
    /** Approved original receipt identity plus the actual source path; this is DATA, never Fresh authority. */
    public record ExpectedOriginalCreation(Plan originalPlan, UUID originalBootUuid, long originalBootGeneration,
            Path expectedWorldPath, int canonicalReceiptLength, String canonicalReceiptSha256,
            String sourceWorldIdentity, String guardFileIdentity, String receiptFileIdentity,
            String anchorFileIdentity, Path anchorPath, String anchorSha256) {
        public ExpectedOriginalCreation {
            Objects.requireNonNull(originalPlan); Objects.requireNonNull(originalBootUuid);
            expectedWorldPath=Objects.requireNonNull(expectedWorldPath).toAbsolutePath().normalize();
            anchorPath=Objects.requireNonNull(anchorPath).toAbsolutePath().normalize();
            if(originalBootGeneration<=0||canonicalReceiptLength<=0||canonicalReceiptLength>65536||
                expectedWorldPath.getParent()==null||anchorPath.getParent()==null)
                throw new IllegalArgumentException("Bounded original creation receipt and explicit paths required");
            hashBytes(canonicalReceiptSha256); hashBytes(anchorSha256);
            requirePinnedIdentity(sourceWorldIdentity,"source world");
            requirePinnedIdentity(guardFileIdentity,"source guard");
            requirePinnedIdentity(receiptFileIdentity,"source receipt");
            requirePinnedIdentity(anchorFileIdentity,"source anchor");
        }
    }
    private static void requirePinnedIdentity(String value,String role) {
        if(value==null||!value.matches("FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}"))
            throw new IllegalArgumentException("Exact pinned native FileId required for "+role);
    }
    public static final class FreshProvisioningProof {
        private final ReceiptData receipt;
        private final WindowsMarkerIo.FileIdentity directoryIdentity, guardIdentity, receiptFileIdentity;
        private final byte[] canonicalBytes;
        private final String receiptSha256;
        private FreshProvisioningProof(ReceiptData receipt, WindowsMarkerIo.FileIdentity directoryIdentity,
                                      WindowsMarkerIo.FileIdentity guardIdentity, NativeReceiptAck ack) {
            this.receipt = receipt; this.directoryIdentity = directoryIdentity; this.guardIdentity = guardIdentity;
            this.receiptFileIdentity = ack.fileIdentity; this.canonicalBytes = ack.bytes.clone();
            this.receiptSha256 = sha256(ack.bytes);
        }
        public Plan plan() { return receipt.plan; }
        public String sourceWorldIdentity() { return directoryIdentity.versionedIdentity(); }
        public String guardFileIdentity() { return guardIdentity.versionedIdentity(); }
        public String receiptFileIdentity() { return receiptFileIdentity.versionedIdentity(); }
        public String receiptSha256() { return receiptSha256; }
        public byte[] receiptCanonicalBytes() { return canonicalBytes.clone(); }
        public long creationProcessPid() { return receipt.creatorPid; }
        public long creationProcessFileTime() { return receipt.creatorCreationTime; }
        public Instant creationProcessStartInstant() { return WindowsMarkerIo.fileTimeInstant(receipt.creatorCreationTime); }
        public BootIdentity creationBoot() { return receipt.creationBoot; }
        public String receiptEncoding() { return "FP_WORLD_PROVISION_RECEIPT_BINARY_BE_V1"; }
    }
    public static final class BootWorldCustodyProof {
        private final WindowsWorldProvisioner provisioner;
        private final Path directory;
        private final WindowsMarkerIo.Handle directoryHandle, guardHandle;
        private final FreshProvisioningProof creationProof;
        private final BootIdentity boot;
        private final WindowsMarkerIo.ProcessIdentity process;
        private final byte[] ownerBytes;
        private final String actualDirectoryPath;
        private boolean completionCloseAttempted;
        private Throwable completionCloseFailure;
        private BootWorldCustodyProof(WindowsWorldProvisioner provisioner, Path directory,
                                     WindowsMarkerIo.Handle directoryHandle, WindowsMarkerIo.Handle guardHandle,
                                     FreshProvisioningProof creationProof, BootIdentity boot,
                                     WindowsMarkerIo.ProcessIdentity process, byte[] ownerBytes, String actualDirectoryPath) {
            this.provisioner = provisioner; this.directory = directory; this.directoryHandle = directoryHandle;
            this.guardHandle = guardHandle; this.creationProof = creationProof; this.boot = boot; this.process = process;
            this.ownerBytes = ownerBytes.clone(); this.actualDirectoryPath = actualDirectoryPath;
        }
        public UUID bootId() { return boot.bootId(); }
        public long generation() { return boot.generation(); }
        public FreshProvisioningProof freshProvisioningProof() { return creationProof; }
        public String sourceWorldIdentity() { return creationProof.sourceWorldIdentity(); }
        public long ownerPid() { return process.pid(); }
        public Instant ownerStartInstant() { return process.startInstant(); }
        public Path directory() { return directory; }
        public String lifetime() { return "PROCESS_LIFETIME_NO_EARLY_RELEASE_WITHOUT_OPAQUE_OWNED_IO_EXIT_V1"; }
        /** 原 share0 handle/actualFS/完整 owner bytes 都须活核；历史 proof 单独不授当前 custody。 */
        public void validateLive() throws IOException { provisioner.validateLive(this); }
    }
    /** 固定production marker位置的实际data-parent身份；不是任意caller路径或新Fresh/Ready权限。 */
    public static final class CanonicalMarkerLocation {
        private final WindowsWorldProvisioner provisioner;
        private final BootWorldCustodyProof custody;
        private final Path canonicalPath;
        private final WindowsMarkerIo.FileIdentity dataIdentity;
        private CanonicalMarkerLocation(WindowsWorldProvisioner provisioner, BootWorldCustodyProof custody,
                                        Path canonicalPath, WindowsMarkerIo.FileIdentity dataIdentity) {
            this.provisioner = provisioner; this.custody = custody;
            this.canonicalPath = canonicalPath; this.dataIdentity = dataIdentity;
        }
        public Path canonicalPath() { return canonicalPath; }
        public WindowsMarkerIo.FileIdentity dataDirectoryIdentity() { return dataIdentity; }
        public UUID bootId() { return custody.bootId(); }
        public long generation() { return custody.generation(); }
        public Plan plan() { return custody.freshProvisioningProof().plan(); }
        public String sourceWorldIdentity() { return custody.sourceWorldIdentity(); }
        public FreshProvisioningProof freshProvisioningProof() { return custody.freshProvisioningProof(); }
        public BootWorldCustodyProof bootWorldCustodyProof() { return custody; }
        /** publication/final消费前核current custody与实际data目录仍是这个位置和FDI。 */
        public void validateLive() throws IOException { provisioner.validateMarkerLocation(this); }
        public WindowsCanonicalMarkerPublicationBarrier.Request barrierRequest() throws IOException {
            validateLive();
            Plan plan = plan();
            return new WindowsCanonicalMarkerPublicationBarrier.Request(canonicalPath, plan.bindingUuid(),
                plan.physicalFormat(), plan.bootstrapOperationUuid(), dataIdentity);
        }
    }

    /** 只创建固定source/data最终component；不递归建目录、不创建或认证marker。 */
    public CanonicalMarkerLocation ensureCanonicalMarkerLocation(BootWorldCustodyProof custody) throws IOException {
        Objects.requireNonNull(custody);
        requireOwnCustody(custody);
        custody.validateLive();
        Path data = custody.directory.resolve("data");
        UUID operation = custody.creationProof.plan().bootstrapOperationUuid();
        try {
            try { io.createDirectoryNew(data, operation); }
            catch (WindowsMarkerIo.NativeFailure existing) {
                if (existing.win32Error() != 183) throw existing; // 只有ALREADY_EXISTS可转实物核验。
            }
            WindowsMarkerIo.FileIdentity identity;
            try (var handle = io.directory(data, operation)) {
                WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(custody.actualDirectoryPath, io.finalPath(handle));
                identity = io.identity(handle);
                io.metadata(handle, true);
                requireIdentity(custody.creationProof.directoryIdentity, io.identity(custody.directoryHandle));
                if (!custody.actualDirectoryPath.equals(io.finalPath(custody.directoryHandle)))
                    throw new IOException("Source directory changed while preparing fixed marker parent");
                requireIdentity(identity, io.identity(handle));
            } // checked CloseHandle false prevents returning thislocation。
            custody.validateLive();
            return new CanonicalMarkerLocation(this, custody, data.resolve("floatingpod-world-binding.bin"), identity);
        } catch (Throwable primary) {
            throw propagate("ensure canonical marker location operation=" + operation, primary);
        }
    }
    private void requireOwnCustody(BootWorldCustodyProof custody) throws IOException {
        if (custody.provisioner != this) throw new IOException("Canonical marker location requires same-factory opaque custody");
    }
    private void validateMarkerLocation(CanonicalMarkerLocation location) throws IOException {
        if (location.provisioner != this) throw new IOException("Canonical marker location belongs to another factory");
        requireOwnCustody(location.custody);
        location.custody.validateLive();
        Path data = location.custody.directory.resolve("data");
        if (!location.canonicalPath.equals(data.resolve("floatingpod-world-binding.bin")))
            throw new IOException("Canonical marker location is not the fixed production path");
        try (var handle = io.directory(data, location.plan().bootstrapOperationUuid())) {
            requireIdentity(location.dataIdentity, io.identity(handle));
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(location.custody.actualDirectoryPath, io.finalPath(handle));
            requireIdentity(location.custody.creationProof.directoryIdentity, io.identity(location.custody.directoryHandle));
            if (!location.custody.actualDirectoryPath.equals(io.finalPath(location.custody.directoryHandle)))
                throw new IOException("Canonical marker source path changed");
        }
        location.custody.validateLive();
    }

    public static final class ProvisionedWorld {
        private final FreshProvisioningProof creation;
        private final BootWorldCustodyProof custody;
        private final boolean createdThisBoot;
        private ProvisionedWorld(FreshProvisioningProof creation, BootWorldCustodyProof custody, boolean createdThisBoot) {
            this.creation = creation; this.custody = custody; this.createdThisBoot = createdThisBoot;
        }
        public FreshProvisioningProof freshProvisioningProof() { return creation; }
        public BootWorldCustodyProof bootWorldCustodyProof() { return custody; }
        public boolean createdThisBoot() { return createdThisBoot; }
    }
    private record NativeReceiptAck(WindowsMarkerIo.FileIdentity fileIdentity, byte[] bytes) {}
    static record ReceiptData(Plan plan, String sourceIdentity, String guardIdentity,
                              long creatorPid, long creatorCreationTime, BootIdentity creationBoot) {}
    private record PreviousOwner(long pid, long creationTime, BootIdentity boot) {}
    private static final List<ExactObservedPreviousProcessExit> PROCESS_LIFETIME_EXIT_PROOFS = new ArrayList<>();

    enum PreviousOwnerEvidenceKind {
        RETAINED_PROCESS_EXIT_ACKNOWLEDGEMENT,
        EXACT_OPEN_PROCESS_HANDLE_EXIT,
        PID_REUSED_NOT_PROCESS_EXIT_ACKNOWLEDGEMENT,
        ABSENT_FROM_COMPLETE_PROCESS_SNAPSHOT_NOT_PROCESS_EXIT_ACKNOWLEDGEMENT
    }
    private abstract static class PreviousOwnerLifetimeProof {
        abstract void requireFor(ResumeInspection inspection) throws IOException;
        abstract PreviousOwnerEvidenceKind kind();
    }

    /** 只有匹配真实持久化 owner 后，OS 退出 ACK 才能成为世界范围的证明。 */
    private static final class ExactObservedPreviousProcessExit extends PreviousOwnerLifetimeProof {
        private final WindowsWorldProvisioner issuer;
        private final WindowsMarkerIo.RetainedProcessObservation observation;
        private final WindowsMarkerIo.ProcessExitAcknowledgement nativeAck;
        private final Plan plan;
        private final String receiptSha256;
        private final WindowsMarkerIo.FileIdentity sourceIdentity, guardIdentity, receiptIdentity;
        private final PreviousOwner previous;
        private final byte[] previousOwnerBytes;
        private final String sourcePath;
        private final BootIdentity targetBoot;
        private final WindowsMarkerIo.ProcessIdentity targetProcess;
        private boolean transferred, consumed;
        private ExactObservedPreviousProcessExit(WindowsWorldProvisioner issuer,
                WindowsMarkerIo.RetainedProcessObservation observation,
                WindowsMarkerIo.ProcessExitAcknowledgement nativeAck, Plan plan, String receiptSha256,
                WindowsMarkerIo.FileIdentity sourceIdentity, WindowsMarkerIo.FileIdentity guardIdentity,
                WindowsMarkerIo.FileIdentity receiptIdentity,
                PreviousOwner previous, byte[] previousOwnerBytes, String sourcePath,
                BootIdentity targetBoot, WindowsMarkerIo.ProcessIdentity targetProcess) {
            this.issuer = issuer; this.observation = observation; this.nativeAck = nativeAck; this.plan = plan;
            this.receiptSha256 = receiptSha256; this.sourceIdentity = sourceIdentity; this.guardIdentity = guardIdentity;
            this.receiptIdentity = receiptIdentity;
            this.previous = previous; this.previousOwnerBytes = previousOwnerBytes.clone(); this.sourcePath = sourcePath;
            this.targetBoot = targetBoot; this.targetProcess = targetProcess;
        }
        @Override void requireFor(ResumeInspection inspection) throws IOException {
            if (inspection == null || inspection.issuer != issuer || inspection.previousOwnerProof != this
                || !transferred || consumed || !plan.equals(inspection.creation.plan())
                || !receiptSha256.equals(sha256(inspection.creation.canonicalBytes))
                || !sourceIdentity.equals(inspection.creation.directoryIdentity)
                || !guardIdentity.equals(inspection.creation.guardIdentity)
                || !receiptIdentity.equals(inspection.creation.receiptFileIdentity)
                || !previous.equals(inspection.previous) || !Arrays.equals(previousOwnerBytes, inspection.originalOwner)
                || !sourcePath.equals(inspection.nativePath) || !targetBoot.equals(inspection.targetBoot)
                || targetProcess.pid() != inspection.targetProcess.pid()
                || targetProcess.creationFileTime() != inspection.targetProcess.creationFileTime())
                throw new IOException("Exact world-scoped retained-process exit proof is not bound to this inspection");
        }
        @Override PreviousOwnerEvidenceKind kind() {
            return PreviousOwnerEvidenceKind.RETAINED_PROCESS_EXIT_ACKNOWLEDGEMENT;
        }
    }

    /** 包装已由同一 factory 对持有中的 durable world 复核并 claim 的非 caller 构造 OS 观察。 */
    private static final class NativePreviousOwnerLifetimeProof extends PreviousOwnerLifetimeProof {
        private final WindowsWorldProvisioner issuer;
        private final WindowsMarkerIo.PreviousProcessLifetimeObservation observation;
        private final Plan plan;
        private final String receiptSha256;
        private final byte[] receiptBytes;
        private final WindowsMarkerIo.FileIdentity sourceIdentity, guardIdentity, receiptIdentity;
        private final PreviousOwner previous;
        private final byte[] previousOwnerBytes;
        private final String sourcePath;
        private final BootIdentity targetBoot;
        private final WindowsMarkerIo.ProcessIdentity targetProcess;

        private NativePreviousOwnerLifetimeProof(WindowsWorldProvisioner issuer,
                WindowsMarkerIo.PreviousProcessLifetimeObservation observation, Plan plan, byte[] receiptBytes,
                WindowsMarkerIo.FileIdentity sourceIdentity, WindowsMarkerIo.FileIdentity guardIdentity,
                WindowsMarkerIo.FileIdentity receiptIdentity, PreviousOwner previous, byte[] previousOwnerBytes,
                String sourcePath, BootIdentity targetBoot, WindowsMarkerIo.ProcessIdentity targetProcess) {
            this.issuer = issuer;
            this.observation = observation;
            this.plan = plan;
            this.receiptBytes = receiptBytes.clone();
            this.receiptSha256 = sha256(this.receiptBytes);
            this.sourceIdentity = sourceIdentity;
            this.guardIdentity = guardIdentity;
            this.receiptIdentity = receiptIdentity;
            this.previous = previous;
            this.previousOwnerBytes = previousOwnerBytes.clone();
            this.sourcePath = sourcePath;
            this.targetBoot = targetBoot;
            this.targetProcess = targetProcess;
        }

        @Override void requireFor(ResumeInspection inspection) throws IOException {
            if (inspection == null || inspection.issuer != issuer || inspection.previousOwnerProof != this
                || !plan.equals(inspection.creation.plan())
                || !receiptSha256.equals(sha256(inspection.creation.canonicalBytes))
                || !Arrays.equals(receiptBytes, inspection.creation.canonicalBytes)
                || !sourceIdentity.equals(inspection.creation.directoryIdentity)
                || !guardIdentity.equals(inspection.creation.guardIdentity)
                || !receiptIdentity.equals(inspection.creation.receiptFileIdentity)
                || !previous.equals(inspection.previous) || !Arrays.equals(previousOwnerBytes, inspection.originalOwner)
                || !sourcePath.equals(inspection.nativePath) || !targetBoot.equals(inspection.targetBoot)
                || targetProcess.pid() != inspection.targetProcess.pid()
                || targetProcess.creationFileTime() != inspection.targetProcess.creationFileTime()) {
                throw new IOException("Native previous-owner lifetime evidence is not bound to this exact inspection");
            }
            issuer.io.requireClaimedPreviousProcessLifetimeObservation(observation, previous.pid,
                previous.creationTime, plan.bootstrapOperationUuid());
        }

        @Override PreviousOwnerEvidenceKind kind() {
            return switch (observation.kind()) {
                case EXACT_HANDLE_PROCESS_EXIT -> PreviousOwnerEvidenceKind.EXACT_OPEN_PROCESS_HANDLE_EXIT;
                case PROCESS_ID_REUSED -> PreviousOwnerEvidenceKind.PID_REUSED_NOT_PROCESS_EXIT_ACKNOWLEDGEMENT;
                case OLD_LIFETIME_ABSENT_FROM_COMPLETE_SNAPSHOT ->
                    PreviousOwnerEvidenceKind.ABSENT_FROM_COMPLETE_PROCESS_SNAPSHOT_NOT_PROCESS_EXIT_ACKNOWLEDGEMENT;
            };
        }
    }

    /** 活进程 observation 明确保持 Pending；Ready 结果只能由本类私有构造。 */
    public sealed interface RetainedResumeAttempt permits ResumePending, ResumeInspectionReady, ResumeCustodyReady {}
    public static final class ResumePending implements RetainedResumeAttempt {
        private final WindowsMarkerIo.RetainedProcessObservation observation;
        private ResumePending(WindowsMarkerIo.RetainedProcessObservation observation) { this.observation = observation; }
        public WindowsMarkerIo.RetainedProcessObservation observation() { return observation; }
    }
    public static final class ResumeInspectionReady implements RetainedResumeAttempt {
        private final ResumeInspection inspection;
        private ResumeInspectionReady(ResumeInspection inspection) { this.inspection = inspection; }
        public ResumeInspection inspection() { return inspection; }
    }
    public static final class ResumeCustodyReady implements RetainedResumeAttempt {
        private final ProvisionedWorld world;
        private ResumeCustodyReady(ProvisionedWorld world) { this.world = world; }
        public ProvisionedWorld world() { return world; }
    }

    private ExactObservedPreviousProcessExit issuePreviousProcessExitProof(
            WindowsMarkerIo.RetainedProcessObservation observation,
            WindowsMarkerIo.ProcessExitAcknowledgement nativeAck, Plan plan, NativeReceiptAck receipt,
            WindowsMarkerIo.FileIdentity sourceIdentity, WindowsMarkerIo.FileIdentity guardIdentity,
            PreviousOwner previous, byte[] previousOwnerBytes, String sourcePath, BootIdentity targetBoot,
            WindowsMarkerIo.ProcessIdentity targetProcess) throws IOException {
        io.claimProcessExitAcknowledgement(nativeAck, observation, previous.pid, previous.creationTime,
            plan.bootstrapOperationUuid());
        var proof = new ExactObservedPreviousProcessExit(this, observation, nativeAck, plan, sha256(receipt.bytes),
            sourceIdentity, guardIdentity, receipt.fileIdentity, previous, previousOwnerBytes, sourcePath,
            targetBoot, targetProcess);
        synchronized (PROCESS_LIFETIME_EXIT_PROOFS) { PROCESS_LIFETIME_EXIT_PROOFS.add(proof); }
        return proof;
    }

    private NativePreviousOwnerLifetimeProof issueNativePreviousOwnerLifetimeProof(
            WindowsMarkerIo.PreviousProcessLifetimeObservation observation, Plan plan, NativeReceiptAck receipt,
            WindowsMarkerIo.FileIdentity sourceIdentity, WindowsMarkerIo.FileIdentity guardIdentity,
            PreviousOwner previous, byte[] previousOwnerBytes, String sourcePath, BootIdentity targetBoot,
            WindowsMarkerIo.ProcessIdentity targetProcess) throws IOException {
        io.claimPreviousProcessLifetimeObservation(observation, previous.pid, previous.creationTime,
            plan.bootstrapOperationUuid());
        return new NativePreviousOwnerLifetimeProof(this, observation, plan, receipt.bytes,
            sourceIdentity, guardIdentity, receipt.fileIdentity, previous, previousOwnerBytes, sourcePath,
            targetBoot, targetProcess);
    }

    private void requirePreviousOwnerWorldUnchanged(Path directory, WindowsMarkerIo.Handle source,
            WindowsMarkerIo.Handle guard, FreshProvisioningProof creation, String sourcePath, byte[] expectedOwnerBytes,
            WindowsMarkerIo.ProcessIdentity expectedTargetProcess, UUID operation) throws IOException {
        io.metadata(source, true);
        requireIdentity(creation.directoryIdentity, io.identity(source));
        if (!sourcePath.equals(io.finalPath(source))) throw new IOException("Source directory changed during previous-owner observation");

        io.metadata(guard, false, expectedOwnerBytes.length);
        requireIdentity(creation.guardIdentity, io.identity(guard));
        WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath, io.finalPath(guard));
        if (expectedOwnerBytes.length != 0)
            requireBytes(expectedOwnerBytes, io.readPayloadExact(guard, expectedOwnerBytes.length),
                "Previous owner bytes changed during native lifetime observation");

        NativeReceiptAck receipt = readReceipt(directory, sourcePath, creation.receiptFileIdentity, operation);
        requireBytes(creation.canonicalBytes, receipt.bytes,
            "Original provisioning receipt changed during native lifetime observation");
        WindowsMarkerIo.ProcessIdentity actualTarget = io.captureCurrentProcessIdentity(operation);
        if (actualTarget.pid() != expectedTargetProcess.pid()
            || actualTarget.creationFileTime() != expectedTargetProcess.creationFileTime())
            throw new IOException("Target process identity changed during previous-owner observation");
    }

    private static void transferExitProofToInspection(ExactObservedPreviousProcessExit proof,
                                                       ResumeInspection inspection) throws IOException {
        synchronized (PROCESS_LIFETIME_EXIT_PROOFS) {
            if (proof == null || proof.transferred || proof.consumed || !PROCESS_LIFETIME_EXIT_PROOFS.remove(proof))
                throw new IOException("Retained process exit proof transfer already attempted");
            proof.transferred = true;
        }
        proof.requireFor(inspection);
    }

    private static void consumeExitProofForCustody(ExactObservedPreviousProcessExit proof) throws IOException {
        synchronized (PROCESS_LIFETIME_EXIT_PROOFS) {
            if (proof == null || proof.transferred || proof.consumed || !PROCESS_LIFETIME_EXIT_PROOFS.remove(proof))
                throw new IOException("Retained process exit proof consumption already attempted");
            proof.consumed = true;
        }
    }

    private static void closePendingResumeHandles(WindowsMarkerIo.Handle guard, WindowsMarkerIo.Handle source) throws IOException {
        Throwable primary = null;
        for (WindowsMarkerIo.Handle handle : new WindowsMarkerIo.Handle[]{guard, source}) {
            if (handle == null || handle.closeAttempted) continue;
            try { handle.close(); }
            catch (Throwable failure) {
                if (primary == null) primary = failure; else if (failure != primary) primary.addSuppressed(failure);
            }
        }
        if (primary != null) throw propagate("retained process pending cleanup", primary);
    }

    public DeploymentReceiptWorld createFreshDeploymentReceipt(Path requestedDirectory,
            WindowsWorldProvisionReceiptDataCodec.PlanData plan,BootIdentity boot)throws IOException {
        Objects.requireNonNull(plan);Objects.requireNonNull(boot);
        if(plan.schemaFormat()!=5 || plan.physicalFormat()!=2)throw new IllegalArgumentException("Dedicated native5 Fresh requires schema5/physical2");
        Path directory=canonicalDirectory(requestedDirectory);
        WindowsMarkerIo.Handle parent=null,source=null,guard=null;
        try {
            parent=io.directory(directory.getParent(),plan.creationOperationUuid());
            String parentPath=io.finalPath(parent);var parentIdentity=io.identity(parent);
            io.createDirectoryNew(directory,plan.creationOperationUuid());
            source=io.directory(directory,plan.creationOperationUuid());String sourcePath=io.finalPath(source);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(parentPath,sourcePath);
            requireIdentity(parentIdentity,io.identity(parent));io.closeChecked(parent);parent=null;
            var sourceIdentity=io.identity(source);
            guard=io.open(directory.resolve(GUARD_NAME),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,WindowsMarkerIo.CREATE_NEW,
                WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,plan.creationOperationUuid());
            io.metadata(guard,false,0);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath,io.finalPath(guard));
            var guardIdentity=io.identity(guard);var process=io.captureCurrentProcessIdentity(plan.creationOperationUuid());
            var receipt=new WindowsWorldProvisionReceiptDataCodec.ReceiptData(plan,sourceIdentity.versionedIdentity(),guardIdentity.versionedIdentity(),
                process.pid(),process.creationFileTime(),boot);
            byte[] bytes=WindowsWorldProvisionReceiptDataCodec.encode(receipt);
            NativeReceiptAck ack=publishNewReceipt(directory,sourcePath,bytes,plan.creationOperationUuid(),ReceiptTemporaryPolicy.KEEP_SCHEMA5_UNCERTAINTY);
            var creation=new DeploymentReceiptCreationProof(receipt,sourceIdentity,guardIdentity,ack);
            byte[] ownerBytes=encodeDeploymentReceiptOwner(creation,boot,process);persistOwner(guard,ownerBytes);
            var custody=new DeploymentReceiptCustodyProof(this,directory,source,guard,creation,boot,process,ownerBytes,sourcePath);
            custody.validateLive();
            synchronized(DEPLOYMENT_RECEIPT_PROCESS_CUSTODIES){DEPLOYMENT_RECEIPT_PROCESS_CUSTODIES.add(custody);}
            source=null;guard=null;
            return new DeploymentReceiptWorld(creation,custody);
        } catch(Throwable primary) {
            WindowsMarkerIo.closeSuppressed(guard,primary);WindowsMarkerIo.closeSuppressed(source,primary);WindowsMarkerIo.closeSuppressed(parent,primary);
            throw propagate("createFreshDeploymentReceipt operation="+plan.creationOperationUuid()+"; created-resource outcome may be unknown; no retry",primary);
        }
    }
    public ProvisionedWorld createFresh(Path requestedDirectory, Plan plan, BootIdentity boot) throws IOException {
        Objects.requireNonNull(plan); Objects.requireNonNull(boot);
        Path directory = canonicalDirectory(requestedDirectory);
        WindowsMarkerIo.Handle parent = null, source = null, guard = null;
        try {
            parent = io.directory(directory.getParent(), plan.creationOperationUuid());
            String parentPath = io.finalPath(parent);
            WindowsMarkerIo.FileIdentity parentIdentity = io.identity(parent);
            io.createDirectoryNew(directory, plan.creationOperationUuid()); // existing directory false 永远不是 Fresh。
            source = io.directory(directory, plan.creationOperationUuid());
            String sourcePath = io.finalPath(source);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(parentPath, sourcePath);
            requireIdentity(parentIdentity, io.identity(parent));
            io.closeChecked(parent); parent = null;
            WindowsMarkerIo.FileIdentity sourceIdentity = io.identity(source);
            guard = io.open(directory.resolve(GUARD_NAME), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.CREATE_NEW, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, plan.creationOperationUuid());
            io.metadata(guard, false, 0);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath, io.finalPath(guard));
            WindowsMarkerIo.FileIdentity guardIdentity = io.identity(guard);
            WindowsMarkerIo.ProcessIdentity process = io.captureCurrentProcessIdentity(plan.creationOperationUuid());
            ReceiptData data = new ReceiptData(plan, sourceIdentity.versionedIdentity(), guardIdentity.versionedIdentity(),
                process.pid(), process.creationFileTime(), boot);
            byte[] receiptBytes = encodeReceipt(data);
            NativeReceiptAck ack = publishNewReceipt(directory, sourcePath, receiptBytes, plan.creationOperationUuid());
            FreshProvisioningProof creation = new FreshProvisioningProof(data, sourceIdentity, guardIdentity, ack);
            byte[] ownerBytes = encodeOwner(creation, boot, process);
            persistOwner(guard, ownerBytes);
            BootWorldCustodyProof custody = new BootWorldCustodyProof(this, directory, source, guard, creation, boot, process, ownerBytes, sourcePath);
            custody.validateLive();
            retainForProcessLifetime(custody);
            source = null; guard = null; // success ownership transferred to process-lifetime proof, never ordinary close。
            return new ProvisionedWorld(creation, custody, true);
        } catch (Throwable primary) {
            WindowsMarkerIo.closeSuppressed(guard, primary); WindowsMarkerIo.closeSuppressed(source, primary);
            WindowsMarkerIo.closeSuppressed(parent, primary);
            throw propagate("createFresh operation=" + plan.creationOperationUuid(), primary);
        }
    }
    /**
     * Historical/special Fresh mechanical custody API；不签发 formal NormalBoot 恢复资格。
     * Formal Normal 必须走 inspection → actual durable HANDOFF/PREGENESIS ACK → installResumeOwner。
     * expectedReceiptSha 必须来自 Main 保存的真实创建记录，不从待验证文件自行学习。
     */
    public ProvisionedWorld resumeExisting(Path requestedDirectory, Plan expectedPlan, String expectedReceiptSha,
                                           BootIdentity boot) throws IOException {
        Objects.requireNonNull(expectedPlan); Objects.requireNonNull(boot); hashBytes(expectedReceiptSha);
        Path directory = canonicalDirectory(requestedDirectory);
        WindowsMarkerIo.Handle source = null, guard = null;
        try {
            source = io.directory(directory, expectedPlan.bootstrapOperationUuid());
            String sourcePath = io.finalPath(source);
            WindowsMarkerIo.FileIdentity sourceIdentity = io.identity(source);
            guard = io.open(directory.resolve(GUARD_NAME), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, expectedPlan.bootstrapOperationUuid());
            WindowsMarkerIo.FileIdentity guardIdentity = io.identity(guard);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath, io.finalPath(guard));
            NativeReceiptAck ack = readReceipt(directory, sourcePath, null, expectedPlan.bootstrapOperationUuid());
            if (!MessageDigest.isEqual(hashBytes(expectedReceiptSha), hashBytes(sha256(ack.bytes))))
                throw new IOException("Controlled full receipt SHA mismatch; no custody");
            ReceiptData data = decodeReceipt(ack.bytes);
            if (!expectedPlan.equals(data.plan)) throw new IOException("Exact persisted provisioning plan mismatch");
            if (!sourceIdentity.versionedIdentity().equals(data.sourceIdentity) || !guardIdentity.versionedIdentity().equals(data.guardIdentity))
                throw new IOException("Actual directory/guard FileId differs from durable creation source");
            FreshProvisioningProof creation = new FreshProvisioningProof(data, sourceIdentity, guardIdentity, ack);
            long guardLength = io.payloadLength(guard);
            PreviousOwner previous;
            byte[] previousOwnerBytes;
            if (guardLength == 0) {
                // receipt 已发布而 owner 尚未首次写入的 gap；仍须核验同一 source/guard 与 creator 生命周期。
                previous = new PreviousOwner(data.creatorPid, data.creatorCreationTime, data.creationBoot);
                previousOwnerBytes = new byte[0];
            } else {
                if (guardLength != OWNER_SIZE) throw new IOException("Partial custody owner record; reconciliation required");
                previousOwnerBytes = io.readPayloadExact(guard, OWNER_SIZE);
                previous = decodeOwner(previousOwnerBytes, creation);
            }
            if (previous.boot.bootId().equals(boot.bootId())) throw new IOException("Retired boot UUID cannot be reused for new custody");
            WindowsMarkerIo.ProcessIdentity process = io.captureCurrentProcessIdentity(expectedPlan.bootstrapOperationUuid());
            WindowsMarkerIo.PreviousProcessLifetimeObservation previousLifetime = io.observePreviousProcessLifetime(
                previous.pid, previous.creationTime, expectedPlan.bootstrapOperationUuid());
            requirePreviousOwnerWorldUnchanged(directory, source, guard, creation, sourcePath,
                previousOwnerBytes, process, expectedPlan.bootstrapOperationUuid());
            io.claimPreviousProcessLifetimeObservation(previousLifetime, previous.pid, previous.creationTime,
                expectedPlan.bootstrapOperationUuid());
            byte[] ownerBytes = encodeOwner(creation, boot, process);
            persistOwner(guard, ownerBytes);
            BootWorldCustodyProof custody = new BootWorldCustodyProof(this, directory, source, guard, creation, boot, process, ownerBytes, sourcePath);
            custody.validateLive(); retainForProcessLifetime(custody);
            source = null; guard = null;
            return new ProvisionedWorld(creation, custody, false);
        } catch (Throwable primary) {
            WindowsMarkerIo.closeSuppressed(guard, primary); WindowsMarkerIo.closeSuppressed(source, primary);
            throw propagate("resumeExisting operation=" + expectedPlan.bootstrapOperationUuid(), primary);
        }
    }

    /**
     * Historical/special Fresh mechanical API, using the actual supervisor's retained HANDLE.
     * A native lifetime observation proves its own OS contract, not formal NormalBoot qualification.
     * Formal Normal still requires inspection → durable record ACK → installResumeOwner.
     */
    public RetainedResumeAttempt resumeExisting(Path requestedDirectory, Plan expectedPlan, String expectedReceiptSha,
            BootIdentity boot, WindowsMarkerIo.RetainedProcessObservation observation) throws IOException {
        Objects.requireNonNull(expectedPlan); Objects.requireNonNull(boot); hashBytes(expectedReceiptSha);
        io.requireRetainedCandidate(observation, expectedPlan.bootstrapOperationUuid());
        if (!observation.isSignaled()) return new ResumePending(observation);
        Path directory = canonicalDirectory(requestedDirectory);
        WindowsMarkerIo.Handle source = null, guard = null;
        ExactObservedPreviousProcessExit exitProof = null;
        try {
            source = io.directory(directory, expectedPlan.bootstrapOperationUuid());
            String sourcePath = io.finalPath(source);
            WindowsMarkerIo.FileIdentity sourceIdentity = io.identity(source);
            guard = io.open(directory.resolve(GUARD_NAME), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE,
                expectedPlan.bootstrapOperationUuid());
            WindowsMarkerIo.FileIdentity guardIdentity = io.identity(guard);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath, io.finalPath(guard));
            NativeReceiptAck ack = readReceipt(directory, sourcePath, null, expectedPlan.bootstrapOperationUuid());
            if (!MessageDigest.isEqual(hashBytes(expectedReceiptSha), hashBytes(sha256(ack.bytes))))
                throw new IOException("Controlled full receipt SHA mismatch; no custody");
            ReceiptData data = decodeReceipt(ack.bytes);
            if (!expectedPlan.equals(data.plan)) throw new IOException("Exact persisted provisioning plan mismatch");
            if (!sourceIdentity.versionedIdentity().equals(data.sourceIdentity)
                || !guardIdentity.versionedIdentity().equals(data.guardIdentity))
                throw new IOException("Actual directory/guard FileId differs from durable creation source");
            FreshProvisioningProof creation = new FreshProvisioningProof(data, sourceIdentity, guardIdentity, ack);
            long guardLength = io.payloadLength(guard);
            PreviousOwner previous;
            byte[] previousOwnerBytes;
            if (guardLength == 0) {
                previous = new PreviousOwner(data.creatorPid, data.creatorCreationTime, data.creationBoot);
                previousOwnerBytes = new byte[0];
            } else {
                if (guardLength != OWNER_SIZE) throw new IOException("Partial custody owner record; reconciliation required");
                previousOwnerBytes = io.readPayloadExact(guard, OWNER_SIZE);
                previous = decodeOwner(previousOwnerBytes, creation);
            }
            if (previous.boot.bootId().equals(boot.bootId()))
                throw new IOException("Retired boot UUID cannot be reused for new custody");
            WindowsMarkerIo.ProcessIdentity process = io.captureCurrentProcessIdentity(expectedPlan.bootstrapOperationUuid());
            WindowsMarkerIo.ProcessExitAcknowledgement nativeAck = io.confirmRetainedProcessExited(
                observation, previous.pid, previous.creationTime, expectedPlan.bootstrapOperationUuid());
            if (nativeAck == null) {
                closePendingResumeHandles(guard, source); guard = null; source = null;
                return new ResumePending(observation);
            }
            exitProof = issuePreviousProcessExitProof(observation, nativeAck, expectedPlan, ack,
                sourceIdentity, guardIdentity, previous, previousOwnerBytes, sourcePath, boot, process);
            byte[] ownerBytes = encodeOwner(creation, boot, process);
            persistOwner(guard, ownerBytes);
            BootWorldCustodyProof custody = new BootWorldCustodyProof(this, directory, source, guard, creation,
                boot, process, ownerBytes, sourcePath);
            custody.validateLive(); retainForProcessLifetime(custody);
            source = null; guard = null;
            consumeExitProofForCustody(exitProof);
            return new ResumeCustodyReady(new ProvisionedWorld(creation, custody, false));
        } catch (Throwable primary) {
            WindowsMarkerIo.closeSuppressed(guard, primary); WindowsMarkerIo.closeSuppressed(source, primary);
            throw propagate("resumeExisting retained process operation=" + expectedPlan.bootstrapOperationUuid(), primary);
        }
    }

    /** Exclusive inspection retains original owner bytes. It is not custody or SQL authority. */
    public static final class ResumeInspection {
        private final WindowsWorldProvisioner issuer;
        private final Thread owner=Thread.currentThread();
        private final Path directory;
        private final WindowsMarkerIo.Handle source,guard;
        private final FreshProvisioningProof creation;
        private final PreviousOwner previous;
        private final byte[] originalOwner;
        private final String nativePath;
        private final BootIdentity targetBoot;
        private final WindowsMarkerIo.ProcessIdentity targetProcess;
        private final PreviousOwnerLifetimeProof previousOwnerProof;
        private boolean publicationAttempted,installAttempted,installed,failed,retirementAttempted;
        private boolean preGenesisPublicationAttempted;
        private ProvisionedWorld installedWorld;
        private WindowsB1TwoSlotStore.DurableHandoffNativeAck installedHandoff;
        private WindowsB1TwoSlotStore.InstalledResumeRecordScope installedRecords;
        private Throwable installedContinuationFailure;
        public ProvisionedWorld installedWorld(){return installedWorld;}
        public WindowsB1TwoSlotStore.DurableHandoffNativeAck installedHandoffAcknowledgement(){return installedHandoff;}
        public WindowsB1TwoSlotStore.InstalledResumeRecordScope installedRecordScope(){return installedRecords;}
        public Throwable installedContinuationFailure(){return installedContinuationFailure;}
        void requireInstalledWorld(ProvisionedWorld world)throws IOException {
            if(Thread.currentThread()!=owner||!installed||world==null||installedWorld!=world)
                throw new IOException("Exact installed world/owner required");
            var custody=world.bootWorldCustodyProof();
            if(custody.provisioner!=issuer||custody.directoryHandle!=source||custody.guardHandle!=guard
                    ||custody.creationProof!=creation||!custody.boot.equals(targetBoot)
                    ||custody.process.pid()!=targetProcess.pid()
                    ||custody.process.creationFileTime()!=targetProcess.creationFileTime()
                    ||!custody.actualDirectoryPath.equals(nativePath))
                throw new IOException("Installed custody changed original native ownership chain");
            custody.validateLive();
        }
        void retainInstalledRecordScope(WindowsB1TwoSlotStore.InstalledResumeRecordScope records)throws IOException {
            requireInstalledWorld(installedWorld);
            if(installedRecords!=null||installedContinuationFailure!=null)throw new IOException("Installed record continuation already retained");
            installedRecords=Objects.requireNonNull(records);
        }
        void retainInstalledContinuationFailure(Throwable failure) {
            if(installedContinuationFailure==null)installedContinuationFailure=failure;
            else if(installedContinuationFailure!=failure)installedContinuationFailure.addSuppressed(failure);
            if(retainedFailure==null)retainedFailure=failure;
            else if(retainedFailure!=failure&&retainedFailure!=installedContinuationFailure)retainedFailure.addSuppressed(failure);
        }
        private Throwable retainedFailure;
        private ResumeInspection(WindowsWorldProvisioner issuer,Path directory,WindowsMarkerIo.Handle source,WindowsMarkerIo.Handle guard,FreshProvisioningProof creation,PreviousOwner previous,byte[] originalOwner,String nativePath,BootIdentity targetBoot,WindowsMarkerIo.ProcessIdentity targetProcess,PreviousOwnerLifetimeProof previousOwnerProof){this.issuer=issuer;this.directory=directory;this.source=source;this.guard=guard;this.creation=creation;this.previous=previous;this.originalOwner=originalOwner.clone();this.nativePath=nativePath;this.targetBoot=targetBoot;this.targetProcess=targetProcess;this.previousOwnerProof=previousOwnerProof;}
        public Path directory(){return directory;}
        public FreshProvisioningProof freshProvisioningProof(){return creation;}
        public String sourceWorldIdentity(){return creation.sourceWorldIdentity();}
        public String nativeDirectoryPath(){return nativePath;}
        PreviousOwnerEvidenceKind previousOwnerEvidenceKind(){return previousOwnerProof.kind();}
        public B1ResumeRecord.NativeOwnerAssertions previousOwner(){return new B1ResumeRecord.NativeOwnerAssertions(previous.pid,previous.creationTime,previous.boot.bootId(),previous.boot.generation());}
        public B1ResumeRecord.NativeOwnerAssertions targetOwner(){return new B1ResumeRecord.NativeOwnerAssertions(targetProcess.pid(),targetProcess.creationFileTime(),targetBoot.bootId(),targetBoot.generation());}
        public boolean ownerPublicationAttempted(){return installAttempted;}
        public boolean nativeHandoffPublicationAttempted(){return publicationAttempted&&!preGenesisPublicationAttempted;}
        public boolean nativePreGenesisPublicationAttempted(){return preGenesisPublicationAttempted;}
        public void validateInspection()throws IOException{issuer.validateInspection(this);}
        boolean uses(WindowsMarkerIo io){return issuer.io==io;}
        void beginHandoffPublication()throws IOException{validateInspection();if(publicationAttempted)throw new IOException("Handoff publication already attempted; retain responsibility");publicationAttempted=true;}
        void beginPreGenesisPublication()throws IOException{beginHandoffPublication();preGenesisPublicationAttempted=true;}
        public Throwable retainedFailure(){return retainedFailure;}
        public byte[] originalOwnerCanonicalBytes(){return originalOwner.clone();}
        void beginInactiveRetirement()throws IOException{validateInspection();if(retirementAttempted)throw new IOException("Inactive retirement already attempted");retirementAttempted=true;}
        void failHandoff(Throwable failure){failed=true;retainedFailure=failure;}
    }
    private static final List<ResumeInspection> PROCESS_LIFETIME_INSPECTIONS=new ArrayList<>();
    /** No guard mutation before a genuine durable HANDOFF_PREPARED ACK. */
    public ResumeInspection beginResumeInspection(Path requestedDirectory,Plan expectedPlan,String expectedReceiptSha,BootIdentity targetBoot)throws IOException {
        Objects.requireNonNull(expectedPlan);Objects.requireNonNull(targetBoot);hashBytes(expectedReceiptSha);
        Path directory=canonicalDirectory(requestedDirectory);
        WindowsMarkerIo.Handle source=null,guard=null;
        ResumeInspection inspection=null;
        try {
            source=io.directory(directory,expectedPlan.bootstrapOperationUuid());
            String sourcePath=io.finalPath(source);
            WindowsMarkerIo.FileIdentity sourceId=io.identity(source);
            guard=io.open(directory.resolve(GUARD_NAME),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,
                expectedPlan.bootstrapOperationUuid());
            io.metadata(guard,false,OWNER_SIZE);
            WindowsMarkerIo.FileIdentity guardId=io.identity(guard);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath,io.finalPath(guard));
            NativeReceiptAck receipt=readReceipt(directory,sourcePath,null,expectedPlan.bootstrapOperationUuid());
            requireBytes(hashBytes(expectedReceiptSha),digest(receipt.bytes),"Controlled receipt SHA mismatch");
            ReceiptData data=decodeReceipt(receipt.bytes);
            if(!expectedPlan.equals(data.plan)||!sourceId.versionedIdentity().equals(data.sourceIdentity)
                ||!guardId.versionedIdentity().equals(data.guardIdentity))
                throw new IOException("Original source/guard/plan identity mismatch");
            FreshProvisioningProof creation=new FreshProvisioningProof(data,sourceId,guardId,receipt);
            byte[] originalOwner=io.readPayloadExact(guard,OWNER_SIZE);
            PreviousOwner previous=decodeOwner(originalOwner,creation);
            if(previous.boot.bootId().equals(targetBoot.bootId()))
                throw new IOException("Old boot cannot be successor boot");

            WindowsMarkerIo.ProcessIdentity targetProcess=io.captureCurrentProcessIdentity(expectedPlan.bootstrapOperationUuid());
            WindowsMarkerIo.PreviousProcessLifetimeObservation nativeObservation=io.observePreviousProcessLifetime(
                previous.pid,previous.creationTime,expectedPlan.bootstrapOperationUuid());
            requirePreviousOwnerWorldUnchanged(directory,source,guard,creation,sourcePath,originalOwner,targetProcess,
                expectedPlan.bootstrapOperationUuid());
            NativePreviousOwnerLifetimeProof proof=issueNativePreviousOwnerLifetimeProof(nativeObservation,
                expectedPlan,receipt,sourceId,guardId,previous,originalOwner,sourcePath,targetBoot,targetProcess);
            inspection=new ResumeInspection(this,directory,source,guard,creation,previous,originalOwner,
                sourcePath,targetBoot,targetProcess,proof);
            synchronized(PROCESS_LIFETIME_INSPECTIONS){PROCESS_LIFETIME_INSPECTIONS.add(inspection);}
            source=null;guard=null;
            validateInspection(inspection);
            return inspection;
        }catch(Throwable primary){
            if(inspection!=null){inspection.failed=true;inspection.retainedFailure=primary;}
            WindowsMarkerIo.closeSuppressed(guard,primary);
            WindowsMarkerIo.closeSuppressed(source,primary);
            throw propagate("beginResumeInspection",primary);
        }
    }

    /** Ordinary-only source seam: preserve approved birth DATA while retiring the actual prior process owner. */
    public ResumeInspection beginCertifiedResumeInspection(Path requestedDirectory,ExpectedOriginalCreation expected,
            BootIdentity currentBoot)throws IOException {
        Objects.requireNonNull(expected);Objects.requireNonNull(currentBoot);
        Path actual=Objects.requireNonNull(requestedDirectory).toAbsolutePath().normalize();
        if(!actual.equals(expected.expectedWorldPath()))
            throw new IOException("Actual ordinary world path differs from the approved original creation source");
        ResumeInspection inspection=beginResumeInspection(actual,expected.originalPlan(),expected.canonicalReceiptSha256(),currentBoot);
        try {
            FreshProvisioningProof actualSource=inspection.freshProvisioningProof();
            byte[] receipt=actualSource.receiptCanonicalBytes();
            if(!actualSource.plan().equals(expected.originalPlan()) ||
                !actualSource.creationBoot().bootId().equals(expected.originalBootUuid()) ||
                actualSource.creationBoot().generation()!=expected.originalBootGeneration() ||
                receipt.length!=expected.canonicalReceiptLength() || !sha256(receipt).equals(expected.canonicalReceiptSha256()) ||
                !actualSource.sourceWorldIdentity().equals(expected.sourceWorldIdentity()) ||
                !actualSource.guardFileIdentity().equals(expected.guardFileIdentity()) ||
                !actualSource.receiptFileIdentity().equals(expected.receiptFileIdentity()))
                throw new IOException("Actual original creation receipt/source native identities differ from pinned approval");
            return inspection;
        } catch(Throwable primary) {
            inspection.failed=true;
            inspection.retainedFailure=primary;
            throw propagate("beginCertifiedResumeInspection",primary);
        }
    }

    /** Returns Pending without touching the world while the same retained process HANDLE is still live. */
    public RetainedResumeAttempt beginResumeInspection(Path requestedDirectory, Plan expectedPlan, String expectedReceiptSha,
            BootIdentity targetBoot, WindowsMarkerIo.RetainedProcessObservation observation) throws IOException {
        Objects.requireNonNull(expectedPlan); Objects.requireNonNull(targetBoot); hashBytes(expectedReceiptSha);
        io.requireRetainedCandidate(observation, expectedPlan.bootstrapOperationUuid());
        if (!observation.isSignaled()) return new ResumePending(observation);
        Path directory = canonicalDirectory(requestedDirectory);
        WindowsMarkerIo.Handle source = null, guard = null;
        ResumeInspection inspection = null;
        try {
            source = io.directory(directory, expectedPlan.bootstrapOperationUuid());
            String sourcePath = io.finalPath(source); WindowsMarkerIo.FileIdentity sourceId = io.identity(source);
            guard = io.open(directory.resolve(GUARD_NAME), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE,
                expectedPlan.bootstrapOperationUuid());
            io.metadata(guard, false, OWNER_SIZE); WindowsMarkerIo.FileIdentity guardId = io.identity(guard);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath, io.finalPath(guard));
            NativeReceiptAck receipt = readReceipt(directory, sourcePath, null, expectedPlan.bootstrapOperationUuid());
            requireBytes(hashBytes(expectedReceiptSha), digest(receipt.bytes), "Controlled receipt SHA mismatch");
            ReceiptData data = decodeReceipt(receipt.bytes);
            if (!expectedPlan.equals(data.plan) || !sourceId.versionedIdentity().equals(data.sourceIdentity)
                || !guardId.versionedIdentity().equals(data.guardIdentity))
                throw new IOException("Original source/guard/plan identity mismatch");
            FreshProvisioningProof creation = new FreshProvisioningProof(data, sourceId, guardId, receipt);
            byte[] originalOwner = io.readPayloadExact(guard, OWNER_SIZE);
            PreviousOwner previous = decodeOwner(originalOwner, creation);
            if (previous.boot.bootId().equals(targetBoot.bootId())) throw new IOException("Old boot cannot be successor boot");
            WindowsMarkerIo.ProcessIdentity targetProcess = io.captureCurrentProcessIdentity(expectedPlan.bootstrapOperationUuid());
            WindowsMarkerIo.ProcessExitAcknowledgement nativeAck = io.confirmRetainedProcessExited(
                observation, previous.pid, previous.creationTime, expectedPlan.bootstrapOperationUuid());
            if (nativeAck == null) {
                closePendingResumeHandles(guard, source); guard = null; source = null;
                return new ResumePending(observation);
            }
            ExactObservedPreviousProcessExit exitProof = issuePreviousProcessExitProof(observation, nativeAck,
                expectedPlan, receipt, sourceId, guardId, previous, originalOwner, sourcePath, targetBoot, targetProcess);
            inspection = new ResumeInspection(this, directory, source, guard, creation, previous, originalOwner,
                sourcePath, targetBoot, targetProcess, exitProof);
            synchronized (PROCESS_LIFETIME_INSPECTIONS) { PROCESS_LIFETIME_INSPECTIONS.add(inspection); }
            transferExitProofToInspection(exitProof, inspection);
            source = null; guard = null;
            validateInspection(inspection);
            return new ResumeInspectionReady(inspection);
        } catch (Throwable primary) {
            if (inspection != null) { inspection.failed = true; inspection.retainedFailure = primary; }
            WindowsMarkerIo.closeSuppressed(guard, primary); WindowsMarkerIo.closeSuppressed(source, primary);
            throw propagate("beginResumeInspection retained process operation=" + expectedPlan.bootstrapOperationUuid(), primary);
        }
    }

    public void validateInspection(ResumeInspection inspection)throws IOException {
        if(inspection==null||inspection.issuer!=this||Thread.currentThread()!=inspection.owner||inspection.installed||inspection.failed||inspection.source.closeAttempted||inspection.guard.closeAttempted)throw new IOException("Exact active owned inspection required");
        inspection.previousOwnerProof.requireFor(inspection);
        var plan=inspection.creation.plan();var process=io.captureCurrentProcessIdentity(plan.bootstrapOperationUuid());if(process.pid()!=inspection.targetProcess.pid()||process.creationFileTime()!=inspection.targetProcess.creationFileTime())throw new IOException("Inspection target process changed");
        io.metadata(inspection.source,true);requireIdentity(inspection.creation.directoryIdentity,io.identity(inspection.source));if(!inspection.nativePath.equals(io.finalPath(inspection.source)))throw new IOException("Inspection source moved");
        try(var reopened=io.directory(inspection.directory,plan.bootstrapOperationUuid())){requireIdentity(inspection.creation.directoryIdentity,io.identity(reopened));}
        io.metadata(inspection.guard,false,OWNER_SIZE);requireIdentity(inspection.creation.guardIdentity,io.identity(inspection.guard));WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(inspection.nativePath,io.finalPath(inspection.guard));requireBytes(inspection.originalOwner,io.readPayloadExact(inspection.guard,OWNER_SIZE),"Original guard owner changed before durable handoff");
        var receipt=readReceipt(inspection.directory,inspection.nativePath,inspection.creation.receiptFileIdentity,plan.bootstrapOperationUuid());requireBytes(inspection.creation.canonicalBytes,receipt.bytes,"Inspection original receipt changed");
    }
    /** 安装后续接失败由 inspection 保留已移交 custody；不可重新 install。 */
    public WindowsB1TwoSlotStore.InstalledResumeRecordScope installResumeOwnerForRecords(
            ResumeInspection inspection,WindowsB1TwoSlotStore.DurableHandoffNativeAck ack)throws IOException {
        var world=installResumeOwner(inspection,ack);
        try{return ack.issueInstalledScope(inspection,world);}
        catch(Throwable primary){inspection.retainInstalledContinuationFailure(primary);
            throw propagate("Installed record continuation failed; new custody retained",primary);}
    }
    public ProvisionedWorld installResumeOwner(ResumeInspection inspection,WindowsB1TwoSlotStore.DurableHandoffNativeAck ack)throws IOException {
        validateInspection(inspection);if(inspection.installAttempted)throw new IOException("Resume owner installation already attempted");Objects.requireNonNull(ack).validateForInstallation(inspection);
        inspection.installAttempted=true;
        try {byte[] ownerBytes=encodeOwner(inspection.creation,inspection.targetBoot,inspection.targetProcess);persistOwner(inspection.guard,ownerBytes);
            var custody=new BootWorldCustodyProof(this,inspection.directory,inspection.source,inspection.guard,inspection.creation,inspection.targetBoot,inspection.targetProcess,ownerBytes,inspection.nativePath);custody.validateLive();retainForProcessLifetime(custody);inspection.installed=true;
            var world=new ProvisionedWorld(inspection.creation,custody,false);
            inspection.installedWorld=world;inspection.installedHandoff=ack;return world;
        }catch(Throwable primary){inspection.failed=true;inspection.retainedFailure=primary;throw propagate("installResumeOwner partial native owner mutation retained",primary);}
    }

    public ProvisionedWorld installResumeOwner(ResumeInspection inspection,WindowsB1TwoSlotStore.DurablePreGenesisNativeAck ack)throws IOException {
        validateInspection(inspection);if(inspection.installAttempted)throw new IOException("Resume owner installation already attempted");Objects.requireNonNull(ack).validateForInstallation(inspection);
        inspection.installAttempted=true;
        try {byte[] ownerBytes=encodeOwner(inspection.creation,inspection.targetBoot,inspection.targetProcess);persistOwner(inspection.guard,ownerBytes);
            var custody=new BootWorldCustodyProof(this,inspection.directory,inspection.source,inspection.guard,inspection.creation,inspection.targetBoot,inspection.targetProcess,ownerBytes,inspection.nativePath);custody.validateLive();retainForProcessLifetime(custody);inspection.installed=true;return new ProvisionedWorld(inspection.creation,custody,false);
        }catch(Throwable primary){inspection.failed=true;inspection.retainedFailure=primary;throw propagate("installPreGenesisResumeOwner partial native owner mutation retained",primary);}
    }

    private static Path canonicalDirectory(Path path) {
        Objects.requireNonNull(path); Path actual = path.toAbsolutePath().normalize();
        if (actual.getParent() == null || actual.getFileName() == null) throw new IllegalArgumentException("World directory requires existing parent");
        return actual;
    }
    private static void retainForProcessLifetime(BootWorldCustodyProof proof) {
        synchronized (PROCESS_LIFETIME_CUSTODIES) { PROCESS_LIFETIME_CUSTODIES.add(proof); }
    }
    /** 只关闭原 Fresh custody 的两个实际句柄；未知其他 native 责任仍由 io.close 拒绝。 */
    public void closeCertifiedFreshCustody(ProvisionedWorld world, MySqlCertifiedFreshExitPermit permit) throws IOException {
        Objects.requireNonNull(world);
        Objects.requireNonNull(permit);
        BootWorldCustodyProof proof = world.bootWorldCustodyProof();
        requireOwnCustody(proof);
        if (!world.createdThisBoot() || proof.completionCloseAttempted)
            throw new IOException("Original Fresh custody completion close is one-shot");
        permit.claimFor(world, io);
        proof.completionCloseAttempted = true;
        Throwable primary = null;
        try {
            validateLive(proof);
            for (WindowsMarkerIo.Handle handle : new WindowsMarkerIo.Handle[]{proof.guardHandle, proof.directoryHandle}) {
                try { handle.close(); }
                catch (Throwable failure) {
                    if (primary == null) primary = failure;
                    else if (primary != failure) primary.addSuppressed(failure);
                }
            }
            if (primary != null) throw propagate("Fresh custody completion close", primary);
            synchronized (PROCESS_LIFETIME_CUSTODIES) {
                if (!PROCESS_LIFETIME_CUSTODIES.remove(proof))
                    throw new IOException("Original Fresh custody lifetime registration missing after close");
            }
        } catch (Throwable failure) {
            proof.completionCloseFailure = failure;
            throw propagate("Fresh custody completion close", failure);
        }
    }
    /** Ordinary仅原Session专用权限；不复用createdThisBoot/Fresh permit。 */
    public void closeCertifiedOrdinaryCustody(ResumeInspection inspection,ProvisionedWorld world,
            WindowsB1TwoSlotStore.InstalledResumeRecordScope installed,
            WindowsB1TwoSlotStore.DurableHandoffNativeAck acknowledgement,
            com.wi.floatingpod.storage.mysql.MySqlCertifiedOrdinaryNativeExitPermit permit)throws IOException {
        Objects.requireNonNull(permit).requireInstalledIdentity(inspection,world,installed,acknowledgement,io);
        if(inspection==null||inspection.issuer!=this||Thread.currentThread()!=inspection.owner||world.createdThisBoot())
            throw new IOException("Exact original Ordinary installed custody required");
        inspection.requireInstalledWorld(world);
        var proof=world.bootWorldCustodyProof();requireOwnCustody(proof);
        if(proof.completionCloseAttempted)throw new IOException("Original Ordinary custody close is one-shot",proof.completionCloseFailure);
        proof.completionCloseAttempted=true;
        try {
            validateLive(proof);
            if(inspection.installedRecords!=installed||inspection.installedHandoff!=acknowledgement)
                throw new IOException("Original installed registry/ACK identity changed");
            synchronized(PROCESS_LIFETIME_INSPECTIONS){
                if(!PROCESS_LIFETIME_INSPECTIONS.contains(inspection))throw new IOException("Original inspection lifetime registration missing");
            }
            synchronized(PROCESS_LIFETIME_CUSTODIES){
                if(!PROCESS_LIFETIME_CUSTODIES.contains(proof))throw new IOException("Original custody lifetime registration missing");
            }
            Throwable primary=null;
            try{proof.guardHandle.close();}catch(Throwable failed){primary=failed;}
            try{proof.directoryHandle.close();}catch(Throwable failed){
                if(primary==null)primary=failed;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,failed);
            }
            if(primary!=null)throw propagate("Ordinary custody checked close",primary);
            synchronized(PROCESS_LIFETIME_CUSTODIES){
                if(!PROCESS_LIFETIME_CUSTODIES.remove(proof))throw new IOException("Original custody registration disappeared");
            }
            synchronized(PROCESS_LIFETIME_INSPECTIONS){
                if(!PROCESS_LIFETIME_INSPECTIONS.remove(inspection))throw new IOException("Original inspection registration disappeared");
            }
        }catch(Throwable failed){proof.completionCloseFailure=failed;inspection.retainInstalledContinuationFailure(failed);
            throw propagate("Ordinary custody terminal close; partial responsibility retained",failed);}
    }
    private void validateLive(BootWorldCustodyProof proof) throws IOException {
        if (proof.completionCloseFailure != null)
            throw new IOException("Fresh custody completion close failed; original responsibility retained", proof.completionCloseFailure);
        if (proof.provisioner != this || proof.directoryHandle.closeAttempted || proof.guardHandle.closeAttempted)
            throw new IOException("Custody original handles are not live");
        Plan plan = proof.creationProof.plan();
        var current = io.captureCurrentProcessIdentity(plan.bootstrapOperationUuid());
        if (current.pid() != proof.process.pid() || current.creationFileTime() != proof.process.creationFileTime())
            throw new IOException("Custody process identity changed");
        io.metadata(proof.directoryHandle, true);
        requireIdentity(proof.creationProof.directoryIdentity, io.identity(proof.directoryHandle));
        if (!proof.actualDirectoryPath.equals(io.finalPath(proof.directoryHandle))) throw new IOException("Custody directory moved");
        try (var reopened = io.directory(proof.directory, plan.bootstrapOperationUuid())) {
            requireIdentity(proof.creationProof.directoryIdentity, io.identity(reopened));
        }
        io.metadata(proof.guardHandle, false, OWNER_SIZE);
        requireIdentity(proof.creationProof.guardIdentity, io.identity(proof.guardHandle));
        WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(proof.actualDirectoryPath, io.finalPath(proof.guardHandle));
        requireBytes(proof.ownerBytes, io.readPayloadExact(proof.guardHandle, OWNER_SIZE), "Live custody owner bytes changed");
        NativeReceiptAck reread = readReceipt(proof.directory, proof.actualDirectoryPath,
            proof.creationProof.receiptFileIdentity, plan.bootstrapOperationUuid());
        requireBytes(proof.creationProof.canonicalBytes, reread.bytes, "Live durable receipt changed");
    }
    private void validateLive(DeploymentReceiptCustodyProof proof)throws IOException {
        if(proof==null || proof.provisioner!=this || proof.thread!=Thread.currentThread() || proof.source.closeAttempted || proof.guard.closeAttempted)
            throw new IOException("Exact native5 provisioner/thread/live original handles required");
        var creation=proof.creation;var p=creation.plan();
        if(p.schemaFormat()!=5 || p.physicalFormat()!=2)throw new IOException("Native5 original receipt family conflict");
        var process=io.captureCurrentProcessIdentity(p.bootstrapOperationUuid());
        if(process.pid()!=proof.process.pid() || process.creationFileTime()!=proof.process.creationFileTime())throw new IOException("Native5 current process identity changed");
        io.metadata(proof.source,true);requireIdentity(creation.sourceIdentity,io.identity(proof.source));
        if(!proof.nativePath.equals(io.finalPath(proof.source)))throw new IOException("Native5 source moved");
        try(var reopened=io.directory(proof.directory,p.bootstrapOperationUuid())){requireIdentity(creation.sourceIdentity,io.identity(reopened));}
        io.metadata(proof.guard,false,OWNER_SIZE);requireIdentity(creation.guardIdentity,io.identity(proof.guard));
        WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(proof.nativePath,io.finalPath(proof.guard));
        requireBytes(proof.ownerBytes,encodeDeploymentReceiptOwner(creation,proof.boot,process),"Native5 expected owner identity differs");
        requireBytes(proof.ownerBytes,io.readPayloadExact(proof.guard,OWNER_SIZE),"Native5 original owner bytes changed");
        var reread=readReceipt(proof.directory,proof.nativePath,creation.receiptIdentity,p.bootstrapOperationUuid());
        requireBytes(creation.bytes,reread.bytes,"Native5 durable original receipt changed");
        requireBytes(creation.bytes,WindowsWorldProvisionReceiptDataCodec.encode(creation.receipt),"Native5 receipt semantic identity changed");
    }
    private static byte[] encodeDeploymentReceiptOwner(DeploymentReceiptCreationProof proof,BootIdentity boot,WindowsMarkerIo.ProcessIdentity process) {
        ByteBuffer out=ByteBuffer.allocate(OWNER_SIZE).order(ByteOrder.BIG_ENDIAN);
        out.putLong(DEPLOYMENT_RECEIPT_OWNER_MAGIC).putInt(5);putIdentity(out,proof.sourceIdentity);putIdentity(out,proof.guardIdentity);
        out.put(digest(proof.bytes));out.putLong(process.pid()).putLong(process.creationFileTime());
        out.putLong(boot.bootId().getMostSignificantBits()).putLong(boot.bootId().getLeastSignificantBits()).putLong(boot.generation());
        out.put(digest(Arrays.copyOf(out.array(),OWNER_SIZE-32)));return out.array();
    }
    private void persistOwner(WindowsMarkerIo.Handle guard, byte[] bytes) throws IOException {
        io.writePayload(guard, bytes); io.metadata(guard, false, OWNER_SIZE); io.flushChecked(guard);
        requireBytes(bytes, io.readPayloadExact(guard, OWNER_SIZE), "Native custody owner readback mismatch");
    }
    private NativeReceiptAck readReceipt(Path directory, String sourcePath, WindowsMarkerIo.FileIdentity expectedIdentity,
                                         UUID operation) throws IOException {
        try (var handle = io.open(directory.resolve(RECEIPT_NAME), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                 WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation)) {
            int length = Math.toIntExact(io.payloadLength(handle)); WindowsMarkerIo.payloadBound(length);
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath, io.finalPath(handle));
            var identity = io.identity(handle); if (expectedIdentity != null) requireIdentity(expectedIdentity, identity);
            byte[] bytes = io.readPayloadExact(handle, length);
            io.flushChecked(handle); requireBytes(bytes, io.readPayloadExact(handle, length), "Receipt native readback changed");
            requireIdentity(identity, io.identity(handle));
            return new NativeReceiptAck(identity, bytes);
        } // try-with-resources checked CloseHandle；close false 不返回这个ACK。
    }
    private NativeReceiptAck publishNewReceipt(Path directory, String sourcePath, byte[] bytes, UUID operation) throws IOException {
        return publishNewReceipt(directory,sourcePath,bytes,operation,ReceiptTemporaryPolicy.OWNED_LEGACY_TEMPORARY);
    }
    private enum ReceiptTemporaryPolicy { OWNED_LEGACY_TEMPORARY, KEEP_SCHEMA5_UNCERTAINTY }
    private NativeReceiptAck publishNewReceipt(Path directory,String sourcePath,byte[] bytes,UUID operation,ReceiptTemporaryPolicy policy)throws IOException {
        WindowsMarkerIo.Handle temporary = null; boolean safeToDelete = false;
        try {
            Path temporaryPath = null; IOException lastCollision = null;
            for (int attempt = 0; attempt < 16 && temporary == null; attempt++) {
                Path path = directory.resolve(".fp-receipt-" + operation + "-" + UUID.randomUUID() + ".tmp");
                try { temporary = io.createMarkerHandle(path, operation); temporaryPath = path; }
                catch (WindowsMarkerIo.NativeFailure collision) {
                    if (collision.win32Error() != 80 && collision.win32Error() != 183) throw collision;
                    lastCollision = collision;
                }
            }
            if (temporary == null) throw new IOException("Receipt CREATE_NEW temporary collision bound", lastCollision);
            safeToDelete = true;
            WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(sourcePath, io.finalPath(temporary));
            io.writePayload(temporary, bytes); io.metadata(temporary, false, bytes.length);
            var identity = io.identity(temporary); io.flushChecked(temporary);
            safeToDelete = false;
            try { io.moveWriteThroughNoReplace(temporaryPath, directory.resolve(RECEIPT_NAME), operation); }
            catch (WindowsMarkerIo.NativeFailure failure) {
                if (failure.win32Error() == 80 || failure.win32Error() == 183) safeToDelete = true;
                throw failure; // Fresh receipt immutable且只发布一次，不采纳foreign已存在文件。
            }
            io.closeChecked(temporary); temporary = null;
            NativeReceiptAck ack = readReceipt(directory, sourcePath, identity, operation);
            requireBytes(bytes, ack.bytes, "Published full receipt bytes mismatch");
            return ack;
        } catch (Throwable primary) {
            if (policy == ReceiptTemporaryPolicy.OWNED_LEGACY_TEMPORARY && temporary != null && safeToDelete && !temporary.closeAttempted) {
                try { io.deleteOwnedTemporary(temporary); } catch (Throwable cleanup) { primary.addSuppressed(cleanup); }
            }
            WindowsMarkerIo.closeSuppressed(temporary, primary);
            throw propagate("native receipt publication operation=" + operation, primary);
        }
    }
    private static IOException propagate(String stage, Throwable primary) {
        if (primary instanceof IOException failure) return failure;
        if (primary instanceof RuntimeException failure) throw failure;
        if (primary instanceof Error failure) throw failure;
        return WindowsMarkerIo.ioFailure(stage, primary);
    }
    private static void requireIdentity(WindowsMarkerIo.FileIdentity expected, WindowsMarkerIo.FileIdentity actual) throws IOException {
        if (!expected.equals(actual)) throw new IOException("Actual filesystem identity conflict");
    }
    private static void requireBytes(byte[] expected, byte[] actual, String stage) throws IOException {
        if (!Arrays.equals(expected, actual)) throw new IOException(stage);
    }
    public static byte[] digest(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException("Required SHA-256 unavailable", failure); }
    }
    static String sha256(byte[] bytes) { return HexFormat.of().withUpperCase().formatHex(digest(bytes)); }
    static byte[] hashBytes(String value) {
        if (value == null || !value.matches("[0-9A-Fa-f]{64}")) throw new IllegalArgumentException("Exact 32-byte SHA-256 required");
        return HexFormat.of().parseHex(value);
    }
    static void validateIdentityText(String identity) throws IOException {
        if (identity == null || !identity.matches("FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}"))
            throw new IOException("Receipt requires full canonical FileId128 identity");
    }
    static byte[] encodeReceipt(ReceiptData data) throws IOException {
        validateIdentityText(data.sourceIdentity);validateIdentityText(data.guardIdentity);
        WindowsMarkerIo.validateProcessIdentity(data.creatorPid,data.creatorCreationTime);
        Plan p=data.plan;
        var plan=new WindowsWorldProvisionReceiptDataCodec.PlanData(p.creationOperationUuid(),p.bootstrapOperationUuid(),
            p.prepareWriteUuid(),p.certifyWriteUuid(),p.bindingUuid(),p.physicalFormat(),p.mysqlServerUuid(),p.schema(),
            p.schemaFormat(),p.ddlSha256(),p.provisioningUuid());
        return WindowsWorldProvisionReceiptDataCodec.encode(new WindowsWorldProvisionReceiptDataCodec.ReceiptData(
            plan,data.sourceIdentity,data.guardIdentity,data.creatorPid,data.creatorCreationTime,data.creationBoot));
    }
    static ReceiptData decodeReceipt(byte[] bytes) throws IOException {
        var data=WindowsWorldProvisionReceiptDataCodec.decode(bytes);var p=data.plan();
        try {
            // Plan remains the strict native schema2/3 boundary; DATA5 has no conversion through it.
            var plan=new Plan(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),p.certifyWriteUuid(),
                p.bindingUuid(),p.physicalFormat(),p.mysqlServerUuid(),p.schema(),p.schemaFormat(),p.ddlSha256(),p.provisioningUuid());
            return new ReceiptData(plan,data.sourceIdentity(),data.guardIdentity(),data.creatorPid(),data.creatorCreationTime(),data.creationBoot());
        } catch(IllegalArgumentException | NullPointerException failure) {throw new IOException("Invalid complete receipt identity",failure);}
    }
    private static void putIdentity(ByteBuffer out, WindowsMarkerIo.FileIdentity identity) {
        out.putLong(identity.volumeSerialNumber()).put(identity.fileId128());
    }
    private static WindowsMarkerIo.FileIdentity getIdentity(ByteBuffer in) {
        long volume = in.getLong(); byte[] id = new byte[16]; in.get(id); return new WindowsMarkerIo.FileIdentity(volume, id);
    }
    private static byte[] encodeOwner(FreshProvisioningProof proof, BootIdentity boot, WindowsMarkerIo.ProcessIdentity process) {
        ByteBuffer out = ByteBuffer.allocate(OWNER_SIZE).order(ByteOrder.BIG_ENDIAN);
        out.putLong(GUARD_MAGIC).putInt(1); putIdentity(out, proof.directoryIdentity); putIdentity(out, proof.guardIdentity);
        out.put(hashBytes(proof.receiptSha256)); out.putLong(process.pid()).putLong(process.creationFileTime());
        out.putLong(boot.bootId().getMostSignificantBits()).putLong(boot.bootId().getLeastSignificantBits()).putLong(boot.generation());
        out.put(digest(Arrays.copyOf(out.array(), OWNER_SIZE - 32))); return out.array();
    }
    private static PreviousOwner decodeOwner(byte[] bytes, FreshProvisioningProof proof) throws IOException {
        if (bytes.length != OWNER_SIZE || !MessageDigest.isEqual(digest(Arrays.copyOf(bytes, OWNER_SIZE - 32)),
            Arrays.copyOfRange(bytes, OWNER_SIZE - 32, OWNER_SIZE))) throw new IOException("Invalid exact custody owner body/digest");
        try {
            ByteBuffer in = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            if (in.getLong() != GUARD_MAGIC || in.getInt() != 1) throw new IOException("Unknown custody owner version");
            requireIdentity(proof.directoryIdentity, getIdentity(in)); requireIdentity(proof.guardIdentity, getIdentity(in));
            byte[] receiptHash = new byte[32]; in.get(receiptHash);
            if (!MessageDigest.isEqual(hashBytes(proof.receiptSha256), receiptHash)) throw new IOException("Custody owner refers to another receipt");
            long pid = in.getLong(), time = in.getLong(); WindowsMarkerIo.validateProcessIdentity(pid, time);
            BootIdentity boot = new BootIdentity(new UUID(in.getLong(), in.getLong()), in.getLong());
            return new PreviousOwner(pid, time, boot);
        } catch (IllegalArgumentException failure) { throw new IOException("Invalid custody owner identity", failure); }
    }
}

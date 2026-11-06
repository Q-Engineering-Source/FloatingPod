package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;
import com.wi.floatingpod.storage.mysql.MySqlFull20NativeTransfer;
import com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor;
import static com.wi.floatingpod.server.B1ResumeRecord.*;

/** 固定两槽 native store；ACK 仅证明记录耐久，不签发 SQL admission 或退休能力。 */
public final class WindowsB1TwoSlotStore {
    private static final String[] NAMES={".floatingpod-b1-resume-v4.0",".floatingpod-b1-resume-v4.1"};
    private static final String TEMP=".floatingpod-b1-resume-v4.tmp";
    private static final String[] LEGACY={".floatingpod-b1-resume-v1.0",".floatingpod-b1-resume-v1.1",".floatingpod-b1-resume-v1.tmp",
        ".floatingpod-b1-resume-v2.0",".floatingpod-b1-resume-v2.1",".floatingpod-b1-resume-v2.tmp",
        ".floatingpod-b1-resume-v3.0",".floatingpod-b1-resume-v3.1",".floatingpod-b1-resume-v3.tmp"};
    private final WindowsMarkerIo io; private final Thread owner=Thread.currentThread();
    public WindowsB1TwoSlotStore(WindowsMarkerIo actualOwnedIo){io=Objects.requireNonNull(actualOwnedIo);}
    public static final class SourceScope {
        private final WindowsB1TwoSlotStore store;
        private WindowsB1SourceAnchorStore anchorStore;
        private WindowsB1SourceAnchorStore.ControlledAnchorLocation location;
        private WindowsB1SourceAnchorStore.DurableB1SourceAnchor sourceAck;
        private WindowsWorldProvisioner.BootWorldCustodyProof custody;
        private final java.util.List<WindowsB1Full20WitnessStore> witnessStores=new java.util.ArrayList<>();
        private WindowsB1V5WitnessRecordStore witnessV5Store;
        private SourceScope(WindowsB1TwoSlotStore store,WindowsB1SourceAnchorStore anchorStore,
                WindowsB1SourceAnchorStore.ControlledAnchorLocation location,WindowsB1SourceAnchorStore.DurableB1SourceAnchor ack,
                WindowsWorldProvisioner.BootWorldCustodyProof custody){this.store=store;this.anchorStore=anchorStore;this.location=location;sourceAck=ack;this.custody=custody;}
        @Override public String toString(){return "B1SourceScope[actual source/custody; not SQL authority]";}
    }
    /** 由真实来源私有发行；只允许 witness artifact，不授予 SQL 或记录转换资格。 */
    public static final class NativeWitnessScope {
        private final WindowsB1TwoSlotStore store;
        private SourceScope source;
        private InstalledResumeRecordScope installed;
        private NativeRecordDurability predecessor;
        private ResumeSourceScope resumeSource;
        private ResumeNativeRecordDurability resumePredecessor;
        private VerifiedWritableCurrentV5 preinstallV5;
        private MySqlFull20NativeTransfer fixedTransfer;
        private final Thread thread;
        private final java.util.ArrayList<WindowsB1Full20WitnessStore> witnessStores=new java.util.ArrayList<>(1);
        private final java.util.HashSet<String> knownNames=new java.util.HashSet<>();
        private WindowsB1V5WitnessRecordStore witnessV5Store;
        private boolean full20CreationAttempted;
        private NativeWitnessScope(WindowsB1TwoSlotStore store,SourceScope source,
                InstalledResumeRecordScope installed,NativeRecordDurability predecessor) {
            this.store=store;this.source=source;this.installed=installed;this.predecessor=predecessor;
            this.resumeSource=null;this.resumePredecessor=null;
            this.preinstallV5=null;this.fixedTransfer=null;this.thread=Thread.currentThread();
        }
        private NativeWitnessScope(WindowsB1TwoSlotStore store,VerifiedWritableCurrentV5 preinstallV5,
                MySqlFull20NativeTransfer fixedTransfer) {
            this.store=store;this.source=null;this.installed=null;this.predecessor=null;
            this.resumeSource=null;this.resumePredecessor=null;
            this.preinstallV5=preinstallV5;this.fixedTransfer=fixedTransfer;this.thread=Thread.currentThread();
        }
        private NativeWitnessScope(WindowsB1TwoSlotStore store,ResumeSourceScope resumeSource,
                ResumeNativeRecordDurability resumePredecessor,MySqlFull20NativeTransfer fixedTransfer) {
            this.store=store;this.source=null;this.installed=null;this.predecessor=null;
            this.resumeSource=resumeSource;this.resumePredecessor=resumePredecessor;
            this.preinstallV5=null;this.fixedTransfer=fixedTransfer;this.thread=Thread.currentThread();
        }
        WindowsMarkerIo witnessIo()throws IOException {return store.witnessIo(this);}
        Path witnessDirectory()throws IOException {return store.witnessDirectory(this);}
        UUID witnessOperation()throws IOException {return store.witnessOperation(this);}
        String witnessNativePath()throws IOException {return store.witnessNativePath(this);}
        void requireInstalledHistoricalArtifact(WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact artifact)
                throws IOException {store.requireInstalledHistoricalArtifact(this,artifact);}
        void retainInstalledWitnessFailure(Throwable cause) {store.retainInstalledWitnessFailure(this,cause);}
        public String predecessorSha256() {
            if(preinstallV5!=null)return WindowsWorldProvisioner.sha256(preinstallV5.v4Bridge.bytes);
            if(resumeSource!=null)return WindowsWorldProvisioner.sha256(resumePredecessor.facts.bytes);
            return predecessor.facts.sha256();
        }
        public long predecessorSequence() {
            if(preinstallV5!=null)return preinstallV5.v4Bridge.record.sequence();
            if(resumeSource!=null)return resumePredecessor.facts.record.sequence();
            return predecessor.facts.record.sequence();
        }
        void associateInstalled(InstalledResumeRecordScope actual)throws IOException {
            if(Thread.currentThread()!=thread||resumeSource==null||installed!=null||actual==null||
                    actual.inspection!=resumeSource.inspection||actual.source==null)
                throw new IOException("Exact same-inspection installed V4 witness custody is required once");
            installed=actual;source=actual.source;
        }
        @Override public String toString(){return "NativeWitnessScope[actual source; no SQL authority]";}
    }
    /** Fresh 来源与 Installed 来源分开发行，不能用数据记录构造 scope。 */
    public NativeWitnessScope witnessScope(SourceScope source,NativeRecordDurability predecessor)throws IOException {
        validateSource(source);requireCurrent(source,predecessor);
        return new NativeWitnessScope(this,source,null,predecessor);
    }
    public NativeWitnessScope witnessScope(InstalledResumeRecordScope installed)throws IOException {
        validateInstalled(installed);
        var actual=requireCurrent(installed.source,installed.initial);
        var handoff=installed.predecessor.durable.facts;
        if(actual.record.kind()!=Kind.HANDOFF_PREPARED||actual.record.sequence()!=handoff.record.sequence())
            throw new IOException("V5 initial source requires the actual installed HANDOFF predecessor");
        B1ResumeRecordCodec.exact(actual.bytes,handoff.bytes,"Installed witness predecessor differs from complete handoff");
        if(!actual.identity.equals(handoff.identity))throw new IOException("Installed witness predecessor file identity differs");
        return new NativeWitnessScope(this,installed.source,installed,installed.initial);
    }
    void requireInstalledHistoricalArtifact(NativeWitnessScope scope,
            WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact artifact)throws IOException {
        requireInstalledWitnessScope(scope);
        if(artifact==null)throw new IOException("Exact retained NO_V5 historical artifact required");
        var permit=artifact.permitForInstalledReissue();
        var original=scope.installed.predecessor;
        if(permit==null||permit.issuer!=this||permit.owner!=Thread.currentThread()||!permit.completed||
                permit.failure!=null||permit.cold==null||permit.cold.retainedFailure!=null||
                original==null||original.v5Flight!=null||original.scope!=permit.cold.raw||
                permit.cold.raw.inspection!=scope.installed.inspection||
                permit.accounting!=artifact.parentForInstalledReissue()||
                permit.verifiedArtifacts.stream().noneMatch(value->value==artifact))
            throw new IOException("Historical DATA must come from the exact completed NO_V5 permit installed by this original inspection");
    }
    void requireInstalledWitnessScope(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        if(scope.installed==null||scope.installed.store!=this||scope.source!=scope.installed.source||
                scope.installed.inspection==null||scope.installed.installedWorld()==null)
            throw new IOException("Exact actual installed witness custody scope required");
    }
    void retainInstalledWitnessFailure(NativeWitnessScope scope,Throwable cause) {
        if(scope==null||scope.store!=this||scope.installed==null||scope.installed.inspection==null||cause==null)
            throw new IllegalStateException("Exact installed witness scope and original failure required");
        scope.installed.inspection.retainInstalledContinuationFailure(cause);
    }
    public NativeWitnessScope witnessScopeForPlannedV5Handoff(VerifiedWritableCurrentV5 verified,
            MySqlFull20NativeTransfer fixedTransfer)throws IOException {
        checkOwner();
        if(verified==null||fixedTransfer==null)throw new IOException("Actual settled V5 source and fixed HANDOFF transfer required");
        verified.requireOwner();fixedTransfer.requireOpen();
        NativeWitnessScope scope=new NativeWitnessScope(this,verified,fixedTransfer);
        verified.bindTransfer(fixedTransfer,scope);
        for(var reference:verified.current.record.allReferences())scope.knownNames.add(reference.basename());
        if(verified.inactive!=null)for(var reference:verified.inactive.record.allReferences())scope.knownNames.add(reference.basename());
        fixedTransfer.reserveV5Publication();
        return scope;
    }
    public WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight beginVerifiedV5Handoff(
            VerifiedWritableCurrentV5 verified,MySqlFull20NativeTransfer fixedTransfer)throws IOException {
        NativeWitnessScope scope=witnessScopeForPlannedV5Handoff(verified,fixedTransfer);
        WindowsB1V5WitnessRecordStore result=new WindowsB1V5WitnessRecordStore(this,scope,verified,fixedTransfer);
        scope.witnessV5Store=result;
        try {
            var flight=result.prepareVerifiedHandoffRetirement();
            verified.cold.raw.inspection.beginHandoffPublication();
            return flight;
        } catch(Throwable primary) {verified.failure=primary;throw propagate(primary);}
    }
    /** Issues the real V4 writer only from the completed current-boot historical source permit. */
    public ResumeSourceScope resumeVerifiedCurrentV4ForOrdinaryHandoff(Full20HistoricalDiscoveryPermit permit,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        checkOwner();
        if(permit==null||accounting==null)throw new IOException("Exact completed V4 historical source permit/accounting required");
        permit.requireForHistoricalReference(io,accounting);
        ColdResumeSourceScope cold=permit.cold;
        if(cold.retainedLookup==null||cold.retainedLookup.observation().status()!=WindowsB1ColdResumeLookup.Status.UNRESOLVED_DATA||
                cold.retainedLookup.observation().unresolvedReason()!=WindowsB1ColdResumeLookup.UnresolvedReason.NO_V5)
            throw new IOException("Only the actual complete NO_V5 cold lookup can continue through the V4 ordinary issuer");
        ResumeSourceScope resume=cold.raw;
        validateResumeSource(resume);
        ResumeSelectedRecords selected=readCurrent(resume);
        if(selected.current()==null||selected.current().scope!=resume||permit.selected.scope!=resume)
            throw new IOException("Exact actual settled V4 current record is required");
        same(permit.selected,selected.current());
        B1ResumeRecord record=selected.current().record;
        var state=record.assertions();
        if((record.kind()!=Kind.CERTIFIED&&record.kind()!=Kind.DB_ACQUIRED)||state.pendingAttempt()!=null||
                state.originalPrepareWitness()==null||state.acknowledgedCertifyWitness()==null||
                state.latestSettlement()!=null&&state.latestSettlement().kind()!=B1ResumeRecord.SettlementKind.CONFIRMED)
            throw new IOException("V4 ordinary source must be settled, certified, and retain its original PREPARE/CERTIFY chain");
        return resume;
    }
    public NativeWitnessScope witnessScopeForOrdinaryV4Handoff(ResumeSourceScope resume,
            ResumeNativeRecordDurability predecessor,MySqlFull20NativeTransfer transfer)throws IOException {
        validateResumeSource(resume);
        if(predecessor==null||transfer==null)throw new IOException("Actual settled V4 predecessor and fixed actor HANDOFF transfer required");
        var current=requireCurrent(resume,predecessor);
        B1ResumeRecord planned=transfer.originalRecord();
        if((current.record.kind()!=Kind.CERTIFIED&&current.record.kind()!=Kind.DB_ACQUIRED)||
                planned.kind()!=Kind.HANDOFF_PREPARED||planned.assertions().pendingAttempt()==null||
                planned.assertions().pendingAttempt().phase()!=Phase.HANDOFF||
                planned.sequence()!=Math.addExact(current.record.sequence(),1L)||
                !Arrays.equals(planned.previousRecordSha256(),WindowsWorldProvisioner.digest(current.bytes))||
                !planned.assertions().currentOwner().equals(resume.inspection.targetOwner())||
                !Objects.equals(planned.assertions().previousOwner(),resume.inspection.previousOwner()))
            throw new IOException("Exact actor-planned V4 HANDOFF must follow the complete settled source under the actual owner guard");
        NativeWitnessScope scope=new NativeWitnessScope(this,resume,predecessor,transfer);
        resume.witnessScopes.add(scope);
        return scope;
    }
    void requireV5PreinstallScope(NativeWitnessScope scope,VerifiedWritableCurrentV5 expected,
            MySqlFull20NativeTransfer transfer,WindowsWorldProvisioner.ResumeInspection inspection)throws IOException {
        requireWitnessScope(scope);
        if(scope.preinstallV5==null||scope.preinstallV5!=expected||scope.installed!=null||
                scope.fixedTransfer!=transfer||expected.fixedTransfer!=transfer||expected.witnessScope!=scope||
                expected.cold.raw.inspection!=inspection||expected.failure!=null||expected.installed)
            throw new IOException("Exact preinstall V5 source/same transfer/active original inspection required");
    }

    void requireWitnessScope(NativeWitnessScope scope)throws IOException {
        checkOwner();
        if(scope==null||scope.store!=this||scope.thread!=Thread.currentThread())
            throw new IOException("Exact privately issued native witness scope required");
        if(scope.preinstallV5!=null) {
            if(scope.installed==null)scope.preinstallV5.requireOwner();
            else validateInstalled(scope.installed);
        } else if(scope.resumeSource!=null) {
            if(scope.installed==null)validateResumeSource(scope.resumeSource);else validateInstalled(scope.installed);
        } else if(scope.installed==null)validateSource(scope.source);else validateInstalled(scope.installed);
    }
    public WindowsB1Full20WitnessStore full20WitnessStore(NativeWitnessScope scope,
            MySqlFull20NativeTransfer transfer)throws IOException {
        requireWitnessScope(scope);
        if(transfer==null||scope.full20CreationAttempted||
                scope.preinstallV5!=null&&transfer!=scope.fixedTransfer||
                scope.resumeSource!=null&&(transfer!=scope.fixedTransfer||scope.resumePredecessor==null))
            throw new IOException("One actual full20 store for the exact registered transfer required");
        scope.full20CreationAttempted=true;
        transfer.reserveNativeStore();
        var result=new WindowsB1Full20WitnessStore(this,scope,transfer);
        scope.witnessStores.add(result);
        if(scope.source!=null)scope.source.witnessStores.add(result);
        if(scope.resumeSource!=null)scope.resumeSource.witnessStores.add(result);
        return result;
    }
    boolean knownWitnessName(NativeWitnessScope scope,String name)throws IOException {
        requireWitnessScope(scope);
        if(scope.knownNames.contains(name))return true;
        for(var store:scope.witnessStores)if(store.ownsKnownName(name))return true;
        if(scope.source!=null)for(var store:scope.source.witnessStores)if(store.ownsKnownName(name))return true;
        if(scope.resumeSource!=null)for(var store:scope.resumeSource.witnessStores)if(store.ownsKnownName(name))return true;
        return false;
    }
    public WindowsB1V5WitnessRecordStore v5WitnessRecordStore(NativeWitnessScope scope,
            MySqlFull20NativeTransfer retainedTransfer)throws IOException {
        requireWitnessScope(scope);
        if(scope.preinstallV5!=null||scope.resumeSource!=null)
            throw new IOException("Ordinary V4/V5 handoff must use its actual installed-source continuation issuer");
        if(scope.source==null||scope.source.witnessV5Store!=null||scope.witnessV5Store!=null)
            throw new IOException("Source already retains its V5 chain holder");
        retainedTransfer.reserveV5Publication();requireInitialV5Witness(scope);
        scope.witnessV5Store=new WindowsB1V5WitnessRecordStore(this,scope,retainedTransfer);
        scope.source.witnessV5Store=scope.witnessV5Store;
        return scope.witnessV5Store;
    }
    WindowsMarkerIo.FileIdentity witnessPredecessorIdentity(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.preinstallV5!=null?scope.preinstallV5.v4Bridge.identity:
            scope.resumeSource!=null?scope.resumePredecessor.facts.identity:scope.predecessor.facts.identity;
    }
    void requireInitialV5Witness(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        if(scope.preinstallV5!=null||scope.installed==null)
            throw new IOException("Initial V5 continuation requires actual installed HANDOFF scope");
        if(scope.resumeSource!=null||scope.installed.v5Flight!=null||
                scope.installed.predecessor==null||scope.installed.predecessor.v5Flight!=null)
            throw new IOException("Initial V5 continuation requires the exact installed V4 ordinary HANDOFF scope");
        var oldSource=scope.installed.predecessor.scope;
        var history=oldSource==null?null:oldSource.historicalDiscoveryPermit;
        if(oldSource==null||oldSource.store!=this||oldSource.inspection!=scope.installed.inspection||history==null||
                history.issuer!=this||history.cold.raw!=oldSource||!history.completed||history.failure!=null||
                history.cold.retainedFailure!=null||scope.installed.ordinaryV4RetirementAck==null)
            throw new IOException("Initial V5 continuation requires completed exact NO_V5 history and the post-commit V4 retirement ACK");
        var current=requireCurrent(scope.source,scope.predecessor);
        if(current.record.kind()!=Kind.HANDOFF_PREPARED)throw new IOException("Initial V5 predecessor is not HANDOFF_PREPARED");
    }
    B1ResumeRecord witnessPredecessorRecord(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.preinstallV5!=null?scope.preinstallV5.v4Bridge.record:
            scope.resumeSource!=null?scope.resumePredecessor.facts.record:scope.predecessor.facts.record;
    }
    byte[] witnessPredecessorBytes(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.preinstallV5!=null?scope.preinstallV5.accounting.copyCompletedV5SourceBytes(scope.preinstallV5.v4Bridge.bytes):
            scope.resumeSource!=null?scope.resumePredecessor.facts.bytes.clone():scope.predecessor.facts.bytes.clone();
    }
    void requireWitnessSourceRecord(NativeWitnessScope scope,B1ResumeRecord record)throws IOException {
        requireWitnessScope(scope);
        if(scope.installed==null&&(scope.preinstallV5!=null||scope.resumeSource!=null))
            requireWitnessHandoffTransition(scope,scope.fixedTransfer,record);
        else requireSourceRecord(scope.installed!=null?scope.installed.source:scope.source,record);
    }
    void requireWitnessHandoffTransition(NativeWitnessScope scope,MySqlFull20NativeTransfer transfer,
            B1ResumeRecord transition)throws IOException {
        requireWitnessScope(scope);
        if((scope.preinstallV5==null&&scope.resumeSource==null)||scope.installed!=null||transfer==null||transfer!=scope.fixedTransfer||
                transition==null||!Arrays.equals(B1ResumeRecordCanonical.encode(transition),
                    B1ResumeRecordCanonical.encode(transfer.originalRecord())))
            throw new IOException("Only the exact planned HANDOFF transition for this V5 preinstall flight is permitted");
        var source=scope.preinstallV5!=null?scope.preinstallV5.v4Bridge.record:scope.resumePredecessor.facts.record;
        B1ResumeRecordCodec.exact(source.sourceAnchorSha256(),transition.sourceAnchorSha256(),"Planned HANDOFF source anchor differs");
        B1ResumeRecordCodec.exact(source.originalReceipt(),transition.originalReceipt(),"Planned HANDOFF original receipt differs");
        if(!source.barrierNamespace().equals(transition.barrierNamespace()))
            throw new IOException("Planned HANDOFF source namespace differs");
        if(scope.resumeSource!=null) {
            var actual=requireCurrent(scope.resumeSource,scope.resumePredecessor);
            if(!Arrays.equals(actual.bytes,scope.resumePredecessor.facts.bytes)||
                    !actual.identity.equals(scope.resumePredecessor.facts.identity)||
                    !actual.record.assertions().currentOwner().equals(scope.resumeSource.inspection.previousOwner())||
                    !transition.assertions().currentOwner().equals(scope.resumeSource.inspection.targetOwner())||
                    !Objects.equals(transition.assertions().previousOwner(),scope.resumeSource.inspection.previousOwner()))
                throw new IOException("Planned V4 HANDOFF differs from the actual old-source/new-target owner guard");
        }
    }
    void requireWitnessOwner(NativeWitnessScope scope,FullStateAssertions state)throws IOException {
        requireWitnessScope(scope);
        if(scope.resumeSource!=null&&scope.installed==null) {
            if(state==null||state!=scope.fixedTransfer.originalRecord().assertions()||
                    !state.currentOwner().equals(scope.resumeSource.inspection.targetOwner())||
                    !Objects.equals(state.previousOwner(),scope.resumeSource.inspection.previousOwner()))
                throw new IOException("V4 preinstall permits only the exact planned successor owner over the actual old guard");
        } else if(scope.preinstallV5!=null&&scope.installed==null) {
            if(state==null||state!=scope.fixedTransfer.originalRecord().assertions()||
                    !state.currentOwner().equals(scope.preinstallV5.cold.raw.inspection.targetOwner())||
                    !Objects.equals(state.previousOwner(),scope.preinstallV5.cold.raw.inspection.previousOwner()))
                throw new IOException("V5 preinstall permits only the exact planned successor owner over the old actual guard");
        } else if(scope.installed!=null)assertCurrentOwner(scope.installed.source,state);
        else if(scope.source!=null)assertCurrentOwner(scope.source,state);
        else throw new IOException("Exact V4/V5 witness owner authority is unavailable");
    }
    WindowsMarkerIo witnessIo(NativeWitnessScope scope)throws IOException {requireWitnessScope(scope);return io;}
    Path witnessDirectory(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.installed!=null?scope.installed.source.custody.directory():
            scope.source!=null?scope.source.custody.directory():
                scope.resumeSource!=null?scope.resumeSource.inspection.directory():scope.preinstallV5.cold.raw.inspection.directory();
    }
    String witnessNativePath(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.installed!=null?scope.installed.source.sourceAck.verifiedInput().worldNativePath():
            scope.source!=null?scope.source.sourceAck.verifiedInput().worldNativePath():
                scope.resumeSource!=null?scope.resumeSource.sourceAck.verifiedInput().worldNativePath():
                    scope.preinstallV5.cold.raw.sourceAck.verifiedInput().worldNativePath();
    }
    UUID witnessOperation(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.installed!=null?operation(scope.installed.source):
            scope.source!=null?operation(scope.source):
                scope.resumeSource!=null?operation(scope.resumeSource):operation(scope.preinstallV5.cold.raw);
    }
    byte[] witnessAnchorSha(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.preinstallV5!=null?scope.preinstallV5.v4Bridge.record.sourceAnchorSha256():
            scope.resumeSource!=null?scope.resumePredecessor.facts.record.sourceAnchorSha256():
                scope.predecessor.facts.record.sourceAnchorSha256();
    }
    byte[] witnessOriginalReceipt(NativeWitnessScope scope)throws IOException {
        requireWitnessScope(scope);
        return scope.preinstallV5!=null?scope.preinstallV5.v4Bridge.record.originalReceipt():
            scope.resumeSource!=null?scope.resumePredecessor.facts.record.originalReceipt():
                scope.predecessor.facts.record.originalReceipt();
    }
    private void revalidatePreinstallV5Reference(VerifiedWritableCurrentV5 current,
            PreinstallV5ArtifactReference reference)throws IOException {
        checkOwner();
        if(current==null||current.issuer!=this||reference==null||reference.current!=current||
                !current.installed||current.witnessScope==null||current.witnessScope.installed==null||
                current.artifactReadFailure!=null)
            throw new IOException("Exact installed V5 source/artifact rebarrier flight required",current==null?null:current.artifactReadFailure);
        var scope=current.witnessScope;var actual=reference.actualSource;var data=reference.reference;
        actual.requireCurrent();
        var matchingAttempt=current.exactAttempt(data);actual.requireCurrent();
        actual.requireCurrent();
        var accounting=current.accounting;long scratch=accounting.beginV5ArtifactReferenceRead(data.fullLength());
        WindowsMarkerIo io=witnessIo(scope);WindowsMarkerIo.Handle handle=null;
        byte[] first=null,second=null,third=null;Throwable primary=null;
        try {
            if(matchingAttempt==null||!matchingAttempt.attemptId().equals(data.attemptId()))
                throw new IOException("Installed rebarrier attempt differs from complete V5 source reference");
            handle=io.open(witnessDirectory(scope).resolve(data.basename()),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,witnessOperation(scope));
            current.artifactReadHandle=handle;
            WindowsB1SourceAnchorStore.requireExactNativeChild(witnessNativePath(scope),io.finalPath(handle),data.basename());
            WindowsMarkerIo.FileIdentity identity=io.identity(handle);
            if(!identity.equals(actual.identity())||!identity.equals(data.fileIdentity()))
                throw new IOException("Installed full20 original artifact native identity differs from actual old cold read");
            io.metadata(handle,false,data.fullLength());
            int length=Math.toIntExact(data.fullLength());long limit=accounting.artifactLimit();
            first=io.readWitnessArtifactExact(handle,length,limit);current.artifactReadFirst=first;
            if(!actual.matchesCompletePayload(first)||!Arrays.equals(WindowsWorldProvisioner.digest(first),data.fullSha256()))
                throw new IOException("Installed full20 complete payload differs from actual old cold actor bytes/reference SHA");
            io.flushChecked(handle);
            second=io.readWitnessArtifactExact(handle,length,limit);current.artifactReadSecond=second;
            if(!Arrays.equals(first,second)||!actual.matchesCompletePayload(second)||!identity.equals(io.identity(handle)))
                throw new IOException("Installed full20 bytes/native identity changed across first flush barrier");
            current.witnessScope.installed.installedWorld().bootWorldCustodyProof().validateLive();
            io.flushChecked(handle);
            third=io.readWitnessArtifactExact(handle,length,limit);current.artifactReadThird=third;
            if(!Arrays.equals(first,third)||!identity.equals(io.identity(handle)))
                throw new IOException("Installed full20 complete bytes changed across final custody barrier");
            io.closeChecked(handle);handle=null;current.artifactReadHandle=null;
            Arrays.fill(first,(byte)0);Arrays.fill(second,(byte)0);Arrays.fill(third,(byte)0);
            current.artifactReadFirst=null;current.artifactReadSecond=null;current.artifactReadThird=null;
            accounting.finishV5ArtifactReferenceRead(data.fullLength(),scratch);
        } catch(Throwable cause) {
            primary=cause;current.artifactReadFailure=cause;current.failure=cause;
            try {accounting.failV5ArtifactReferenceRead(data.fullLength(),scratch,cause);}
            catch(Throwable cleanup) {if(cleanup!=cause)cause.addSuppressed(cleanup);}
            WindowsMarkerIo.closeSuppressed(handle,cause);
            throw propagate(cause);
        }
    }

    public DurableHandoffNativeAck handoffAcknowledgementForInstall(
            WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight flight,
            WindowsB1V5WitnessRecordStore.NativeV5RecordDurability actualHandoff)throws IOException {
        checkOwner();
        if(flight==null||actualHandoff==null||flight.actualHandoffRecord()!=actualHandoff)
            throw new IOException("Exact actual V5 HANDOFF native acknowledgement required");
        var store=flight.store();
        var preinstall=store.preinstallSourceForInstall(flight);
        var resume=preinstall.resumeSourceForInstall();
        if(resume.store!=this)throw new IOException("V5 source belongs to a different original two-slot store");
        validateResumeSourceIdentity(resume);
        store.validateForInstallation(flight,resume.inspection);
        return new DurableHandoffNativeAck(resume,store,flight,actualHandoff);
    }

    void requireInstalledV5Flight(WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight flight,
            InstalledResumeRecordScope installed)throws IOException {
        checkOwner();
        if(flight==null||installed==null||installed.v5Flight!=flight||installed.store!=this||
                installed.predecessor==null||installed.predecessor.v5Flight!=flight||
                installed.predecessor.v5Store==null||installed.initial!=null)
            throw new IOException("Exact installed V5 HANDOFF source/scope/flight relation required");
        installed.inspection.requireInstalledWorld(installed.installedWorld());
        var store=installed.predecessor.v5Store;
        store.requireInstalledBindingIdentity(this,installed,flight);
        var preinstall=store.preinstallForAssociation(flight);
        if(preinstall.issuer!=this||preinstall.witnessScope==null||preinstall.witnessScope.installed!=null||
                preinstall.witnessScope.preinstallV5!=preinstall||preinstall.witnessScope.fixedTransfer==null)
            throw new IOException("Original V5 preinstall witness scope is unavailable or already associated");
        preinstall.witnessScope.installed=installed;
        preinstall.bindInstalled(preinstall.witnessScope);
        validateInstalled(installed);
        store.rebarrierInstalledHandoff(flight,installed);
    }

    void requireVerifiedV5Current(VerifiedWritableCurrentV5 current)throws IOException {
        checkOwner();
        if(current==null||current.issuer!=this||Thread.currentThread()!=current.owner||current.failure!=null||current.artifactReadFailure!=null)
            throw new IOException("Exact live actual V5 preinstall source required",current==null?null:current.failure!=null?current.failure:current.artifactReadFailure);
        if(current.installed) {
            if(current.witnessScope==null||current.witnessScope.installed==null)
                throw new IOException("Installed V5 source lacks its new custody association");
            validateInstalled(current.witnessScope.installed);
        } else {
            validateColdSource(current.cold);current.inventory.requireCurrent();validateResumeSourceIdentity(current.cold.raw);
        }
    }

    /** Original preinstall source alone issues this read-only historical responsibility. */
    public WindowsB1V5HistoricalLookup readVerifiedV5History(VerifiedWritableCurrentV5 current,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        checkOwner();
        if(current==null||current.issuer!=this||current.failure!=null||current.artifactReadFailure!=null||
                current.installed||current.scopeIssued||current.history!=null||current.accounting!=accounting)
            throw new IOException("One original preinstall V5 history lookup with exact accounting required");
        accounting.reserveV5HistoryHolder(Math.subtractExact(current.current.record.transition().sequence(),current.v4Bridge.record.sequence()));
        var lookup=new WindowsB1V5HistoricalLookup(this,current,current.cold,accounting);
        current.history=lookup;
        lookup.read();
        return lookup;
    }
    void requireV5History(WindowsB1V5HistoricalLookup lookup,VerifiedWritableCurrentV5 current,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        checkOwner();
        if(current==null||current.issuer!=this||current.failure!=null||current.artifactReadFailure!=null||
                current.history!=lookup||current.accounting!=accounting||current.installed||current.scopeIssued)
            throw new IOException("Exact original live preinstall V5 history owner required");
    }
    int historyCapturedWorldLength(WindowsB1V5HistoricalLookup lookup,VerifiedWritableCurrentV5 current,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        requireV5History(lookup,current,accounting);
        return current.cold.raw.sourceAck.verifiedInput().worldNativePath().length();
    }
    void revalidateV5HistorySource(WindowsB1V5HistoricalLookup lookup,VerifiedWritableCurrentV5 current,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        requireV5History(lookup,current,accounting);accounting.requireV5HistoryPass(lookup);
        requireVerifiedV5Current(current);
    }


    public static final class StoredRecordFacts {
        private final SourceScope scope; private final int slot; private final B1ResumeRecord record;
        private final byte[] bytes; private final WindowsMarkerIo.FileIdentity identity;
        private StoredRecordFacts(SourceScope scope,int slot,B1ResumeRecord record,byte[] bytes,WindowsMarkerIo.FileIdentity identity){
            this.scope=scope;this.slot=slot;this.record=record;this.bytes=bytes.clone();this.identity=identity;}
        public B1ResumeRecord record(){return record;}
        public int slot(){return slot;}
        public byte[] canonicalBytes(){return bytes.clone();}
        public String sha256(){return WindowsWorldProvisioner.sha256(bytes);}
        public String fileIdentity(){return identity.versionedIdentity();}
        @Override public String toString(){return "StoredRecordFacts[disk caller assertions; no capability]";}
    }
    public record SelectedRecords(StoredRecordFacts current,StoredRecordFacts inactive){ }
    public static final class NativeRecordDurability {
        private final StoredRecordFacts facts;
        private NativeRecordDurability(StoredRecordFacts facts){this.facts=facts;}
        public StoredRecordFacts facts(){return facts;}
        public String protocol(){return "WIN32_B1_FULL_WRITE_FLUSH_NO_REPLACE_MOVE_CANONICAL_READ_FLUSH_READ_CLOSE_V4";}
        @Override public String toString(){return "NativeRecordDurability[native bytes only; no SQL/genesis permission]";}
    }
    public static final class RetiredInactiveSlot {
        private final int slot; private final NativeRecordDurability retained;
        private RetiredInactiveSlot(int slot,NativeRecordDurability retained){this.slot=slot;this.retained=retained;}
        public int absentSlot(){return slot;}
        public NativeRecordDurability retainedRecord(){return retained;}
    }
    public SourceScope bindSource(WindowsB1SourceAnchorStore actualAnchorStore,
            WindowsB1SourceAnchorStore.ControlledAnchorLocation location,WindowsB1SourceAnchorStore.DurableB1SourceAnchor opaqueAck,
            WindowsWorldProvisioner.BootWorldCustodyProof actualCustody)throws IOException {
        checkOwner();var scope=new SourceScope(this,Objects.requireNonNull(actualAnchorStore),Objects.requireNonNull(location),
            Objects.requireNonNull(opaqueAck),Objects.requireNonNull(actualCustody));validateSource(scope);return scope;
    }
    private void checkOwner(){if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact two-slot owned worker required");}
    private void validateSource(SourceScope s)throws IOException {
        checkOwner();if(s==null||s.store!=this)throw new IOException("Exact issued source scope required");
        s.sourceAck=s.anchorStore.rebarrierExisting(s.location,s.sourceAck.verifiedInput());s.custody.validateLive();
        var expected=s.sourceAck.verifiedInput();var proof=s.custody.freshProvisioningProof();
        B1ResumeRecordCodec.exact(expected.originalReceiptCanonicalBytes(),proof.receiptCanonicalBytes(),"Current custody differs from original anchored receipt");
        if(!expected.expectedPlan().equals(proof.plan())||!expected.originalSourceIdentity().equals(proof.sourceWorldIdentity()))
            throw new IOException("Anchored source/plan differs from live custody");
        try(var handle=io.directory(s.custody.directory(),proof.plan().bootstrapOperationUuid())){
            if(!expected.worldNativePath().equals(io.finalPath(handle))||!proof.sourceWorldIdentity().equals(io.identity(handle).versionedIdentity()))
                throw new IOException("Live world native path/identity differs from anchor");
        }
        s.custody.validateLive();
    }
    private Path path(SourceScope s,int slot){if(slot<0||slot>1)throw new IllegalArgumentException("Fixed slot 0 or 1 required");return s.custody.directory().resolve(NAMES[slot]);}
    /** Inventory专用borrowed链；不经过validateColdSource，不更新raw.sourceAck。 */
    private void requireInventoryOwner(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                       com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        checkOwner();
        if (cold == null || cold.store != this || cold.retainedLookup != lookup || cold.accounting != accounting ||
                cold.retainedFailure != null || !cold.raw.inspection.uses(io))
            throw new IOException("Exact original cold lookup/source/accounting required", cold == null ? null : cold.retainedFailure);
        accounting.requireOpen();
    }
    int inventoryCapturedWorldLength(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                    com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        requireInventoryOwner(cold, lookup, accounting);
        return cold.raw.sourceAck.verifiedInput().worldNativePath().length();
    }
    WindowsMarkerIo inventoryBorrowedIo(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                       com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        requireInventoryOwner(cold, lookup, accounting); return io;
    }
    Path inventoryBorrowedDirectory(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                   com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        requireInventoryOwner(cold, lookup, accounting); return cold.raw.inspection.directory();
    }
    UUID inventoryBorrowedOperation(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                   com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        requireInventoryOwner(cold, lookup, accounting); return operation(cold.raw);
    }
    String inventoryBorrowedNativePath(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                     com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        requireInventoryOwner(cold, lookup, accounting); return cold.raw.sourceAck.verifiedInput().worldNativePath();
    }
    void inventoryBorrowedGuard(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                               com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        requireInventoryOwner(cold, lookup, accounting);
        var s = cold.raw;
        s.inspection.validateInspection();
        // local ACK/input是当前检查结果；原scope的ACK原样保留。
        var currentAnchor = s.anchorStore.rebarrierExisting(s.location, s.sourceAck.verifiedInput());
        var input = currentAnchor.verifiedInput(); var creation = s.inspection.freshProvisioningProof();
        B1ResumeRecordCodec.exact(input.originalReceiptCanonicalBytes(), creation.receiptCanonicalBytes(),
            "Inventory original receipt differs from anchor");
        if (!input.expectedPlan().equals(creation.plan()) ||
                !input.originalSourceIdentity().equals(creation.sourceWorldIdentity()) ||
                !input.worldNativePath().equals(s.inspection.nativeDirectoryPath()))
            throw new IOException("Inventory complete anchor/source/plan/native path differs");
        s.inspection.validateInspection();
    }
    void inventoryCompareV4AndAbsent(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                    com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        requireInventoryOwner(cold, lookup, accounting);
        for (int slot = 0; slot < 2; slot++) {
            ResumeStoredRecordFacts known = null;
            for (var read : cold.retainedV4Reads) if (read.facts != null && read.facts.slot == slot) known = read.facts;
            inventoryCompareFile(cold, lookup, accounting, NAMES[slot], known);
        }
        inventoryCompareFile(cold, lookup, accounting, TEMP, null);
        for (String legacy : LEGACY) inventoryCompareFile(cold, lookup, accounting, legacy, null);
    }
    private void inventoryCompareFile(ColdResumeSourceScope cold, WindowsB1ColdResumeLookup lookup,
                                     com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting,
                                     String name, ResumeStoredRecordFacts known) throws IOException {
        requireInventoryOwner(cold, lookup, accounting);
        try {
            cold.inventoryHandle = openExisting(cold.raw.inspection.directory().resolve(name), WindowsMarkerIo.READ, operation(cold.raw));
            if (known == null) {
                if (cold.inventoryHandle != null) throw new IOException("Inventory expected native absence differs");
                return;
            }
            if (cold.inventoryHandle == null) throw new IOException("Inventory original V4 slot disappeared");
            WindowsB1SourceAnchorStore.requireExactNativeChild(cold.raw.sourceAck.verifiedInput().worldNativePath(),
                io.finalPath(cold.inventoryHandle), name);
            for (int pass = 0; pass < 2; pass++)
                io.compareWitnessArtifactExact(cold.inventoryHandle, known.bytes, known.identity, 65536L);
            io.closeChecked(cold.inventoryHandle); cold.inventoryHandle = null;
        } catch (Throwable primary) {
            cold.retainedFailure = primary;
            WindowsMarkerIo.closeSuppressed(cold.inventoryHandle, primary);
            throw propagate(primary);
        }
    }
    private void requireNoV5Continuation(SourceScope source)throws IOException {
        absent(source,".floatingpod-b1-resume-v5.0");absent(source,".floatingpod-b1-resume-v5.1");
        absent(source,".floatingpod-b1-resume-v5.tmp");
    }
    private UUID operation(SourceScope s){return s.custody.freshProvisioningProof().plan().bootstrapOperationUuid();}
    private WindowsMarkerIo.Handle openExisting(Path path,int access,UUID op)throws IOException {
        try{return io.open(path,access,0,WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,op);}
        catch(WindowsMarkerIo.NativeFailure failure){if(failure.win32Error()==2)return null;throw failure;}
    }
    private void absent(SourceScope s,String name)throws IOException {
        WindowsMarkerIo.Handle handle=null;
        try{handle=openExisting(s.custody.directory().resolve(name),WindowsMarkerIo.READ,operation(s));
            if(handle!=null)throw new IOException("Fixed B1 file exists; conflict retained: "+name);
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(handle,failure);throw propagate(failure);}
    }
    private void requireSourceRecord(SourceScope s,B1ResumeRecord r)throws IOException {
        B1ResumeRecordCodec.exact(HexFormat.of().parseHex(s.sourceAck.anchorSha256()),r.sourceAnchorSha256(),"Resume record source anchor differs");
        B1ResumeRecordCodec.exact(s.sourceAck.verifiedInput().originalReceiptCanonicalBytes(),r.originalReceipt(),"Resume record original receipt differs");
        if(!s.sourceAck.verifiedInput().barrierNamespace().equals(r.barrierNamespace()))throw new IOException("Resume namespace differs from anchored configuration");
    }
    private StoredRecordFacts readSlot(SourceScope s,int slot)throws IOException {
        WindowsMarkerIo.Handle handle=null;
        try{handle=openExisting(path(s,slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(handle==null)return null;
            var facts=readHeld(s,slot,handle);io.closeChecked(handle);handle=null;return facts;
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(handle,failure);throw propagate(failure);}
    }
    private StoredRecordFacts readHeld(SourceScope s,int slot,WindowsMarkerIo.Handle handle)throws IOException {
        int size=Math.toIntExact(io.payloadLength(handle));byte[] bytes=io.readPayloadExact(handle,size);var record=B1ResumeRecordCodec.decode(bytes);
        if(record.sequence()%2!=slot)throw new IOException("Sequence stored in wrong fixed slot");requireSourceRecord(s,record);
        WindowsB1SourceAnchorStore.requireExactNativeChild(s.sourceAck.verifiedInput().worldNativePath(),io.finalPath(handle),NAMES[slot]);
        var id=io.identity(handle);B1ResumeRecordCodec.exact(bytes,io.readPayloadExact(handle,size),"Full slot bytes changed");
        if(!id.equals(io.identity(handle)))throw new IOException("Slot native identity changed");return new StoredRecordFacts(s,slot,record,bytes,id);
    }
    public SelectedRecords readCurrent(SourceScope s)throws IOException {
        validateSource(s);absent(s,TEMP);for(String legacy:LEGACY)absent(s,legacy);
        var a=readSlot(s,0);var b=readSlot(s,1);SelectedRecords selected=select(a,b);s.custody.validateLive();return selected;
    }
    static SelectedRecords select(StoredRecordFacts a,StoredRecordFacts b)throws IOException {
        if(a==null)return new SelectedRecords(b,null);if(b==null)return new SelectedRecords(a,null);
        if(a.record.sequence()==b.record.sequence())throw new IOException("Same-sequence two-slot conflict");
        var older=a.record.sequence()<b.record.sequence()?a:b;var newer=older==a?b:a;
        B1ResumeRecordCodec.consecutive(older.record,older.bytes,newer.record);return new SelectedRecords(newer,older);
    }
    private void assertCurrentOwner(SourceScope s,FullStateAssertions state)throws IOException {
        var actual=io.captureCurrentProcessIdentity(operation(s));var owner=state.currentOwner();
        if(owner.pid()!=actual.pid()||owner.fileTime()!=actual.creationFileTime()||!owner.boot().equals(s.custody.bootId())||owner.generation()!=s.custody.generation())
            throw new IOException("Caller current owner assertions differ from actual live custody");
    }
    public NativeRecordDurability publishGenesis(SourceScope s,FullStateAssertions callerAssertions)throws IOException {
        validateSource(s);assertCurrentOwner(s,callerAssertions);
        var r=new B1ResumeRecord(0,new byte[32],Kind.GENESIS,HexFormat.of().parseHex(s.sourceAck.anchorSha256()),
            s.sourceAck.verifiedInput().originalReceiptCanonicalBytes(),s.sourceAck.verifiedInput().barrierNamespace(),callerAssertions);
        byte[] bytes=B1ResumeRecordCodec.encode(r);var records=readCurrent(s);
        if(records.current()!=null){if(records.current().record.sequence()!=0||records.inactive()!=null)throw new IOException("Genesis cannot replace existing recovery state");
            B1ResumeRecordCodec.exact(bytes,records.current().bytes,"Existing genesis differs from exact original assertions");return rebarrierCurrent(s,records.current());}
        return publishAbsent(s,0,r,bytes);
    }
    public NativeRecordDurability publishNext(SourceScope s,NativeRecordDurability currentAck,Kind kind,FullStateAssertions callerAssertions)throws IOException {
        validateSource(s);requireNoV5Continuation(s);assertCurrentOwner(s,callerAssertions);var current=requireCurrent(s,currentAck);long next=Math.addExact(current.record.sequence(),1);
        var r=new B1ResumeRecord(next,WindowsWorldProvisioner.digest(current.bytes),kind,current.record.sourceAnchorSha256(),current.record.originalReceipt(),current.record.barrierNamespace(),callerAssertions);
        byte[] bytes=B1ResumeRecordCodec.encode(r);B1ResumeRecordCodec.consecutive(current.record,current.bytes,r);
        // No overwrite: old inactive must have been retired through its exact checked path first.
        absent(s,NAMES[(int)(next%2)]);return publishAbsent(s,(int)(next%2),r,bytes);
    }
    private StoredRecordFacts requireCurrent(SourceScope s,NativeRecordDurability ack)throws IOException {
        if(ack==null||ack.facts.scope!=s)throw new IOException("Exact current native record ACK required");var selected=readCurrent(s);
        if(selected.current()==null)throw new IOException("Current anchor record missing");same(ack.facts,selected.current());return selected.current();
    }
    private static void same(StoredRecordFacts expected,StoredRecordFacts actual)throws IOException {
        if(expected.slot!=actual.slot||!expected.identity.equals(actual.identity))throw new IOException("Exact fixed slot native identity differs");
        B1ResumeRecordCodec.exact(expected.bytes,actual.bytes,"Exact full slot record differs");
    }
    public NativeRecordDurability rebarrierCurrent(SourceScope s,StoredRecordFacts expected)throws IOException {
        validateSource(s);if(expected==null||expected.scope!=s)throw new IOException("Exact issued stored facts required");
        var selected=readCurrent(s);if(selected.current()==null)throw new IOException("Current record absent");same(expected,selected.current());
        WindowsMarkerIo.Handle handle=null;
        try{handle=openExisting(path(s,expected.slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(handle==null)throw new IOException("Current record disappeared");
            var actual=barrierHeld(s,expected.slot,handle,expected);s.custody.validateLive();io.closeChecked(handle);handle=null;return new NativeRecordDurability(actual);
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(handle,failure);throw propagate(failure);}
    }
    private StoredRecordFacts barrierHeld(SourceScope s,int slot,WindowsMarkerIo.Handle handle,StoredRecordFacts expected)throws IOException {
        var before=readHeld(s,slot,handle);if(expected!=null)same(expected,before);io.flushChecked(handle);var after=readHeld(s,slot,handle);same(before,after);return after;
    }
    private NativeRecordDurability publishAbsent(SourceScope s,int slot,B1ResumeRecord record,byte[] bytes)throws IOException {
        WindowsMarkerIo.Handle temporary=null,canonical=null;
        try{validateSource(s);absent(s,TEMP);absent(s,NAMES[slot]);
            temporary=io.createMarkerHandle(s.custody.directory().resolve(TEMP),operation(s));io.metadata(temporary,false,0);
            WindowsB1SourceAnchorStore.requireExactNativeChild(s.sourceAck.verifiedInput().worldNativePath(),io.finalPath(temporary),TEMP);
            io.writePayload(temporary,bytes);io.metadata(temporary,false,bytes.length);var createdId=io.identity(temporary);io.flushChecked(temporary);
            io.moveWriteThroughNoReplace(s.custody.directory().resolve(TEMP),path(s,slot),operation(s));io.closeChecked(temporary);temporary=null;
            canonical=openExisting(path(s,slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(canonical==null)throw new IOException("Published canonical slot missing; no ACK");
            var actual=readHeld(s,slot,canonical);if(!createdId.equals(actual.identity))throw new IOException("Published slot identity differs from actual CREATE_NEW file");
            B1ResumeRecordCodec.exact(bytes,actual.bytes,"Published canonical differs from complete planned bytes");actual=barrierHeld(s,slot,canonical,actual);
            s.custody.validateLive();io.closeChecked(canonical);canonical=null;return new NativeRecordDurability(actual);
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(canonical,failure);WindowsMarkerIo.closeSuppressed(temporary,failure);throw propagate(failure);}
    }
    public RetiredInactiveSlot retireInactiveSlot(SourceScope s,NativeRecordDurability liveAnchorAck,StoredRecordFacts expectedInactive)throws IOException {
        validateSource(s);requireNoV5Continuation(s);var current=requireCurrent(s,liveAnchorAck);
        if(expectedInactive==null||expectedInactive.scope!=s||expectedInactive.slot==current.slot)throw new IOException("Exact inactive canonical facts required");
        B1ResumeRecordCodec.consecutive(expectedInactive.record,expectedInactive.bytes,current.record);
        WindowsMarkerIo.Handle anchor=null,inactive=null;
        try{anchor=openExisting(path(s,current.slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(anchor==null)throw new IOException("Live current anchor missing");
            current=barrierHeld(s,current.slot,anchor,current);
            inactive=openExisting(path(s,expectedInactive.slot),WindowsMarkerIo.READ|WindowsMarkerIo.DELETE,operation(s));if(inactive==null)throw new IOException("Expected inactive file missing before retirement");
            var actual=readHeld(s,expectedInactive.slot,inactive);same(expectedInactive,actual);B1ResumeRecordCodec.consecutive(actual.record,actual.bytes,current.record);
            s.custody.validateLive();io.deleteInactiveCanonicalChecked(inactive);io.closeChecked(inactive);inactive=null;
            absent(s,NAMES[expectedInactive.slot]);current=barrierHeld(s,current.slot,anchor,current);s.custody.validateLive();
            io.closeChecked(anchor);anchor=null;return new RetiredInactiveSlot(expectedInactive.slot,new NativeRecordDurability(current));
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(inactive,failure);WindowsMarkerIo.closeSuppressed(anchor,failure);throw propagate(failure);}
    }

    /** 新 custody 下的原槽续接；不暴露 SourceScope，不签发 SQL settlement。 */
    public static final class InstalledResumeRecordScope {
        private final WindowsB1TwoSlotStore store;
        private SourceScope source;
        private WindowsWorldProvisioner.ResumeInspection inspection;
        private DurableHandoffNativeAck predecessor;
        private NativeRecordDurability initial;
        private WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight v5Flight;
        private boolean ordinaryV4RetirementAttempted;
        private RetiredInactiveSlot ordinaryV4RetirementAck;
        private InstalledResumeRecordScope(WindowsB1TwoSlotStore store,SourceScope source,
                WindowsWorldProvisioner.ResumeInspection inspection,DurableHandoffNativeAck predecessor,
                NativeRecordDurability initial) {
            this(store,source,inspection,predecessor,initial,null);
        }
        private InstalledResumeRecordScope(WindowsB1TwoSlotStore store,SourceScope source,
                WindowsWorldProvisioner.ResumeInspection inspection,DurableHandoffNativeAck predecessor,
                NativeRecordDurability initial,WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight v5Flight) {
            this.store=store;this.source=source;this.inspection=inspection;this.predecessor=predecessor;
            this.initial=initial;this.v5Flight=v5Flight;
        }
        public WindowsWorldProvisioner.ProvisionedWorld installedWorld(){return inspection.installedWorld();}
        public void requireCurrent()throws IOException {store.validateInstalled(this);}
        public NativeRecordDurability initialRecord(){return initial;}
        public B1ResumeRecordV5 v5HandoffRecord()throws IOException {
            if(v5Flight==null)throw new IOException("Installed scope is not an actual V5 HANDOFF scope");
            return v5Flight.actualHandoffRecord().record();
        }
        public Throwable retainedFailure(){return inspection.installedContinuationFailure();}
    }
    // 仅同原字段身份核验；不得调用flight.store或完整scope验证。
    void requireInstalledWitnessAssociation(NativeWitnessScope source,VerifiedWritableCurrentV5 preinstall,
            MySqlFull20NativeTransfer transfer,InstalledResumeRecordScope installed)throws IOException {
        checkOwner();
        if(source==null||source.store!=this||source.thread!=Thread.currentThread()||preinstall==null||
                preinstall.issuer!=this||preinstall.witnessScope!=source||source.preinstallV5!=preinstall||
                source.fixedTransfer!=transfer||installed==null||installed.store!=this||source.installed!=installed)
            throw new IOException("Exact original installed V5 witness association required");
    }
    private void validateInstalled(InstalledResumeRecordScope scope)throws IOException {
        checkOwner();
        if(scope==null||scope.store!=this||scope.inspection==null||scope.inspection.installedRecordScope()!=scope
                ||scope.inspection.installedHandoffAcknowledgement()!=scope.predecessor)
            throw new IOException("Exact installed resume scope required");
        scope.inspection.requireInstalledWorld(scope.installedWorld());
        if(scope.retainedFailure()!=null)throw new IOException("Installed continuation failure retained",scope.retainedFailure());
        if(scope.v5Flight!=null) {
            if(scope.initial!=null||scope.predecessor.v5Flight!=scope.v5Flight||scope.predecessor.v5Store==null)
                throw new IOException("Installed V5 scope differs from exact retained V5 HANDOFF acknowledgement");
            scope.predecessor.v5Store.requireInstalledAssociation(this,scope,scope.v5Flight);
        } else if(scope.initial==null||scope.predecessor.v5Flight!=null)
            throw new IOException("Installed V4 scope requires its exact actual V4 predecessor");
        validateSource(scope.source);
    }
    private InstalledResumeRecordScope issueInstalledScope(DurableHandoffNativeAck ack,
            WindowsWorldProvisioner.ResumeInspection inspection,WindowsWorldProvisioner.ProvisionedWorld world)throws IOException {
        checkOwner();
        if(ack==null||ack.scope.store!=this||ack.scope.inspection!=inspection)
            throw new IOException("Exact original handoff ACK/store/inspection required");
        inspection.requireInstalledWorld(world);
        if(inspection.installedHandoffAcknowledgement()!=ack||ack.installedScopeAttempted)
            throw new IOException("Installed handoff continuation already attempted or foreign");
        ack.installedScopeAttempted=true;
        try {
            // 原 ResumeSourceScope 已失效；仅冻结来源和新实际 custody 可参与续接。
            var source=bindSource(ack.scope.anchorStore,ack.scope.location,ack.scope.sourceAck,world.bootWorldCustodyProof());
            var current=readCurrent(source).current();var previous=ack.durable.facts;
            if(current==null||current.slot!=previous.slot||!current.identity.equals(previous.identity))
                throw new IOException("Installed handoff fixed slot identity differs");
            B1ResumeRecordCodec.exact(previous.bytes,current.bytes,"Installed handoff complete predecessor differs");
            if(current.record.kind()!=Kind.HANDOFF_PREPARED
                    ||!current.record.assertions().currentOwner().equals(inspection.targetOwner())
                    ||!Objects.equals(current.record.assertions().previousOwner(),inspection.previousOwner()))
                throw new IOException("Installed handoff owner chain differs");
            var initial=rebarrierCurrent(source,current);
            var issued=new InstalledResumeRecordScope(this,source,inspection,ack,initial);
            inspection.retainInstalledRecordScope(issued);
            for(var witnessScope:ack.scope.witnessScopes)witnessScope.associateInstalled(issued);
            return issued;
        } catch(Throwable primary) {
            inspection.retainInstalledContinuationFailure(primary);throw propagate(primary);
        }
    }
    private InstalledResumeRecordScope issueInstalledV5Scope(DurableHandoffNativeAck ack,
            WindowsWorldProvisioner.ResumeInspection inspection,WindowsWorldProvisioner.ProvisionedWorld world)throws IOException {
        checkOwner();
        if(ack==null||ack.v5Flight==null||ack.v5Store==null||ack.scope==null||ack.scope.store!=this||
                ack.scope.inspection!=inspection)throw new IOException("Exact V5 handoff ACK/store/inspection required");
        inspection.requireInstalledWorld(world);
        if(inspection.installedHandoffAcknowledgement()!=ack||ack.installedScopeAttempted)
            throw new IOException("Installed V5 handoff continuation already attempted or foreign");
        // The exact V5 ACK was validated before owner mutation; revalidating its preinstall guard here would be stale.
        ack.installedScopeAttempted=true;
        try {
            var source=bindSource(ack.scope.anchorStore,ack.scope.location,ack.scope.sourceAck,world.bootWorldCustodyProof());
            var issued=new InstalledResumeRecordScope(this,source,inspection,ack,null,ack.v5Flight);
            inspection.retainInstalledRecordScope(issued);
            ack.v5Store.associateInstalledScope(ack.v5Flight,issued);
            return issued;
        } catch(Throwable primary) {
            inspection.retainInstalledContinuationFailure(primary);throw propagate(primary);
        }
    }

    public SelectedRecords readCurrent(InstalledResumeRecordScope scope)throws IOException {
        validateInstalled(scope);return readCurrent(scope.source);
    }
    public NativeRecordDurability rebarrierCurrent(InstalledResumeRecordScope scope,StoredRecordFacts facts)throws IOException {
        validateInstalled(scope);return rebarrierCurrent(scope.source,facts);
    }
    public NativeRecordDurability publishNext(InstalledResumeRecordScope scope,NativeRecordDurability current,
            Kind kind,FullStateAssertions assertions)throws IOException {
        validateInstalled(scope);
        try{return publishNext(scope.source,current,kind,assertions);}
        catch(Throwable primary){scope.inspection.retainInstalledContinuationFailure(primary);throw propagate(primary);}
    }
    public RetiredInactiveSlot retireInactiveSlot(InstalledResumeRecordScope scope,NativeRecordDurability current,
            StoredRecordFacts inactive)throws IOException {
        validateInstalled(scope);
        try{return retireInactiveSlot(scope.source,current,inactive);}
        catch(Throwable primary){scope.inspection.retainInstalledContinuationFailure(primary);throw propagate(primary);}
    }

    /** Retires the exact V4 predecessor only after the same actor's committed W readback and post-audit. */
    public RetiredInactiveSlot retireOrdinaryV4SourceAfterCommit(InstalledResumeRecordScope scope,
            com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor.CommittedCertifiedHandoff committed)throws IOException {
        validateInstalled(scope);
        if(scope.v5Flight!=null||scope.initial==null||scope.ordinaryV4RetirementAttempted||committed==null)
            throw new IOException("One exact installed V4 ordinary post-commit retirement is required");
        scope.ordinaryV4RetirementAttempted=true;
        try {
            committed.requireForNativeContinuation(null,scope);
            var selected=readCurrent(scope);
            StoredRecordFacts current=selected.current(),original=selected.inactive();
            if(current==null||original==null||current.record.kind()!=Kind.HANDOFF_PREPARED||
                    !Arrays.equals(current.bytes,scope.initial.facts.bytes)||
                    !current.identity.equals(scope.initial.facts.identity))
                throw new IOException("Installed V4 current is not the exact durable HANDOFF predecessor");
            committed.requireOriginalV4Predecessor(original.record,original.canonicalBytes(),original.fileIdentity());
            RetiredInactiveSlot actual=retireInactiveSlot(scope,scope.initial,original);
            if(actual==null||actual.retainedRecord()==null)
                throw new IOException("Actual original V4 retirement returned no retained current barrier");
            scope.ordinaryV4RetirementAck=actual;
            return actual;
        } catch(Throwable primary) {
            scope.inspection.retainInstalledContinuationFailure(primary);
            throw propagate(primary);
        }
    }

    /** The old V4 current is retired only after an opaque same-actor SQL-commit/post-audit permit. */
    public NativeRecordDurability publishSettledOrdinaryDbAcquired(InstalledResumeRecordScope scope,
            com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor.CommittedCertifiedHandoff committed,
            B1ResumeRecord settled)throws IOException {
        validateInstalled(scope);
        if(scope.v5Flight!=null||scope.initial==null||committed==null||settled==null||
                settled.kind()!=Kind.DB_ACQUIRED||settled.assertions().pendingAttempt()!=null||
                settled.assertions().latestSettlement()==null||
                settled.assertions().latestSettlement().kind()!=B1ResumeRecord.SettlementKind.CONFIRMED)
            throw new IOException("Exact installed V4 HANDOFF and actual settled DB_ACQUIRED target are required");
        try {
            committed.requireForNativeContinuation(null,scope);
            committed.requireSettledRecord(settled);
            SelectedRecords selected=readCurrent(scope);
            StoredRecordFacts current=selected.current();
            StoredRecordFacts original=selected.inactive();
            if(current==null||original==null||current.record.kind()!=Kind.HANDOFF_PREPARED||
                    !Arrays.equals(current.bytes,scope.initial.facts.bytes)||
                    !current.identity.equals(scope.initial.facts.identity))
                throw new IOException("Installed V4 current is not the exact actual HANDOFF predecessor");
            committed.requireOriginalV4Predecessor(original.record,original.canonicalBytes(),original.fileIdentity());
            RetiredInactiveSlot retired=retireInactiveSlot(scope,scope.initial,original);
            NativeRecordDurability actual=publishNext(scope,retired.retainedRecord(),Kind.DB_ACQUIRED,settled.assertions());
            if(actual==null||!Arrays.equals(B1ResumeRecordCanonical.encode(actual.facts.record),
                    B1ResumeRecordCanonical.encode(settled)))
                throw new IOException("Published V4 DB_ACQUIRED readback differs from the exact actor-settled transition");
            return actual;
        } catch(Throwable primary) {
            scope.inspection.retainInstalledContinuationFailure(primary);
            throw propagate(primary);
        }
    }

    /** Separate authority: old owner remains in guard; only fixed handoff publication is permitted. */
    public static final class ResumeSourceScope {
        private final WindowsB1TwoSlotStore store;private final WindowsB1SourceAnchorStore anchorStore;
        private final WindowsB1SourceAnchorStore.ControlledAnchorLocation location;
        private WindowsB1SourceAnchorStore.DurableB1SourceAnchor sourceAck;
        private final WindowsWorldProvisioner.ResumeInspection inspection;
        private final java.util.ArrayList<NativeWitnessScope> witnessScopes=new java.util.ArrayList<>(1);
        private final java.util.ArrayList<WindowsB1Full20WitnessStore> witnessStores=new java.util.ArrayList<>(1);
        private Full20HistoricalDiscoveryPermit historicalDiscoveryPermit;
        private ResumeSourceScope(WindowsB1TwoSlotStore store,WindowsB1SourceAnchorStore anchorStore,WindowsB1SourceAnchorStore.ControlledAnchorLocation location,WindowsB1SourceAnchorStore.DurableB1SourceAnchor ack,WindowsWorldProvisioner.ResumeInspection inspection){this.store=store;this.anchorStore=anchorStore;this.location=location;sourceAck=ack;this.inspection=inspection;}
    }
    public static final class ResumeStoredRecordFacts {
        private final ResumeSourceScope scope;private final int slot;private final B1ResumeRecord record;private final byte[] bytes;private final WindowsMarkerIo.FileIdentity identity;
        private ResumeStoredRecordFacts(ResumeSourceScope scope,int slot,B1ResumeRecord record,byte[] bytes,WindowsMarkerIo.FileIdentity identity){this.scope=scope;this.slot=slot;this.record=record;this.bytes=bytes.clone();this.identity=identity;}
        public B1ResumeRecord record(){return record;}public int slot(){return slot;}public byte[] canonicalBytes(){return bytes.clone();}public String fileIdentity(){return identity.versionedIdentity();}
        public int canonicalByteLength(){return bytes.length;}
    }
    public record ResumeSelectedRecords(ResumeStoredRecordFacts current,ResumeStoredRecordFacts inactive){}
    public static final class ResumeNativeRecordDurability {
        private final ResumeStoredRecordFacts facts;private ResumeNativeRecordDurability(ResumeStoredRecordFacts facts){this.facts=facts;}public ResumeStoredRecordFacts facts(){return facts;}
    }
    public static final class ResumeRetiredInactiveSlot {
        private final int slot;private final ResumeNativeRecordDurability retained;
        private ResumeRetiredInactiveSlot(int slot,ResumeNativeRecordDurability retained){this.slot=slot;this.retained=retained;}
        public int absentSlot(){return slot;}public ResumeNativeRecordDurability retainedRecord(){return retained;}
    }
    public static final class DurableHandoffNativeAck {
        private final ResumeSourceScope scope;
        private final ResumeNativeRecordDurability durable;
        private final WindowsB1V5WitnessRecordStore v5Store;
        private final WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight v5Flight;
        private final WindowsB1V5WitnessRecordStore.NativeV5RecordDurability v5Durable;
        private boolean installedScopeAttempted;
        InstalledResumeRecordScope issueInstalledScope(WindowsWorldProvisioner.ResumeInspection inspection,
                WindowsWorldProvisioner.ProvisionedWorld world)throws IOException {
            if(v5Flight!=null)return scope.store.issueInstalledV5Scope(this,inspection,world);
            return scope.store.issueInstalledScope(this,inspection,world);
        }
        private DurableHandoffNativeAck(ResumeSourceScope scope,ResumeNativeRecordDurability durable) {
            this.scope=scope;this.durable=durable;this.v5Store=null;this.v5Flight=null;this.v5Durable=null;
        }
        private DurableHandoffNativeAck(ResumeSourceScope scope,WindowsB1V5WitnessRecordStore v5Store,
                WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight v5Flight,
                WindowsB1V5WitnessRecordStore.NativeV5RecordDurability v5Durable) {
            this.scope=scope;this.durable=null;this.v5Store=v5Store;this.v5Flight=v5Flight;this.v5Durable=v5Durable;
        }
        public B1ResumeRecord record() {return v5Flight==null?durable.facts.record():v5Durable.record().transition();}
        public byte[] canonicalBytes()throws IOException {
            return v5Flight==null?durable.facts.canonicalBytes():v5Durable.canonicalBytes();
        }
        void validateForInstallation(WindowsWorldProvisioner.ResumeInspection inspection)throws IOException {
            if(scope==null||scope.inspection!=inspection)throw new IOException("Handoff ACK belongs to different inspection");
            if(v5Flight!=null) {
                v5Store.validateForInstallation(v5Flight,inspection);
                if(v5Durable!=v5Flight.actualHandoffRecord())throw new IOException("V5 install ACK differs from actual HANDOFF current");
            } else {
                scope.store.validateResumeSource(scope);scope.store.requireCurrent(scope,durable);
            }
        }
    }

    public static final class DurablePreGenesisNativeAck {
        private final ResumeSourceScope scope;private final ResumeNativeRecordDurability durable;
        private DurablePreGenesisNativeAck(ResumeSourceScope scope,ResumeNativeRecordDurability durable){this.scope=scope;this.durable=durable;}
        public B1ResumeRecord record(){return durable.facts.record();}
        public byte[] canonicalBytes(){return durable.facts.canonicalBytes();}
        void validateForInstallation(WindowsWorldProvisioner.ResumeInspection inspection)throws IOException {
            if(scope.inspection!=inspection)throw new IOException("Pre-genesis ACK belongs to different inspection");
            scope.store.requireSolePreGenesis(scope,durable);
        }
    }
    private ResumeNativeRecordDurability requireSolePreGenesis(ResumeSourceScope s,ResumeNativeRecordDurability ack)throws IOException {
        validateResumeSource(s);absent(s,TEMP);absent(s,NAMES[1]);for(String name:LEGACY)absent(s,name);
        var current=requireCurrent(s,ack);
        if(current.slot!=0||current.record.sequence()!=0||current.record.kind()!=Kind.GENESIS||!Arrays.equals(current.record.previousRecordSha256(),new byte[32]))throw new IOException("Exact sole sequence0 GENESIS required");
        var assertions=current.record.assertions();if(!assertions.currentOwner().equals(s.inspection.targetOwner())||!Objects.equals(assertions.previousOwner(),s.inspection.previousOwner()))throw new IOException("Pre-genesis native owner linkage changed");
        return rebarrierCurrent(s,current);
    }
    public DurablePreGenesisNativeAck publishPreGenesisRecovery(ResumeSourceScope s,FullStateAssertions assertions)throws IOException {
        validateResumeSource(s);Objects.requireNonNull(assertions);
        if(!assertions.currentOwner().equals(s.inspection.targetOwner())||!Objects.equals(assertions.previousOwner(),s.inspection.previousOwner()))throw new IOException("Pre-genesis owners must actual successor and full old guarded owner");
        var attempt=assertions.pendingAttempt();var plan=s.inspection.freshProvisioningProof().plan();
        if(attempt==null||attempt.phase()!=Phase.PREPARE||attempt.bootGeneration()!=s.inspection.targetOwner().generation()||!attempt.context().boot().equals(s.inspection.targetOwner().boot())||!attempt.logicalWriteUuid().equals(plan.prepareWriteUuid())||!attempt.context().operation().equals(plan.prepareWriteUuid())||!attempt.context().binding().equals(plan.bindingUuid()))throw new IOException("Complete fixed PREPARE same-target context required");
        if(assertions.originalPrepareWitness()!=null||assertions.acknowledgedCertifyWitness()!=null||assertions.latestSettlement()!=null||assertions.marker()!=null)throw new IOException("Pre-genesis cannot invent prior SQL or marker history");
        absent(s,NAMES[0]);absent(s,NAMES[1]);absent(s,TEMP);for(String name:LEGACY)absent(s,name);
        var record=new B1ResumeRecord(0,new byte[32],Kind.GENESIS,HexFormat.of().parseHex(s.sourceAck.anchorSha256()),s.sourceAck.verifiedInput().originalReceiptCanonicalBytes(),s.sourceAck.verifiedInput().barrierNamespace(),assertions);
        byte[] bytes=B1ResumeRecordCodec.encode(record); // Actual existing V4 completecodec and64KiB bound before mutation.
        s.inspection.beginPreGenesisPublication();
        try {var durable=publishAbsent(s,0,record,bytes);durable=requireSolePreGenesis(s,durable);return new DurablePreGenesisNativeAck(s,durable);}
        catch(Throwable primary){s.inspection.failHandoff(primary);throw propagate(primary);}
    }

    public ResumeSourceScope bindResumeSource(WindowsB1SourceAnchorStore anchorStore,WindowsB1SourceAnchorStore.ControlledAnchorLocation location,WindowsB1SourceAnchorStore.DurableB1SourceAnchor ack,WindowsWorldProvisioner.ResumeInspection inspection)throws IOException {
        checkOwner();var s=new ResumeSourceScope(this,Objects.requireNonNull(anchorStore),Objects.requireNonNull(location),Objects.requireNonNull(ack),Objects.requireNonNull(inspection));validateResumeSource(s);return s;
    }
    /** 只读冷类型；不会向 caller 暴露内部原 ResumeSourceScope。 */
    public static final class ColdResumeSourceScope {
        private final WindowsB1TwoSlotStore store;
        private final ResumeSourceScope raw;
        private com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting;
        private final java.util.ArrayList<ColdV4Read> retainedV4Reads=new java.util.ArrayList<>(10);
        private WindowsMarkerIo.Handle retainedV4Handle;
        private Throwable retainedFailure;
        private WindowsB1ColdResumeLookup retainedLookup;
        private final java.util.ArrayList<VerifiedV5SlotFacts> retainedV5PreinstallSlots=new java.util.ArrayList<>(2);
        private WindowsMarkerIo.Handle retainedV5PreinstallHandle;
        private String retainedV5PreinstallName;
        private byte[] retainedV5PreinstallFirst,retainedV5PreinstallSecond;
        private WindowsMarkerIo.Handle inventoryHandle;
        private Full20HistoricalDiscoveryPermit discoveryPermit;
        private ColdResumeSourceScope(WindowsB1TwoSlotStore store,ResumeSourceScope raw){this.store=store;this.raw=raw;}
    }
    private static final String[] V5_NAMES={".floatingpod-b1-resume-v5.0",".floatingpod-b1-resume-v5.1"};
    private static final class VerifiedV5SlotFacts {
        final int slot;
        final B1ResumeRecordV5 record;
        final byte[] bytes;
        final WindowsMarkerIo.FileIdentity identity;
        VerifiedV5SlotFacts(int slot,B1ResumeRecordV5 record,byte[] bytes,WindowsMarkerIo.FileIdentity identity) {
            this.slot=slot;this.record=record;this.bytes=bytes;this.identity=identity;
        }
    }
    /** Actual same-boot inspection plus complete V5 container and referenced artifact rebarriers. */
    public static final class VerifiedWritableCurrentV5 {
        private final WindowsB1TwoSlotStore issuer;
        private ColdResumeSourceScope cold;
        private ResumeStoredRecordFacts v4Bridge;
        private WindowsB1ColdResumeLookup lookup;
        private WindowsB1ColdResumeLookup.NativeCurrentInventoryAck inventory;
        private com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting;
        private VerifiedV5SlotFacts current,inactive;
        private final java.util.ArrayList<MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource> actualArtifactSources=new java.util.ArrayList<>(4);
        private final java.util.ArrayList<WindowsB1Full20WitnessStore.VerifiedArtifactReference> artifacts=new java.util.ArrayList<>(4);
        private WindowsMarkerIo.Handle artifactReadHandle;
        private byte[] artifactReadFirst,artifactReadSecond,artifactReadThird;
        private Throwable artifactReadFailure;
        private final Thread owner=Thread.currentThread();
        private MySqlFull20NativeTransfer fixedTransfer;
        private NativeWitnessScope witnessScope;
        private boolean scopeIssued,installed;
        private WindowsB1V5HistoricalLookup history;
        private Throwable failure;
        private VerifiedWritableCurrentV5(WindowsB1TwoSlotStore issuer,ColdResumeSourceScope cold,
                ResumeStoredRecordFacts v4Bridge,WindowsB1ColdResumeLookup lookup,
                WindowsB1ColdResumeLookup.NativeCurrentInventoryAck inventory,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting,
                VerifiedV5SlotFacts current,VerifiedV5SlotFacts inactive,
                java.util.List<MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource> artifactSources) {
            this.issuer=issuer;this.cold=cold;this.v4Bridge=v4Bridge;this.lookup=lookup;this.inventory=inventory;
            this.accounting=accounting;this.current=current;this.inactive=inactive;this.actualArtifactSources.addAll(artifactSources);
        }
        private void bindReferences()throws IOException {
            if(!artifacts.isEmpty())throw new IOException("Actual preinstall V5 references are already bound");
            for(var reference:current.record.allReferences()) {
                if(reference==null)continue;
                MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource actual=null;
                for(var candidate:actualArtifactSources)if(candidate.reference().exactlyEquals(reference)){
                    if(actual!=null)throw new IOException("Actual V5 source has duplicate full20 artifact capability");
                    actual=candidate;
                }
                if(actual==null)throw new IOException("Actual V5 full20 source reference lacks its private complete-read capability");
                artifacts.add(new PreinstallV5ArtifactReference(this,reference,actual));
            }
            if(artifacts.size()!=actualArtifactSources.size())throw new IOException("Actual V5 full20 source capability set differs from complete reference topology");
        }
        private void requireOwner()throws IOException {
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact verified V5 preinstall owner required");
            if(accounting==null)throw new IOException("Original V5 preinstall consumers ended");
            if(failure!=null)throw new IOException("Verified V5 preinstall source failure retained",failure);
            issuer.requireVerifiedV5Current(this);
        }
        WindowsWorldProvisioner.ResumeInspection inspectionForStore() throws IOException {
            requireOwner();return cold.raw.inspection;
        }
        public B1ResumeRecordV5 currentRecord()throws IOException {requireOwner();return current.record;}
        public byte[] currentCanonicalBytes()throws IOException {requireOwner();return accounting.copyCompletedV5SourceBytes(current.bytes);}
        public WindowsMarkerIo.FileIdentity currentIdentity()throws IOException {requireOwner();return current.identity;}
        public boolean hasInactiveRecord()throws IOException {requireOwner();return inactive!=null;}
        public B1ResumeRecordV5 inactiveRecord()throws IOException {requireOwner();return inactive==null?null:inactive.record;}
        public byte[] inactiveCanonicalBytes()throws IOException {requireOwner();return inactive==null?null:accounting.copyCompletedV5SourceBytes(inactive.bytes);}
        public WindowsMarkerIo.FileIdentity inactiveIdentity()throws IOException {requireOwner();return inactive==null?null:inactive.identity;}
        public ResumeStoredRecordFacts v4Bridge()throws IOException {requireOwner();return v4Bridge;}
        public java.util.List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> artifactReferences()throws IOException {
            requireOwner();return java.util.List.copyOf(artifacts);
        }
        void requireReference(B1Full20WitnessReference reference)throws IOException {
            requireOwner();
            if(reference==null)throw new IOException("Actual V5 full20 reference required");
            boolean found=false;
            for(var ref:current.record.allReferences())if(ref!=null&&ref.exactlyEquals(reference))found=true;
            if(!found)throw new IOException("Full20 reference is not present in the actual settled V5 current source");
        }
        B1ResumeRecord.AttemptAssertions exactAttempt(B1Full20WitnessReference reference)throws IOException {
            requireReference(reference);
            var state=current.record.transition().assertions();
            for(var attempt:new B1ResumeRecord.AttemptAssertions[]{state.originalPrepareWitness(),
                    state.pendingAttempt(),state.latestSettlement()==null?null:state.latestSettlement().settledAttempt(),
                    state.acknowledgedCertifyWitness()})
                if(attempt!=null&&attempt.attemptId().equals(reference.attemptId()))return attempt;
            throw new IOException("Actual V5 reference has no matching complete attempt assertion");
        }
        void bindTransfer(MySqlFull20NativeTransfer transfer,NativeWitnessScope scope)throws IOException {
            requireOwner();
            if(scopeIssued||transfer==null||scope==null||scope.preinstallV5!=this)
                throw new IOException("Exact planned V5 transfer scope can be bound only once");
            if(transfer.originalRecord().kind()!=Kind.HANDOFF_PREPARED||
                    transfer.originalRecord().assertions().pendingAttempt()==null)
                throw new IOException("Exact planned HANDOFF transfer required for V5 preinstall");
            var planned=transfer.originalRecord();
            if(planned.sequence()!=Math.addExact(current.record.transition().sequence(),1L)||
                    !Arrays.equals(planned.previousRecordSha256(),WindowsWorldProvisioner.digest(
                        B1ResumeRecordCanonical.encode(current.record.transition()))))
                throw new IOException("Planned V5 HANDOFF does not follow exact actual current source transition");
            this.fixedTransfer=transfer;this.witnessScope=scope;scopeIssued=true;
        }
        void bindInstalled(NativeWitnessScope scope)throws IOException {
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact V5 preinstall owner required");
            if(failure!=null)throw new IOException("V5 source failure retained",failure);
            if(!scopeIssued||witnessScope!=scope||installed||scope==null||scope.installed==null)
                throw new IOException("Exact V5 source scope install association is one-shot");
            installed=true;
        }
        ResumeSourceScope resumeSourceForInstall()throws IOException {
            if(Thread.currentThread()!=owner||failure!=null||cold==null||cold.raw==null)
                throw new IOException("Exact actual preinstall resume source is required",failure);
            return cold.raw;
        }
        void requireForReference(PreinstallV5ArtifactReference reference)throws IOException {
            requireOwner();
            if(reference==null||reference.current!=this||reference.actualSource==null||
                    !reference.actualSource.reference().exactlyEquals(reference.reference))
                throw new IOException("Exact source artifact reference and retained full20 payload capability required");
            reference.actualSource.requireCurrent();
            if(installed) {
                if(witnessScope==null||witnessScope.installed==null)throw new IOException("Installed V5 source reference lacks new custody");
                issuer.validateInstalled(witnessScope.installed);
                B1ResumeRecordV5 handoff=witnessScope.installed.v5HandoffRecord();
                boolean retained=false;for(B1Full20WitnessReference actual:handoff.allReferences())
                    if(actual!=null&&actual.exactlyEquals(reference.reference))retained=true;
                if(!retained)throw new IOException("Installed HANDOFF V5 record does not retain this exact original artifact DATA reference");
                issuer.revalidatePreinstallV5Reference(this,reference);
            } else inventory.requireCurrent();
        }
            private void detachOriginalTerminalConsumers(MySqlCertifiedRecoveryInspectionActor actor,
                MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state,
                MySqlFull20NativeTransfer transfer)throws IOException {
            if(Thread.currentThread()!=owner||accounting==null||fixedTransfer!=transfer||!installed||witnessScope==null)
                throw new IOException("Exact installed V5 source terminal owner required");
            if(failure!=null)throw new IOException("Original V5 source failure retained",failure);
            if(artifactReadFailure!=null)throw new IOException("Original artifact read failure retained",artifactReadFailure);
            if(artifactReadHandle!=null||artifactReadFirst!=null||artifactReadSecond!=null||artifactReadThird!=null)
                throw new IOException("Original V5 native artifact window remains live");
            accounting.requireOriginalTerminalIdentity(actor,state);
            requireColdTerminal(cold,actor,state);
            for(int i=0;i<artifacts.size();i++)
                if(!(artifacts.get(i) instanceof PreinstallV5ArtifactReference exact)||exact.current!=this)
                    throw new IOException("Original private V5 artifact registration changed");
            for(int i=0;i<artifacts.size();i++)
                if(artifacts.get(i) instanceof PreinstallV5ArtifactReference exact)exact.actualSource=null;
            artifacts.clear();actualArtifactSources.clear();
            detachColdTerminal(cold);
            accounting=null;cold=null;lookup=null;inventory=null;v4Bridge=null;current=null;inactive=null;
            fixedTransfer=null;witnessScope=null;history=null;
        }
    }
    private static final class PreinstallV5ArtifactReference implements WindowsB1Full20WitnessStore.VerifiedArtifactReference {
        private final VerifiedWritableCurrentV5 current;
        private final B1Full20WitnessReference reference;
        private MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource actualSource;
        private final Thread owner=Thread.currentThread();
        private PreinstallV5ArtifactReference(VerifiedWritableCurrentV5 current,B1Full20WitnessReference reference,
                MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource actualSource) {
            this.current=current;this.reference=reference;this.actualSource=actualSource;
        }
        public B1Full20WitnessReference reference(){return reference;}
        public void requireExactAttempt(AttemptAssertions attempt)throws IOException {
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact preinstall V5 artifact owner required");
            var actual=current.exactAttempt(reference);
            if(attempt==null)throw new IOException("Exact original source attempt required");
            B1ResumeRecordCodec.exact(B1ResumeRecordCodec.attemptBytes(attempt),B1ResumeRecordCodec.attemptBytes(actual),
                "Preinstall V5 reference differs from actual complete source attempt/context/writer/common graphs");
        }
        public void revalidateForReference()throws IOException {current.requireForReference(this);}
    }

    private static B1ResumeRecord.AttemptAssertions attemptForReference(B1ResumeRecordV5 record,
            B1Full20WitnessReference reference)throws IOException {
        if(record==null||reference==null)throw new IOException("Exact V5 record/reference required");
        var state=record.transition().assertions();
        var candidates=new B1ResumeRecord.AttemptAssertions[]{state.originalPrepareWitness(),state.pendingAttempt(),
            state.latestSettlement()==null?null:state.latestSettlement().settledAttempt(),state.acknowledgedCertifyWitness()};
        B1ResumeRecord.AttemptAssertions found=null;
        for(var candidate:candidates)if(candidate!=null&&candidate.attemptId().equals(reference.attemptId())) {
            if(candidate.phase()!=reference.phase()||!candidate.logicalWriteUuid().equals(reference.logicalOperation()))
                throw new IOException("V5 reference differs from complete source attempt identity");
            if(found!=null&&!Arrays.equals(B1ResumeRecordCanonical.encodeAttemptData(found),
                    B1ResumeRecordCanonical.encodeAttemptData(candidate)))
                throw new IOException("V5 source has conflicting complete attempt assertions for one reference");
            found=candidate;
        }
        if(found==null)throw new IOException("V5 full20 reference lacks an exact source attempt");
        return found;
    }

    /** Existing settled V5 source is re-read under a separate exact parent-ledger source pass. */
    public VerifiedWritableCurrentV5 resumeVerifiedCurrentV5ForHandoff(ColdResumeSourceScope cold,
            ResumeStoredRecordFacts selectedV4,com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting,
            MySqlFull20NativeTransfer fixedTransfer)throws IOException {
        WindowsB1ColdResumeLookup lookup=null;
        WindowsB1ColdResumeLookup.NativeCurrentInventoryAck inventory=null;
        VerifiedV5SlotFacts first=null,second=null;
        boolean passStarted=false;
        try {
            validateColdSource(cold);
            if(cold.accounting!=accounting||selectedV4==null||selectedV4.scope!=cold.raw)
                throw new IOException("Exact original cold V4 bridge/accounting required for V5 continuation");
            lookup=cold.retainedLookup;
            if(lookup==null||lookup.observation().status()!=WindowsB1ColdResumeLookup.Status.RESOLVED_DATA||
                    lookup.observation().currentRecord()==null)
                throw new IOException("Only an actual resolved settled V5 cold source can become writable preinstall DATA");
            if(fixedTransfer!=null) {
                if(fixedTransfer.originalRecord().kind()!=Kind.HANDOFF_PREPARED)
                    throw new IOException("Exact actor-planned HANDOFF transfer required before writable V5 preinstall binding");
                var planned=fixedTransfer.originalRecord();
                var attempt=planned.assertions().pendingAttempt();
                if(attempt==null||attempt.phase()!=Phase.HANDOFF||
                        !planned.assertions().currentOwner().equals(cold.raw.inspection.targetOwner())||
                        !Objects.equals(planned.assertions().previousOwner(),cold.raw.inspection.previousOwner())||
                        !planned.barrierNamespace().equals(selectedV4.record.barrierNamespace()))
                    throw new IOException("Planned HANDOFF owner/source namespace differs from the actual inspection");
            }
            inventory=lookup.revalidateCurrentInventory();
            inventory.requireCurrent();
            int worldLength=cold.raw.sourceAck.verifiedInput().worldNativePath().length();
            accounting.beginV5PreinstallPass(worldLength);passStarted=true;
            first=readPreinstallV5Slot(cold,accounting,0);if(first!=null)cold.retainedV5PreinstallSlots.add(first);
            second=readPreinstallV5Slot(cold,accounting,1);if(second!=null)cold.retainedV5PreinstallSlots.add(second);
            VerifiedV5SlotFacts current=first==null?second:second==null?first:
                first.record.transition().sequence()>second.record.transition().sequence()?first:second;
            VerifiedV5SlotFacts inactive=(first==null||second==null)?null:(current==first?second:first);
            if(current==null)throw new IOException("Resolved V5 source lost both fixed slots during complete native reread");
            B1ResumeRecordV5 observed=lookup.observation().currentRecord();
            B1ResumeRecordCodec.exact(B1ResumeRecordV5.encodeData(observed),current.bytes,
                "Complete actual preinstall V5 current bytes differ from same cold inspection");
            if(current.record.transition().kind()!=Kind.CERTIFIED&&current.record.transition().kind()!=Kind.DB_ACQUIRED)
                throw new IOException("Only settled actual current V5 may start an ordinary handoff");
            var state=current.record.transition().assertions();
            if(state.pendingAttempt()!=null||state.currentOwner()==null||state.targetWriter()==null||
                    state.originalPrepareWitness()==null||state.acknowledgedCertifyWitness()==null||
                    state.latestSettlement()!=null&&state.latestSettlement().kind()!=SettlementKind.CONFIRMED)
                throw new IOException("Existing V5 current source has pending/unknown or incomplete settled state");
            WindowsB1ColdResumeLookup.requireBridgeData(current.record,selectedV4.record,selectedV4.bytes,
                selectedV4.identity.versionedIdentity(),cold.raw.inspection.previousOwner());
            if(inactive!=null) {
                B1ResumeRecordV5.consecutive(inactive.record,inactive.bytes,current.record);
                WindowsB1ColdResumeLookup.requireBridgeData(inactive.record,selectedV4.record,selectedV4.bytes,
                    selectedV4.identity.versionedIdentity(),cold.raw.inspection.previousOwner());
            } else if(current.record.transition().sequence()!=Math.addExact(selectedV4.record.sequence(),1L))
                throw new IOException("Resolved single V5 slot lacks its actual consecutive V4 predecessor");
            if(current.record.transition().sequence()==Math.addExact(selectedV4.record.sequence(),1L)) {
                B1ResumeRecordCodec.consecutive(selectedV4.record,selectedV4.bytes,current.record.transition());
                B1ResumeRecordCodec.exact(WindowsWorldProvisioner.digest(selectedV4.bytes),current.record.previousContainerSha256(),
                    "First V5 container differs from complete V4 bridge bytes");
            }
            var artifactSources=new java.util.ArrayList<MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource>(4);
            var currentReferences=current.record.allReferences();
            for(B1Full20WitnessReference reference:currentReferences) {
                if(reference==null)continue;
                WindowsB1ColdResumeLookup.ArtifactObservation observedArtifact=null;
                for(var candidate:lookup.observation().artifacts())
                    if(candidate.reference().exactlyEquals(reference)) {observedArtifact=candidate;break;}
                if(observedArtifact==null||observedArtifact.fullLength()!=reference.fullLength()||
                        !observedArtifact.identity().equals(reference.fileIdentity()))
                    throw new IOException("Existing V5 full20 reference is not backed by an exact actual cold artifact observation");
                var exactAttempt=attemptForReference(current.record,reference);
                artifactSources.add(accounting.issueVerifiedV5ArtifactSource(reference,exactAttempt,observedArtifact.identity()));
            }
            if(inactive!=null)for(B1Full20WitnessReference reference:inactive.record.allReferences()) {
                if(reference==null)continue;
                boolean retained=false;for(B1Full20WitnessReference actual:currentReferences)
                    if(actual!=null&&actual.exactlyEquals(reference))retained=true;
                if(!retained)throw new IOException("Inactive V5 record has an artifact reference outside the actual current source set");
            }
            var result=new VerifiedWritableCurrentV5(this,cold,selectedV4,lookup,inventory,accounting,current,inactive,artifactSources);
            result.bindReferences();
            inventory.requireCurrent();validateResumeSourceIdentity(cold.raw);
            accounting.finishV5PreinstallPass();passStarted=false;
            return result;
        } catch(Throwable primary) {
            if(passStarted)try{accounting.failV5PreinstallPass(primary);}catch(Throwable cleanup){if(cleanup!=primary)primary.addSuppressed(cleanup);}
            cold.retainedFailure=primary;
            throw propagate(primary);
        }
    }
    private VerifiedV5SlotFacts readPreinstallV5Slot(ColdResumeSourceScope cold,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting,int slot)throws IOException {
        WindowsMarkerIo.Handle handle=null;
        long scratch=0L;byte[] first=null,second=null;Throwable primary=null;
        Path target=cold.raw.inspection.directory().resolve(V5_NAMES[slot]);
        try {
            WindowsMarkerIo io=coldIo(cold);
            try {handle=io.open(target,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,WindowsMarkerIo.OPEN_EXISTING,
                    WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,operation(cold.raw));}
            catch(WindowsMarkerIo.NativeFailure absent) {if(absent.win32Error()==2)return null;throw absent;}
            cold.retainedV5PreinstallHandle=handle;cold.retainedV5PreinstallName=V5_NAMES[slot];
            WindowsB1SourceAnchorStore.requireExactNativeChild(cold.raw.sourceAck.verifiedInput().worldNativePath(),
                io.finalPath(handle),V5_NAMES[slot]);
            long length=io.payloadLength(handle);
            if(length<=0||length>B1ResumeRecordV5.MAX_RECORD_BYTES||length>Integer.MAX_VALUE)
                throw new IOException("Actual preinstall V5 container length is outside fixed codec bound");
            scratch=accounting.beforeV5PreinstallContainerRead(length);
            WindowsMarkerIo.FileIdentity identity=io.identity(handle);io.metadata(handle,false,length);
            first=io.readWitnessArtifactExact(handle,Math.toIntExact(length),B1ResumeRecordV5.MAX_RECORD_BYTES);
            cold.retainedV5PreinstallFirst=first;
            io.flushChecked(handle);
            second=io.readWitnessArtifactExact(handle,Math.toIntExact(length),B1ResumeRecordV5.MAX_RECORD_BYTES);
            cold.retainedV5PreinstallSecond=second;
            if(!Arrays.equals(first,second)||!identity.equals(io.identity(handle)))
                throw new IOException("Actual preinstall V5 complete bytes or FileId changed across flush");
            var record=B1ResumeRecordV5.decodeData(first);
            B1ResumeRecordCodec.exact(first,B1ResumeRecordV5.encodeData(record),"Actual preinstall V5 bytes are not canonical DATA");
            if(record.transition().sequence()%2!=slot)throw new IOException("Actual preinstall V5 sequence occupies the wrong fixed slot");
            io.closeChecked(handle);handle=null;cold.retainedV5PreinstallHandle=null;cold.retainedV5PreinstallName=null;
            accounting.completeV5PreinstallContainerRead(length,scratch);scratch=0L;
            VerifiedV5SlotFacts facts=new VerifiedV5SlotFacts(slot,record,first,identity);
            cold.retainedV5PreinstallFirst=null;cold.retainedV5PreinstallSecond=null;
            return facts;
        } catch(Throwable cause) {primary=cause;}
        finally {
            if(second!=null)Arrays.fill(second,(byte)0);
            if(handle!=null&&primary==null) {
                try {coldIo(cold).closeChecked(handle);handle=null;}
                catch(Throwable cleanup) {primary=cleanup;}
            }
        }
        if(primary!=null) {
            if(handle!=null)WindowsMarkerIo.closeSuppressed(handle,primary);
            if(scratch!=0L)try{accounting.failV5PreinstallPass(primary);}catch(Throwable cleanup){if(cleanup!=primary)primary.addSuppressed(cleanup);}
            throw propagate(primary);
        }
        throw new IOException("Unreachable V5 preinstall read termination");
    }

    /**
     * One boot's finite V4-only artifact locator. It is issued from the live cold source and its
     * actual accounting; it cannot be reconstructed from a record, filename, or source DTO.
     */
    public static final class Full20HistoricalDiscoveryPermit {
        private final WindowsB1TwoSlotStore issuer;
        private ColdResumeSourceScope cold;
        private final ResumeStoredRecordFacts selected;
        private com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting;
        private final java.util.List<B1ResumeRecord.AttemptAssertions> expectedAttempts;
        private final Thread owner=Thread.currentThread();
        private final Path directory;
        private final String nativeWorld;
        private final UUID operation;
        private WindowsMarkerIo.FileIdentity directoryIdentity;
        private int maxCandidates;
        private int findPasses;
        private WindowsMarkerIo.Full20WitnessFindSequence activeSequence;
        private WindowsMarkerIo.Handle activeArtifactHandle;
        private com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting activeArtifactAccounting;
        private byte[] activeArtifactFirst,activeArtifactSecond;
        private final java.util.ArrayList<WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact> verifiedArtifacts=new java.util.ArrayList<>(4);
        private Throwable failure;
        private String overflowName;
        private boolean completed;
        private Full20HistoricalDiscoveryPermit(WindowsB1TwoSlotStore issuer,ColdResumeSourceScope cold,
                ResumeStoredRecordFacts selected,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting,
                java.util.List<B1ResumeRecord.AttemptAssertions> expectedAttempts,Path directory,String nativeWorld,
                UUID operation) {
            this.issuer=issuer;this.cold=cold;this.selected=selected;this.accounting=accounting;
            this.expectedAttempts=expectedAttempts;this.directory=directory;this.nativeWorld=nativeWorld;this.operation=operation;
        }
        private void owner()throws IOException {
            if(accounting==null||cold==null)throw new IOException("Original historical consumers ended");
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact historical discovery owner required");
            issuer.validateColdSource(cold);
            if(failure!=null)throw new IOException("Historical Full20 discovery responsibility retained",failure);
            if(completed)throw new IOException("Historical Full20 discovery already completed");
        }
        void requireFor(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            owner();
            if(actualIo!=issuer.io||actualAccounting!=accounting||cold.accounting!=accounting||
                    selected==null||selected.scope!=cold.raw||cold.discoveryPermit!=this)
                throw new IOException("Exact registered historical source/actor accounting required");
            accounting.requireHistoricalDiscovery(this);
        }
        Path directoryFor(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            requireFor(actualIo,actualAccounting);return directory;
        }
        UUID operationFor(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            requireFor(actualIo,actualAccounting);return operation;
        }
        void beginFindAttempt(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting,int patternLength)throws IOException {
            requireFor(actualIo,actualAccounting);
            if(activeSequence!=null||findPasses>=2||patternLength<1||patternLength>32766)
                throw new IOException("Exactly two bounded historical discovery passes are available");
            findPasses++;
        }
        void registerFindSequence(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting,
                WindowsMarkerIo.Full20WitnessFindSequence sequence)throws IOException {
            requireFor(actualIo,actualAccounting);
            if(sequence==null||activeSequence!=null||findPasses<1||findPasses>2)
                throw new IOException("Exact native historical FindHandle registration required");
            activeSequence=sequence;
        }
        void findSequenceClosed(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting,
                WindowsMarkerIo.Full20WitnessFindSequence sequence)throws IOException {
            // Cleanup is allowed after discovery failed or the source barrier became unhealthy.
            // It only drops this exact handle registration; it cannot resume discovery.
            if(accounting==null||cold==null||Thread.currentThread()!=owner||actualIo!=issuer.io||actualAccounting!=accounting||
                    sequence==null||activeSequence!=sequence)
                throw new IOException("Foreign historical FindHandle close");
            activeSequence=null;
        }
        void failFindAttempt(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting,
                Throwable primary) {
            if(primary==null)return;
            if(failure==null)failure=primary;else if(failure!=primary)failure.addSuppressed(primary);
            if(cold.retainedFailure==null)cold.retainedFailure=primary;
            else if(cold.retainedFailure!=primary)cold.retainedFailure.addSuppressed(primary);
            try { if(actualIo==issuer.io&&actualAccounting==accounting)accounting.failHistoricalDiscovery(this,primary); }
            catch(Throwable cleanup){if(cleanup!=primary)primary.addSuppressed(cleanup);}
        }
        public int maxCandidates()throws IOException {owner();return maxCandidates;}
        public int expectedAttemptCount()throws IOException {owner();return expectedAttempts.size();}
        public B1ResumeRecord.AttemptAssertions expectedAttempt(int index)throws IOException {
            owner();if(index<0||index>=expectedAttempts.size())throw new IOException("Historical expected attempt index outside bound");
            return expectedAttempts.get(index);
        }
        B1ResumeRecord.AttemptAssertions expectedAttempt(UUID actualAttemptId)throws IOException {
            owner();if(actualAttemptId==null)throw new IOException("Exact historical attempt identity required");
            B1ResumeRecord.AttemptAssertions found=null;
            for(var expected:expectedAttempts)if(expected.attemptId().equals(actualAttemptId)) {
                if(found!=null)throw new IOException("Duplicate registered historical expected attempt ID");
                found=expected;
            }
            if(found==null)throw new IOException("Foreign historical Full20 attempt in controlled directory");
            return found;
        }
        byte[] selectedV4Canonical()throws IOException {owner();return selected.canonicalBytes();}
        byte[] selectedV4CanonicalView()throws IOException {owner();return selected.bytes;}
        WindowsMarkerIo.FileIdentity directoryIdentity()throws IOException {owner();return directoryIdentity;}
        String exactNativeWorldPath()throws IOException {owner();return nativeWorld;}
        void rememberOverflowName(String name)throws IOException {
            owner();
            if(name==null||name.isEmpty()||overflowName!=null)throw new IOException("Single prepaid historical overflow name required");
            overflowName=name;
        }
        void retainHistoricalArtifactOpen(com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting,
                WindowsMarkerIo.Handle handle)throws IOException {
            owner();
            if(actualAccounting==null||actualAccounting==accounting||handle==null||activeArtifactHandle!=null)
                throw new IOException("One exact historical artifact child-accounting and handle responsibility is required");
            actualAccounting.requireOpen();
            activeArtifactAccounting=actualAccounting;activeArtifactHandle=handle;
        }
        void retainHistoricalArtifactFirst(com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting,
                byte[] bytes)throws IOException {
            owner();
            if(actualAccounting==null||actualAccounting!=activeArtifactAccounting)throw new IOException("Exact historical child accounting required");
            if(activeArtifactHandle==null||bytes==null||activeArtifactFirst!=null)throw new IOException("Actual historical first payload required once");
            activeArtifactFirst=bytes;
        }
        void retainHistoricalArtifactSecond(byte[] bytes)throws IOException {
            owner();if(activeArtifactHandle==null||bytes==null||activeArtifactSecond!=null)
                throw new IOException("Actual historical second payload required once");
            activeArtifactSecond=bytes;
        }
        void historicalArtifactHandleClosed(WindowsMarkerIo.Handle handle)throws IOException {
            if(Thread.currentThread()!=owner||handle==null||activeArtifactHandle!=handle)
                throw new IOException("Exact historical artifact checked-close required");
            activeArtifactHandle=null;
        }
        void historicalArtifactSecondDropped()throws IOException {
            owner();if(activeArtifactSecond==null)throw new IOException("Historical second payload drop responsibility missing");
            activeArtifactSecond=null;
        }
        void registerHistoricalArtifact(WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact artifact)throws IOException {
            owner();if(artifact==null||completed||activeArtifactHandle!=null||activeArtifactAccounting==null)
                throw new IOException("Actual completed historical artifact required before registration");
            for(var prior:verifiedArtifacts)if(prior.reference().attemptId().equals(artifact.reference().attemptId()))
                throw new IOException("Duplicate actual historical artifact capability");
            verifiedArtifacts.add(artifact);activeArtifactAccounting=null;activeArtifactFirst=null;activeArtifactSecond=null;
        }
        java.util.List<WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact> verifiedHistoricalArtifacts()throws IOException {
            if(accounting==null||cold==null||Thread.currentThread()!=owner||failure!=null||!completed||verifiedArtifacts.size()!=expectedAttempts.size())
                throw new IOException("Complete two-pass historical artifact set required",failure);
            issuer.validateColdSource(cold);
            return java.util.List.copyOf(verifiedArtifacts);
        }
        void requireForHistoricalReference(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            if(Thread.currentThread()!=owner||actualIo!=issuer.io||actualAccounting!=accounting||
                    cold.accounting!=accounting||cold.discoveryPermit!=this||failure!=null||!completed)
                throw new IOException("Exact completed historical artifact source/actor authority required",failure);
            issuer.validateColdSource(cold);
            accounting.requireOpen();
        }
        Path directoryForHistoricalReference(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            requireForHistoricalReference(actualIo,actualAccounting);return directory;
        }
        UUID operationForHistoricalReference(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            requireForHistoricalReference(actualIo,actualAccounting);return operation;
        }
        ResumeStoredRecordFacts selectedForHistoricalReference(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            requireForHistoricalReference(actualIo,actualAccounting);return selected;
        }
        WindowsMarkerIo.FileIdentity directoryIdentityForHistoricalReference(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            requireForHistoricalReference(actualIo,actualAccounting);return directoryIdentity;
        }
        String exactNativeWorldPathForHistoricalReference(WindowsMarkerIo actualIo,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting actualAccounting)throws IOException {
            requireForHistoricalReference(actualIo,actualAccounting);return nativeWorld;
        }
        void setCapacityFromAccounting(int value)throws IOException {
            owner();if(value<expectedAttempts.size()||value>64||maxCandidates!=0)throw new IOException("Historical discovery candidate budget is insufficient or duplicate");
            maxCandidates=value;
        }
        void setDirectoryIdentity(WindowsMarkerIo.FileIdentity value)throws IOException {
            owner();if(value==null||directoryIdentity!=null)throw new IOException("Historical native directory identity required once");
            directoryIdentity=value;
        }
        void completeAfterSecondPass()throws IOException {
            owner();if(findPasses!=2||activeSequence!=null)throw new IOException("Both actual historical FindHandle passes must close before completion");
            accounting.finishHistoricalDiscovery(this);completed=true;
        }
        private boolean expectedContains(byte[] attemptBytes)throws IOException {
            for(var expected:expectedAttempts)
                if(Arrays.equals(B1ResumeRecordCodec.attemptBytes(expected),attemptBytes))return true;
            return false;
        }
        boolean containsExpectedAttempt(byte[] attemptBytes)throws IOException {owner();return expectedContains(attemptBytes);}
        @Override public String toString(){return "Full20HistoricalDiscoveryPermit[actual V4 source, finite current-boot locator]";}
    }
    private static final class ColdV4Read {
        byte[] first,second;
        B1ResumeRecord decoded;
        WindowsMarkerIo.FileIdentity identity;
        ResumeStoredRecordFacts facts;
    }
    public ColdResumeSourceScope bindResumeSourceForColdLookup(WindowsB1SourceAnchorStore anchors,
            WindowsB1SourceAnchorStore.ControlledAnchorLocation location,
            WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor,
            WindowsWorldProvisioner.ResumeInspection inspection)throws IOException {
        checkOwner();var raw=new ResumeSourceScope(this,Objects.requireNonNull(anchors),Objects.requireNonNull(location),
            Objects.requireNonNull(anchor),Objects.requireNonNull(inspection));
        validateResumeSourceIdentity(raw);return new ColdResumeSourceScope(this,raw);
    }
    private void validateColdSource(ColdResumeSourceScope cold)throws IOException {
        checkOwner();if(cold==null||cold.store!=this)throw new IOException("Exact privately issued cold resume scope required");
        if(cold.retainedFailure!=null)throw new IOException("Original cold native responsibility retained",cold.retainedFailure);
        validateResumeSourceIdentity(cold.raw);
    }
    public ResumeSelectedRecords readCurrentForColdLookup(ColdResumeSourceScope cold,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        checkOwner();
        Objects.requireNonNull(accounting).requireOpen();
        if(cold==null)throw new IOException("Exact cold source required");
        if(cold.accounting==null)cold.accounting=accounting;
        else if(cold.accounting!=accounting)throw new IOException("Exact original successor accounting required");
        validateColdSource(cold);var s=cold.raw;absent(s,TEMP);for(String legacy:LEGACY)absent(s,legacy);
        var selected=select(readColdSlot(cold,0),readColdSlot(cold,1));s.inspection.validateInspection();return selected;
    }
    public WindowsB1ColdResumeLookup inspectExistingV5(ColdResumeSourceScope cold,ResumeStoredRecordFacts selected,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        validateColdSource(cold);
        if(selected==null||selected.scope!=cold.raw||cold.retainedLookup!=null||cold.accounting!=accounting)
            throw new IOException("Exact issued selected V4 facts and one cold lookup required");
        Objects.requireNonNull(accounting).requireOpen();
        cold.retainedLookup=new WindowsB1ColdResumeLookup(this,cold,selected,accounting);
        cold.retainedLookup.observe();return cold.retainedLookup;
    }
    public WindowsB1ColdResumeLookup retainedColdLookup(ColdResumeSourceScope cold)throws IOException {
        // 故障后 guard/anchor 可能漂移；责任 getter 不重试操作或要求来源重新健康。
        checkOwner();if(cold==null||cold.store!=this)throw new IOException("Exact cold responsibility owner required");
        return cold.retainedLookup;
    }
    public Throwable coldSourceFailure(ColdResumeSourceScope cold)throws IOException {
        checkOwner();if(cold==null||cold.store!=this)throw new IOException("Exact cold responsibility owner required");return cold.retainedFailure;
    }
    /** Actual V4-only locator issuer. Existing/pending V5 and arbitrary records cannot enter this path. */
    public Full20HistoricalDiscoveryPermit issueFull20HistoricalDiscoveryPermit(ColdResumeSourceScope cold,
            ResumeStoredRecordFacts selected,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        checkOwner();Objects.requireNonNull(accounting);
        validateColdSource(cold);
        if(selected==null||selected.scope!=cold.raw||cold.accounting!=accounting||cold.discoveryPermit!=null||
                cold.retainedLookup==null||cold.retainedLookup.observation().status()!=WindowsB1ColdResumeLookup.Status.UNRESOLVED_DATA||
                cold.retainedLookup.observation().unresolvedReason()!=WindowsB1ColdResumeLookup.UnresolvedReason.NO_V5)
            throw new IOException("V4 historical discovery requires the exact live NO_V5 cold source");
        B1ResumeRecord sourceRecord=selected.record;
        var assertions=sourceRecord.assertions();
        if((sourceRecord.kind()!=Kind.CERTIFIED&&sourceRecord.kind()!=Kind.DB_ACQUIRED)||assertions.pendingAttempt()!=null||
                assertions.currentOwner()==null||assertions.originalPrepareWitness()==null||assertions.acknowledgedCertifyWitness()==null||
                assertions.latestSettlement()!=null&&assertions.latestSettlement().kind()!=SettlementKind.CONFIRMED)
            throw new IOException("Historical Full20 discovery requires an actual settled V4 source with complete prepare/certify witnesses");
        accounting.reserveHistoricalExpected(3);
        var attempts=new java.util.ArrayList<B1ResumeRecord.AttemptAssertions>(3);
        addHistoricalExpected(attempts,assertions.originalPrepareWitness(),sourceRecord,accounting);
        if(assertions.latestSettlement()!=null)addHistoricalExpected(attempts,assertions.latestSettlement().settledAttempt(),sourceRecord,accounting);
        addHistoricalExpected(attempts,assertions.acknowledgedCertifyWitness(),sourceRecord,accounting);
        if(attempts.isEmpty())throw new IOException("Historical V4 source has no actual Full20 witness attempts");
        var retained=java.util.List.copyOf(attempts);
        Path directory=cold.raw.inspection.directory();
        String nativeWorld=cold.raw.inspection.nativeDirectoryPath();
        UUID operation=operation(cold.raw);
        if(cold.raw.historicalDiscoveryPermit!=null)
            throw new IOException("One exact V4 historical discovery permit is already retained");
        var permit=new Full20HistoricalDiscoveryPermit(this,cold,selected,accounting,retained,directory,nativeWorld,operation);
        cold.raw.historicalDiscoveryPermit=permit;
        cold.discoveryPermit=permit; // responsibility is retained before budget/native work.
        try {
            int capacity=accounting.beginHistoricalDiscovery(permit,attempts.size(),directory.toAbsolutePath().normalize().toString().length());
            permit.setCapacityFromAccounting(capacity);
            // The original NO_V5 lookup already consumed its fixed ten V4 reads and closed the
            // inspection barrier. Re-reading here would exceed that reviewed read contract.
            validateColdSource(cold);
            var identity=io.captureDirectoryIdentity(directory,operation);
            permit.setDirectoryIdentity(identity);
            validateColdSource(cold);
            return permit;
        } catch(Throwable primary) {
            permit.failFindAttempt(io,accounting,primary);
            throw propagate(primary);
        }
    }
    public java.util.List<WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact> discoverFull20HistoricalArtifacts(
            Full20HistoricalDiscoveryPermit permit,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        checkOwner();Objects.requireNonNull(permit);Objects.requireNonNull(accounting);
        permit.requireFor(io,accounting);
        return WindowsB1Full20WitnessStore.discoverHistorical(io,permit,accounting);
    }
    private static void addHistoricalExpected(java.util.List<B1ResumeRecord.AttemptAssertions> attempts,
            B1ResumeRecord.AttemptAssertions actual,B1ResumeRecord source,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        if(actual==null)throw new IOException("Required historical V4 attempt is absent");
        if(actual.phase()!=Phase.PREPARE&&actual.phase()!=Phase.CERTIFY&&actual.phase()!=Phase.HANDOFF||
                !actual.namespace().equals(source.barrierNamespace()))
            throw new IOException("V4-only historical artifact attempt phase/namespace is invalid");
        accounting.reserveHistoricalAttemptCompare();
        byte[] encoded=null;
        Throwable primary=null;
        try {
            encoded=B1ResumeRecordCanonical.encodeAttemptData(actual);
            for(var old:attempts) {
                if(old.attemptId().equals(actual.attemptId())) {
                    byte[] prior=B1ResumeRecordCanonical.encodeAttemptData(old);
                    boolean same;
                    try {same=Arrays.equals(prior,encoded);}
                    finally {Arrays.fill(prior,(byte)0);}
                    if(!same)throw new IOException("Duplicate historical attempt ID has different complete bytes");
                    return;
                }
            }
            attempts.add(actual);
        } catch(Throwable failure) {
            primary=failure;throw propagate(failure);
        } finally {
            if(encoded!=null)Arrays.fill(encoded,(byte)0);
            try {accounting.finishHistoricalAttemptCompare();}
            catch(Throwable cleanup) {
                if(primary!=null&&cleanup!=primary)primary.addSuppressed(cleanup);
                else if(primary==null)throw cleanup;
            }
        }
    }
    void rebarrierColdV4(ColdResumeSourceScope cold,ResumeStoredRecordFacts expected)throws IOException {
        validateColdSource(cold);if(expected==null||expected.scope!=cold.raw)throw new IOException("Exact cold selected V4 facts required");
        var selected=readCurrentForColdLookup(cold,cold.accounting);if(selected.current()==null)throw new IOException("Cold V4 predecessor missing");
        same(expected,selected.current());
        try {
            cold.retainedV4Handle=openExisting(path(cold.raw,expected.slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(cold.raw));
            if(cold.retainedV4Handle==null)throw new IOException("Cold V4 predecessor disappeared");
            same(expected,readColdHeld(cold,expected.slot,cold.retainedV4Handle));io.flushChecked(cold.retainedV4Handle);
            same(expected,readColdHeld(cold,expected.slot,cold.retainedV4Handle));
            validateColdSource(cold);io.closeChecked(cold.retainedV4Handle);cold.retainedV4Handle=null;
        } catch(Throwable primary){cold.retainedFailure=primary;WindowsMarkerIo.closeSuppressed(cold.retainedV4Handle,primary);throw propagate(primary);}
    }
    private ResumeStoredRecordFacts readColdSlot(ColdResumeSourceScope cold,int slot)throws IOException {
        try {
            cold.retainedV4Handle=openExisting(path(cold.raw,slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(cold.raw));
            if(cold.retainedV4Handle==null)return null;
            var facts=readColdHeld(cold,slot,cold.retainedV4Handle);
            io.closeChecked(cold.retainedV4Handle);cold.retainedV4Handle=null;return facts;
        } catch(Throwable primary){cold.retainedFailure=primary;WindowsMarkerIo.closeSuppressed(cold.retainedV4Handle,primary);throw propagate(primary);}
    }
    private ResumeStoredRecordFacts readColdHeld(ColdResumeSourceScope cold,int slot,WindowsMarkerIo.Handle handle)throws IOException {
        long length=io.payloadLength(handle);
        if(length<=0||length>65536)throw new IOException("Cold V4 fixed complete record bound");
        if(cold.retainedV4Reads.size()>=10)throw new IOException("Cold V4 fixed read lifecycle exhausted; no replay");
        cold.accounting.beforeV4Read(Math.toIntExact(length)); // 原 readHeld 第一 array 之前。
        var draft=new ColdV4Read();cold.retainedV4Reads.add(draft); // 部分 decoder 失败仍保原 bytes。
        draft.first=io.readPayloadExact(handle,Math.toIntExact(length));draft.decoded=B1ResumeRecordCodec.decode(draft.first);
        if(draft.decoded.sequence()%2!=slot)throw new IOException("Cold V4 sequence stored in wrong fixed slot");
        requireSourceRecord(cold.raw,draft.decoded);
        WindowsB1SourceAnchorStore.requireExactNativeChild(cold.raw.sourceAck.verifiedInput().worldNativePath(),io.finalPath(handle),NAMES[slot]);
        draft.identity=io.identity(handle);draft.second=io.readPayloadExact(handle,Math.toIntExact(length));
        B1ResumeRecordCodec.exact(draft.first,draft.second,"Cold full V4 bytes changed");
        if(!draft.identity.equals(io.identity(handle)))throw new IOException("Cold V4 native identity changed");
        draft.facts=new ResumeStoredRecordFacts(cold.raw,slot,draft.decoded,draft.first,draft.identity);return draft.facts;
    }
    WindowsMarkerIo coldIo(ColdResumeSourceScope cold)throws IOException {validateColdSource(cold);return io;}
    Path coldDirectory(ColdResumeSourceScope cold)throws IOException {validateColdSource(cold);return cold.raw.inspection.directory();}
    String coldNativePath(ColdResumeSourceScope cold)throws IOException {validateColdSource(cold);return cold.raw.inspection.nativeDirectoryPath();}
    UUID coldOperation(ColdResumeSourceScope cold)throws IOException {validateColdSource(cold);return operation(cold.raw);}
    NativeOwnerAssertions coldPreviousOwner(ColdResumeSourceScope cold)throws IOException {validateColdSource(cold);return cold.raw.inspection.previousOwner();}
    void requireColdRecord(ColdResumeSourceScope cold,B1ResumeRecord record)throws IOException {validateColdSource(cold);requireSourceRecord(cold.raw,record);}
    void requireColdLive(ColdResumeSourceScope cold)throws IOException {validateColdSource(cold);}
    private void requireNoV5Continuation(ResumeSourceScope source)throws IOException {
        checkOwner();
        absent(source,".floatingpod-b1-resume-v5.0");
        absent(source,".floatingpod-b1-resume-v5.1");
        absent(source,".floatingpod-b1-resume-v5.tmp");
    }
    private void validateResumeSource(ResumeSourceScope s)throws IOException {
        checkOwner();if(s==null||s.store!=this||!s.inspection.uses(io))throw new IOException("Exact resume inspection/source/native issuer required");s.inspection.validateInspection();s.sourceAck=s.anchorStore.rebarrierExisting(s.location,s.sourceAck.verifiedInput());var input=s.sourceAck.verifiedInput();var creation=s.inspection.freshProvisioningProof();
        requireNoV5Continuation(s); // 所有resume V4入口统一拒绝existing/pending V5，早于native mutation。
        B1ResumeRecordCodec.exact(input.originalReceiptCanonicalBytes(),creation.receiptCanonicalBytes(),"Resume original receipt differs from anchor");if(!input.expectedPlan().equals(creation.plan())||!input.originalSourceIdentity().equals(creation.sourceWorldIdentity())||!input.worldNativePath().equals(s.inspection.nativeDirectoryPath()))throw new IOException("Resume full anchor/source/plan/native path mismatch");s.inspection.validateInspection();
    }
    private void validateResumeSourceIdentity(ResumeSourceScope s)throws IOException {
        checkOwner();if(s==null||s.store!=this||!s.inspection.uses(io))throw new IOException("Exact resume inspection/source/native issuer required");s.inspection.validateInspection();s.sourceAck=s.anchorStore.rebarrierExisting(s.location,s.sourceAck.verifiedInput());var input=s.sourceAck.verifiedInput();var creation=s.inspection.freshProvisioningProof();
        B1ResumeRecordCodec.exact(input.originalReceiptCanonicalBytes(),creation.receiptCanonicalBytes(),"Resume original receipt differs from anchor");if(!input.expectedPlan().equals(creation.plan())||!input.originalSourceIdentity().equals(creation.sourceWorldIdentity())||!input.worldNativePath().equals(s.inspection.nativeDirectoryPath()))throw new IOException("Resume full anchor/source/plan/native path mismatch");s.inspection.validateInspection();
    }
    public DurableHandoffNativeAck publishHandoffPrepared(ResumeSourceScope s,ResumeNativeRecordDurability predecessor,FullStateAssertions assertions)throws IOException {
        validateResumeSource(s);if(!s.inspection.targetOwner().equals(assertions.currentOwner())||!s.inspection.previousOwner().equals(assertions.previousOwner()))throw new IOException("HANDOFF owners must be actual successor and old guarded owner");
        var attempt=assertions.pendingAttempt();if(attempt==null||attempt.phase()!=Phase.HANDOFF||attempt.bootGeneration()!=s.inspection.targetOwner().generation()||!attempt.context().boot().equals(s.inspection.targetOwner().boot()))throw new IOException("Complete same-successor HANDOFF attempt assertions required");
        var current=requireCurrent(s,predecessor);if(!current.record.assertions().currentOwner().equals(s.inspection.previousOwner()))throw new IOException("Predecessor current owner differs from original guard");
        long next=Math.addExact(current.record.sequence(),1);var record=new B1ResumeRecord(next,WindowsWorldProvisioner.digest(current.bytes),Kind.HANDOFF_PREPARED,current.record.sourceAnchorSha256(),current.record.originalReceipt(),current.record.barrierNamespace(),assertions);byte[] bytes=B1ResumeRecordCodec.encode(record);B1ResumeRecordCodec.consecutive(current.record,current.bytes,record);absent(s,NAMES[(int)(next%2)]);
        s.inspection.beginHandoffPublication();try{return new DurableHandoffNativeAck(s,publishAbsent(s,(int)(next%2),record,bytes));}catch(Throwable primary){s.inspection.failHandoff(primary);throw propagate(primary);}
    }

    private Path path(ResumeSourceScope s,int slot){if(slot<0||slot>1)throw new IllegalArgumentException("Fixed slot 0 or 1 required");return s.inspection.directory().resolve(NAMES[slot]);}
    private UUID operation(ResumeSourceScope s){return s.inspection.freshProvisioningProof().plan().bootstrapOperationUuid();}
    private void absent(ResumeSourceScope s,String name)throws IOException {
        WindowsMarkerIo.Handle handle=null;
        try{handle=openExisting(s.inspection.directory().resolve(name),WindowsMarkerIo.READ,operation(s));
            if(handle!=null)throw new IOException("Fixed B1 file exists; conflict retained: "+name);
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(handle,failure);throw propagate(failure);}
    }
    private void requireSourceRecord(ResumeSourceScope s,B1ResumeRecord r)throws IOException {
        B1ResumeRecordCodec.exact(HexFormat.of().parseHex(s.sourceAck.anchorSha256()),r.sourceAnchorSha256(),"Resume record source anchor differs");
        B1ResumeRecordCodec.exact(s.sourceAck.verifiedInput().originalReceiptCanonicalBytes(),r.originalReceipt(),"Resume record original receipt differs");
        if(!s.sourceAck.verifiedInput().barrierNamespace().equals(r.barrierNamespace()))throw new IOException("Resume namespace differs from anchored configuration");
    }
    private ResumeStoredRecordFacts readSlot(ResumeSourceScope s,int slot)throws IOException {
        WindowsMarkerIo.Handle handle=null;
        try{handle=openExisting(path(s,slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(handle==null)return null;
            var facts=readHeld(s,slot,handle);io.closeChecked(handle);handle=null;return facts;
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(handle,failure);throw propagate(failure);}
    }
    private ResumeStoredRecordFacts readHeld(ResumeSourceScope s,int slot,WindowsMarkerIo.Handle handle)throws IOException {
        int size=Math.toIntExact(io.payloadLength(handle));byte[] bytes=io.readPayloadExact(handle,size);var record=B1ResumeRecordCodec.decode(bytes);
        if(record.sequence()%2!=slot)throw new IOException("Sequence stored in wrong fixed slot");requireSourceRecord(s,record);
        WindowsB1SourceAnchorStore.requireExactNativeChild(s.sourceAck.verifiedInput().worldNativePath(),io.finalPath(handle),NAMES[slot]);
        var id=io.identity(handle);B1ResumeRecordCodec.exact(bytes,io.readPayloadExact(handle,size),"Full slot bytes changed");
        if(!id.equals(io.identity(handle)))throw new IOException("Slot native identity changed");return new ResumeStoredRecordFacts(s,slot,record,bytes,id);
    }
    public ResumeSelectedRecords readCurrent(ResumeSourceScope s)throws IOException {
        validateResumeSource(s);absent(s,TEMP);for(String legacy:LEGACY)absent(s,legacy);
        var a=readSlot(s,0);var b=readSlot(s,1);ResumeSelectedRecords selected=select(a,b);s.inspection.validateInspection();return selected;
    }
    static ResumeSelectedRecords select(ResumeStoredRecordFacts a,ResumeStoredRecordFacts b)throws IOException {
        if(a==null)return new ResumeSelectedRecords(b,null);if(b==null)return new ResumeSelectedRecords(a,null);
        if(a.record.sequence()==b.record.sequence())throw new IOException("Same-sequence two-slot conflict");
        var older=a.record.sequence()<b.record.sequence()?a:b;var newer=older==a?b:a;
        B1ResumeRecordCodec.consecutive(older.record,older.bytes,newer.record);return new ResumeSelectedRecords(newer,older);
    }
    private ResumeStoredRecordFacts requireCurrent(ResumeSourceScope s,ResumeNativeRecordDurability ack)throws IOException {
        if(ack==null||ack.facts.scope!=s)throw new IOException("Exact current native record ACK required");var selected=readCurrent(s);
        if(selected.current()==null)throw new IOException("Current anchor record missing");same(ack.facts,selected.current());return selected.current();
    }
    private static void same(ResumeStoredRecordFacts expected,ResumeStoredRecordFacts actual)throws IOException {
        if(expected.slot!=actual.slot||!expected.identity.equals(actual.identity))throw new IOException("Exact fixed slot native identity differs");
        B1ResumeRecordCodec.exact(expected.bytes,actual.bytes,"Exact full slot record differs");
    }
    public ResumeNativeRecordDurability rebarrierCurrent(ResumeSourceScope s,ResumeStoredRecordFacts expected)throws IOException {
        validateResumeSource(s);if(expected==null||expected.scope!=s)throw new IOException("Exact issued stored facts required");
        var selected=readCurrent(s);if(selected.current()==null)throw new IOException("Current record absent");same(expected,selected.current());
        WindowsMarkerIo.Handle handle=null;
        try{handle=openExisting(path(s,expected.slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(handle==null)throw new IOException("Current record disappeared");
            var actual=barrierHeld(s,expected.slot,handle,expected);s.inspection.validateInspection();io.closeChecked(handle);handle=null;return new ResumeNativeRecordDurability(actual);
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(handle,failure);throw propagate(failure);}
    }
    private ResumeStoredRecordFacts barrierHeld(ResumeSourceScope s,int slot,WindowsMarkerIo.Handle handle,ResumeStoredRecordFacts expected)throws IOException {
        var before=readHeld(s,slot,handle);if(expected!=null)same(expected,before);io.flushChecked(handle);var after=readHeld(s,slot,handle);same(before,after);return after;
    }
    private ResumeNativeRecordDurability publishAbsent(ResumeSourceScope s,int slot,B1ResumeRecord record,byte[] bytes)throws IOException {
        WindowsMarkerIo.Handle temporary=null,canonical=null;
        try{validateResumeSource(s);absent(s,TEMP);absent(s,NAMES[slot]);
            temporary=io.createMarkerHandle(s.inspection.directory().resolve(TEMP),operation(s));io.metadata(temporary,false,0);
            WindowsB1SourceAnchorStore.requireExactNativeChild(s.sourceAck.verifiedInput().worldNativePath(),io.finalPath(temporary),TEMP);
            io.writePayload(temporary,bytes);io.metadata(temporary,false,bytes.length);var createdId=io.identity(temporary);io.flushChecked(temporary);
            io.moveWriteThroughNoReplace(s.inspection.directory().resolve(TEMP),path(s,slot),operation(s));io.closeChecked(temporary);temporary=null;
            canonical=openExisting(path(s,slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(canonical==null)throw new IOException("Published canonical slot missing; no ACK");
            var actual=readHeld(s,slot,canonical);if(!createdId.equals(actual.identity))throw new IOException("Published slot identity differs from actual CREATE_NEW file");
            B1ResumeRecordCodec.exact(bytes,actual.bytes,"Published canonical differs from complete planned bytes");actual=barrierHeld(s,slot,canonical,actual);
            s.inspection.validateInspection();io.closeChecked(canonical);canonical=null;return new ResumeNativeRecordDurability(actual);
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(canonical,failure);WindowsMarkerIo.closeSuppressed(temporary,failure);throw propagate(failure);}
    }
    public ResumeRetiredInactiveSlot retireInactiveSlot(ResumeSourceScope s,ResumeNativeRecordDurability liveAnchorAck,ResumeStoredRecordFacts expectedInactive)throws IOException {
        validateResumeSource(s);var current=requireCurrent(s,liveAnchorAck);
        if(expectedInactive==null||expectedInactive.scope!=s||expectedInactive.slot==current.slot)throw new IOException("Exact inactive canonical facts required");
        B1ResumeRecordCodec.consecutive(expectedInactive.record,expectedInactive.bytes,current.record);
        WindowsMarkerIo.Handle anchor=null,inactive=null;
        try{anchor=openExisting(path(s,current.slot),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,operation(s));if(anchor==null)throw new IOException("Live current anchor missing");
            current=barrierHeld(s,current.slot,anchor,current);
            inactive=openExisting(path(s,expectedInactive.slot),WindowsMarkerIo.READ|WindowsMarkerIo.DELETE,operation(s));if(inactive==null)throw new IOException("Expected inactive file missing before retirement");
            var actual=readHeld(s,expectedInactive.slot,inactive);same(expectedInactive,actual);B1ResumeRecordCodec.consecutive(actual.record,actual.bytes,current.record);
            s.inspection.validateInspection();s.inspection.beginInactiveRetirement();io.deleteInactiveCanonicalChecked(inactive);io.closeChecked(inactive);inactive=null;
            absent(s,NAMES[expectedInactive.slot]);current=barrierHeld(s,current.slot,anchor,current);s.inspection.validateInspection();
            io.closeChecked(anchor);anchor=null;return new ResumeRetiredInactiveSlot(expectedInactive.slot,new ResumeNativeRecordDurability(current));
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(inactive,failure);WindowsMarkerIo.closeSuppressed(anchor,failure);s.inspection.failHandoff(failure);throw propagate(failure);}
    }

    private static IOException propagate(Throwable failure){if(failure instanceof IOException io)return io;if(failure instanceof RuntimeException r)throw r;if(failure instanceof Error e)throw e;return WindowsMarkerIo.ioFailure("B1 two-slot boundary",failure);}
    /** 物理关闭后才撤原冷读取长owner；不重新进入已结束的DATA验证器。 */
    private static void requireColdTerminal(ColdResumeSourceScope cold,MySqlCertifiedRecoveryInspectionActor actor,
            MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state)throws IOException {
        if(cold==null||cold.accounting==null)throw new IOException("Original cold terminal accounting required");
        cold.accounting.requireOriginalTerminalIdentity(actor,state);
        if(cold.retainedFailure!=null)throw new IOException("Original cold failure retained",cold.retainedFailure);
        if(cold.retainedV4Handle!=null||cold.retainedV5PreinstallHandle!=null||cold.inventoryHandle!=null||
                cold.retainedV5PreinstallFirst!=null||cold.retainedV5PreinstallSecond!=null)
            throw new IOException("Original cold native window remains live");
        var history=cold.discoveryPermit;
        if(history!=null) {
            if(history.cold!=cold||history.accounting!=cold.accounting||!history.completed||
                    history.activeSequence!=null||history.activeArtifactHandle!=null||history.activeArtifactAccounting!=null||
                    history.activeArtifactFirst!=null||history.activeArtifactSecond!=null)
                throw new IOException("Original historical native window remains live");
            if(history.failure!=null)throw new IOException("Original discovery failure retained",history.failure);
        }
    }
    private static void detachColdTerminal(ColdResumeSourceScope cold) {
        var history=cold.discoveryPermit;
        if(history!=null) {
            history.verifiedArtifacts.clear();history.accounting=null;history.cold=null;
        }
        cold.raw.historicalDiscoveryPermit=null;
        cold.discoveryPermit=null;cold.accounting=null;cold.retainedLookup=null;
        cold.retainedV4Reads.clear();cold.retainedV5PreinstallSlots.clear();
    }

    void requireOrdinaryTerminalFull20Store(NativeWitnessScope scope,WindowsB1Full20WitnessStore actual,
            InstalledResumeRecordScope installed,MySqlFull20NativeTransfer transfer)throws IOException {
        checkOwner();
        if(scope==null||scope.store!=this||scope.thread!=Thread.currentThread()||scope.installed!=installed||
                installed==null||installed.store!=this||!scope.full20CreationAttempted||scope.fixedTransfer!=transfer||
                scope.witnessStores.size()!=1||scope.witnessStores.get(0)!=actual)
            throw new IOException("Exact originally registered Ordinary Full20 store/scope/transfer required");
    }
    void requireOrdinaryBridgeTerminalStore(NativeWitnessScope scope,WindowsB1V5WitnessRecordStore actual,
            InstalledResumeRecordScope installed,MySqlFull20NativeTransfer transfer)throws IOException {
        checkOwner();
        if(scope==null||installed==null||installed.predecessor==null||scope.store!=this||scope.installed!=installed||scope.source!=installed.source||
                installed.predecessor.v5Flight!=null||scope.preinstallV5!=null||scope.resumeSource!=null||
                scope.witnessV5Store!=actual||scope.fixedTransfer!=null||transfer==null)
            throw new IOException("Exact installed V4-to-V5 registered bridge store required");
    }
    public WindowsB1V5WitnessRecordStore originalOrdinaryBridgeStore(NativeWitnessScope scope,
            InstalledResumeRecordScope installed,MySqlFull20NativeTransfer transfer)throws IOException {
        if(scope==null)throw new IOException("Original V4 bridge witness scope required");
        requireOrdinaryBridgeTerminalStore(scope,scope.witnessV5Store,installed,transfer);
        if(scope.witnessV5Store==null)throw new IOException("Original V4 bridge store was not registered");
        return scope.witnessV5Store;
    }
    /** DATA已封后仍执行原source/guard/custody完整活核；不重取已ended Full20 ACK。 */
    public void requireOrdinaryTerminalInstalled(InstalledResumeRecordScope installed,
            com.wi.floatingpod.storage.mysql.MySqlCertifiedOrdinaryNativeExitPermit permit)throws IOException {
        checkOwner();if(installed==null)throw new IOException("Original installed scope required");
        permit.requireInstalledIdentity(installed.inspection,installed.installedWorld(),installed,installed.predecessor,io);
        validateInstalled(installed);
    }
    public void detachOrdinaryWitnessIdentity(NativeWitnessScope scope,
            MySqlCertifiedRecoveryInspectionActor actor,MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state,
            MySqlFull20NativeTransfer transfer)throws IOException {
        checkOwner();transfer.requireOriginalTerminalIdentity(actor,state);
        if(scope==null||scope.store!=this||scope.thread!=Thread.currentThread()||scope.installed==null)
            throw new IOException("Original installed witness identity required");
        if(scope.preinstallV5!=null)scope.preinstallV5.detachOriginalTerminalConsumers(actor,state,transfer);
        else if(scope.resumeSource!=null&&scope.resumeSource.historicalDiscoveryPermit!=null) {
            var history=scope.resumeSource.historicalDiscoveryPermit;
            requireColdTerminal(history.cold,actor,state);
            detachColdTerminal(history.cold);
        }
        if(scope.witnessV5Store!=null)scope.witnessV5Store.detachOrdinaryTerminalIdentity(actor,state);
        scope.witnessStores.clear();scope.knownNames.clear();scope.witnessV5Store=null;
        scope.fixedTransfer=null;scope.preinstallV5=null;scope.resumeSource=null;scope.resumePredecessor=null;
        scope.predecessor=null;scope.source=null;scope.installed=null;
    }
    public void detachOrdinaryInstalledIdentity(InstalledResumeRecordScope installed,
            com.wi.floatingpod.storage.mysql.MySqlCertifiedOrdinaryNativeExitPermit permit)throws IOException {
        checkOwner();if(installed==null||installed.store!=this||installed.inspection==null)
            throw new IOException("Original installed terminal scope required");
        permit.requireClosedInstalledIdentity(installed);
        installed.source.witnessStores.clear();installed.source.witnessV5Store=null;
        installed.source.sourceAck=null;installed.source.anchorStore=null;installed.source.location=null;installed.source.custody=null;
        installed.source=null;installed.inspection=null;installed.predecessor=null;installed.initial=null;installed.v5Flight=null;
    }

    public void detachOrdinaryV5FlightIdentity(WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight flight,
            MySqlCertifiedRecoveryInspectionActor actor,MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state,
            MySqlFull20NativeTransfer transfer)throws IOException {
        checkOwner();if(flight==null)throw new IOException("Original V5 flight required");
        detachOrdinaryWitnessIdentity(flight.originalTerminalScope(this,actor,state),actor,state,transfer);
    }

}

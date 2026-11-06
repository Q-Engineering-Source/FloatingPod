package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlFull20NativeTransfer;
import com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 原 witness 专用 native artifact；不签 SQL/普通恢复/退休资格。 */
public final class WindowsB1Full20WitnessStore {
    private WindowsB1TwoSlotStore issuer;
    private WindowsB1TwoSlotStore.NativeWitnessScope scope;
    private MySqlFull20NativeTransfer retainedTransfer;
    private final Thread owner=Thread.currentThread();
    private Pending pending;
    private NativeArtifactDurability published;
    private Throwable retainedFailure;
    private WindowsMarkerIo.Handle coldHandle;
    private byte[] revalidationBytes;
    private int consumerPhase;
    private static final class Pending {
        MySqlFull20NativeTransfer transfer;
        final String temporary,canonical;
        WindowsMarkerIo.Handle temporaryHandle,canonicalHandle;
        WindowsMarkerIo.FileIdentity createdIdentity;
        byte[] readback;
        Pending(MySqlFull20NativeTransfer transfer,String temporary,String canonical) {
            this.transfer=transfer;this.temporary=temporary;this.canonical=canonical;
        }
    }
    /** Opaque revalidation capability minted only after this store completed a real native read. */
    public interface VerifiedArtifactReference {
        B1Full20WitnessReference reference();
        void requireExactAttempt(B1ResumeRecord.AttemptAssertions attempt) throws IOException;
        void revalidateForReference() throws IOException;
    }
    /** 仅实际全写/flush/move/同 identity 全读/flush/全读/checked close 后私有发行。 */
    public static final class NativeArtifactDurability implements VerifiedArtifactReference {
        private final WindowsB1Full20WitnessStore store;
        private MySqlFull20NativeTransfer transfer;
        private B1Full20WitnessReference reference;
        private byte[] completeReadback;
        private NativeArtifactDurability(WindowsB1Full20WitnessStore store,MySqlFull20NativeTransfer transfer,
                B1Full20WitnessReference reference,byte[] completeReadback) {
            this.store=store;this.transfer=transfer;this.reference=reference;this.completeReadback=completeReadback;
        }
        public B1Full20WitnessReference reference(){store.requireDataLive();return reference;}
        public void requireExactAttempt(B1ResumeRecord.AttemptAssertions attempt)throws IOException {
            store.requireDataLive();
            if(attempt==null)throw new IOException("Exact original attempt required for witness reference");
            var actual=transfer.originalRecord().assertions().pendingAttempt();
            if(actual==null)throw new IOException("Published artifact transfer has no original pending attempt");
            B1ResumeRecordCodec.exact(B1ResumeRecordCodec.attemptBytes(attempt),B1ResumeRecordCodec.attemptBytes(actual),
                "Native artifact reference differs from complete original attempt/context/writer/common graphs");
        }
        public MySqlFull20NativeTransfer originalTransfer(){store.requireDataLive();return transfer;}
        public void revalidateForReference()throws IOException {store.revalidateForReference(this);}
        public void beginOrdinaryConsumerEnd(MySqlCertifiedRecoveryInspectionActor actor,
                MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state,
                WindowsB1TwoSlotStore.InstalledResumeRecordScope installed)throws IOException {
            store.beginOrdinaryConsumerEnd(actor,state,installed,this);
        }
        public void finishOrdinaryConsumerEnd(MySqlCertifiedRecoveryInspectionActor actor,
                MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state)throws IOException {
            store.finishOrdinaryConsumerEnd(actor,state,this);
        }
        public String protocol(){return "WIN32_FULL20_WRITE_FLUSH_MOVE_ID_READ_FLUSH_READ_CHECKED_CLOSE_V1";}
    }
    WindowsB1Full20WitnessStore(WindowsB1TwoSlotStore issuer,WindowsB1TwoSlotStore.NativeWitnessScope scope,
            MySqlFull20NativeTransfer retainedTransfer)throws IOException {
        this.issuer=Objects.requireNonNull(issuer);this.scope=Objects.requireNonNull(scope);
        this.retainedTransfer=Objects.requireNonNull(retainedTransfer);issuer.requireWitnessScope(scope);
    }
    boolean ownsKnownName(String name) {
        return pending!=null&&(name.equals(pending.temporary)||name.equals(pending.canonical)) ||
            published!=null&&name.equals(published.reference.basename());
    }
    private void requireDataLive(){
        if(Thread.currentThread()!=owner||consumerPhase!=0||retainedTransfer==null)
            throw new IllegalStateException("Original Full20 DATA consumers ended");
    }
    private void requireOwner()throws IOException {
        requireDataLive();
        if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact witness native owner required");
        issuer.requireWitnessScope(scope);
        if(retainedFailure!=null)throw new IOException("Original witness artifact failure retained",retainedFailure);
    }
    public Throwable retainedFailure(){if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact witness owner required");return retainedFailure;}
    private static final Pattern HISTORICAL_BASENAME=Pattern.compile("^\\.floatingpod-b1-full20-v1-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-([0-9a-f]{64})\\.bin$");
    private record HistoricalCandidate(String basename,UUID attemptId,long length,byte[] sha256) {
        private HistoricalCandidate { sha256=sha256.clone(); }
        @Override public byte[] sha256(){return sha256.clone();}
        boolean same(HistoricalCandidate other){return other!=null&&basename.equals(other.basename)&&attemptId.equals(other.attemptId)&&length==other.length&&Arrays.equals(sha256,other.sha256);}
    }

    /** A complete old V4 artifact read under the actual NO_V5 source permit; this is DATA, never an old native ACK. */
    public static final class VerifiedHistoricalArtifact implements VerifiedArtifactReference {
        private WindowsMarkerIo io;
        private WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit;
        private com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting parent,child;
        private B1Full20WitnessReference reference;
        private byte[] completeReadback;
        private B1ResumeRecord.AttemptAssertions expectedAttempt;
        private final Thread owner=Thread.currentThread();
        private Throwable failure;
        private VerifiedHistoricalArtifact(WindowsMarkerIo io,
                WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting parent,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting child,
                B1Full20WitnessReference reference,byte[] completeReadback,
                B1ResumeRecord.AttemptAssertions expectedAttempt) {
            this.io=io;this.permit=permit;this.parent=parent;this.child=child;this.reference=reference;
            this.completeReadback=completeReadback;this.expectedAttempt=expectedAttempt;
        }
        private void requireDataLive(){if(Thread.currentThread()!=owner||parent==null)throw new IllegalStateException("Historical artifact consumers ended");}
        public B1Full20WitnessReference reference(){requireDataLive();return reference;}
        public void requireExactAttempt(B1ResumeRecord.AttemptAssertions attempt)throws IOException {
            requireDataLive();
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact historical artifact owner required");
            if(attempt==null)throw new IOException("Exact historical attempt required");
            B1ResumeRecordCodec.exact(B1ResumeRecordCodec.attemptBytes(attempt),B1ResumeRecordCodec.attemptBytes(expectedAttempt),
                "Historical artifact reference differs from full original attempt/context/writer/common graphs");
        }
        WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permitForInstalledReissue(){requireDataLive();return permit;}
        com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting parentForInstalledReissue(){requireDataLive();return parent;}
        Thread ownerForInstalledReissue(){return owner;}
        byte[] completeReadbackForInstalledReissue(){requireDataLive();return completeReadback;}
        public VerifiedArtifactReference reissueUnderInstalledScope(
                WindowsB1TwoSlotStore.NativeWitnessScope installedScope,MySqlFull20NativeTransfer transfer)throws IOException {
            requireDataLive();
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact historical artifact owner required");
            if(failure!=null)throw new IOException("Historical artifact failure remains sticky",failure);
            if(installedScope==null||transfer==null)throw new IOException("Actual installed scope and original full20 transfer required");
            installedScope.requireInstalledHistoricalArtifact(this);
            transfer.requireSameOriginalRoot(parent);
            if(!Arrays.equals(reference.fullSha256(),WindowsWorldProvisioner.digest(completeReadback))||
                    !Arrays.equals(reference.profileSha256(),transfer.profileSha256()))
                throw new IOException("Historical complete bytes/source profile differ from the original handoff root");
            requireExactAttempt(expectedAttempt);
            return new InstalledHistoricalArtifactReference(this,installedScope,transfer);
        }
        public void revalidateForReference()throws IOException {
            requireDataLive();
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact historical artifact owner required");
            if(failure!=null)throw new IOException("Historical artifact reference responsibility retained",failure);
            WindowsMarkerIo.Handle handle=null;
            try {
                permit.requireForHistoricalReference(io,parent);
                Path directory=permit.directoryForHistoricalReference(io,parent);
                UUID operation=permit.operationForHistoricalReference(io,parent);
                String nativeWorld=permit.exactNativeWorldPathForHistoricalReference(io,parent);
                child.beginHistoricalReferenceRead(Math.toIntExact(reference.fullLength()));
                handle=io.open(directory.resolve(reference.basename()),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                    WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,operation);
                WindowsB1SourceAnchorStore.requireExactNativeChild(nativeWorld,io.finalPath(handle),reference.basename());
                io.metadata(handle,false,reference.fullLength());
                if(!reference.fileIdentity().equals(io.identity(handle)))throw new IOException("Historical Full20 native identity changed");
                io.compareWitnessArtifactExact(handle,completeReadback,reference.fileIdentity(),parent.artifactLimit());
                io.flushChecked(handle);
                io.compareWitnessArtifactExact(handle,completeReadback,reference.fileIdentity(),parent.artifactLimit());
                permit.requireForHistoricalReference(io,parent);
                io.closeChecked(handle);handle=null;
                child.finishHistoricalReferenceRead();
            } catch(Throwable primary) {
                failure=primary;
                try { child.failHistoricalReferenceRead(primary); }
                catch(Throwable cleanup) { if(cleanup!=primary)primary.addSuppressed(cleanup); }
                WindowsMarkerIo.closeSuppressed(handle,primary);
                throw propagate(primary);
            }
        }
        public void endOrdinaryConsumers(MySqlCertifiedRecoveryInspectionActor actor,
                MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state)throws IOException {
            requireDataLive();if(failure!=null)throw new IOException("Historical artifact failure retained",failure);
            parent.requireOriginalTerminalIdentity(actor,state);child.requireOriginalTerminalIdentity(actor,state);
            io=null;permit=null;parent=null;child=null;reference=null;completeReadback=null;expectedAttempt=null;
        }

    }

    /** Reissued DATA capability is bound to installed custody and the same original actor root. */
    private static final class InstalledHistoricalArtifactReference implements VerifiedArtifactReference {
        private final VerifiedHistoricalArtifact historical;
        private final WindowsB1TwoSlotStore.NativeWitnessScope installedScope;
        private final MySqlFull20NativeTransfer transfer;
        private final Thread owner=Thread.currentThread();
        private WindowsMarkerIo.Handle pendingHandle;
        private byte[] pendingFirst,pendingSecond;
        private Throwable failure;
        InstalledHistoricalArtifactReference(VerifiedHistoricalArtifact historical,
                WindowsB1TwoSlotStore.NativeWitnessScope installedScope,MySqlFull20NativeTransfer transfer) {
            this.historical=historical;this.installedScope=installedScope;this.transfer=transfer;
        }
        public B1Full20WitnessReference reference(){requireDataLive();return historical.reference();}
        public void requireExactAttempt(B1ResumeRecord.AttemptAssertions attempt)throws IOException {
            requireOwner();historical.requireExactAttempt(attempt);
        }
        private void requireDataLive(){
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact installed historical reference owner required");
            historical.requireDataLive();
        }
        private void requireOwner()throws IOException {
            requireDataLive();
            if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact installed historical reference owner required");
            if(failure!=null)throw new IOException("Installed historical reference failure remains sticky",failure);
            installedScope.requireInstalledHistoricalArtifact(historical);
            transfer.requireSameOriginalRoot(historical.parentForInstalledReissue());
        }
        public void revalidateForReference()throws IOException {
            requireOwner();
            long length=reference().fullLength();
            if(pendingHandle!=null||pendingFirst!=null||pendingSecond!=null)
                throw new IOException("Installed historical reference retained an unfinished native read workspace",failure);
            transfer.beginReferenceRevalidation(length);
            WindowsMarkerIo io=installedScope.witnessIo();
            WindowsMarkerIo.Handle handle=null;
            byte[] first=null,second=null;
            try {
                var path=installedScope.witnessDirectory().resolve(reference().basename());
                var operation=installedScope.witnessOperation();
                handle=io.open(path,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                    WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,operation);
                pendingHandle=handle;
                WindowsB1SourceAnchorStore.requireExactNativeChild(installedScope.witnessNativePath(),
                    io.finalPath(handle),reference().basename());
                io.metadata(handle,false,length);
                var identity=io.identity(handle);
                if(!reference().fileIdentity().equals(identity))throw new IOException("Installed historical full20 native identity changed");
                first=io.readWitnessArtifactExact(handle,Math.toIntExact(length),transfer.artifactLimit());
                pendingFirst=first;
                if(!Arrays.equals(first,historical.completeReadbackForInstalledReissue())||
                        !Arrays.equals(reference().fullSha256(),WindowsWorldProvisioner.digest(first)))
                    throw new IOException("Installed historical full20 bytes differ from exact retained source DATA");
                io.flushChecked(handle);
                second=io.readWitnessArtifactExact(handle,Math.toIntExact(length),transfer.artifactLimit());
                pendingSecond=second;
                if(!Arrays.equals(first,second)||!Arrays.equals(second,historical.completeReadbackForInstalledReissue())||
                        !identity.equals(io.identity(handle)))
                    throw new IOException("Installed historical full20 bytes/identity changed across flush");
                installedScope.requireInstalledHistoricalArtifact(historical);
                io.closeChecked(handle);handle=null;pendingHandle=null;
                Arrays.fill(first,(byte)0);first=null;pendingFirst=null;
                Arrays.fill(second,(byte)0);second=null;pendingSecond=null;
                transfer.referenceRevalidationReturned(length);
            } catch(Throwable primary) {
                failure=primary;
                try {installedScope.retainInstalledWitnessFailure(primary);}
                catch(Throwable cleanup) {if(cleanup!=primary)primary.addSuppressed(cleanup);}
                if(handle!=null)try {io.closeChecked(handle);pendingHandle=null;handle=null;}
                catch(Throwable cleanup) {if(cleanup!=primary)primary.addSuppressed(cleanup);}
                throw propagate(primary);
            }
        }
    }

    private static HistoricalCandidate parseHistoricalCandidate(WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting,
            WindowsMarkerIo.Full20WitnessFindEntry entry)throws IOException {
        if(entry==null)throw new IOException("Native historical discovery returned a null entry");
        if((entry.attributes()&0x10)!=0||(entry.attributes()&0x400)!=0)
            throw new IOException("Historical Full20 discovery found a directory or reparse entry");
        String name=entry.basename();Matcher matcher=HISTORICAL_BASENAME.matcher(name);
        if(!matcher.matches()||name.indexOf((char)92)!=-1||name.indexOf((char)47)!=-1)
            throw new IOException("Historical Full20 candidate is not one exact canonical basename");
        UUID attemptId;byte[] sha;
        try {
            attemptId=UUID.fromString(matcher.group(1));sha=HexFormat.of().parseHex(matcher.group(2));
            if(!attemptId.toString().equals(matcher.group(1)))throw new IllegalArgumentException("noncanonical UUID");
        } catch(IllegalArgumentException malformed) { throw new IOException("Historical Full20 candidate identity is malformed",malformed); }
        permit.expectedAttempt(attemptId);
        long length=entry.length();
        if(length<=0||length>Integer.MAX_VALUE||length>accounting.artifactLimit())
            throw new IOException("Historical Full20 candidate length is outside the original profile bound");
        return new HistoricalCandidate(name,attemptId,length,sha);
    }

    private static Map<String,HistoricalCandidate> enumerateHistoricalPass(WindowsMarkerIo io,
            WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        WindowsMarkerIo.Full20WitnessFindSequence sequence=null;
        Throwable primary=null;Map<String,HistoricalCandidate> result=null;
        try {
            sequence=io.beginFull20WitnessDiscovery(permit,accounting);
            int expectedCount=permit.expectedAttemptCount();int capacity=permit.maxCandidates();
            Map<String,HistoricalCandidate> found=new LinkedHashMap<>(Math.max(4,expectedCount));
            HashSet<UUID> ids=new HashSet<>(Math.max(4,expectedCount));
            WindowsMarkerIo.Full20WitnessFindEntry entry;
            while((entry=sequence.next())!=null) {
                if(found.size()>=expectedCount) { permit.rememberOverflowName(entry.basename());throw new IOException("Historical Full20 candidate count exceeds exact V4 attempt set"); }
                HistoricalCandidate candidate=parseHistoricalCandidate(permit,accounting,entry);
                if(found.size()>=capacity) { permit.rememberOverflowName(candidate.basename());throw new IOException("Historical Full20 enumeration exceeded prepaid native candidate capacity"); }
                if(found.containsKey(candidate.basename())||!ids.add(candidate.attemptId())) {
                    permit.rememberOverflowName(candidate.basename());throw new IOException("Duplicate historical Full20 basename or attempt ID");
                }
                found.put(candidate.basename(),candidate);
            }
            if(found.size()!=expectedCount)throw new IOException("Historical Full20 enumeration omitted an exact V4 witness attempt");
            result=Map.copyOf(found);
        } catch(Throwable cause) { primary=cause; }
        if(sequence!=null) {
            try { sequence.close(); }
            catch(Throwable cleanup) { if(primary==null)primary=cleanup;else if(cleanup!=primary)primary.addSuppressed(cleanup); }
        }
        if(primary!=null) { permit.failFindAttempt(io,accounting,primary);throw propagate(primary); }
        return result;
    }

    private static void requireSameHistoricalPass(Map<String,HistoricalCandidate> first,Map<String,HistoricalCandidate> second)throws IOException {
        if(first==null||second==null||first.size()!=second.size())throw new IOException("Two bounded historical passes have different candidate counts");
        for(var entry:first.entrySet())if(!entry.getValue().same(second.get(entry.getKey())))
            throw new IOException("Two bounded historical passes differ in exact name/length/attempt/hash claim");
    }

    static List<VerifiedHistoricalArtifact> discoverHistorical(WindowsMarkerIo io,
            WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting)throws IOException {
        Objects.requireNonNull(io);Objects.requireNonNull(permit);Objects.requireNonNull(accounting);
        permit.requireFor(io,accounting);
        Path directory=permit.directoryFor(io,accounting);UUID operation=permit.operationFor(io,accounting);
        String nativeWorld=permit.exactNativeWorldPath();WindowsMarkerIo.FileIdentity directoryIdentity=permit.directoryIdentity();
        Map<String,HistoricalCandidate> first=enumerateHistoricalPass(io,permit,accounting);
        Map<String,HistoricalCandidate> second=enumerateHistoricalPass(io,permit,accounting);
        requireSameHistoricalPass(first,second);
        var result=new ArrayList<VerifiedHistoricalArtifact>(permit.expectedAttemptCount());
        try {
            for(HistoricalCandidate candidate:second.values()) {
                var child=accounting.newHistoricalReadAccounting(permit);
                child.beforeRead(candidate.length()); // Native OPEN and both exact read arrays are charged first.
                WindowsMarkerIo.Handle handle=null,openedHandle=null;byte[] secondBytes=null;
                try {
                    handle=io.open(directory.resolve(candidate.basename()),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                        WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,operation);
                    openedHandle=handle;permit.retainHistoricalArtifactOpen(child,handle);
                    WindowsB1SourceAnchorStore.requireExactNativeChild(nativeWorld,io.finalPath(handle),candidate.basename());
                    WindowsMarkerIo.FileIdentity identity=io.identity(handle);io.metadata(handle,false,candidate.length());
                    byte[] firstBytes=io.readWitnessArtifactExact(handle,Math.toIntExact(candidate.length()),accounting.artifactLimit());
                    child.retainReadBytes(firstBytes);permit.retainHistoricalArtifactFirst(child,firstBytes);
                    io.flushChecked(handle);
                    secondBytes=io.readWitnessArtifactExact(handle,firstBytes.length,accounting.artifactLimit());
                    permit.retainHistoricalArtifactSecond(secondBytes);
                    if(!Arrays.equals(firstBytes,secondBytes)||!identity.equals(io.identity(handle))||
                            !Arrays.equals(candidate.sha256(),WindowsWorldProvisioner.digest(firstBytes))||
                            !B1Full20WitnessReference.basename(candidate.attemptId(),candidate.sha256()).equals(candidate.basename()))
                        throw new IOException("Historical Full20 complete bytes/native identity differ from canonical candidate");
                    permit.requireFor(io,accounting);io.closeChecked(handle);handle=null;
                    permit.historicalArtifactHandleClosed(openedHandle);
                    child.nativeReadClosed();
                    permit.historicalArtifactSecondDropped();Arrays.fill(secondBytes,(byte)0);secondBytes=null;
                    var expected=permit.expectedAttempt(candidate.attemptId());
                    child.decodeHistoricalWitness(child.retainedHistoricalBytes(),expected,permit.selectedV4CanonicalView());
                    var decoded=child.decodedHistoricalWitness();var original=decoded.originalRecord();
                    byte[] profileHash=HexFormat.of().parseHex(accounting.profileSha256());
                    B1Full20WitnessReference reference=new B1Full20WitnessReference(candidate.basename(),identity,candidate.length(),
                        candidate.sha256(),original.sourceAnchorSha256(),WindowsWorldProvisioner.digest(original.originalReceipt()),
                        profileHash,expected.attemptId(),expected.phase(),expected.logicalWriteUuid());
                    var verified=new VerifiedHistoricalArtifact(io,permit,accounting,child,reference,child.retainedHistoricalBytes(),expected);
                    permit.registerHistoricalArtifact(verified);result.add(verified);
                } catch(Throwable cause) {
                    try { child.failHistoricalReferenceRead(cause); }
                    catch(Throwable cleanup) { if(cleanup!=cause)cause.addSuppressed(cleanup); }
                    WindowsMarkerIo.closeSuppressed(handle,cause);throw propagate(cause);
                }
            }
            permit.requireFor(io,accounting);
            WindowsMarkerIo.FileIdentity after=io.captureDirectoryIdentity(directory,operation);
            if(!directoryIdentity.equals(after))throw new IOException("Controlled historical Full20 directory identity changed across discovery");
            permit.completeAfterSecondPass();
            return permit.verifiedHistoricalArtifacts();
        } catch(Throwable primary) {
            try { permit.failFindAttempt(io,accounting,primary); }
            catch(Throwable cleanup) { if(cleanup!=primary)primary.addSuppressed(cleanup); }
            throw propagate(primary);
        }
    }
    public NativeArtifactDurability publish(MySqlFull20NativeTransfer transfer)throws IOException {
        requireOwner();
        if(transfer==null||pending!=null||published!=null)throw new IOException("Witness artifact publication is one-shot");
        transfer.requireOpen();
        if(transfer!=retainedTransfer)throw new IOException("Exact store-bound original full20 transfer required");
        var bytes=transfer.ownedPayload();var original=transfer.originalRecord();
        issuer.requireWitnessSourceRecord(scope,original);
        var attempt=original.assertions().pendingAttempt();
        if(attempt==null)throw new IOException("Original witness requires the actual pending attempt");
        byte[] digest=WindowsWorldProvisioner.digest(bytes);
        String canonical=B1Full20WitnessReference.basename(attempt.attemptId(),digest);
        String temporary=canonical+".tmp";
        pending=new Pending(transfer,temporary,canonical); // 在第一 native create 前登记责任。
        WindowsMarkerIo io=issuer.witnessIo(scope);Path directory=issuer.witnessDirectory(scope);UUID operation=issuer.witnessOperation(scope);
        try {
            pending.temporaryHandle=io.createMarkerHandle(directory.resolve(temporary),operation);
            io.metadata(pending.temporaryHandle,false,0);
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(scope),io.finalPath(pending.temporaryHandle),temporary);
            pending.createdIdentity=io.identity(pending.temporaryHandle);
            io.writeWitnessArtifact(pending.temporaryHandle,bytes,transfer.artifactLimit());
            io.metadata(pending.temporaryHandle,false,bytes.length);io.flushChecked(pending.temporaryHandle);
            io.moveWriteThroughNoReplace(directory.resolve(temporary),directory.resolve(canonical),operation);
            io.closeChecked(pending.temporaryHandle);pending.temporaryHandle=null;
            pending.canonicalHandle=io.open(directory.resolve(canonical),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,operation);
            var id=io.identity(pending.canonicalHandle);
            if(!pending.createdIdentity.equals(id))throw new IOException("Published witness identity differs from original CREATE_NEW");
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(scope),io.finalPath(pending.canonicalHandle),canonical);
            io.metadata(pending.canonicalHandle,false,bytes.length);
            pending.readback=io.readWitnessArtifactExact(pending.canonicalHandle,bytes.length,transfer.artifactLimit());
            if(!Arrays.equals(bytes,pending.readback))throw new IOException("Published complete witness differs from original payload");
            io.flushChecked(pending.canonicalHandle);
            byte[] second=io.readWitnessArtifactExact(pending.canonicalHandle,bytes.length,transfer.artifactLimit());
            try {if(!Arrays.equals(pending.readback,second)||!id.equals(io.identity(pending.canonicalHandle)))
                throw new IOException("Complete witness changed across native barrier");}
            finally {Arrays.fill(second,(byte)0);second=null;}
            issuer.requireWitnessScope(scope);io.closeChecked(pending.canonicalHandle);pending.canonicalHandle=null;
            var reference=new B1Full20WitnessReference(canonical,id,bytes.length,digest,original.sourceAnchorSha256(),
                WindowsWorldProvisioner.digest(original.originalReceipt()),transfer.profileSha256(),
                attempt.attemptId(),attempt.phase(),attempt.logicalWriteUuid());
            published=new NativeArtifactDurability(this,transfer,reference,pending.readback);
            transfer.nativeReturned(published);
            return published;
        } catch(Throwable primary) {
            retainedFailure=primary;
            closeRetained(io,pending.canonicalHandle,primary);closeRetained(io,pending.temporaryHandle,primary);
            throw propagate(primary);
        }
    }
    /** 真冷 reopen；不靠 path/hash 替 native identity 和全 bytes，原 holder仍保留。 */
    public byte[] readCold(NativeArtifactDurability ack)throws IOException {
        requireOwner();if(ack==null||ack.store!=this||ack!=published)throw new IOException("Exact private artifact ACK required");
        ack.transfer.beginColdRead();WindowsMarkerIo io=issuer.witnessIo(scope);
        WindowsMarkerIo.Handle handle=null;byte[] first=null;
        try {
            var ref=ack.reference;
            handle=io.open(issuer.witnessDirectory(scope).resolve(ref.basename()),WindowsMarkerIo.READ,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,issuer.witnessOperation(scope));
            coldHandle=handle;
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(scope),io.finalPath(handle),ref.basename());
            if(!ref.fileIdentity().equals(io.identity(handle)))throw new IOException("Cold witness native identity differs");
            io.metadata(handle,false,ref.fullLength());
            first=io.readWitnessArtifactExact(handle,Math.toIntExact(ref.fullLength()),ack.transfer.artifactLimit());
            ack.transfer.retainColdBytes(first); // close/readback异常仍由原 transfer保留实际读入bytes。
            byte[] second=io.readWitnessArtifactExact(handle,first.length,ack.transfer.artifactLimit());
            try {if(!Arrays.equals(first,second)||!Arrays.equals(first,ack.completeReadback)||
                    !Arrays.equals(WindowsWorldProvisioner.digest(first),ref.fullSha256())||!ref.fileIdentity().equals(io.identity(handle)))
                throw new IOException("Cold complete witness bytes/identity differ");}
            finally {Arrays.fill(second,(byte)0);second=null;}
            issuer.requireWitnessScope(scope);io.closeChecked(handle);handle=null;coldHandle=null;
            ack.transfer.coldNativeReturned();return first;
        } catch(Throwable primary) {
            retainedFailure=primary;closeRetained(io,handle,primary);throw propagate(primary);
        }
    }
    private void revalidateForReference(NativeArtifactDurability ack)throws IOException {
        requireOwner();if(ack==null||ack.store!=this||ack!=published)throw new IOException("Exact native artifact ACK required");
        ack.transfer.beginReferenceRevalidation();var io=issuer.witnessIo(scope);
        try {
            var ref=ack.reference;
            coldHandle=io.open(issuer.witnessDirectory(scope).resolve(ref.basename()),WindowsMarkerIo.READ,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,issuer.witnessOperation(scope));
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(scope),io.finalPath(coldHandle),ref.basename());
            io.metadata(coldHandle,false,ref.fullLength());
            if(!ref.fileIdentity().equals(io.identity(coldHandle)))throw new IOException("Referenced witness identity changed");
            revalidationBytes=io.readWitnessArtifactExact(coldHandle,Math.toIntExact(ref.fullLength()),ack.transfer.artifactLimit());
            if(!Arrays.equals(ack.completeReadback,revalidationBytes)||!Arrays.equals(ref.fullSha256(),WindowsWorldProvisioner.digest(revalidationBytes)))
                throw new IOException("Referenced full witness bytes changed");
            issuer.requireWitnessScope(scope);io.closeChecked(coldHandle);coldHandle=null;
            Arrays.fill(revalidationBytes,(byte)0);revalidationBytes=null;
            ack.transfer.referenceRevalidationReturned();
        }catch(Throwable primary) {
            retainedFailure=primary;closeRetained(io,coldHandle,primary);throw propagate(primary);
        }
    }
    private static void closeRetained(WindowsMarkerIo io,WindowsMarkerIo.Handle handle,Throwable primary) {
        if(handle==null||handle.closeAttempted)return;
        try {io.closeChecked(handle);}catch(Throwable cleanup){if(cleanup!=primary)primary.addSuppressed(cleanup);}
        // Pending字段不清除：close失败必须保原 handle，亦无删除/reclaim。
    }
    private static IOException propagate(Throwable cause) {
        if(cause instanceof IOException io)return io;
        if(cause instanceof RuntimeException runtime)throw runtime;
        if(cause instanceof Error error)throw error;
        return new IOException("Witness native boundary failed",cause);
    }
    public void beginOrdinaryConsumerEnd(MySqlCertifiedRecoveryInspectionActor actor,
            MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state,
            WindowsB1TwoSlotStore.InstalledResumeRecordScope installed,
            NativeArtifactDurability actual)throws IOException {
        if(Thread.currentThread()!=owner||consumerPhase!=0||issuer==null||scope==null||retainedTransfer==null)
            throw new IOException("Exact live original Full20 owner required once");
        if(retainedFailure!=null)throw new IOException("Original Full20 failure retained",retainedFailure);
        retainedTransfer.requireOriginalTerminalAcknowledgement(actor,state,actual);
        issuer.requireOrdinaryTerminalFull20Store(scope,this,installed,retainedTransfer);
        if(actual!=published||actual.store!=this||actual.transfer!=retainedTransfer||pending==null||
                pending.transfer!=retainedTransfer||pending.temporaryHandle!=null||pending.canonicalHandle!=null||
                coldHandle!=null||revalidationBytes!=null)
            throw new IOException("Original Full20 ACK/store/pending/readback/native window differs");
        consumerPhase=1;
    }
    public void finishOrdinaryConsumerEnd(MySqlCertifiedRecoveryInspectionActor actor,
            MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state,
            NativeArtifactDurability actual)throws IOException {
        if(Thread.currentThread()!=owner||consumerPhase!=1||actual==null||actual!=published||actual.store!=this||
                actual.transfer!=retainedTransfer||retainedFailure!=null)
            throw new IOException("Original ending Full20 ACK/store identity required",retainedFailure);
        retainedTransfer.requireOriginalTerminalAcknowledgement(actor,state,actual);
        actual.transfer=null;actual.reference=null;actual.completeReadback=null;
        pending.transfer=null;pending.readback=null;pending=null;published=null;
        retainedTransfer=null;scope=null;issuer=null;consumerPhase=2;
    }

}

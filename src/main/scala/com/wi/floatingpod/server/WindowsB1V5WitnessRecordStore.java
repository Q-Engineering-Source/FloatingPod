package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlFull20NativeTransfer;
import com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static com.wi.floatingpod.server.B1ResumeRecord.*;

/** 独立V5槽链。记录/native ACK只表达完整耐久DATA，不授 ordinary/Fresh SQL权。 */
public final class WindowsB1V5WitnessRecordStore {
    private static final String[] NAMES={".floatingpod-b1-resume-v5.0",".floatingpod-b1-resume-v5.1"};
    private static final String TEMP=".floatingpod-b1-resume-v5.tmp";
    private static final String HISTORY_PREFIX=".floatingpod-b1-v5-hist-v1.";
    private static final String HISTORY_ID_PREFIX=".floatingpod-b1-v5-hist-id-v1.";
    private static final byte[] HISTORY_ID_DOMAIN="FP_V5_RETIRED_ID_DATA_V1".getBytes(StandardCharsets.US_ASCII);
    private static final int HISTORY_ID_LENGTH=260;
    private WindowsB1TwoSlotStore issuer;
    private WindowsB1TwoSlotStore.NativeWitnessScope source;
    private MySqlFull20NativeTransfer retainedTransfer;
    private WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 preinstall;
    private final Thread owner=Thread.currentThread();
    private final byte[] originalV4Bytes;
    private final B1ResumeRecord originalV4;
    private final WindowsMarkerIo.FileIdentity originalV4Identity;
    private NativeV5RecordDurability current;
    private NativeV5RecordDurability previous;
    private WindowsMarkerIo.Handle temporary,canonical;
    private byte[] pendingBytes;
    private Throwable retainedFailure;
    private B1ResumeRecordV5 coldData;
    private VerifiedHandoffRetirementFlight retirementFlight;
    private boolean retirementPreparationAttempted;
    private int consumerPhase;
    private WindowsMarkerIo.Handle retirementIdentityTemporary,retirementIdentityCanonical;
    private WindowsMarkerIo.Handle retirementCurrentHandle,retirementInactiveHandle,retirementHistoryHandle;
    private WindowsMarkerIo.Handle retirementIdentityReadHandle;
    private byte[] retirementIdentityBytes,retirementCurrentReadback,retirementInactiveReadback;
    private byte[] retirementPostRenameReadback,retirementHistoryReadback,retirementIdentityReadback;

    /** Bounded DATA only. It preserves the old container/native identity, never an ACK or retirement proof. */
    record RetiredIdentity(long sequence,int containerLength,byte[] containerSha256,
            WindowsMarkerIo.FileIdentity originalIdentity,byte[] sourceAnchorSha256,byte[] originalReceiptSha256,
            byte[] namespaceSha256,long bridgeSequence,byte[] bridgeSha256,WindowsMarkerIo.FileIdentity bridgeIdentity) {
        RetiredIdentity {
            if(sequence<=0||containerLength<=0||containerLength>B1ResumeRecordV5.MAX_RECORD_BYTES)
                throw new IllegalArgumentException("Retired V5 identity sequence/length bound");
            containerSha256=exactHash(containerSha256);sourceAnchorSha256=exactHash(sourceAnchorSha256);
            originalReceiptSha256=exactHash(originalReceiptSha256);namespaceSha256=exactHash(namespaceSha256);
            bridgeSha256=exactHash(bridgeSha256);Objects.requireNonNull(originalIdentity);Objects.requireNonNull(bridgeIdentity);
            if(bridgeSequence<0||sequence<=bridgeSequence)throw new IllegalArgumentException("Retired V5 bridge sequence bound");
        }
        private static byte[] exactHash(byte[] value) {
            if(value==null||value.length!=32)throw new IllegalArgumentException("Exact retired V5 SHA-256 required");
            return value.clone();
        }
        @Override public byte[] containerSha256(){return containerSha256.clone();}
        @Override public byte[] sourceAnchorSha256(){return sourceAnchorSha256.clone();}
        @Override public byte[] originalReceiptSha256(){return originalReceiptSha256.clone();}
        @Override public byte[] namespaceSha256(){return namespaceSha256.clone();}
        @Override public byte[] bridgeSha256(){return bridgeSha256.clone();}
    }

    private static final class RetirementIntent {
        private final NativeV5RecordDurability original;
        private final RetiredIdentity identity;
        private final byte[] encoded;
        private final String historyName,identityName,identityTemporaryName;
        private WindowsMarkerIo.FileIdentity identityFileIdentity;
        private boolean identityDurable,retired;
        private RetirementIntent(NativeV5RecordDurability original,RetiredIdentity identity,byte[] encoded,
                String historyName,String identityName,String identityTemporaryName) {
            this.original=original;this.identity=identity;this.encoded=encoded;
            this.historyName=historyName;this.identityName=identityName;this.identityTemporaryName=identityTemporaryName;
        }
    }

    /** Private actual-source lifecycle; neither ID DATA nor its native ACK is SQL/install authority. */
    public static final class VerifiedHandoffRetirementFlight {
        private final WindowsB1V5WitnessRecordStore store;
        private final NativeV5RecordDurability originalCurrent,originalInactive;
        private final RetirementIntent currentIntent,inactiveIntent;
        private NativeV5RecordDurability handoff;
        private boolean firstRenameClosed,firstHistoryReopened,secondRenameClosed,secondHistoryReopened;
        private Throwable failure;
        private WindowsB1Full20WitnessStore.NativeArtifactDurability handoffArtifact;
        private boolean artifactCompared,firstRetirementAttempted,secondRetirementAttempted,handoffPublicationAttempted;
        private FirstSlotAvailability firstSlotAvailability;
        private RetiredV5HistoryDurability secondRetirement;
        private NativeV5RecordDurability actualHandoff,actualDbAcquired;
        private WindowsB1Full20WitnessStore full20ArtifactStore;
        private boolean artifactStoreCreationAttempted;
        private WindowsB1TwoSlotStore.InstalledResumeRecordScope installedScope;
        private VerifiedHandoffRetirementFlight(WindowsB1V5WitnessRecordStore store,
                NativeV5RecordDurability current,NativeV5RecordDurability inactive,
                RetirementIntent currentIntent,RetirementIntent inactiveIntent) {
            this.store=store;this.originalCurrent=current;this.originalInactive=inactive;
            this.currentIntent=currentIntent;this.inactiveIntent=inactiveIntent;
        }
        public NativeV5RecordDurability actualHandoffRecord()throws IOException {
            store.requireFlight(this);
            if(actualHandoff==null)throw new IOException("Actual V5 HANDOFF native acknowledgement is not yet issued");
            return actualHandoff;
        }
        WindowsB1TwoSlotStore.NativeWitnessScope originalTerminalScope(WindowsB1TwoSlotStore actualIssuer,
                MySqlCertifiedRecoveryInspectionActor actor,MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state)throws IOException {
            store.requireOwnerIdentity();store.retainedTransfer.requireOriginalTerminalIdentity(actor,state);
            if(store.issuer!=actualIssuer||store.retirementFlight!=this||store.consumerPhase!=1)
                throw new IOException("Exact original ended V5 flight/store association required");
            return store.source;
        }
        public WindowsB1V5WitnessRecordStore store()throws IOException {
            store.requireFlight(this);return store;
        }
    }

    private static final class RetiredIdentityDurability {
        private final VerifiedHandoffRetirementFlight flight;
        private final RetirementIntent intent;
        private RetiredIdentityDurability(VerifiedHandoffRetirementFlight flight,RetirementIntent intent) {
            this.flight=flight;this.intent=intent;
        }
    }

    private static final class VerifiedRetiredHistory {
        private final RetirementIntent intent;
        private VerifiedRetiredHistory(RetirementIntent intent){this.intent=intent;}
    }


    public static final class RetiredV5HistoryDurability {
        private final VerifiedHandoffRetirementFlight flight;
        private final RetirementIntent intent;
        private final NativeV5RecordDurability currentBarrier;
        private RetiredV5HistoryDurability(VerifiedHandoffRetirementFlight flight,RetirementIntent intent,
                NativeV5RecordDurability currentBarrier) { this.flight=flight;this.intent=intent;this.currentBarrier=currentBarrier; }
    }
    public static final class FirstSlotAvailability {
        private final VerifiedHandoffRetirementFlight flight;
        private final RetiredV5HistoryDurability retired;
        private final int slot;
        private FirstSlotAvailability(VerifiedHandoffRetirementFlight flight,RetiredV5HistoryDurability retired,int slot) {
            this.flight=flight;this.retired=retired;this.slot=slot;
        }
    }

    static String historyBasename(long sequence,byte[] containerSha256)throws IOException {
        if(sequence<=0||containerSha256==null||containerSha256.length!=32)throw new IOException("Exact positive V5 history identity required");
        return HISTORY_PREFIX+Long.toString(sequence)+"."+HexFormat.of().formatHex(containerSha256);
    }
    static String historyIdentityBasename(long sequence,byte[] containerSha256)throws IOException {
        if(sequence<=0||containerSha256==null||containerSha256.length!=32)throw new IOException("Exact positive V5 history identity required");
        return HISTORY_ID_PREFIX+Long.toString(sequence)+"."+HexFormat.of().formatHex(containerSha256);
    }
    private static String historyIdentityTemporaryBasename(long sequence,byte[] containerSha256)throws IOException {
        return historyIdentityBasename(sequence,containerSha256)+".tmp";
    }
    private static byte[] encodeRetiredIdentity(RetiredIdentity value)throws IOException {
        Objects.requireNonNull(value);
        ByteArrayOutputStream bytes=new ByteArrayOutputStream(HISTORY_ID_LENGTH);
        try(DataOutputStream out=new DataOutputStream(bytes)) {
            out.writeInt(HISTORY_ID_DOMAIN.length);out.write(HISTORY_ID_DOMAIN);out.writeInt(1);out.writeLong(value.sequence());
            out.writeInt(value.containerLength());out.write(value.containerSha256());writeIdentity(out,value.originalIdentity());
            out.write(value.sourceAnchorSha256());out.write(value.originalReceiptSha256());out.write(value.namespaceSha256());
            out.writeLong(value.bridgeSequence());out.write(value.bridgeSha256());writeIdentity(out,value.bridgeIdentity());
        }
        byte[] encoded=bytes.toByteArray();if(encoded.length!=HISTORY_ID_LENGTH||encoded.length>512)
            throw new IOException("Private retired V5 identity DATA has an unexpected exact length");
        return encoded;
    }
    static int retiredIdentityLength(){return HISTORY_ID_LENGTH;}
    static RetiredIdentity decodeRetiredIdentity(byte[] encoded)throws IOException {
        if(encoded==null||encoded.length!=HISTORY_ID_LENGTH||encoded.length>512)
            throw new IOException("Retired V5 identity DATA has an unexpected exact length");
        try(DataInputStream in=new DataInputStream(new ByteArrayInputStream(encoded))) {
            int domainLength=in.readInt();if(domainLength!=HISTORY_ID_DOMAIN.length||
                !Arrays.equals(readExact(in,domainLength),HISTORY_ID_DOMAIN))throw new IOException("Unknown retired V5 identity DATA domain");
            if(in.readInt()!=1)throw new IOException("Unknown retired V5 identity DATA version");
            RetiredIdentity result=new RetiredIdentity(in.readLong(),in.readInt(),readExact(in,32),readIdentity(in),
                readExact(in,32),readExact(in,32),readExact(in,32),in.readLong(),readExact(in,32),readIdentity(in));
            if(in.read()!=-1||!Arrays.equals(encoded,encodeRetiredIdentity(result)))
                throw new IOException("Noncanonical/trailing retired V5 identity DATA");
            return result;
        } catch(IllegalArgumentException|EOFException failure) {
            throw new IOException("Malformed complete retired V5 identity DATA",failure);
        }
    }
    private static byte[] readExact(DataInputStream in,int size)throws IOException {
        byte[] value=in.readNBytes(size);if(value.length!=size)throw new EOFException("Truncated retired V5 identity DATA");return value;
    }
    private static void writeIdentity(DataOutputStream out,WindowsMarkerIo.FileIdentity identity)throws IOException {
        out.writeLong(identity.volumeSerialNumber());byte[] fileId=identity.fileId128();
        if(fileId.length!=16)throw new IOException("Native file identity must contain exactly 128 bits");out.write(fileId);
    }
    private static WindowsMarkerIo.FileIdentity readIdentity(DataInputStream in)throws IOException {
        return new WindowsMarkerIo.FileIdentity(in.readLong(),readExact(in,16));
    }
    private static RetiredIdentity identityFor(B1ResumeRecordV5 record,byte[] container,
            WindowsMarkerIo.FileIdentity originalIdentity)throws IOException {
        if(record==null||container==null||container.length==0||container.length>B1ResumeRecordV5.MAX_RECORD_BYTES||originalIdentity==null)
            throw new IOException("Complete actual V5 container/native identity required for history");
        B1ResumeRecord transition=record.transition();
        byte[] namespace=transition.barrierNamespace().getBytes(StandardCharsets.US_ASCII);
        return new RetiredIdentity(transition.sequence(),container.length,WindowsWorldProvisioner.digest(container),
            originalIdentity,transition.sourceAnchorSha256(),WindowsWorldProvisioner.digest(transition.originalReceipt()),
            WindowsWorldProvisioner.digest(namespace),record.bridgeSequence(),record.bridgeSha256(),record.bridgeIdentity());
    }
    static void requireIdentityMatches(RetiredIdentity actual,B1ResumeRecordV5 expectedRecord,
            byte[] expectedContainer,WindowsMarkerIo.FileIdentity expectedNativeIdentity)throws IOException {
        RetiredIdentity expected=identityFor(expectedRecord,expectedContainer,expectedNativeIdentity);
        if(!Arrays.equals(encodeRetiredIdentity(actual),encodeRetiredIdentity(expected)))
            throw new IOException("Retired V5 identity DATA differs from exact original sequence/container/source/bridge/native ID");
    }
    public static final class NativeV5RecordDurability {
        private final WindowsB1V5WitnessRecordStore store;
        private final B1ResumeRecordV5 record;
        private final byte[] bytes;
        private final WindowsMarkerIo.FileIdentity identity;
        private List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> artifacts;
        private NativeV5RecordDurability(WindowsB1V5WitnessRecordStore store,B1ResumeRecordV5 record,byte[] bytes,
                WindowsMarkerIo.FileIdentity identity,List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> artifacts) {
            this.store=store;this.record=record;this.bytes=bytes;this.identity=identity;this.artifacts=artifacts;
        }
        public B1ResumeRecordV5 record(){store.requireDataConsumersLive();return record;}
        public byte[] canonicalBytes()throws IOException {store.requireDataConsumersLive();return store.retainedTransfer.copyV5CanonicalBytes(bytes);}
        public WindowsMarkerIo.FileIdentity identity(){store.requireDataConsumersLive();return identity;}
    }
    WindowsB1V5WitnessRecordStore(WindowsB1TwoSlotStore issuer,WindowsB1TwoSlotStore.NativeWitnessScope source,
            MySqlFull20NativeTransfer retainedTransfer)throws IOException {
        this.issuer=issuer;this.source=source;this.retainedTransfer=retainedTransfer;
        this.preinstall=null;
        issuer.requireInitialV5Witness(source);
        originalV4Bytes=issuer.witnessPredecessorBytes(source);originalV4=issuer.witnessPredecessorRecord(source);
        originalV4Identity=issuer.witnessPredecessorIdentity(source);
    }

    WindowsB1V5WitnessRecordStore(WindowsB1TwoSlotStore issuer,WindowsB1TwoSlotStore.NativeWitnessScope source,
            WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 preinstall,MySqlFull20NativeTransfer retainedTransfer)throws IOException {
        this.issuer=Objects.requireNonNull(issuer);this.source=Objects.requireNonNull(source);
        this.retainedTransfer=Objects.requireNonNull(retainedTransfer);
        this.preinstall=Objects.requireNonNull(preinstall);
        issuer.requireV5PreinstallScope(source,preinstall,retainedTransfer,preinstall.inspectionForStore());
        originalV4Bytes=issuer.witnessPredecessorBytes(source);
        originalV4=issuer.witnessPredecessorRecord(source);
        originalV4Identity=issuer.witnessPredecessorIdentity(source);
        B1ResumeRecordV5 currentData=preinstall.currentRecord();
        byte[] currentBytes=preinstall.currentCanonicalBytes();
        var references=preinstall.artifactReferences();
        current=new NativeV5RecordDurability(this,currentData,currentBytes,preinstall.currentIdentity(),references);
        if(preinstall.hasInactiveRecord()) {
            B1ResumeRecordV5 inactiveData=preinstall.inactiveRecord();
            byte[] inactiveBytes=preinstall.inactiveCanonicalBytes();
            var inactiveReferences=new ArrayList<WindowsB1Full20WitnessStore.VerifiedArtifactReference>(references.size());
            for(var reference:references)for(var expected:inactiveData.allReferences())
                if(expected!=null&&reference.reference().exactlyEquals(expected)){
                    retainUnique(inactiveReferences,reference);break;
                }
            previous=new NativeV5RecordDurability(this,inactiveData,inactiveBytes,preinstall.inactiveIdentity(),List.copyOf(inactiveReferences));
        }
    }
    private void requireOwnerIdentity()throws IOException {
        if(retainedTransfer==null)throw new IOException("Original V5 identity consumers ended");
        if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact V5 native owner required");
        if(retainedFailure!=null)throw new IOException("V5 original pending responsibility retained",retainedFailure);
        retainedTransfer.requireOpen();
    }
    private void requireOwner()throws IOException {
        requireOwnerIdentity();if(consumerPhase!=0)throw new IOException("Original V5 DATA consumers ended");issuer.requireWitnessScope(source);
    }
    public Throwable retainedFailure(){if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact V5 owner required");return retainedFailure;}
    private Path path(String name)throws IOException {return issuer.witnessDirectory(source).resolve(name);}
    private WindowsMarkerIo.Handle open(String name)throws IOException {
        try {return issuer.witnessIo(source).open(path(name),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
            WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,issuer.witnessOperation(source));}
        catch(WindowsMarkerIo.NativeFailure failure){if(failure.win32Error()==2)return null;throw failure;}
    }
    private void absent(String name)throws IOException {
        WindowsMarkerIo.Handle handle=null;
        try {handle=open(name);if(handle!=null)throw new IOException("Existing V5 file responsibility; no overwrite");}
        catch(Throwable primary){WindowsMarkerIo.closeSuppressed(handle,primary);throw propagate(primary);}
    }
    /** 首ref桥只能从真实installed HANDOFF完整前件接续。每个live attempt都需原native artifact ACK。 */
    public NativeV5RecordDurability publishNext(NativeV5RecordDurability predecessor,Kind kind,FullStateAssertions state,
            WindowsB1Full20WitnessStore.VerifiedArtifactReference originalPrepare,
            WindowsB1Full20WitnessStore.VerifiedArtifactReference pending,
            WindowsB1Full20WitnessStore.VerifiedArtifactReference settled,
            WindowsB1Full20WitnessStore.VerifiedArtifactReference acknowledged)throws IOException {
        requireOwner();issuer.requireWitnessOwner(source,state);
        if(predecessor!=current)throw new IOException("Exact V5 current private ACK required");
        WindowsMarkerIo io=issuer.witnessIo(source);
        try {
            var artifacts=new ArrayList<WindowsB1Full20WitnessStore.VerifiedArtifactReference>(4);
            if(state.pendingAttempt()!=null&&!(pending instanceof WindowsB1Full20WitnessStore.NativeArtifactDurability))
                throw new IOException("Live pending attempt requires its actual new native publication ACK");
            var refs=new B1ResumeRecordV5.References(check(state.originalPrepareWitness(),originalPrepare,artifacts),
                check(state.pendingAttempt(),pending,artifacts),check(state.latestSettlement()==null?null:state.latestSettlement().settledAttempt(),settled,artifacts),
                check(state.acknowledgedCertifyWitness(),acknowledged,artifacts));
            absent(TEMP);
            B1ResumeRecord old;byte[] oldTransition;byte[] containerPrevious;
            if(predecessor==null) {
                issuer.requireInitialV5Witness(source);absent(NAMES[0]);absent(NAMES[1]);
                old=originalV4;oldTransition=originalV4Bytes;containerPrevious=WindowsWorldProvisioner.digest(originalV4Bytes);
            } else {
                requireCurrent(predecessor);old=predecessor.record.transition();
                oldTransition=B1ResumeRecordCanonical.encode(old);containerPrevious=WindowsWorldProvisioner.digest(predecessor.bytes);
            }
            var transition=new B1ResumeRecord(Math.addExact(old.sequence(),1),WindowsWorldProvisioner.digest(oldTransition),kind,
                old.sourceAnchorSha256(),old.originalReceipt(),old.barrierNamespace(),state);
            B1ResumeRecordCodec.consecutive(old,oldTransition,transition);issuer.requireWitnessSourceRecord(source,transition);
            var next=new B1ResumeRecordV5(transition,originalV4.sequence(),WindowsWorldProvisioner.digest(originalV4Bytes),
                originalV4Identity,containerPrevious,refs);
            if(predecessor!=null)B1ResumeRecordV5.consecutive(predecessor.record,predecessor.bytes,next);
            pendingBytes=B1ResumeRecordV5.encodeData(next); // 全record容量失败在第一mutation前。
            String name=NAMES[(int)(transition.sequence()%2)];absent(name);
            temporary=io.createMarkerHandle(path(TEMP),issuer.witnessOperation(source));io.metadata(temporary,false,0);
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(temporary),TEMP);
            var created=io.identity(temporary);io.writeWitnessArtifact(temporary,pendingBytes,B1ResumeRecordV5.MAX_RECORD_BYTES);
            io.metadata(temporary,false,pendingBytes.length);io.flushChecked(temporary);
            io.moveWriteThroughNoReplace(path(TEMP),path(name),issuer.witnessOperation(source));io.closeChecked(temporary);temporary=null;
            canonical=open(name);if(canonical==null)throw new IOException("Published V5 slot missing");
            byte[] bytes=readHeld(name,canonical,pendingBytes.length,created,next);
            if(!Arrays.equals(pendingBytes,bytes))throw new IOException("V5 published complete bytes differ");
            io.flushChecked(canonical);byte[] after=readHeld(name,canonical,bytes.length,created,next);
            if(!Arrays.equals(bytes,after))throw new IOException("V5 bytes changed across flush");
            issuer.requireWitnessScope(source);io.closeChecked(canonical);canonical=null;
            previous=predecessor;current=new NativeV5RecordDurability(this,next,after,created,List.copyOf(artifacts));return current;
        } catch(Throwable primary) {
            retainedFailure=primary;WindowsMarkerIo.closeSuppressed(canonical,primary);WindowsMarkerIo.closeSuppressed(temporary,primary);
            throw propagate(primary);
        }
    }
    private B1Full20WitnessReference check(AttemptAssertions attempt,WindowsB1Full20WitnessStore.VerifiedArtifactReference ack,
            List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> retained)throws IOException {
        if((attempt==null)!=(ack==null))throw new IOException("Every original attempt requires complete native witness DATA reference");
        if(ack==null)return null;
        if(ack instanceof WindowsB1Full20WitnessStore.NativeArtifactDurability nativeAck) {
            retainedTransfer.requireSameOriginalRoot(nativeAck.originalTransfer());
            nativeAck.originalTransfer().requireCompleteColdComparison();
        }
        ack.requireExactAttempt(attempt);ack.revalidateForReference();
        retainUnique(retained,ack);return ack.reference();
    }
    private byte[] readHeld(String name,WindowsMarkerIo.Handle handle,int length,WindowsMarkerIo.FileIdentity id,B1ResumeRecordV5 record)throws IOException {
        var io=issuer.witnessIo(source);
        WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(handle),name);
        if(!id.equals(io.identity(handle)))throw new IOException("V5 slot native identity changed");
        io.metadata(handle,false,length);byte[] bytes=io.readWitnessArtifactExact(handle,length,B1ResumeRecordV5.MAX_RECORD_BYTES);
        if(record.transition().sequence()%2!=Arrays.asList(NAMES).indexOf(name)||record.bridgeSequence()!=originalV4.sequence()||
            !record.bridgeIdentity().equals(originalV4Identity)||!Arrays.equals(record.bridgeSha256(),WindowsWorldProvisioner.digest(originalV4Bytes)))
            throw new IOException("V5 bridge/fixed slot differs");
        issuer.requireWitnessSourceRecord(source,record.transition());return bytes;
    }
    private void requireCurrent(NativeV5RecordDurability ack)throws IOException {
        if(ack==null||ack.store!=this||ack!=current)throw new IOException("Exact current V5 ACK required");
        canonical=open(NAMES[(int)(ack.record.transition().sequence()%2)]);
        if(canonical==null)throw new IOException("Current V5 slot missing");
        try {for(var artifact:ack.artifacts)artifact.revalidateForReference();
            var bytes=readHeld(NAMES[(int)(ack.record.transition().sequence()%2)],canonical,ack.bytes.length,ack.identity,ack.record);
            if(!Arrays.equals(bytes,ack.bytes))throw new IOException("Current complete V5 bytes changed");
            for(var artifact:ack.artifacts)artifact.revalidateForReference();
            issuer.witnessIo(source).closeChecked(canonical);canonical=null;
        }catch(Throwable primary){WindowsMarkerIo.closeSuppressed(canonical,primary);throw propagate(primary);}
    }
    /** V5真实冷reopen全bytes核验后strict DATA解码，并重核全部referenced artifacts。 */
    public B1ResumeRecordV5 readCold(NativeV5RecordDurability ack)throws IOException {
        requireOwner();
        try {
            requireCurrent(ack);
            for(var artifact:ack.artifacts)artifact.revalidateForReference();
            retainedTransfer.reserveV5ColdDecode(ack.bytes.length);
            coldData=B1ResumeRecordV5.decodeData(ack.bytes);
            if(!Arrays.equals(B1ResumeRecordV5.encodeData(coldData),ack.bytes))throw new IOException("Cold V5 complete canonical bytes differ");
            return coldData;
        }catch(Throwable primary){retainedFailure=primary;throw propagate(primary);}
    }

    // inspection已安装、witness scope尚未绑定时，只核原first-hang身份，不重走preinstall验证。
    void requireInstalledBindingIdentity(WindowsB1TwoSlotStore actualIssuer,
            WindowsB1TwoSlotStore.InstalledResumeRecordScope installed,
            VerifiedHandoffRetirementFlight flight)throws IOException {
        requireFlightIdentity(flight);
        if(actualIssuer!=issuer||installed==null||flight.installedScope!=installed||!handoffStage(flight))
            throw new IOException("Exact first-hung installed HANDOFF V5 binding identity required");
    }
    WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 preinstallForAssociation(
            VerifiedHandoffRetirementFlight flight)throws IOException {
        if(Thread.currentThread()!=owner||flight==null||flight.store!=this||flight!=retirementFlight||
                preinstall==null||flight.actualHandoff==null||flight.failure!=null)
            throw new IOException("Exact retained V5 flight/preinstall source required for install association",flight==null?null:flight.failure);
        return preinstall;
    }
    void rebarrierInstalledHandoff(VerifiedHandoffRetirementFlight flight,
            WindowsB1TwoSlotStore.InstalledResumeRecordScope installed)throws IOException {
        requireHandoffFlight(flight);
        if(flight.actualHandoff==null||installed==null||installed.v5HandoffRecord()!=flight.actualHandoff.record())
            throw new IOException("Exact actual installed V5 HANDOFF record association required");
        requireCurrent(flight.actualHandoff);
    }
    public VerifiedHandoffRetirementFlight preparedRetirementFlight()throws IOException {
        requireOwner();if(retirementFlight==null)throw new IOException("No actual prepared V5 retirement flight is retained");
        requireFlight(retirementFlight);return retirementFlight;
    }
    WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 preinstallSourceForInstall(
            VerifiedHandoffRetirementFlight flight)throws IOException {
        requireHandoffFlight(flight);if(preinstall==null)throw new IOException("Exact actual V5 preinstall source required");
        return preinstall;
    }
    public WindowsB1Full20WitnessStore beginHandoffArtifactStore(VerifiedHandoffRetirementFlight flight)throws IOException {
        requireOriginalFlight(flight);
        if(flight.artifactStoreCreationAttempted||flight.artifactCompared||flight.firstRetirementAttempted)
            throw new IOException("Exact pre-retirement full20 artifact store creation is one-shot");
        flight.artifactStoreCreationAttempted=true;
        try {flight.full20ArtifactStore=issuer.full20WitnessStore(source,retainedTransfer);return flight.full20ArtifactStore;}
        catch(Throwable primary){flight.failure=primary;retainedFailure=primary;throw propagate(primary);}
    }

    private void requireFlightIdentity(VerifiedHandoffRetirementFlight flight)throws IOException {
        requireOwnerIdentity();
        if(flight==null||flight.store!=this||flight!=retirementFlight||flight.failure!=null)
            throw new IOException("Exact live original V5 retirement flight required",flight==null?retainedFailure:flight.failure);
    }
    private boolean originalStage(VerifiedHandoffRetirementFlight flight) {
        return flight.originalCurrent==current&&flight.originalInactive==previous;
    }
    private boolean handoffStage(VerifiedHandoffRetirementFlight flight) {
        return flight.actualHandoff!=null&&current==flight.actualHandoff&&previous==flight.originalCurrent;
    }
    private boolean settledStage(VerifiedHandoffRetirementFlight flight) {
        return flight.actualDbAcquired!=null&&flight.actualDbAcquired.store==this&&
            flight.actualHandoff!=null&&flight.actualHandoff.store==this&&
            current==flight.actualDbAcquired&&previous==flight.actualHandoff&&
            flight.secondRetirement!=null&&flight.installedScope!=null;
    }
    private void requireDataConsumersLive(){
        if(Thread.currentThread()!=owner||consumerPhase!=0||retainedTransfer==null)
            throw new IllegalStateException("Original V5 DATA consumers ended");
        if(retainedFailure!=null)throw new IllegalStateException("Original V5 failure retained",retainedFailure);
    }
    private void requireFlight(VerifiedHandoffRetirementFlight flight)throws IOException {
        requireDataConsumersLive();
        requireFlightIdentity(flight);
        if(!originalStage(flight)&&!handoffStage(flight)&&!settledStage(flight))
            throw new IOException("Original, HANDOFF, or exact settled V5 native identity stage changed");
        issuer.requireWitnessScope(source);
    }
    private void requireOriginalFlight(VerifiedHandoffRetirementFlight flight)throws IOException {
        requireDataConsumersLive();
        requireFlightIdentity(flight);
        if(!originalStage(flight))throw new IOException("Exact original V5 flight stage required");
        issuer.requireWitnessScope(source);
    }
    private void requireHandoffFlight(VerifiedHandoffRetirementFlight flight)throws IOException {
        requireDataConsumersLive();
        requireFlightIdentity(flight);
        if(!handoffStage(flight))throw new IOException("Exact actual HANDOFF V5 flight stage required");
        issuer.requireWitnessScope(source);
    }
    private void requireInstalledHandoffFlight(VerifiedHandoffRetirementFlight flight)throws IOException {
        requireHandoffFlight(flight);
        if(flight.installedScope==null)throw new IOException("Exact installed HANDOFF V5 flight scope required");
        issuer.requireInstalledWitnessAssociation(source,preinstall,retainedTransfer,flight.installedScope);
    }
    // 只核原关联，不反向进入issuer的完整installed检查，避免递归。
    void requireInstalledAssociation(WindowsB1TwoSlotStore actualIssuer,
            WindowsB1TwoSlotStore.InstalledResumeRecordScope installed,
            VerifiedHandoffRetirementFlight flight)throws IOException {
        requireFlightIdentity(flight);
        if(actualIssuer!=issuer||installed==null||flight.installedScope!=installed||preinstall==null||
                (!handoffStage(flight)&&!settledStage(flight)))
            throw new IOException("Exact installed V5 source/store/flight identity required");
        issuer.requireInstalledWitnessAssociation(source,preinstall,retainedTransfer,installed);
    }
    /** Freeze exact current/inactive IDs and both IDDATA records before any flight CREATE_NEW. */
    public VerifiedHandoffRetirementFlight prepareVerifiedHandoffRetirement()throws IOException {
        requireOwner();
        if(retirementPreparationAttempted)throw new IOException("V5 retirement flight preparation is sticky once attempted",retainedFailure);
        retirementPreparationAttempted=true;
        try {
            if(retirementFlight!=null||current==null||
                    (current.record.transition().kind()!=Kind.CERTIFIED&&current.record.transition().kind()!=Kind.DB_ACQUIRED))
                throw new IOException("One settled actual current V5 record is required for a new ordinary handoff");
            B1ResumeRecord planned=retainedTransfer.originalRecord();
            if(planned.kind()!=Kind.HANDOFF_PREPARED||planned.assertions().pendingAttempt()==null||
                    planned.assertions().pendingAttempt().phase()!=Phase.HANDOFF)
                throw new IOException("Exact actor-planned HANDOFF witness must be retained before retirement flight");
            retainedTransfer.reserveV5HistoryRetirements(2,HISTORY_ID_LENGTH);
            RetirementIntent currentIntent=prepareIntent(current);
            RetirementIntent inactiveIntent=previous==null?null:prepareIntent(previous);
            retirementFlight=new VerifiedHandoffRetirementFlight(this,current,previous,currentIntent,inactiveIntent);
            return retirementFlight;
        } catch(Throwable primary) {retainedFailure=primary;throw propagate(primary);}
    }
    private RetirementIntent prepareIntent(NativeV5RecordDurability original)throws IOException {
        if(original==null||original.store!=this||original.bytes.length==0||original.identity==null)
            throw new IOException("Exact actual V5 record identity/full container required before retirement");
        RetiredIdentity identity=identityFor(original.record,original.bytes,original.identity);
        byte[] encoded=encodeRetiredIdentity(identity);
        return new RetirementIntent(original,identity,encoded,
            historyBasename(identity.sequence(),identity.containerSha256()),
            historyIdentityBasename(identity.sequence(),identity.containerSha256()),
            historyIdentityTemporaryBasename(identity.sequence(),identity.containerSha256()));
    }
    /** The actual HANDOFF artifact and full cold comparison must precede the first retirement. */
    void validateForInstallation(VerifiedHandoffRetirementFlight flight,
            WindowsWorldProvisioner.ResumeInspection inspection)throws IOException {
        requireHandoffFlight(flight);
        if(inspection==null||flight.actualHandoff==null||flight.handoffArtifact==null||!flight.artifactCompared||
                flight.firstSlotAvailability==null||flight.handoffPublicationAttempted==false||
                flight.installedScope!=null||flight.failure!=null)
            throw new IOException("Actual compared artifact, first-slot proof, and published HANDOFF ACK required once",flight.failure);
        issuer.requireV5PreinstallScope(source,preinstall,retainedTransfer,inspection);
        if(!inspection.nativeHandoffPublicationAttempted())
            throw new IOException("Exact original HANDOFF publication responsibility required");
        if(current!=flight.actualHandoff)throw new IOException("Only the exact actual V5 HANDOFF current may be installed");
    }

    public void retainVerifiedHandoffArtifact(VerifiedHandoffRetirementFlight flight,
            WindowsB1Full20WitnessStore.NativeArtifactDurability artifact)throws IOException {
        requireOriginalFlight(flight);
        if(flight.artifactCompared||artifact==null)throw new IOException("One actual compared HANDOFF artifact is required");
        try {
            retainedTransfer.requireSameOriginalRoot(artifact.originalTransfer());
            artifact.originalTransfer().requireCompleteColdComparison();
            B1ResumeRecord.AttemptAssertions attempt=retainedTransfer.originalRecord().assertions().pendingAttempt();
            B1Full20WitnessReference reference=artifact.reference();
            if(attempt==null||attempt.phase()!=Phase.HANDOFF||!attempt.attemptId().equals(reference.attemptId())||
                    !attempt.logicalWriteUuid().equals(reference.logicalOperation())||reference.phase()!=Phase.HANDOFF||
                    !Arrays.equals(reference.profileSha256(),retainedTransfer.profileSha256()))
                throw new IOException("Native artifact ACK differs from the exact actor-planned HANDOFF attempt/profile");
            artifact.requireExactAttempt(attempt);artifact.revalidateForReference();
            flight.handoffArtifact=artifact;flight.artifactCompared=true;
        } catch(Throwable primary) {flight.failure=primary;retainedFailure=primary;throw propagate(primary);}
    }
    private WindowsB1Full20WitnessStore.VerifiedArtifactReference existingReference(
            List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> retained,
            AttemptAssertions expected,byte[] profileSha256)throws IOException {
        if(expected==null)return null;
        WindowsB1Full20WitnessStore.VerifiedArtifactReference found=null;
        for(var artifact:retained) {
            if(artifact==null)throw new IOException("Null verified artifact reference retained in V5 source");
            B1Full20WitnessReference reference=artifact.reference();
            if(reference==null)throw new IOException("V5 source retained a reference without complete DATA identity");
            if(!reference.attemptId().equals(expected.attemptId()))continue;
            if(!reference.logicalOperation().equals(expected.logicalWriteUuid())||reference.phase()!=expected.phase()||
                    !Arrays.equals(reference.profileSha256(),profileSha256))
                throw new IOException("Existing V5 artifact identity conflicts with the exact complete source attempt/profile");
            artifact.requireExactAttempt(expected);
            artifact.revalidateForReference();
            if(found!=null&&!found.reference().exactlyEquals(reference))
                throw new IOException("Existing V5 source retained conflicting full20 identities for one attempt");
            found=artifact;
        }
        if(found==null)throw new IOException("Exact original V5 full20 artifact reference is missing");
        return found;
    }
    private static void retainUnique(List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> target,
            WindowsB1Full20WitnessStore.VerifiedArtifactReference artifact)throws IOException {
        if(artifact==null)return;
        for(var prior:target) {
            B1Full20WitnessReference left=prior.reference(),right=artifact.reference();
            if(left.attemptId().equals(right.attemptId())) {
                if(!left.exactlyEquals(right))throw new IOException("One attempt resolves to conflicting complete full20 artifact identities");
                return;
            }
        }
        target.add(artifact);
    }
    private NativeV5RecordDurability publishExactV5Successor(NativeV5RecordDurability predecessor,
            B1ResumeRecordV5 next,List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> artifacts)throws IOException {
        WindowsMarkerIo io=issuer.witnessIo(source);
        pendingBytes=B1ResumeRecordV5.encodeData(next);
        String name=NAMES[(int)(next.transition().sequence()%2)];
        absent(TEMP);absent(name);
        temporary=io.createMarkerHandle(path(TEMP),issuer.witnessOperation(source));io.metadata(temporary,false,0);
        WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(temporary),TEMP);
        WindowsMarkerIo.FileIdentity created=io.identity(temporary);
        io.writeWitnessArtifact(temporary,pendingBytes,B1ResumeRecordV5.MAX_RECORD_BYTES);
        io.metadata(temporary,false,pendingBytes.length);io.flushChecked(temporary);
        io.moveWriteThroughNoReplace(path(TEMP),path(name),issuer.witnessOperation(source));
        io.closeChecked(temporary);temporary=null;
        canonical=open(name);if(canonical==null)throw new IOException("Published V5 successor missing after one no-replace move");
        byte[] read=readHeld(name,canonical,pendingBytes.length,created,next);
        if(!Arrays.equals(pendingBytes,read))throw new IOException("Published V5 successor complete bytes differ");
        io.flushChecked(canonical);
        byte[] after=readHeld(name,canonical,read.length,created,next);
        if(!Arrays.equals(read,after))throw new IOException("Published V5 successor changed across flush");
        for(var artifact:artifacts)artifact.revalidateForReference();
        issuer.requireWitnessScope(source);io.closeChecked(canonical);canonical=null;
        previous=predecessor;
        current=new NativeV5RecordDurability(this,next,after,created,List.copyOf(artifacts));
        return current;
    }
    /** Publish only the HANDOFF record retained by this exact preinstall flight, after first-slot proof. */
    public NativeV5RecordDurability publishVerifiedHandoffPrepared(VerifiedHandoffRetirementFlight flight,
            FirstSlotAvailability availability)throws IOException {
        requireOriginalFlight(flight);
        if(!flight.artifactCompared||flight.handoffArtifact==null||flight.handoffPublicationAttempted||
                availability==null||availability.flight!=flight||availability!=flight.firstSlotAvailability)
            throw new IOException("Exact retained artifact and privately issued first-slot availability required once");
        flight.handoffPublicationAttempted=true;
        try {
            RetirementIntent inactive=flight.inactiveIntent;
            int target=(int)(Math.addExact(flight.originalCurrent.record.transition().sequence(),1L)%2);
            if(availability.slot!=target)throw new IOException("First-slot availability does not designate the exact successor parity");
            if(inactive==null) {
                if(availability.retired!=null)throw new IOException("Initially absent slot cannot become a retirement ACK");
            } else {
                if(availability.retired==null||availability.retired.flight!=flight||availability.retired.intent!=inactive||
                        !inactive.retired||availability.retired.currentBarrier!=flight.originalCurrent)
                    throw new IOException("Exact old inactive V5 history retirement ACK required before HANDOFF publication");
            }
            requireCurrent(flight.originalCurrent);
            B1ResumeRecord handoff=retainedTransfer.originalRecord();
            if(handoff.kind()!=Kind.HANDOFF_PREPARED||handoff.assertions().pendingAttempt()==null||
                    handoff.assertions().pendingAttempt().phase()!=Phase.HANDOFF)
                throw new IOException("Exact retained original HANDOFF transition required");
            if(handoff.sequence()!=Math.addExact(flight.originalCurrent.record.transition().sequence(),1L)||
                    !Arrays.equals(handoff.previousRecordSha256(),
                        WindowsWorldProvisioner.digest(B1ResumeRecordCanonical.encode(flight.originalCurrent.record.transition()))))
                throw new IOException("Planned HANDOFF does not continue the exact current V5 transition");
            issuer.requireWitnessHandoffTransition(source,retainedTransfer,handoff);
            AttemptAssertions attempt=handoff.assertions().pendingAttempt();
            flight.handoffArtifact.requireExactAttempt(attempt);
            B1Full20WitnessReference handoffReference=flight.handoffArtifact.reference();
            if(!handoffReference.attemptId().equals(attempt.attemptId())||
                    !handoffReference.logicalOperation().equals(attempt.logicalWriteUuid())||
                    handoffReference.phase()!=Phase.HANDOFF||
                    !Arrays.equals(handoffReference.profileSha256(),retainedTransfer.profileSha256()))
                throw new IOException("HANDOFF witness ACK differs from exact planned complete attempt/profile");
            var artifacts=new ArrayList<WindowsB1Full20WitnessStore.VerifiedArtifactReference>(5);
            var state=handoff.assertions();
            retainUnique(artifacts,existingReference(flight.originalCurrent.artifacts,state.originalPrepareWitness(),retainedTransfer.profileSha256()));
            retainUnique(artifacts,flight.handoffArtifact);
            retainUnique(artifacts,existingReference(flight.originalCurrent.artifacts,
                state.latestSettlement()==null?null:state.latestSettlement().settledAttempt(),retainedTransfer.profileSha256()));
            retainUnique(artifacts,existingReference(flight.originalCurrent.artifacts,state.acknowledgedCertifyWitness(),retainedTransfer.profileSha256()));
            var refs=new B1ResumeRecordV5.References(
                referenceFor(state.originalPrepareWitness(),artifacts),
                handoffReference,
                referenceFor(state.latestSettlement()==null?null:state.latestSettlement().settledAttempt(),artifacts),
                referenceFor(state.acknowledgedCertifyWitness(),artifacts));
            var next=new B1ResumeRecordV5(handoff,originalV4.sequence(),WindowsWorldProvisioner.digest(originalV4Bytes),
                originalV4Identity,WindowsWorldProvisioner.digest(flight.originalCurrent.bytes),refs);
            B1ResumeRecordV5.consecutive(flight.originalCurrent.record,flight.originalCurrent.bytes,next);
            var actual=publishExactV5Successor(flight.originalCurrent,next,artifacts);
            flight.handoff=actual;flight.actualHandoff=actual;
            return actual;
        } catch(Throwable primary) {
            flight.failure=primary;retainedFailure=primary;
            WindowsMarkerIo.closeSuppressed(canonical,primary);WindowsMarkerIo.closeSuppressed(temporary,primary);
            throw propagate(primary);
        }
    }
    private static B1Full20WitnessReference referenceFor(AttemptAssertions attempt,
            List<WindowsB1Full20WitnessStore.VerifiedArtifactReference> retained)throws IOException {
        if(attempt==null)return null;
        for(var artifact:retained)if(artifact.reference().attemptId().equals(attempt.attemptId())) {
            artifact.requireExactAttempt(attempt);return artifact.reference();
        }
        throw new IOException("Full20 DATA reference for exact V5 attempt is missing");
    }
    private WindowsMarkerIo.Handle openRetirementPath(String name,int access,int share)throws IOException {
        try {return issuer.witnessIo(source).open(path(name),access,share,WindowsMarkerIo.OPEN_EXISTING,
            WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,issuer.witnessOperation(source));}
        catch(WindowsMarkerIo.NativeFailure failure) {if(failure.win32Error()==2)return null;throw failure;}
    }
    private void requireExactV5Handle(String name,WindowsMarkerIo.Handle handle,NativeV5RecordDurability expected)throws IOException {
        WindowsMarkerIo io=issuer.witnessIo(source);
        for(var artifact:expected.artifacts)artifact.revalidateForReference();
        WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(handle),name);
        if(!expected.identity.equals(io.identity(handle)))throw new IOException("V5 retirement native FileId changed");
        io.metadata(handle,false,expected.bytes.length);
        byte[] read=io.readWitnessArtifactExact(handle,expected.bytes.length,B1ResumeRecordV5.MAX_RECORD_BYTES);
        if(!Arrays.equals(read,expected.bytes))throw new IOException("V5 retirement complete container bytes changed");
        if(!Arrays.equals(B1ResumeRecordV5.encodeData(B1ResumeRecordV5.decodeData(read)),expected.bytes))
            throw new IOException("V5 retirement container is not complete canonical DATA");
        for(var artifact:expected.artifacts)artifact.revalidateForReference();
    }
    private void rebarrierHeldCurrent(WindowsMarkerIo.Handle handle,NativeV5RecordDurability expected)throws IOException {
        WindowsMarkerIo io=issuer.witnessIo(source);String name=NAMES[(int)(expected.record.transition().sequence()%2)];
        requireExactV5Handle(name,handle,expected);io.flushChecked(handle);requireExactV5Handle(name,handle,expected);
    }
    private void requireAbsentAfterRetirement(String name)throws IOException {
        WindowsMarkerIo.Handle check=null;
        Throwable primary=null;
        try {
            check=openRetirementPath(name,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(check!=null)throw new IOException("Original V5 canonical path remains after exact history rename");
        } catch(WindowsMarkerIo.NativeFailure absent) {
            if(absent.win32Error()!=2)primary=absent;
        } catch(Throwable cause) { primary=cause; }
        finally {
            if(check!=null) {
                try { issuer.witnessIo(source).closeChecked(check); }
                catch(Throwable cleanup) { if(primary==null)primary=cleanup;else if(cleanup!=primary)primary.addSuppressed(cleanup); }
            }
        }
        if(primary!=null)throw propagate(primary);
    }
    private void publishIdentityData(VerifiedHandoffRetirementFlight flight,RetirementIntent intent)throws IOException {
        WindowsMarkerIo io=issuer.witnessIo(source);
        if(intent.identityDurable||intent.identityFileIdentity!=null)throw new IOException("Retired identity CREATE_NEW is sticky once");
        absent(intent.identityTemporaryName);absent(intent.identityName);
        retirementIdentityTemporary=io.createMarkerHandle(path(intent.identityTemporaryName),issuer.witnessOperation(source));
        io.metadata(retirementIdentityTemporary,false,0);
        WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(retirementIdentityTemporary),intent.identityTemporaryName);
        WindowsMarkerIo.FileIdentity created=io.identity(retirementIdentityTemporary);
        io.writeWitnessArtifact(retirementIdentityTemporary,intent.encoded,512);io.metadata(retirementIdentityTemporary,false,intent.encoded.length);
        io.flushChecked(retirementIdentityTemporary);
        io.moveWriteThroughNoReplace(path(intent.identityTemporaryName),path(intent.identityName),issuer.witnessOperation(source));
        io.closeChecked(retirementIdentityTemporary);retirementIdentityTemporary=null;
        retirementIdentityCanonical=io.open(path(intent.identityName),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
            WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,issuer.witnessOperation(source));
        WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(retirementIdentityCanonical),intent.identityName);
        if(!created.equals(io.identity(retirementIdentityCanonical)))throw new IOException("Retired identity canonical FileId differs from CREATE_NEW");
        io.metadata(retirementIdentityCanonical,false,intent.encoded.length);
        retirementIdentityReadback=io.readWitnessArtifactExact(retirementIdentityCanonical,intent.encoded.length,512);
        if(!Arrays.equals(intent.encoded,retirementIdentityReadback))throw new IOException("Retired IDDATA complete bytes differ before flush");
        io.flushChecked(retirementIdentityCanonical);
        byte[] after=io.readWitnessArtifactExact(retirementIdentityCanonical,intent.encoded.length,512);
        if(!Arrays.equals(intent.encoded,after)||!created.equals(io.identity(retirementIdentityCanonical)))
            throw new IOException("Retired IDDATA changed across flush");
        decodeRetiredIdentity(after);
        io.closeChecked(retirementIdentityCanonical);retirementIdentityCanonical=null;
        intent.identityFileIdentity=created;intent.identityDurable=true;
    }
    /** First old inactive retirement. Rename happens once under the same held SHARE_DELETE handle. */
    public FirstSlotAvailability retireVerifiedInactiveV5(VerifiedHandoffRetirementFlight flight)throws IOException {
        requireOriginalFlight(flight);
        if(!flight.artifactCompared||flight.firstRetirementAttempted||flight.firstSlotAvailability!=null)
            throw new IOException("Compared HANDOFF artifact and one first-slot retirement attempt are required");
        flight.firstRetirementAttempted=true;
        RetirementIntent intent=flight.inactiveIntent;
        WindowsMarkerIo io=issuer.witnessIo(source);
        String currentName=NAMES[(int)(flight.originalCurrent.record.transition().sequence()%2)];
        String inactiveName=NAMES[1-(int)(flight.originalCurrent.record.transition().sequence()%2)];
        try {
            retirementCurrentHandle=openRetirementPath(currentName,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(retirementCurrentHandle==null)throw new IOException("Actual V5 current source disappeared before inactive retirement");
            rebarrierHeldCurrent(retirementCurrentHandle,flight.originalCurrent);
            if(intent==null) {
                retirementInactiveHandle=openRetirementPath(inactiveName,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
                if(retirementInactiveHandle!=null)throw new IOException("Actual first V5 inactive slot is present despite absence source DATA");
                rebarrierHeldCurrent(retirementCurrentHandle,flight.originalCurrent);
                io.closeChecked(retirementCurrentHandle);retirementCurrentHandle=null;
                flight.firstSlotAvailability=new FirstSlotAvailability(flight,null,1-(int)(flight.originalCurrent.record.transition().sequence()%2));
                return flight.firstSlotAvailability;
            }
            if(intent.original!=flight.originalInactive||intent.original.record.transition().sequence()+1!=flight.originalCurrent.record.transition().sequence())
                throw new IOException("Frozen inactive V5 record is not the exact current predecessor");
            retirementInactiveHandle=io.open(path(inactiveName),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE|WindowsMarkerIo.DELETE,
                WindowsMarkerIo.SHARE_DELETE,WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,
                issuer.witnessOperation(source));
            requireExactV5Handle(inactiveName,retirementInactiveHandle,intent.original);
            io.flushChecked(retirementInactiveHandle);requireExactV5Handle(inactiveName,retirementInactiveHandle,intent.original);
            absent(intent.historyName);absent(intent.identityName);absent(intent.identityTemporaryName);
            publishIdentityData(flight,intent);
            io.moveWriteThroughNoReplace(path(inactiveName),path(intent.historyName),issuer.witnessOperation(source));
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(retirementInactiveHandle),intent.historyName);
            requireExactV5Handle(intent.historyName,retirementInactiveHandle,intent.original);io.flushChecked(retirementInactiveHandle);
            requireExactV5Handle(intent.historyName,retirementInactiveHandle,intent.original);
            requireAbsentAfterRetirement(inactiveName);
            rebarrierHeldCurrent(retirementCurrentHandle,flight.originalCurrent);
            io.closeChecked(retirementInactiveHandle);retirementInactiveHandle=null;flight.firstRenameClosed=true;
            retirementHistoryHandle=openRetirementPath(intent.historyName,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(retirementHistoryHandle==null)throw new IOException("Exact retired V5 history disappeared after checked held-handle close");
            requireExactV5Handle(intent.historyName,retirementHistoryHandle,intent.original);io.flushChecked(retirementHistoryHandle);
            requireExactV5Handle(intent.historyName,retirementHistoryHandle,intent.original);
            io.closeChecked(retirementHistoryHandle);retirementHistoryHandle=null;flight.firstHistoryReopened=true;
            retirementIdentityReadHandle=openRetirementPath(intent.identityName,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(retirementIdentityReadHandle==null)throw new IOException("Exact IDDATA disappeared after retirement");
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),io.finalPath(retirementIdentityReadHandle),intent.identityName);
            if(!intent.identityFileIdentity.equals(io.identity(retirementIdentityReadHandle)))throw new IOException("Retired IDDATA FileId changed");
            io.metadata(retirementIdentityReadHandle,false,HISTORY_ID_LENGTH);
            retirementIdentityReadback=io.readWitnessArtifactExact(retirementIdentityReadHandle,HISTORY_ID_LENGTH,512);
            if(!Arrays.equals(intent.encoded,retirementIdentityReadback))throw new IOException("Retired IDDATA readback differs");
            io.flushChecked(retirementIdentityReadHandle);
            if(!Arrays.equals(intent.encoded,io.readWitnessArtifactExact(retirementIdentityReadHandle,HISTORY_ID_LENGTH,512)))
                throw new IOException("Retired IDDATA changed across final flush");
            io.closeChecked(retirementIdentityReadHandle);retirementIdentityReadHandle=null;
            requireAbsentAfterRetirement(inactiveName);
            rebarrierHeldCurrent(retirementCurrentHandle,flight.originalCurrent);
            io.closeChecked(retirementCurrentHandle);retirementCurrentHandle=null;
            retainedTransfer.v5HistoryRetirementReturned();intent.retired=true;
            var ack=new RetiredV5HistoryDurability(flight,intent,flight.originalCurrent);
            flight.firstSlotAvailability=new FirstSlotAvailability(flight,ack,(int)(intent.original.record.transition().sequence()%2));
            return flight.firstSlotAvailability;
        } catch(Throwable primary) {
            flight.failure=primary;retainedFailure=primary;
            WindowsMarkerIo.closeSuppressed(retirementIdentityReadHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementHistoryHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementInactiveHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementCurrentHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementIdentityCanonical,primary);
            WindowsMarkerIo.closeSuppressed(retirementIdentityTemporary,primary);
            throw propagate(primary);
        }
    }

    void associateInstalledScope(VerifiedHandoffRetirementFlight flight,
            WindowsB1TwoSlotStore.InstalledResumeRecordScope installed)throws IOException {
        if(Thread.currentThread()!=owner||flight==null||flight.store!=this||flight!=retirementFlight||
                flight.installedScope!=null||flight.actualHandoff==null||flight.failure!=null||installed==null)
            throw new IOException("One exact installed V5 HANDOFF continuation scope is required",flight==null?null:flight.failure);
        // 先挂原责任槽；后续关联失败保留partial，不能回退preinstall。
        flight.installedScope=installed;
        issuer.requireInstalledV5Flight(flight,installed);
    }
    /** Second retirement of the retained original current, under the actual installed HANDOFF custody. */
    public RetiredV5HistoryDurability retireOriginalCurrentV5AfterInstall(
            VerifiedHandoffRetirementFlight flight,
            com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor.CommittedCertifiedHandoff committed)
            throws IOException {
        requireInstalledHandoffFlight(flight);
        if(flight.installedScope==null||flight.actualHandoff==null||!flight.artifactCompared||committed==null||
                flight.secondRetirementAttempted||flight.secondRetirement!=null)
            throw new IOException("Exact installed HANDOFF plus actual committed SQL/post-audit permit requires one original-current retirement");
        committed.requireForNativeContinuation(flight,flight.installedScope);
        flight.secondRetirementAttempted=true;
        RetirementIntent intent=flight.currentIntent;
        WindowsMarkerIo io=issuer.witnessIo(source);
        String currentName=NAMES[(int)(flight.actualHandoff.record.transition().sequence()%2)];
        String inactiveName=NAMES[(int)(flight.originalCurrent.record.transition().sequence()%2)];
        try {
            if(intent==null||intent.original!=flight.originalCurrent)
                throw new IOException("Frozen original-current V5 identity DATA is missing");
            retirementCurrentHandle=openRetirementPath(currentName,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(retirementCurrentHandle==null)throw new IOException("Installed actual V5 HANDOFF current disappeared");
            requireExactV5Handle(currentName,retirementCurrentHandle,flight.actualHandoff);
            io.flushChecked(retirementCurrentHandle);requireExactV5Handle(currentName,retirementCurrentHandle,flight.actualHandoff);
            retirementInactiveHandle=io.open(path(inactiveName),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE|WindowsMarkerIo.DELETE,
                WindowsMarkerIo.SHARE_DELETE,WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,
                issuer.witnessOperation(source));
            requireExactV5Handle(inactiveName,retirementInactiveHandle,intent.original);
            io.flushChecked(retirementInactiveHandle);requireExactV5Handle(inactiveName,retirementInactiveHandle,intent.original);
            absent(intent.historyName);absent(intent.identityName);absent(intent.identityTemporaryName);
            publishIdentityData(flight,intent);
            io.moveWriteThroughNoReplace(path(inactiveName),path(intent.historyName),issuer.witnessOperation(source));
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),
                io.finalPath(retirementInactiveHandle),intent.historyName);
            requireExactV5Handle(intent.historyName,retirementInactiveHandle,intent.original);
            io.flushChecked(retirementInactiveHandle);requireExactV5Handle(intent.historyName,retirementInactiveHandle,intent.original);
            requireAbsentAfterRetirement(inactiveName);
            rebarrierHeldCurrent(retirementCurrentHandle,flight.actualHandoff);
            io.closeChecked(retirementInactiveHandle);retirementInactiveHandle=null;flight.secondRenameClosed=true;
            retirementHistoryHandle=openRetirementPath(intent.historyName,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(retirementHistoryHandle==null)throw new IOException("Exact original-current history missing after checked close");
            requireExactV5Handle(intent.historyName,retirementHistoryHandle,intent.original);
            io.flushChecked(retirementHistoryHandle);requireExactV5Handle(intent.historyName,retirementHistoryHandle,intent.original);
            io.closeChecked(retirementHistoryHandle);retirementHistoryHandle=null;flight.secondHistoryReopened=true;
            retirementIdentityReadHandle=openRetirementPath(intent.identityName,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(retirementIdentityReadHandle==null)throw new IOException("Original-current IDDATA missing after retirement");
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.witnessNativePath(source),
                io.finalPath(retirementIdentityReadHandle),intent.identityName);
            if(!intent.identityFileIdentity.equals(io.identity(retirementIdentityReadHandle)))
                throw new IOException("Original-current IDDATA native FileId changed");
            io.metadata(retirementIdentityReadHandle,false,HISTORY_ID_LENGTH);
            byte[] id=io.readWitnessArtifactExact(retirementIdentityReadHandle,HISTORY_ID_LENGTH,512);
            if(!Arrays.equals(intent.encoded,id))throw new IOException("Original-current IDDATA complete bytes differ");
            io.flushChecked(retirementIdentityReadHandle);
            if(!Arrays.equals(intent.encoded,io.readWitnessArtifactExact(retirementIdentityReadHandle,HISTORY_ID_LENGTH,512)))
                throw new IOException("Original-current IDDATA changed across flush");
            io.closeChecked(retirementIdentityReadHandle);retirementIdentityReadHandle=null;
            requireAbsentAfterRetirement(inactiveName);
            rebarrierHeldCurrent(retirementCurrentHandle,flight.actualHandoff);
            io.closeChecked(retirementCurrentHandle);retirementCurrentHandle=null;
            retainedTransfer.v5HistoryRetirementReturned();intent.retired=true;
            var ack=new RetiredV5HistoryDurability(flight,intent,flight.actualHandoff);
            flight.secondRetirement=ack;
            return ack;
        } catch(Throwable primary) {
            flight.failure=primary;retainedFailure=primary;
            WindowsMarkerIo.closeSuppressed(retirementIdentityReadHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementHistoryHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementInactiveHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementCurrentHandle,primary);
            WindowsMarkerIo.closeSuppressed(retirementIdentityCanonical,primary);
            WindowsMarkerIo.closeSuppressed(retirementIdentityTemporary,primary);
            throw propagate(primary);
        }
    }

    /** Publish DB_ACQUIRED only after the same actor committed/read back W, completed post-audit,
     *  and retired the exact old-current slot under installed custody. */
    public NativeV5RecordDurability publishSettledDbAcquired(
            VerifiedHandoffRetirementFlight flight,
            com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor.CommittedCertifiedHandoff committed,
            B1ResumeRecord settled)throws IOException {
        requireInstalledHandoffFlight(flight);
        if(committed==null||settled==null||flight.secondRetirement==null||flight.actualHandoff==null||
                flight.actualDbAcquired!=null||flight.failure!=null)
            throw new IOException("Exact committed ordinary continuation, second retirement, and one DB_ACQUIRED ACK are required");
        committed.requireForNativeContinuation(flight,flight.installedScope);
        committed.requireSettledRecord(settled);
        B1ResumeRecord handoff=flight.actualHandoff.record.transition();
        if(settled.kind()!=Kind.DB_ACQUIRED||settled.sequence()!=Math.addExact(handoff.sequence(),1L)||
                !Arrays.equals(settled.previousRecordSha256(),WindowsWorldProvisioner.digest(
                        B1ResumeRecordCanonical.encode(flight.actualHandoff.record.transition())))||
                !Arrays.equals(settled.sourceAnchorSha256(),handoff.sourceAnchorSha256())||
                !Arrays.equals(settled.originalReceipt(),handoff.originalReceipt())||
                !settled.barrierNamespace().equals(handoff.barrierNamespace())||
                settled.assertions().pendingAttempt()!=null||settled.assertions().latestSettlement()==null||
                settled.assertions().latestSettlement().kind()!=B1ResumeRecord.SettlementKind.CONFIRMED)
            throw new IOException("DB_ACQUIRED must be the exact settled successor of the actual HANDOFF record");
        try {
            var state=settled.assertions();
            var prepare=existingReference(flight.originalCurrent.artifacts,state.originalPrepareWitness(),retainedTransfer.profileSha256());
            var certify=existingReference(flight.originalCurrent.artifacts,state.acknowledgedCertifyWitness(),retainedTransfer.profileSha256());
            NativeV5RecordDurability ack=publishNext(flight.actualHandoff,Kind.DB_ACQUIRED,state,
                prepare,null,flight.handoffArtifact,certify);
            if(ack==null||
                    !Arrays.equals(B1ResumeRecordCanonical.encode(ack.record().transition()),B1ResumeRecordCanonical.encode(settled)))
                throw new IOException("Published V5 DB_ACQUIRED readback differs from the exact actor-settled transition");
            flight.actualDbAcquired=ack;
            return ack;
        } catch(Throwable primary) {
            flight.failure=primary;retainedFailure=primary;throw propagate(primary);
        }
    }
    private static IOException propagate(Throwable cause) {
        if(cause instanceof IOException io)return io;if(cause instanceof RuntimeException r)throw r;if(cause instanceof Error e)throw e;
        return new IOException("V5 native record boundary failed",cause);
    }
    // 没有inactive删除或reclaim：当前仍缺exact全部settlement/last-reference release issuer。
    /** 原Holder在SQL/业务退栈后调用；保current/flight身份供最终installed完整活核。 */
    public void endOrdinaryDataConsumers(MySqlCertifiedRecoveryInspectionActor actor,
            MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state,
            WindowsB1TwoSlotStore.InstalledResumeRecordScope installed,MySqlFull20NativeTransfer transfer)throws IOException {
        requireOwnerIdentity();transfer.requireOriginalTerminalIdentity(actor,state);
        if(transfer!=retainedTransfer||consumerPhase!=0||installed==null||temporary!=null||canonical!=null||
                retirementIdentityTemporary!=null||retirementIdentityCanonical!=null||retirementCurrentHandle!=null||
                retirementInactiveHandle!=null||retirementHistoryHandle!=null||retirementIdentityReadHandle!=null)
            throw new IOException("Exact completed V5 native windows required before DATA retirement");
        if(retirementFlight!=null){
            requireInstalledAssociation(issuer,installed,retirementFlight);
            if(!settledStage(retirementFlight))throw new IOException("Actual settled V5 flight required");
        }else issuer.requireOrdinaryBridgeTerminalStore(source,this,installed,transfer);
        endArtifactConsumers(current);endArtifactConsumers(previous);
        if(retirementFlight!=null){
            endArtifactConsumers(retirementFlight.originalCurrent);endArtifactConsumers(retirementFlight.originalInactive);
            endArtifactConsumers(retirementFlight.handoff);endArtifactConsumers(retirementFlight.actualHandoff);
            endArtifactConsumers(retirementFlight.actualDbAcquired);
            retirementFlight.handoffArtifact=null;retirementFlight.full20ArtifactStore=null;
        }
        consumerPhase=1;
    }
    private static void endArtifactConsumers(NativeV5RecordDurability value){
        if(value!=null)value.artifacts=List.of();
    }
    void detachOrdinaryTerminalIdentity(MySqlCertifiedRecoveryInspectionActor actor,
            MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state)throws IOException {
        requireOwnerIdentity();retainedTransfer.requireOriginalTerminalIdentity(actor,state);
        if(consumerPhase!=1)throw new IOException("V5 DATA consumers must end before identity retirement");
        current=null;previous=null;pendingBytes=null;coldData=null;retirementFlight=null;
        source=null;preinstall=null;issuer=null;retainedTransfer=null;consumerPhase=2;
    }

}

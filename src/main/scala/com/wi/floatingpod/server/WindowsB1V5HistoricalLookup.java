package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting;
import java.io.IOException;
import com.wi.floatingpod.storage.mysql.MySqlCertifiedRecoveryInspectionActor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/** Original cold-source history reader. Complete DATA provenance is not SQL commit authority. */
public final class WindowsB1V5HistoricalLookup {
    private static final String[] SLOTS={".floatingpod-b1-resume-v5.0",".floatingpod-b1-resume-v5.1"};
    private WindowsB1TwoSlotStore issuer;
    private WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 current;
    private WindowsB1TwoSlotStore.ColdResumeSourceScope source;
    private MySqlFull20ColdReadAccounting accounting;
    private final Thread owner=Thread.currentThread();
    private final ArrayList<Read> reads=new ArrayList<>();
    private final ArrayList<B1ResumeRecordV5> records=new ArrayList<>();
    private final HashMap<UUID,Artifact> artifacts=new HashMap<>();
    private boolean attempted,complete,consumed;
    private int consumerPhase;
    private Throwable failure;
    private byte[] v4Bytes;
    private static final class Read {
        final String name;final MySqlFull20ColdReadAccounting budget;
        WindowsMarkerIo.Handle handle;WindowsMarkerIo.FileIdentity identity;
        byte[] first,second;boolean absent;
        Read(String name,MySqlFull20ColdReadAccounting budget){this.name=name;this.budget=budget;}
    }
    private record Artifact(B1Full20WitnessReference reference,byte[] attempt,Read read){}
    WindowsB1V5HistoricalLookup(WindowsB1TwoSlotStore issuer,WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 current,
            WindowsB1TwoSlotStore.ColdResumeSourceScope source,MySqlFull20ColdReadAccounting accounting){
        this.issuer=issuer;this.current=current;this.source=source;this.accounting=accounting;
    }
    public void requireAccounting(MySqlFull20ColdReadAccounting exact)throws IOException {
        if(consumerPhase!=0||accounting==null)throw new IOException("Historical DATA consumers ended");
        if(Thread.currentThread()!=owner||exact!=accounting)throw new IOException("Exact original history thread/accounting required");
        issuer.requireV5History(this,current,accounting);
        if(failure!=null)throw new IOException("Original V5 history failure retained",failure);
    }
    void read()throws IOException {
        requireAccounting(accounting);
        if(attempted)throw new IOException("Original history lookup is one-shot");attempted=true;
        try {
            accounting.beginV5HistoryPass(this,issuer.historyCapturedWorldLength(this,current,accounting));
            issuer.revalidateV5HistorySource(this,current,accounting);
            var v4=current.v4Bridge();v4Bytes=v4.canonicalBytes();
            var bridgeRead=readFile(".floatingpod-b1-resume-v4."+v4.slot(),v4Bytes.length,null,
                WindowsWorldProvisioner.digest(v4Bytes),65536,false);
            if(!bridgeRead.identity.versionedIdentity().equals(v4.fileIdentity())||!Arrays.equals(bridgeRead.first,v4Bytes))
                throw new IOException("Original V4 bridge identity/complete bytes changed before history read");
            B1ResumeRecordV5 latest=current.currentRecord();
            byte[] latestBytes=current.currentCanonicalBytes();
            var latestRead=readFile(SLOTS[(int)(latest.transition().sequence()%2)],latestBytes.length,
                current.currentIdentity(),WindowsWorldProvisioner.digest(latestBytes),B1ResumeRecordV5.MAX_RECORD_BYTES,false);
            if(!Arrays.equals(latestBytes,latestRead.first))throw new IOException("History current changed from original preinstall source");
            requireLiveNotRetired(latest.transition().sequence(),WindowsWorldProvisioner.digest(latestBytes));
            B1ResumeRecordV5 cursor=decode(latestRead);
            while(true) {
                requireSourceData(cursor,v4.record(),v4Bytes,v4.fileIdentity());
                records.add(cursor);readArtifacts(cursor);
                if(cursor.transition().sequence()==Math.addExact(v4.record().sequence(),1L)) {
                    requireFirstData(cursor,v4.record(),v4Bytes,v4.fileIdentity());break;
                }
                long previous=Math.subtractExact(cursor.transition().sequence(),1L);
                byte[] hash=cursor.previousContainerSha256();
                Read prior;
                if(current.hasInactiveRecord()&&current.inactiveRecord().transition().sequence()==previous) {
                    byte[] bytes=current.inactiveCanonicalBytes();
                    prior=readFile(SLOTS[(int)(previous%2)],bytes.length,current.inactiveIdentity(),hash,
                        B1ResumeRecordV5.MAX_RECORD_BYTES,false);
                    if(!Arrays.equals(bytes,prior.first))throw new IOException("History inactive changed from original source");
                    // A live slot and a retired file for this exact identity cannot both own the predecessor.
                    requireLiveNotRetired(previous,hash);
                } else {
                    var id=readFile(WindowsB1V5WitnessRecordStore.historyIdentityBasename(previous,hash),
                        WindowsB1V5WitnessRecordStore.retiredIdentityLength(),null,null,512,false);
                    var identity=WindowsB1V5WitnessRecordStore.decodeRetiredIdentity(id.first);
                    readFile(WindowsB1V5WitnessRecordStore.historyIdentityBasename(previous,hash)+".tmp",0,null,null,512,true);
                    if(identity.sequence()!=previous||!Arrays.equals(identity.containerSha256(),hash))
                        throw new IOException("Historical IDDATA differs from exact predecessor");
                    prior=readFile(WindowsB1V5WitnessRecordStore.historyBasename(previous,hash),identity.containerLength(),
                        identity.originalIdentity(),hash,B1ResumeRecordV5.MAX_RECORD_BYTES,false);
                    var older=decode(prior);
                    WindowsB1V5WitnessRecordStore.requireIdentityMatches(identity,older,prior.first,prior.identity);
                    requirePreviousData(older,prior.first,cursor);
                    cursor=older;continue;
                }
                var older=decode(prior);requirePreviousData(older,prior.first,cursor);cursor=older;
            }
            issuer.revalidateV5HistorySource(this,current,accounting);
            accounting.finishV5HistoryPass(this);complete=true;
        } catch(Throwable cause){fail(cause);}
    }

    /** A fresh complete reread of every retained file/absence precedes one consumption. */
    public void consume(MySqlFull20ColdReadAccounting exact)throws IOException {
        requireAccounting(exact);
        if(!complete||consumed)throw new IOException("One complete original historical chain required");
        try {
            accounting.beginV5HistoryPass(this,issuer.historyCapturedWorldLength(this,current,accounting));
            issuer.revalidateV5HistorySource(this,current,accounting);
            // Bound is registered before construction. Re-read records only; do not recursively decode copies.
            var original=List.copyOf(reads);
            for(var old:original) {
                var actual=readFile(old.name,old.absent?0:old.first.length,old.identity,
                    old.absent?null:WindowsWorldProvisioner.digest(old.first),
                    old.absent?B1ResumeRecordV5.MAX_RECORD_BYTES:Math.max(old.first.length,512),old.absent);
                if(!old.absent&&!Arrays.equals(old.first,actual.first))throw new IOException("Historical complete bytes changed before consumption");
            }
            issuer.revalidateV5HistorySource(this,current,accounting);
            accounting.finishV5HistoryPass(this);consumed=true;
        } catch(Throwable cause){fail(cause);}
    }

    /** Immutable DATA view after actual complete consumption; it cannot reconstruct the private source holder. */
    public List<B1ResumeRecordV5> consumedRecords(MySqlFull20ColdReadAccounting exact)throws IOException {
        requireAccounting(exact);
        if(!complete||!consumed)throw new IOException("Complete consumed original history required");
        accounting.reserveV5HistoryResult(this,records.size());
        return List.copyOf(records);
    }

    private B1ResumeRecordV5 decode(Read read)throws IOException {
        read.budget.beforeV5Decode(read.first.length);
        var result=B1ResumeRecordV5.decodeData(read.first);
        if(!Arrays.equals(read.first,B1ResumeRecordV5.encodeData(result)))throw new IOException("Noncanonical historical V5 container");
        return result;
    }
    private void requireLiveNotRetired(long sequence,byte[] hash)throws IOException {
        readFile(WindowsB1V5WitnessRecordStore.historyBasename(sequence,hash),0,null,null,B1ResumeRecordV5.MAX_RECORD_BYTES,true);
        String identity=WindowsB1V5WitnessRecordStore.historyIdentityBasename(sequence,hash);
        readFile(identity,0,null,null,512,true);readFile(identity+".tmp",0,null,null,512,true);
    }
    private void readArtifacts(B1ResumeRecordV5 record)throws IOException {
        var state=record.transition().assertions();
        var attempts=new B1ResumeRecord.AttemptAssertions[]{state.originalPrepareWitness(),state.pendingAttempt(),
            state.latestSettlement()==null?null:state.latestSettlement().settledAttempt(),state.acknowledgedCertifyWitness()};
        var refs=record.references();
        var references=new B1Full20WitnessReference[]{refs.originalPrepare(),refs.pending(),refs.settled(),refs.acknowledgedCertify()};
        for(int i=0;i<references.length;i++) {
            var ref=references[i];var attempt=attempts[i];
            if((ref==null)!=(attempt==null))throw new IOException("Historical reference/attempt topology differs");
            if(ref==null)continue;
            if(!accounting.profileSha256().equals(HexFormat.of().formatHex(ref.profileSha256())))
                throw new IOException("Historical reference profile differs from original root");
            var previous=artifacts.get(ref.attemptId());
            if(previous==null)accounting.reserveV5HistoryAttempt(this);
            byte[] expected=B1ResumeRecordCanonical.encodeAttemptData(attempt);
            if(previous!=null) {
                if(!previous.reference.exactlyEquals(ref)||!Arrays.equals(previous.attempt,expected))
                    throw new IOException("Historical attempt has conflicting complete reference/bytes");
                continue;
            }
            var read=readFile(ref.basename(),ref.fullLength(),ref.fileIdentity(),ref.fullSha256(),accounting.artifactLimit(),false);
            read.budget.decodeWitness(read.first,expected,v4Bytes);
            artifacts.put(ref.attemptId(),new Artifact(ref,expected,read));
        }
    }
    private Read readFile(String name,long length,WindowsMarkerIo.FileIdentity identity,byte[] hash,long limit,
            boolean requireAbsent)throws IOException {
        var child=accounting.newV5HistoryReadAccounting(this);
        var read=new Read(name,child);reads.add(read);
        var io=issuer.coldIo(source);
        try {
            try {read.handle=io.open(issuer.coldDirectory(source).resolve(name),WindowsMarkerIo.READ,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,issuer.coldOperation(source));}
            catch(WindowsMarkerIo.NativeFailure missing){if(missing.win32Error()!=2)throw missing;}
            if(read.handle==null) {
                if(!requireAbsent)throw new IOException("Exact historical file missing: "+name);
                read.absent=true;return read;
            }
            if(requireAbsent)throw new IOException("Duplicate live/retired historical predecessor: "+name);
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.coldNativePath(source),io.finalPath(read.handle),name);
            if(length<=0||length>limit||length>Integer.MAX_VALUE)throw new IOException("Historical full file bound");
            read.identity=io.identity(read.handle);
            if(identity!=null&&!identity.equals(read.identity))throw new IOException("Historical native identity differs");
            io.metadata(read.handle,false,length);child.beforeRead(length);
            read.first=io.readWitnessArtifactExact(read.handle,Math.toIntExact(length),limit);child.retainReadBytes(read.first);
            read.second=io.readWitnessArtifactExact(read.handle,Math.toIntExact(length),limit);
            if(!Arrays.equals(read.first,read.second)||!read.identity.equals(io.identity(read.handle))||
                hash!=null&&!Arrays.equals(hash,WindowsWorldProvisioner.digest(read.first)))throw new IOException("Historical bytes/hash/native identity changed");
            io.metadata(read.handle,false,length);
            WindowsB1SourceAnchorStore.requireExactNativeChild(issuer.coldNativePath(source),io.finalPath(read.handle),name);
            issuer.requireColdLive(source);io.closeChecked(read.handle);read.handle=null;
            Arrays.fill(read.second,(byte)0);read.second=null;child.nativeReadClosed();return read;
        } catch(Throwable cause){WindowsMarkerIo.closeSuppressed(read.handle,cause);throw propagate(cause);}
    }
    static void requireSourceData(B1ResumeRecordV5 record,B1ResumeRecord v4,byte[] bytes,String identity)throws IOException {
        if(record.bridgeSequence()!=v4.sequence()||!record.bridgeIdentity().versionedIdentity().equals(identity)||
            !Arrays.equals(record.bridgeSha256(),WindowsWorldProvisioner.digest(bytes))||
            !Arrays.equals(record.transition().sourceAnchorSha256(),v4.sourceAnchorSha256())||
            !Arrays.equals(record.transition().originalReceipt(),v4.originalReceipt())||
            !record.transition().barrierNamespace().equals(v4.barrierNamespace()))throw new IOException("Historical original V4 bridge/source differs");
    }
    static void requireFirstData(B1ResumeRecordV5 first,B1ResumeRecord v4,byte[] bytes,String identity)throws IOException {
        requireSourceData(first,v4,bytes,identity);
        var handoff=v4.assertions().pendingAttempt();
        if(v4.kind()!=B1ResumeRecord.Kind.HANDOFF_PREPARED||handoff==null||handoff.phase()!=B1ResumeRecord.Phase.HANDOFF||
            v4.assertions().previousOwner()==null||v4.assertions().currentOwner()==null||
            handoff.bootGeneration()!=v4.assertions().currentOwner().generation()||
            !handoff.context().boot().equals(v4.assertions().currentOwner().boot())||
            !first.transition().assertions().currentOwner().equals(v4.assertions().currentOwner())||
            !Arrays.equals(bytes,B1ResumeRecordCanonical.encode(v4))||
            first.transition().sequence()!=Math.addExact(v4.sequence(),1L)||
            !Arrays.equals(first.previousContainerSha256(),WindowsWorldProvisioner.digest(bytes)))
            throw new IOException("Historical first V5 predecessor differs from original V4");
        B1ResumeRecordCodec.consecutive(v4,bytes,first.transition());
    }
    static void requirePreviousData(B1ResumeRecordV5 older,byte[] bytes,B1ResumeRecordV5 newer)throws IOException {
        if(!Arrays.equals(bytes,B1ResumeRecordV5.encodeData(older)))throw new IOException("Historical predecessor canonical bytes differ");
        B1ResumeRecordV5.consecutive(older,bytes,newer);
    }
    private void fail(Throwable cause)throws IOException {
        failure=cause;
        try{accounting.failV5History(this,cause);}catch(Throwable cleanup){if(cleanup!=cause)cause.addSuppressed(cleanup);}
        throw propagate(cause);
    }
    private static IOException propagate(Throwable cause)throws IOException {
        if(cause instanceof IOException io)return io;
        if(cause instanceof RuntimeException runtime)throw runtime;
        if(cause instanceof Error error)throw error;
        return new IOException("V5 original history read failed",cause);
    }
    public void endOrdinaryConsumers(MySqlCertifiedRecoveryInspectionActor actor,
            MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement state)throws IOException {
        if(Thread.currentThread()!=owner||consumerPhase!=0||!complete||!consumed||accounting==null)
            throw new IOException("Exact completed/consumed historical owner required once");
        if(failure!=null)throw new IOException("Historical failure remains retained",failure);
        accounting.requireOriginalTerminalIdentity(actor,state);
        for(int i=0;i<reads.size();i++){
            Read read=reads.get(i);
            if(read.handle!=null)throw new IOException("Historical native read handle remains retained");
            read.budget.requireOriginalTerminalIdentity(actor,state);
        }
        reads.clear();records.clear();artifacts.clear();v4Bytes=null;
        current=null;source=null;accounting=null;issuer=null;consumerPhase=1;
    }

}

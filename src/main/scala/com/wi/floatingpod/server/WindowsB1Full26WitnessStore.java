package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlFormat3ReadBudget;
import com.wi.floatingpod.storage.mysql.MySqlDeploymentReceiptSession;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.function.Consumer;

/** 原schema5 source的完整artifact持久化；ACK仅证明实际bytes，不签SQL/Ready。 */
public final class WindowsB1Full26WitnessStore {
    private final WindowsWorldProvisioner.DeploymentReceiptIoScope source;
    private final MySqlFormat3ReadBudget root;
    private final Thread thread=Thread.currentThread();
    private Pending pending;
    private final java.util.ArrayList<Pending> publications=new java.util.ArrayList<>();
    private Throwable failure;
    private long retainedFee,temporaryFee;
    private boolean retirementAttempted,retired;
    private static final class Pending {
        final String temporary,canonical;
        byte[] bytes,readback;
        WindowsMarkerIo.Handle temporaryHandle,canonicalHandle;
        WindowsMarkerIo.FileIdentity identity;
        DurableFull26Ack ack;
        Pending(String canonical){this.canonical=canonical;temporary=canonical+".tmp";}
    }
    public static final class DurableFull26Ack {
        private final WindowsB1Full26WitnessStore issuer;
        private final Pending publication;
        private final B1Full26WitnessReference reference;
        private DurableFull26Ack(WindowsB1Full26WitnessStore issuer,Pending publication,B1Full26WitnessReference reference){
            this.issuer=issuer;this.publication=publication;this.reference=reference;
        }
        public B1Full26WitnessReference reference() throws IOException {issuer.requireLive();return reference;}
        public void rebarrier() throws IOException {issuer.rebarrier(this);}
        void requireSource(WindowsWorldProvisioner.DeploymentReceiptIoScope exact) throws IOException {
            issuer.requireLive();if(issuer.source!=exact||publication.ack!=this)throw new IOException("exact Full26 artifact source/publication required");
        }
    }
    public WindowsB1Full26WitnessStore(WindowsWorldProvisioner.DeploymentReceiptIoScope source,MySqlFormat3ReadBudget root) throws IOException {
        this.source=Objects.requireNonNull(source);this.root=Objects.requireNonNull(root);source.validateLive();
        source.bindAllocationRoot(root);
        root.reserveRetainedMemory(256L,"schema5 native Full26 store holder");
        retainedFee=256L;
    }
    private void requireLive() throws IOException {
        if(Thread.currentThread()!=thread||root.isClosed())throw new IOException("original Full26 native store thread/root required");
        if(retired || retirementAttempted || failure!=null)throw new IOException("Full26 artifact publication/retirement unavailable",failure);
        source.validateLive();
    }
    static void requireBodyBound(long length,long maximum) throws IOException {
        if(length<=0||length>Integer.MAX_VALUE||length>maximum)throw new IOException("complete Full26 artifact exceeds explicit native limit");
    }
    public DurableFull26Ack publish(B1ResumeRecord record,byte[] completeBody,byte[] configurationSha256) throws IOException {
        requireLive();
        if(pending!=null && pending.ack==null)throw new IOException("Full26 artifact publication uncertain; original pending retained");
        Objects.requireNonNull(completeBody);Objects.requireNonNull(record);
        B1ResumeRecordCodec.validateSchema5Data(record);
        requireBodyBound(completeBody.length,root.profile().maxNativeArtifactBytes());
        B1ResumeRecordCodec.exact(source.originalReceipt(),record.originalReceipt(),"artifact original receipt differs from native source");
        B1ResumeRecordCodec.exact(HexFormat.of().parseHex(source.anchorSha256()),record.sourceAnchorSha256(),"artifact source anchor differs");
        var attempt=record.assertions().pendingAttempt();
        if(attempt==null)throw new IOException("artifact requires exact pending original attempt");
        var hash=WindowsWorldProvisioner.digest(completeBody);
        String name=B1Full26WitnessReference.basename(attempt.attemptId(),hash);
        long fee=Math.addExact(512L,Math.multiplyExact(3L,completeBody.length));
        root.reserveRetainedMemory(fee,"Full26 native bytes/readback/reference BEFORE allocation");
        retainedFee=Math.addExact(retainedFee,fee);
        pending=new Pending(name);publications.add(pending);pending.bytes=completeBody.clone();
        try {
            var io=source.io();Path directory=source.directory();
            pending.temporaryHandle=io.createMarkerHandle(directory.resolve(pending.temporary),attempt.logicalWriteUuid());
            requireChild(io,pending.temporaryHandle,pending.temporary);
            io.metadata(pending.temporaryHandle,false,0);
            io.writeWitnessArtifact(pending.temporaryHandle,pending.bytes,root.profile().maxNativeArtifactBytes());
            pending.identity=io.identity(pending.temporaryHandle);io.flushChecked(pending.temporaryHandle);
            io.moveWriteThroughNoReplace(directory.resolve(pending.temporary),directory.resolve(pending.canonical),attempt.logicalWriteUuid());
            io.closeChecked(pending.temporaryHandle);pending.temporaryHandle=null;
            pending.canonicalHandle=io.open(directory.resolve(pending.canonical),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,attempt.logicalWriteUuid());
            requireChild(io,pending.canonicalHandle,pending.canonical);
            io.compareWitnessArtifactExact(pending.canonicalHandle,pending.bytes,pending.identity,root.profile().maxNativeArtifactBytes());
            io.flushChecked(pending.canonicalHandle);
            pending.readback=io.readWitnessArtifactExact(pending.canonicalHandle,pending.bytes.length,root.profile().maxNativeArtifactBytes());
            B1ResumeRecordCodec.exact(pending.bytes,pending.readback,"Full26 complete native readback differs");
            if(!pending.identity.equals(io.identity(pending.canonicalHandle)))throw new IOException("Full26 native FileId changed");
            source.validateLive();io.closeChecked(pending.canonicalHandle);pending.canonicalHandle=null;
            var plan=WindowsWorldProvisionReceiptDataCodec.decode(record.originalReceipt()).plan();
            var reference=new B1Full26WitnessReference(name,pending.identity,pending.bytes.length,hash,record.sourceAnchorSha256(),
                WindowsWorldProvisioner.digest(record.originalReceipt()),root.profile().sha256(),configurationSha256,HexFormat.of().parseHex(plan.ddlSha256()),
                attempt.attemptId(),attempt.phase(),attempt.logicalWriteUuid());
            pending.ack=new DurableFull26Ack(this,pending,reference);return pending.ack;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    private void requireChild(WindowsMarkerIo io,WindowsMarkerIo.Handle handle,String name) throws IOException {
        String actual=io.finalPath(handle);
        WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(source.nativePath(),actual);
        if(!actual.endsWith("\\"+name))throw new IOException("Full26 fixed native child name changed");
    }
    private void rebarrier(DurableFull26Ack ack) throws IOException {
        requireLive();ack.requireSource(source);
        withReadBody(ack.reference,bytes-> {
            if(!Arrays.equals(bytes,ack.publication.bytes))throw new IllegalStateException("Full26 original complete bytes changed");
        });
    }
    /** 完整native读取在原root预付，callback结束才退临时bytes，不返回裸持久副本。 */
    public void withReadBody(B1Full26WitnessReference reference,Consumer<byte[]> consumer) throws IOException {
        requireLive();Objects.requireNonNull(reference);Objects.requireNonNull(consumer);
        requireBodyBound(reference.fullLength(),root.profile().maxNativeArtifactBytes());
        long fee=Math.addExact(256L,Math.multiplyExact(reference.fullLength(),2L));
        root.reserveTemporaryMemory(fee,"Full26 native cold read BEFORE allocation");
        temporaryFee=Math.addExact(temporaryFee,fee);
        WindowsMarkerIo.Handle handle=null;Throwable primary=null;
        try {
            var io=source.io();handle=io.open(source.directory().resolve(reference.basename()),WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0,
                WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,reference.logicalOperation());
            requireChild(io,handle,reference.basename());
            if(!reference.fileIdentity().equals(io.identity(handle)))throw new IOException("Full26 referenced FileId differs");
            byte[] bytes=io.readWitnessArtifactExact(handle,Math.toIntExact(reference.fullLength()),root.profile().maxNativeArtifactBytes());
            B1ResumeRecordCodec.exact(reference.fullSha256(),WindowsWorldProvisioner.digest(bytes),"Full26 referenced complete hash differs");
            io.flushChecked(handle);io.compareWitnessArtifactExact(handle,bytes,reference.fileIdentity(),root.profile().maxNativeArtifactBytes());
            io.closeChecked(handle);handle=null;source.validateLive();consumer.accept(bytes);
        }catch(Throwable cause){primary=cause;failure=cause;throw propagate(cause);}
        finally {
            // 失败句柄不能丢成局部变量；原store持有实际未结槽。
            if(handle!=null){if(pending==null)pending=new Pending(reference.basename());pending.canonicalHandle=handle;}
            if(primary==null){
                try{root.releaseTemporaryMemory(fee,"Full26 native cold read complete");temporaryFee=Math.subtractExact(temporaryFee,fee);}
                catch(Throwable cleanup){failure=cleanup;throw propagate(cleanup);}
            }
        }
    }
    public void retireAfterSqlSettlement(MySqlDeploymentReceiptSession.NativeRetirementPermit5 permit)throws IOException {
        Objects.requireNonNull(permit);var io=source.retirementIo(permit);
        if(Thread.currentThread()!=thread || retirementAttempted || retired || failure!=null)
            throw new IOException("original Full26 store retirement is one-shot and requires settled publication",failure);
        retirementAttempted=true;
        try {
            Throwable primary=null;
            for(var publication:publications){
                try{closePending(io,publication);}
                catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
            }
            if(pending!=null && !publications.contains(pending)){
                try{closePending(io,pending);}
                catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
            }
            if(primary!=null)throw propagate(primary);
            publications.clear();pending=null;
            if(temporaryFee!=0L)throw new IOException("Full26 native store retains unsettled temporary read fee");
            root.releaseRetainedMemory(retainedFee,"Full26 native store actual references retired");retainedFee=0L;retired=true;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    private static void closePending(WindowsMarkerIo io,Pending publication)throws IOException {
        Throwable primary=null;
        if(publication.temporaryHandle!=null){
            try{io.closeChecked(publication.temporaryHandle);publication.temporaryHandle=null;}
            catch(Throwable cause){primary=cause;}
        }
        if(publication.canonicalHandle!=null){
            try{io.closeChecked(publication.canonicalHandle);publication.canonicalHandle=null;}
            catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
        }
        if(primary!=null)throw propagate(primary);
        publication.bytes=null;publication.readback=null;publication.ack=null;publication.identity=null;
    }
    public Throwable retainedFailure(){return failure;}
    private static IOException propagate(Throwable cause) throws IOException {
        if(cause instanceof IOException io)return io;
        if(cause instanceof RuntimeException runtime)throw runtime;
        if(cause instanceof Error error)throw error;
        return new IOException("schema5 Full26 native operation failed",cause);
    }
}

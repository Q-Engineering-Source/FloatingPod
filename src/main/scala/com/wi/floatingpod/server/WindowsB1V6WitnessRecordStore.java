package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlFormat3ReadBudget;
import com.wi.floatingpod.storage.mysql.MySqlDeploymentReceiptSession;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** schema5双链/双槽原生记录；保留全部历史及references，不承接旧V5/V4。 */
public final class WindowsB1V6WitnessRecordStore {
    private final WindowsWorldProvisioner.DeploymentReceiptIoScope source;
    private final WindowsB1Full26WitnessStore artifacts;
    private final MySqlFormat3ReadBudget root;
    private final Thread thread=Thread.currentThread();
    private final ArrayList<DurableV6Ack> history=new ArrayList<>();
    private final ArrayList<DurableV6Ack> inspected=new ArrayList<>();
    private final ArrayList<CurrentInventory> inventories=new ArrayList<>();
    private DurableV6Ack current;
    private Pending pending;
    private Throwable failure;
    private boolean discoveryAttempted;
    private long retainedFee;
    private boolean retirementAttempted,retired;
    private WindowsMarkerIo.DeploymentReceiptFindSequence pendingFind;
    private Map<String,Long> discoveredFiles;
    private ConfirmationReservation confirmation;
    private boolean consumingConfirmation;
    /** 只由原store签发，绑定实际plan、当前ACK和完整已编码confirmation。 */
    public static final class ConfirmationReservation {
        private final WindowsB1V6WitnessRecordStore store;
        private final Object plan;
        private final DurableV6Ack predecessor;
        private B1ResumeRecordV6 record;
        private byte[] encoded;
        private long protectedPeak;
        private ConfirmationReservation(WindowsB1V6WitnessRecordStore store,Object plan,DurableV6Ack predecessor){
            this.store=store;this.plan=plan;this.predecessor=predecessor;
        }
        public void requirePrepared(Object actualPlan,DurableV6Ack actualPredecessor)throws IOException {
            store.requireMemoryOwner();
            if(store.confirmation!=this||plan!=actualPlan||predecessor!=actualPredecessor||store.current!=predecessor||
                    record==null||encoded==null||protectedPeak==0L||store.consumingConfirmation)
                throw new IOException("exact original plan/current V6 confirmation reservation required");
        }
    }
    public ConfirmationReservation reserveConfirmation(Object plan,DurableV6Ack predecessor,
            java.util.function.Supplier<B1ResumeRecordV6> construct)throws IOException {
        requireLive();Objects.requireNonNull(plan);Objects.requireNonNull(construct);
        if(confirmation!=null||pending!=null)throw new IOException("original confirmation reservation is single-use");
        rebarrierCurrent(predecessor);
        long workspace=Math.addExact(512L,Math.multiplyExact(4L,(long)B1ResumeRecordV6.MAX_RECORD_BYTES));
        root.reserveRetainedMemory(workspace,"V6 complete confirmation bounded encoding BEFORE allocation");
        retainedFee=Math.addExact(retainedFee,workspace);
        confirmation=new ConfirmationReservation(this,plan,predecessor);
        try {
            var record=Objects.requireNonNull(construct.get());
            confirmation.record=record;validateSource(record);
            B1ResumeRecordV6.consecutiveData(predecessor.record,predecessor.bytes,record);
            validateCompleteHistory(record);
            confirmation.encoded=B1ResumeRecordV6.encodeData(record);
            long actual=Math.addExact(512L,Math.multiplyExact(4L,(long)confirmation.encoded.length));
            root.releaseRetainedMemory(workspace-actual,"V6 confirmation unused bounded encoding capacity");
            retainedFee=Math.subtractExact(retainedFee,workspace-actual);
            long peak=historyValidationFee(record);
            for(var old:history)for(var reference:old.record.allReferences())
                peak=Math.max(peak,Math.addExact(256L,Math.multiplyExact(2L,reference.fullLength())));
            for(var reference:record.allReferences())
                peak=Math.max(peak,Math.addExact(256L,Math.multiplyExact(2L,reference.fullLength())));
            root.reserveTemporaryMemory(peak,"V6 exact confirmation publication peak protected across SQL/readback");
            confirmation.protectedPeak=peak;
            return confirmation;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    public DurableV6Ack publishConfirmation(ConfirmationReservation reserved,Object plan,DurableV6Ack predecessor)throws IOException {
        Objects.requireNonNull(reserved).requirePrepared(plan,predecessor);
        if(reserved.store!=this)throw new IOException("same original confirmation store required");
        // 原owner thread内无调用方callback；退保护后立即按原publication路径实收相同峰值。
        consumingConfirmation=true;
        try {
            root.releaseTemporaryMemory(reserved.protectedPeak,"V6 protected publication peak consumed by original store");
            reserved.protectedPeak=0L;
            rebarrierCurrent(predecessor);
            var ack=publish(reserved.record,List.of(),reserved.encoded);
            confirmation=null;consumingConfirmation=false;return ack;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    private static final class Pending {
        String temporary,canonical,retired;
        WindowsMarkerIo.Handle temporaryHandle,canonicalHandle,retiredHandle;
        WindowsMarkerIo.FileIdentity identity;
        byte[] bytes,readback;
    }
    public static final class DurableV6Ack {
        private final WindowsB1V6WitnessRecordStore store;
        private final B1ResumeRecordV6 record;
        private final byte[] bytes;
        private final WindowsMarkerIo.FileIdentity identity;
        private String name;
        private DurableV6Ack(WindowsB1V6WitnessRecordStore store,B1ResumeRecordV6 record,byte[] bytes,WindowsMarkerIo.FileIdentity identity,String name) {
            this.store=store;this.record=record;this.bytes=bytes;this.identity=identity;this.name=name;
        }
        public B1ResumeRecordV6 record() throws IOException {store.requireLive();return record;}
        public int retainedCanonicalByteLength() throws IOException {store.requireMemoryOwner();return bytes.length;}
        public void requireRegisteredCurrent() throws IOException {
            store.requireMemoryOwner();if(store.current!=this)throw new IOException("original V6 durable ACK was superseded");
        }
        public void requireCurrent() throws IOException {store.rebarrierCurrent(this);}
        public void requireForInstallation(WindowsWorldProvisioner.DeploymentReceiptIoScope scope) throws IOException {
            requireCurrent();
            if(store.source!=scope||record.transition().kind()!=B1ResumeRecord.Kind.HANDOFF_PREPARED||
                    !record.transition().assertions().currentOwner().equals(scope.currentOwner()))
                throw new IOException("exact native schema5 HANDOFF_PREPARED ACK for target owner required");
        }
    }
    public static final class CurrentInventory {
        public enum Kind { PRE_GENESIS, RECORDED }
        private final WindowsB1V6WitnessRecordStore store;
        private final List<DurableV6Ack> records;
        private final DurableV6Ack current;
        private CurrentInventory(WindowsB1V6WitnessRecordStore store,List<DurableV6Ack> records,DurableV6Ack current){this.store=store;this.records=records;this.current=current;}
        public List<DurableV6Ack> records() throws IOException {requireCurrent();return records;}
        public Kind kind()throws IOException {requireCurrent();return current==null?Kind.PRE_GENESIS:Kind.RECORDED;}
        public DurableV6Ack current() throws IOException {requireCurrent();if(current==null)throw new IOException("actual pre-genesis inventory has no durable V6 ACK");return current;}
        public void requireCurrent() throws IOException {
            store.requireLive();
            if(store.current!=current)throw new IOException("original V6 inventory superseded");
            for(var record:records)store.rebarrier(record);
        }
        public void requireRegisteredCurrent() throws IOException {
            store.requireMemoryOwner();
            if(store.current!=current || store.history.size()!=records.size())throw new IOException("original V6 inventory changed after external rebarrier");
            for(int index=0;index<records.size();index++)if(store.history.get(index)!=records.get(index))
                throw new IOException("original V6 inventory member replaced");
        }
        public void requireRetiredOriginalExecutorsForInstallation(WindowsWorldProvisioner.DeploymentReceiptIoScope scope)throws IOException {
            store.requireMemoryOwner();
            if(scope!=store.source||store.history.size()<records.size())
                throw new IOException("exact original cold inventory/source prefix required");
            if(records.isEmpty()){
                if(current!=null||!store.discoveryAttempted||store.discoveredFiles==null||!store.discoveredFiles.isEmpty())
                    throw new IOException("exact complete empty pre-genesis discovery required");
                scope.verifyRelatedExecutorRetired(scope.previousOwner());
                return;
            }
            for(int index=0;index<records.size();index++){
                if(store.history.get(index)!=records.get(index))throw new IOException("original cold history member replaced");
                var state=records.get(index).record.transition().assertions();
                scope.verifyRelatedExecutorRetired(state.currentOwner());
                if(state.previousOwner()!=null)scope.verifyRelatedExecutorRetired(state.previousOwner());
            }
        }
    }
    public WindowsB1V6WitnessRecordStore(WindowsWorldProvisioner.DeploymentReceiptIoScope source,WindowsB1Full26WitnessStore artifacts,
            MySqlFormat3ReadBudget root) throws IOException {
        this.source=Objects.requireNonNull(source);this.artifacts=Objects.requireNonNull(artifacts);this.root=Objects.requireNonNull(root);
        source.bindAllocationRoot(root);
        source.validateLive();root.reserveRetainedMemory(256L,"schema5 V6 native record store holder");
        retainedFee=256L;
    }
    private void requireLive() throws IOException {
        requireMemoryOwner();source.validateLive();
    }
    private void requireMemoryOwner() throws IOException {
        if(Thread.currentThread()!=thread||root.isClosed())throw new IOException("original V6 native store thread/root required");
        if(retirementAttempted || retired || failure!=null)throw new IOException("V6 native publication/discovery/retirement unavailable",failure);
    }
    static String slot(long sequence) throws IOException {
        if(sequence<0)throw new IOException("nonnegative V6 sequence required");return sequence%2==0?B1ResumeRecordV6.SLOT_0:B1ResumeRecordV6.SLOT_1;
    }
    static String historyName(long sequence,byte[] sha) throws IOException {
        slot(sequence);return ".floatingpod-b1-resume-v6-history-"+sequence+"-"+HexFormat.of().formatHex(B1Full26WitnessReference.hash(sha))+".bin";
    }
    private WindowsMarkerIo.Handle open(String name,int access,int share) throws IOException {
        try{return source.io().open(source.directory().resolve(name),access,share,WindowsMarkerIo.OPEN_EXISTING,
            WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,source.operation());}
        catch(WindowsMarkerIo.NativeFailure absent){if(absent.win32Error()==2)return null;throw absent;}
    }
    private void requireChild(WindowsMarkerIo io,WindowsMarkerIo.Handle handle,String name) throws IOException {
        String path=io.finalPath(handle);WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(source.nativePath(),path);
        if(!path.endsWith("\\"+name))throw new IOException("V6 fixed child name changed");
    }
    private void requireAbsent(String name) throws IOException {
        var io=source.io();var handle=open(name,WindowsMarkerIo.READ,0);
        if(handle!=null){
            if(pending==null)pending=new Pending();pending.canonicalHandle=handle;
            throw new IOException("existing V6 file responsibility retained: "+name);
        }
    }
    private void validateSource(B1ResumeRecordV6 record) throws IOException {
        B1ResumeRecordCodec.exact(source.originalReceipt(),record.transition().originalReceipt(),"V6 original receipt differs");
        B1ResumeRecordCodec.exact(HexFormat.of().parseHex(source.anchorSha256()),record.transition().sourceAnchorSha256(),"V6 source anchor differs");
        if(!source.namespace().equals(record.transition().barrierNamespace()))throw new IOException("V6 namespace differs from native source");
    }
    private void validateReferences(B1ResumeRecordV6 record) throws IOException {
        for(var reference:record.allReferences())artifacts.withReadBody(reference,bytes->{});
    }
    public DurableV6Ack publishGenesis(B1ResumeRecordV6 genesis,List<WindowsB1Full26WitnessStore.DurableFull26Ack> acknowledged) throws IOException {
        if(confirmation!=null)throw new IOException("reserved confirmation owns original publication slot");
        requireLive();if(current!=null||!history.isEmpty()||pending!=null||genesis.transition().sequence()!=0)throw new IOException("fresh V6 genesis is single-use");
        requireAbsent(B1ResumeRecordV6.SLOT_0);requireAbsent(B1ResumeRecordV6.SLOT_1);requireAbsent(B1ResumeRecordV6.TEMPORARY);
        return publish(genesis,acknowledged);
    }
    public DurableV6Ack publishNext(DurableV6Ack predecessor,B1ResumeRecordV6 next,List<WindowsB1Full26WitnessStore.DurableFull26Ack> acknowledged) throws IOException {
        if(confirmation!=null)throw new IOException("reserved confirmation owns original publication slot");
        requireLive();rebarrierCurrent(predecessor);
        B1ResumeRecordV6.consecutiveData(predecessor.record,predecessor.bytes,next);
        return publish(next,acknowledged);
    }
    private DurableV6Ack publish(B1ResumeRecordV6 record,List<WindowsB1Full26WitnessStore.DurableFull26Ack> acknowledged) throws IOException {
        return publish(record,acknowledged,null);
    }
    private DurableV6Ack publish(B1ResumeRecordV6 record,List<WindowsB1Full26WitnessStore.DurableFull26Ack> acknowledged,byte[] reservedBytes) throws IOException {
        requireLive();validateSource(record);
        validateCompleteHistory(record);
        for(var reference:record.allReferences()) {
            boolean found=false;
            for(var ack:acknowledged){ack.requireSource(source);if(reference.exactlyEquals(ack.reference())){ack.rebarrier();found=true;break;}}
            // 已由本store真实冷读/过去ACK核验的reference仍需再做完整native rebarrier。
            if(!found) {
                boolean historical=history.stream().anyMatch(old->old.record.allReferences().stream().anyMatch(reference::exactlyEquals));
                if(!historical)throw new IOException("V6 reference has no original durable Full26 ACK/history");
                artifacts.withReadBody(reference,bytes->{});
            }
        }
        byte[] encoded=reservedBytes;
        if(encoded==null){
            try {
                long workspace=Math.addExact(512L,Math.multiplyExact(4L,(long)B1ResumeRecordV6.MAX_RECORD_BYTES));
                root.reserveRetainedMemory(workspace,"V6 bounded pending/bytes/ACK BEFORE encoding");retainedFee=Math.addExact(retainedFee,workspace);
                encoded=B1ResumeRecordV6.encodeData(record);
                long unused=Math.subtractExact(workspace,Math.addExact(512L,Math.multiplyExact(4L,(long)encoded.length)));
                root.releaseRetainedMemory(unused,"V6 unused bounded encoding capacity");retainedFee=Math.subtractExact(retainedFee,unused);
            }catch(Throwable cause){failure=cause;throw propagate(cause);}
        }
        pending=new Pending();pending.bytes=encoded;pending.temporary=B1ResumeRecordV6.TEMPORARY;pending.canonical=slot(record.transition().sequence());
        try {
            requireAbsent(pending.temporary);
            DurableV6Ack inactive=null;
            for(var old:history)if(old.name.equals(pending.canonical))inactive=old;
            if(inactive!=null)retireInactive(inactive);else requireAbsent(pending.canonical);
            var io=source.io();var directory=source.directory();
            pending.temporaryHandle=io.createMarkerHandle(directory.resolve(pending.temporary),source.operation());
            requireChild(io,pending.temporaryHandle,pending.temporary);io.metadata(pending.temporaryHandle,false,0);
            io.writePayload(pending.temporaryHandle,pending.bytes);io.metadata(pending.temporaryHandle,false,pending.bytes.length);
            pending.identity=io.identity(pending.temporaryHandle);io.flushChecked(pending.temporaryHandle);
            io.moveWriteThroughNoReplace(directory.resolve(pending.temporary),directory.resolve(pending.canonical),source.operation());
            io.closeChecked(pending.temporaryHandle);pending.temporaryHandle=null;
            pending.canonicalHandle=open(pending.canonical,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
            if(pending.canonicalHandle==null)throw new IOException("published V6 slot missing");
            requireChild(io,pending.canonicalHandle,pending.canonical);
            if(!pending.identity.equals(io.identity(pending.canonicalHandle)))throw new IOException("V6 publication FileId differs");
            pending.readback=io.readPayloadExact(pending.canonicalHandle,pending.bytes.length);
            B1ResumeRecordCodec.exact(pending.bytes,pending.readback,"V6 complete readback differs");io.flushChecked(pending.canonicalHandle);
            B1ResumeRecordCodec.exact(pending.bytes,io.readPayloadExact(pending.canonicalHandle,pending.bytes.length),"V6 flushed readback differs");
            source.validateLive();io.closeChecked(pending.canonicalHandle);pending.canonicalHandle=null;
            var ack=new DurableV6Ack(this,record,pending.bytes,pending.identity,pending.canonical);
            history.add(ack);current=ack;pending=null;return ack;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    private void retireInactive(DurableV6Ack inactive) throws IOException {
        if(inactive==current)throw new IOException("cannot retire current V6 slot");
        rebarrier(inactive);var io=source.io();Path directory=source.directory();
        pending.retired=historyName(inactive.record.transition().sequence(),WindowsWorldProvisioner.digest(inactive.bytes));
        requireAbsent(pending.retired);
        pending.retiredHandle=open(inactive.name,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE|WindowsMarkerIo.DELETE,WindowsMarkerIo.SHARE_DELETE);
        if(pending.retiredHandle==null)throw new IOException("V6 inactive slot disappeared");
        requireChild(io,pending.retiredHandle,inactive.name);
        if(!inactive.identity.equals(io.identity(pending.retiredHandle)))throw new IOException("V6 inactive identity changed");
        B1ResumeRecordCodec.exact(inactive.bytes,io.readPayloadExact(pending.retiredHandle,inactive.bytes.length),"V6 inactive bytes changed");
        io.moveWriteThroughNoReplace(directory.resolve(inactive.name),directory.resolve(pending.retired),source.operation());
        io.closeChecked(pending.retiredHandle);pending.retiredHandle=null;
        inactive.name=pending.retired;rebarrier(inactive);
    }
    public void rebarrierCurrent(DurableV6Ack ack) throws IOException {
        requireLive();if(ack==null||ack.store!=this||current!=ack)throw new IOException("exact current V6 native ACK required");rebarrier(ack);
    }
    private void rebarrier(DurableV6Ack ack) throws IOException {
        requireLive();var io=source.io();var handle=open(ack.name,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);
        if(handle==null)throw new IOException("V6 retained record missing");
        // 所有失败handle均在原store登记，不以局部close异常覆盖原primary。
        if(pending==null)pending=new Pending();pending.canonicalHandle=handle;
        try {
            requireChild(io,handle,ack.name);if(!ack.identity.equals(io.identity(handle)))throw new IOException("V6 retained FileId differs");
            B1ResumeRecordCodec.exact(ack.bytes,io.readPayloadExact(handle,ack.bytes.length),"V6 complete retained bytes differ");
            io.flushChecked(handle);B1ResumeRecordCodec.exact(ack.bytes,io.readPayloadExact(handle,ack.bytes.length),"V6 complete rebarrier bytes differ");
            io.closeChecked(handle);pending.canonicalHandle=null;validateReferences(ack.record);source.validateLive();
            if(pending.bytes==null)pending=null;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    private DurableV6Ack read(String name) throws IOException {
        var io=source.io();var handle=open(name,WindowsMarkerIo.READ|WindowsMarkerIo.WRITE,0);if(handle==null)return null;
        if(pending==null)pending=new Pending();pending.canonicalHandle=handle;
        requireChild(io,handle,name);long n=io.payloadLength(handle);
        if(n<=0||n>B1ResumeRecordV6.MAX_RECORD_BYTES)throw new IOException("V6 bounded actual length required");
        long fee=Math.addExact(512L,Math.multiplyExact(n,4L));
        root.reserveRetainedMemory(fee,"V6 cold actual bytes/record BEFORE allocation");retainedFee=Math.addExact(retainedFee,fee);
        byte[] bytes=io.readPayloadExact(handle,(int)n);pending.bytes=bytes;var identity=io.identity(handle);io.flushChecked(handle);
        B1ResumeRecordCodec.exact(bytes,io.readPayloadExact(handle,(int)n),"V6 cold full rebarrier differs");
        io.closeChecked(handle);pending.canonicalHandle=null;
        var record=B1ResumeRecordV6.decodeData(bytes);validateSource(record);validateReferences(record);
        var ack=new DurableV6Ack(this,record,bytes,identity,name);inspected.add(ack);pending=null;return ack;
    }
    public CurrentInventory discoverCurrent() throws IOException {
        requireLive();if(discoveryAttempted||current!=null)throw new IOException("V6 cold discovery is one-shot");discoveryAttempted=true;
        try {
            scanDiscoveryFiles();
            requireAbsent(B1ResumeRecordV6.TEMPORARY);
            var a=read(B1ResumeRecordV6.SLOT_0);var b=read(B1ResumeRecordV6.SLOT_1);
            if(a==null&&b==null){
                if(!discoveredFiles.isEmpty())throw new IOException("orphan schema5 history/body without current V6 slots; retained unresolved");
                source.verifyRelatedExecutorRetired(source.previousOwner());
                return retainInventorySnapshot();
            }
            if(a!=null&&b!=null&&a.record.transition().sequence()==b.record.transition().sequence())throw new IOException("conflicting V6 current sequences");
            current=a==null?b:b==null?a:a.record.transition().sequence()>b.record.transition().sequence()?a:b;
            var reverse=new ArrayList<DurableV6Ack>();reverse.add(current);var next=current;
            while(next.record.transition().sequence()>0) {
                if(reverse.size()>=root.profile().maxMembers())throw new IOException("V6 full history exceeds explicit member limit");
                long sequence=next.record.transition().sequence()-1;DurableV6Ack older=null;
                if(a!=null&&a.record.transition().sequence()==sequence)older=a;
                if(b!=null&&b.record.transition().sequence()==sequence)older=b;
                if(older==null)older=read(historyName(sequence,next.record.previousContainerSha256()));
                if(older==null)throw new IOException("missing complete V6 predecessor history; unresolved");
                B1ResumeRecordV6.consecutiveData(older.record,older.bytes,next.record);reverse.add(older);next=older;
            }
            for(int index=reverse.size()-1;index>=0;index--)history.add(reverse.get(index));
            validateCompleteHistory(null);
            for(var retained:history){
                var assertions=retained.record.transition().assertions();
                source.verifyRelatedExecutorRetired(assertions.currentOwner());
                if(assertions.previousOwner()!=null)source.verifyRelatedExecutorRetired(assertions.previousOwner());
            }
            requireDiscoveryClosure();
            return retainCurrentInventory(current);
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    private void scanDiscoveryFiles()throws IOException {
        if(discoveredFiles!=null||pendingFind!=null)throw new IOException("original schema5 physical discovery is one-shot");
        root.reserveRetainedMemory(256L,"schema5 original directory inventory map BEFORE allocation");retainedFee=Math.addExact(retainedFee,256L);
        discoveredFiles=new HashMap<>();
        pendingFind=source.io().allocateDeploymentReceiptWitnessDiscovery(source,root);
        Throwable primary=null;
        try {
            pendingFind.start();
            while(pendingFind.next()){
                String name=pendingFind.basename();long length=pendingFind.length();int attributes=pendingFind.attributes();
                long fee=Math.addExact(256L,Math.multiplyExact(name.length(),2L));
                root.reserveRetainedMemory(fee,"schema5 actual native filename/map entry BEFORE retention");retainedFee=Math.addExact(retainedFee,fee);
                if(discoveredFiles.putIfAbsent(name,length)!=null)throw new IOException("duplicate schema5 native inventory entry: "+name);
                long validation=Math.addExact(512L,Math.multiplyExact(name.length(),2L));
                root.reserveTemporaryMemory(validation,"schema5 exact native basename UUID/hash comparison BEFORE allocation");
                Throwable validationFailure=null;
                try{requireValidDiscoveryEntry(name,length,attributes,root.profile().maxNativeArtifactBytes());}
                catch(Throwable cause){validationFailure=cause;throw propagate(cause);}
                finally{
                    try{root.releaseTemporaryMemory(validation,"schema5 native basename comparison dropped");}
                    catch(Throwable cleanup){if(validationFailure!=null)com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(validationFailure,cleanup);else throw propagate(cleanup);}
                }
            }
        }catch(Throwable cause){primary=cause;throw propagate(cause);}
        finally {
            try{pendingFind.close();pendingFind=null;}
            catch(Throwable cleanup){if(primary!=null)com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else throw propagate(cleanup);}
        }
    }
    /** 名称/长度/属性DATA校验；不读取文件、不创建inventory/ACK或安装许可。 */
    static void requireValidDiscoveryEntry(String name,long length,int attributes,long maximumArtifactBytes)throws IOException {
        Objects.requireNonNull(name);
        if(maximumArtifactBytes<=0)throw new IllegalArgumentException("actual positive artifact profile bound required");
        if((attributes&(0x10|0x400))!=0||length<=0)
            throw new IOException("schema5 physical inventory directory/reparse/empty responsibility: "+name+" attributes="+attributes);
        if(name.equals(B1ResumeRecordV6.SLOT_0)||name.equals(B1ResumeRecordV6.SLOT_1)){
            if(length>B1ResumeRecordV6.MAX_RECORD_BYTES)throw new IOException("schema5 current record exceeds fixed V6 bound");
            return;
        }
        String historyPrefix=".floatingpod-b1-resume-v6-history-",bodyPrefix=".floatingpod-b1-full26-v1-";
        try {
            if(name.startsWith(historyPrefix)&&name.endsWith(".bin")){
                int separator=name.length()-4-64-1;
                if(separator<=historyPrefix.length()||name.charAt(separator)!='-')throw new IOException("malformed V6 historical basename");
                long sequence=Long.parseLong(name,historyPrefix.length(),separator,10);
                byte[] hash=HexFormat.of().parseHex(name,separator+1,name.length()-4);
                if(!historyName(sequence,hash).equals(name)||length>B1ResumeRecordV6.MAX_RECORD_BYTES)
                    throw new IOException("noncanonical or oversized V6 historical file");
                return;
            }
            if(name.startsWith(bodyPrefix)&&name.endsWith(".bin")&&name.length()==bodyPrefix.length()+36+1+64+4){
                int start=bodyPrefix.length();
                if(name.charAt(start+36)!='-')throw new IOException("malformed Full26 artifact basename");
                var attempt=java.util.UUID.fromString(name.substring(start,start+36));
                byte[] hash=HexFormat.of().parseHex(name,start+37,name.length()-4);
                WindowsB1Full26WitnessStore.requireBodyBound(length,maximumArtifactBytes);
                if(!B1Full26WitnessReference.basename(attempt,hash).equals(name))
                    throw new IOException("noncanonical or oversized Full26 artifact");
                return;
            }
        }catch(IllegalArgumentException cause){throw new IOException("invalid schema5 native inventory basename: "+name,cause);}
        throw new IOException("schema5 physical inventory temporary/legacy/unknown responsibility: "+name);
    }
    private void requireDiscoveryClosure()throws IOException {
        long workspace=Math.addExact(256L,Math.multiplyExact((long)discoveredFiles.size(),128L));
        root.reserveTemporaryMemory(workspace,"schema5 native inventory exact history/reference index BEFORE allocation");
        Throwable primary=null;
        try {
            Set<String> expected=new HashSet<>();
            for(var retained:history){
                if(!discoveredFiles.containsKey(retained.name)||discoveredFiles.get(retained.name)!=retained.bytes.length)
                    throw new IOException("schema5 native history inventory differs from actual complete record");
                expected.add(retained.name);
                for(var reference:retained.record.allReferences()){
                    if(!discoveredFiles.containsKey(reference.basename())||discoveredFiles.get(reference.basename())!=reference.fullLength())
                        throw new IOException("schema5 native artifact inventory differs from retained complete reference");
                    expected.add(reference.basename());
                }
            }
            if(expected.size()!=discoveredFiles.size()||!expected.equals(discoveredFiles.keySet()))
                throw new IOException("orphan/unreferenced schema5 native history/body responsibility retained");
        }catch(Throwable cause){primary=cause;throw propagate(cause);}
        finally{
            try{root.releaseTemporaryMemory(workspace,"schema5 native inventory exact index dropped");}
            catch(Throwable cleanup){if(primary!=null)com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);else throw propagate(cleanup);}
        }
    }
    private long historyValidationFee(B1ResumeRecordV6 next)throws IOException {
        long count=Math.addExact((long)history.size(),next==null?0L:1L);
        if(count>root.profile().maxMembers())throw new IOException("V6 complete confirmation history exceeds explicit member limit");
        // 完整 attempt/引用索引按实际保留记录数收费，成功/失败都由原 ledger 结算临时工作区。
        long fee=Math.addExact(256L,Math.multiplyExact(count,256L));
        for(var old:history)fee=Math.addExact(fee,Math.multiplyExact((long)old.bytes.length,4L));
        if(next!=null)fee=Math.addExact(fee,Math.multiplyExact((long)B1ResumeRecordV6.MAX_RECORD_BYTES,4L));
        return fee;
    }
    private void validateCompleteHistory(B1ResumeRecordV6 next) throws IOException {
        long fee=historyValidationFee(next);
        root.reserveTemporaryMemory(fee,"V6 complete confirmation history DATA validation");
        Throwable primary=null;
        try {
            var records=new ArrayList<B1ResumeRecordV6>();
            for(var old:history)records.add(old.record);
            if(next!=null)records.add(next);
            B1ResumeRecordV6.validateHistoryData(records);
        }catch(Throwable cause){primary=cause;failure=cause;throw propagate(cause);}
        finally{
            try{root.releaseTemporaryMemory(fee,"V6 complete confirmation history DATA validation complete");}
            catch(Throwable cleanup){if(primary!=null){com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);}else{failure=cleanup;throw propagate(cleanup);}}
        }
    }
    public Throwable retainedFailure(){return failure;}
    /** 同一store实际发布后的完整历史；不是第二次cold discovery，也不从调用方DATA铸ACK。 */
    public CurrentInventory retainCurrentInventory(DurableV6Ack expected)throws IOException {
        requireLive();
        if(expected==null||expected.store!=this||current!=expected||pending!=null)
            throw new IOException("exact original current durable V6 ACK and idle publication required");
        validateCompleteHistory(null);
        return retainInventorySnapshot();
    }
    private CurrentInventory retainInventorySnapshot()throws IOException {
        long fee=Math.addExact(264L,Math.multiplyExact((long)history.size(),8L));
        root.reserveRetainedMemory(fee,"V6 actual immutable inventory/list registration BEFORE allocation");
        retainedFee=Math.addExact(retainedFee,fee);
        try {
            var inventory=new CurrentInventory(this,List.copyOf(history),current);
            inventories.add(inventory);
            inventory.requireCurrent();
            return inventory;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    public void retireAfterSqlSettlement(MySqlDeploymentReceiptSession.NativeRetirementPermit5 permit)throws IOException {
        Objects.requireNonNull(permit);var io=source.retirementIo(permit);
        if(Thread.currentThread()!=thread || retirementAttempted || retired || failure!=null || pendingFind!=null)
            throw new IOException("original V6 store retirement is one-shot and requires settled publication",failure);
        retirementAttempted=true;
        try {
            if(pending!=null){
                if(pending.temporaryHandle!=null){io.closeChecked(pending.temporaryHandle);pending.temporaryHandle=null;}
                if(pending.canonicalHandle!=null){io.closeChecked(pending.canonicalHandle);pending.canonicalHandle=null;}
                if(pending.retiredHandle!=null){io.closeChecked(pending.retiredHandle);pending.retiredHandle=null;}
                pending.bytes=null;pending.readback=null;pending=null;
            }
            current=null;history.clear();inspected.clear();inventories.clear();
            if(discoveredFiles!=null){discoveredFiles.clear();discoveredFiles=null;}
            root.releaseRetainedMemory(retainedFee,"V6 native store actual history/ACK references retired");retainedFee=0L;retired=true;
        }catch(Throwable cause){failure=cause;throw propagate(cause);}
    }
    private static IOException propagate(Throwable cause) throws IOException {
        if(cause instanceof IOException io)return io;if(cause instanceof RuntimeException runtime)throw runtime;if(cause instanceof Error error)throw error;
        return new IOException("schema5 V6 native operation failed",cause);
    }
}

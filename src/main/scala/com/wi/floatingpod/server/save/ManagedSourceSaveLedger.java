package com.wi.floatingpod.server.save;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** 观察责任而非write permit/ACK。小锁不跨任何原操作、编码或IO；所有历史保留。 */
public final class ManagedSourceSaveLedger {
    public record Capacity(int maximumAttempts, int maximumRetainedOutputs, int maximumCauses) {
        public Capacity { if(maximumAttempts<1 || maximumRetainedOutputs<1 || maximumCauses<1)throw new IllegalArgumentException("Explicit observer count capacity required"); }
    }
    public record Slot(int x, int z) { }
    public enum Progress { PRODUCING, FINAL_NBT, QUEUED, SUPERSEDED, NO_PUT, INFLIGHT, CALLS_RETURNED, FAULTED }
    public enum Resolution { UNRESOLVED }
    public static final class Payload {
        private final byte[] bytes;
        private final String sha256;
        private Payload(byte[] bytes) {
            this.bytes=bytes.clone();
            try { sha256=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(this.bytes)); }
            catch (NoSuchAlgorithmException cause) { throw new IllegalStateException("SHA-256 unavailable", cause); }
        }
        public byte[] bytes() { return bytes.clone(); }
        public int length() { return bytes.length; }
        public String sha256() { return sha256; }
        public int unsignedByteAt(int index) { return bytes[index]&0xFF; }
        /** Writes the immutable original payload without manufacturing an unbudgeted defensive copy. */
        public void writeTo(DataOutput output)throws IOException { Objects.requireNonNull(output).write(bytes,0,bytes.length); }
    }
    public record Observation(UUID attempt, Slot slot, long acceptedGeneration, Progress progress,
            Payload finalPayload, int encoderFalseCount, int dataWritesReturned, int headerWritesReturned,
            boolean regionCallReturnedWithoutWrite, long encoderSubmittedBytes, Throwable primary, int retainedOutputCount, boolean largeDisabledReturn, List<String> events, Resolution resolution) { public Observation { events=List.copyOf(events); } }
    public record RegionObservation(UUID id,long generation,boolean closeBegun,int closeCallsReturned,Throwable primary,Resolution resolution) { }
    public record Snapshot(UUID boot, SourceLimitProfile profile, List<Observation> attempts, int producers, int flights,
            boolean closeStarted, int closeCallsReturned, Throwable primary, boolean observationSealed, int outputReservations, long reservedOutputBytes, List<RegionObservation> regions, long retainedPayloadBytes) {
        public Snapshot { attempts=List.copyOf(attempts);regions=List.copyOf(regions); }
    }
    /** 不可自行构造；绑定本ledger、原payload引用及真实consume线程。 */
    public static final class Attempt {
        private final ManagedSourceSaveLedger ledger;
        private final UUID id=UUID.randomUUID();
        private UUID flightId,outputObservationId;
        private UUID pendingFlightId,pendingOutputObservationId;
        private final Slot slot;
        private final Thread producer;
        private Object compound,actualPrevious;
        private Payload payload;
        private long generation;
        private Progress progress=Progress.PRODUCING;
        private boolean producerEnded, flightEnded;
        private Thread consumer;
        private int encoderFalse, writes, headers;
        private long encoderBytes;
        private boolean outputReserved;
        private boolean noWrite,largeDisabled;
        private final List<String> events=new ArrayList<>(List.of("PRODUCER_BEGIN"));
        private Throwable primary;
        private Object actualOutput;
        private boolean abandonedOutput;
        private Attempt(ManagedSourceSaveLedger ledger, Slot slot) {
            this.ledger=ledger;this.slot=slot;producer=Thread.currentThread();
        }
    }
    private static final class RegionState {
        private final UUID id=UUID.randomUUID();private final long generation;
        private boolean closeBegun;private int closeReturned;private Throwable primary;
        private RegionState(long generation){this.generation=generation;}
    }
    private final UUID boot;
    private final UUID loaderObservationId=UUID.randomUUID();
    private final Thread installer;
    private final Object loader;
    private final SourceLimitProfile limits;
    private final Capacity capacity;
    private com.wi.floatingpod.server.source.SourceForestAllocationBudget allocation;
    private long sourceRevision;
    private final List<Attempt> history=new ArrayList<>();
    private final Map<Slot,Attempt> queued=new HashMap<>();
    private long acceptedGeneration;
    private int producers,flights,closeReturned,outputReservations;
    private long reservedOutputBytes,retainedPayloadBytes;
    private final Map<Object,RegionState> regions=new java.util.IdentityHashMap<>();
    private final java.util.Set<Throwable> causes=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private RuntimeException causeOverflow;
    // 原 ledger holder 的单一紧急槽；不递归收费或累积未获 admission 的 allocation cause。
    private Throwable allocationFailure;
    private long regionGeneration;
    private boolean closeStarted,observationSealed;
    private boolean consumerDetached;
    private int activeAllocationEntries;
    private Throwable primary;
    private final Thread ioThread;
    public ManagedSourceSaveLedger(UUID boot, Thread installer, Object exactLoader, Thread exactIoThread, SourceLimitProfile limits, Capacity capacity) {
        this(boot,installer,exactLoader,exactIoThread,limits,Objects.requireNonNull(capacity),null);
    }
    /** Recovery counts are bounded by the original root, not invented observer count policy. */
    public static ManagedSourceSaveLedger forRecall(UUID boot,Thread main,Object loader,Thread io,SourceLimitProfile limits,
            com.wi.floatingpod.server.source.SourceForestAllocationBudget allocation)throws java.io.IOException{
        Objects.requireNonNull(allocation);if(allocation.exactProfile()!=limits)throw new IllegalArgumentException("Original Recall save profile required");
        allocation.reserveTemporary(1024L,"original Recall save ledger/queue owner BEFORE allocation");
        allocation.reserveTemporary(16L,"exact original loader observation UUID BEFORE allocation");
        ManagedSourceSaveLedger ledger=new ManagedSourceSaveLedger(boot,main,loader,io,limits,null,allocation);
        ledger.allocation=allocation.forSave(ledger);return ledger;
    }
    private ManagedSourceSaveLedger(UUID boot,Thread installer,Object exactLoader,Thread exactIoThread,SourceLimitProfile limits,Capacity capacity,
            com.wi.floatingpod.server.source.SourceForestAllocationBudget allocation){
        this.allocation=allocation;
        this.boot=Objects.requireNonNull(boot);this.installer=Objects.requireNonNull(installer);
        loader=Objects.requireNonNull(exactLoader);this.limits=Objects.requireNonNull(limits);this.capacity=capacity;ioThread=Objects.requireNonNull(exactIoThread);
        if(limits.maxDecompressedBytes()>Integer.MAX_VALUE)throw new IllegalArgumentException("Observer full NBT byte array requires bounded int32 decompressed budget");
        if(ioThread==installer)throw new IllegalArgumentException("IO writer cannot be installer Main");
        if (Thread.currentThread()!=installer) throw new IllegalStateException("Only exact installer constructs ledger");
    }
    private final ThreadLocal<AllocationEntry> allocationEntry=new ThreadLocal<>();
    public final class AllocationEntry implements AutoCloseable {
        private final Thread actor=Thread.currentThread();private boolean closed;
        private AllocationEntry(){}
        public void close(){
            synchronized(ManagedSourceSaveLedger.this){
            if(closed||Thread.currentThread()!=actor||allocationEntry.get()!=this)throw new IllegalStateException("Exact save allocation entry required");
            if(allocation!=null){try{allocation.releaseTemporary(128L,"original source save lexical entry dropped");}catch(java.io.IOException cause){retainAllocationFailure(cause);throw new java.io.UncheckedIOException(cause);}}
            closed=true;allocationEntry.remove();activeAllocationEntries--;
            }
        }
    }
    public synchronized AllocationEntry enterProducerAllocation(Object actualLoader){assertInstallation(actualLoader);if(closeStarted)throw new IllegalStateException("Original source producers are stopping");return enterAllocation();}
    public synchronized AllocationEntry enterConsumerAllocation(Object actualLoader){requireIo(actualLoader);return enterAllocation();}
    private AllocationEntry enterAllocation(){
        if(consumerDetached)throw new IllegalStateException("Original source save consumer detached");
        if(allocationEntry.get()!=null)throw new IllegalStateException("Repeated save allocation entry");
        try{int next=Math.incrementExact(activeAllocationEntries);if(allocation!=null)allocation.reserveSaveEntry(this);AllocationEntry entry=new AllocationEntry();allocationEntry.set(entry);activeAllocationEntries=next;return entry;}
        catch(java.io.IOException cause){retainAllocationFailure(cause);throw new java.io.UncheckedIOException(cause);}
        catch(RuntimeException|Error cause){retainAllocationFailure(cause);throw cause;}
    }
    private synchronized void retainAllocationFailure(Throwable cause){
        if(allocationFailure!=null)return;
        allocationFailure=cause;
        if(primary==null)primary=cause;else suppressSecondary(primary,cause);
    }
    public void requireAllocationEntryActor(){if(Thread.currentThread()!=installer&&Thread.currentThread()!=ioThread)throw new IllegalStateException("Exact source save entry actor required");}
    public boolean mainAllocationActor(){return Thread.currentThread()==installer;}
    public void requireAllocationOwner(){
        if(Thread.currentThread()==installer)return;
        if(Thread.currentThread()==ioThread&&allocationEntry.get()!=null)return;
        var read=com.wi.floatingpod.server.source.SourceReadOperation.currentRead();
        if(read!=null){read.requireQueuedAllocationOwner(loader);return;}
        throw new IllegalStateException("Exact source save/read lexical allocation owner required");
    }
    public SourceLimitProfile limits() { return limits; }
    public void assertExactIoThread(Thread actualThread){if(actualThread!=ioThread)throw new IllegalStateException("Declared IO owner differs from actual constructor Thread.start receiver");}
    public void assertQueueActor(){if(Thread.currentThread()!=installer && Thread.currentThread()!=ioThread)throw new IllegalStateException("Exact Main or IO queue actor required");}
    public synchronized void assertInstallation(Object actualLoader) {
        if (Thread.currentThread()!=installer || actualLoader!=loader) throw new IllegalStateException("Exact installation owner/loader required");
    }
    public synchronized Attempt begin(Object actualLoader, Slot slot) {
        assertInstallation(actualLoader);
        if(consumerDetached)throw new IllegalStateException("Original source save consumer detached");
        if(primary!=null && !closeStarted && !observationSealed)throw new IllegalStateException("Managed observation admission frozen",primary);
        if (capacity!=null && history.size()>=capacity.maximumAttempts()) {
            IllegalStateException cause=new IllegalStateException("Observation attempt budget exhausted before save mutation");
            sticky(cause);throw cause;
        }
        chargeOriginal(2048L,"actual source save attempt/events BEFORE allocation");changed();
        Attempt a=new Attempt(this,Objects.requireNonNull(slot));history.add(a);producers++;
        if (closeStarted || observationSealed) { observationSealed=false;IllegalStateException cause=new IllegalStateException("Late producer after stopping/seal; responsibility retained");fail(a,cause);changed();a.producerEnded=true;producers--;throw cause; }
        return a;
    }
    public synchronized void finalReference(Attempt a,Object actualCompound) {
        producer(a);
        if(a.compound!=null || actualCompound==null)throw defect(a,"Repeated/null final SaveEvent reference");
        changed();a.compound=actualCompound;a.events.add("FINAL_SAVE_EVENT_REFERENCE");
    }
    public void finalNbt(Attempt a,Object actualCompound,byte[] actualCompleteBytes) {
        synchronized(this) {
            producer(a);
            if(a.compound!=actualCompound || actualCompound==null || a.payload!=null || actualCompleteBytes==null || actualCompleteBytes.length<1 || actualCompleteBytes.length>limits.maxDecompressedBytes())
                throw defect(a,"Invalid/repeated final NBT observation");
        }
        // 原 root 在 clone/hash 前预收；失败时原 partial attempt 与收费仍保留。
        chargeOriginal(Math.addExact(256L,Math.multiplyExact(2L,actualCompleteBytes.length)),"actual final source payload/hash BEFORE allocation");
        Payload immutable=new Payload(actualCompleteBytes);
        synchronized(this) {
            producer(a);
            if(a.payload!=null)throw defect(a,"Repeated final immutable payload");
            a.events.add("FINAL_SAVE_EVENT_NBT");
            if(immutable.length()>limits.maxRetainedBytes()-retainedPayloadBytes-reservedOutputBytes-sourceReadLeaseBytes)throw defect(a,"Full NBT retention budget exhausted before queue put; raw lineage retained without new immutable copy");
            changed();a.payload=immutable;retainedPayloadBytes+=immutable.length();
            if(a.primary==null && a.progress!=Progress.FAULTED)a.progress=Progress.FINAL_NBT;
        }
    }
    public synchronized void encoderFalse(Attempt a) { producer(a);a.encoderFalse++; }
    public synchronized void noPut(Attempt a,Object compound) {
        producer(a);exactPayload(a,compound);
        a.events.add("CONTAINS_NO_PUT");if (a.primary==null && a.progress!=Progress.FAULTED) a.progress=Progress.NO_PUT;
    }
    private long sourceReadLeaseBytes;
    /** Exact get/reference/generation observation; external mutators are not fenced. */
    public final class QueuedReadObservation implements AutoCloseable {
        private final Thread actor=Thread.currentThread();private final Object actualCompound;
        private final long generation;private final UUID acceptedAttempt;private final Payload payload;private final Throwable cause;private final Progress progress;
        private boolean closed;
        private QueuedReadObservation(Object compound,Attempt accepted){actualCompound=compound;generation=accepted==null?0:accepted.generation;acceptedAttempt=accepted==null?null:accepted.id;payload=accepted==null?null:accepted.payload;cause=accepted==null?null:accepted.primary;progress=accepted==null?null:accepted.progress;}
        public long acceptedGeneration(){return generation;}
        public UUID acceptedAttempt(){return acceptedAttempt;}
        public Payload acceptedPayload(){return payload;}
        public Throwable originalFailure(){return cause;}
        public Progress observedProgress(){return progress;}
        public boolean matchesActual(Object compound){return actualCompound==compound;}
        @Override public void close(){synchronized(ManagedSourceSaveLedger.this){if(closed||Thread.currentThread()!=actor)throw new IllegalStateException("Exact once source read lease actor required");closed=true;releaseOriginal(128L,"actual queued source read lease dropped");sourceReadLeaseBytes-=128L;}}
    }
    /** Caller owns shared short queue lock around the ORIGINAL map.get and this method. */
    public synchronized QueuedReadObservation observeSourceQueuedGet(Object actualLoader,Slot slot,Object actualCompound){
        if(consumerDetached)throw new IllegalStateException("Original source save consumer detached");
        if(actualLoader!=loader)throw new IllegalArgumentException("Foreign source queue loader");
        if(128L>limits.maxRetainedBytes()-retainedPayloadBytes-reservedOutputBytes-sourceReadLeaseBytes)throw new IllegalStateException("Source queued-read observation retention budget exhausted");
        Attempt accepted=queued.get(slot);
        if(accepted!=null&&accepted.compound!=actualCompound)accepted=null;
        chargeOriginal(128L,"actual queued source read lease BEFORE allocation");sourceReadLeaseBytes+=128L;return new QueuedReadObservation(actualCompound,accepted);
    }
    public synchronized void assertProducerSlot(Attempt a,Slot actualSlot){producer(a);if(!a.slot.equals(actualSlot))throw defect(a,"Actual pending-map key differs from producer slot");}
    public synchronized void queuePutBeginning(Attempt a,Object actualCompound) { producer(a);exactPayload(a,actualCompound);a.events.add("MAP_PUT_CALL_BEGIN_EFFECT_NOT_YET_KNOWN"); }
    public synchronized void queueRemoveBeginning(Object actualLoader,Slot slot) {
        requireIo(actualLoader);Attempt a=queued.get(slot);queueRemoveBeginning(actualLoader,slot,a==null?null:a.compound);
    }
    /** Actual Map.get value is sampled by the original Anvil IO owner under the same queue guard before Map.remove. */
    public synchronized void queueRemoveBeginning(Object actualLoader,Slot slot,Object actualQueuedCompound) {
        requireIo(actualLoader);Attempt a=queued.get(slot);if(a!=null){
            if(actualQueuedCompound==null||a.compound!=actualQueuedCompound)
                throw defect(a,"Actual pending-map value changed before exact queued-row removal");
            if(a.pendingFlightId!=null||a.flightId!=null)throw defect(a,"Repeated actual queued-map removal attempt");
            a.pendingFlightId=UUID.randomUUID();a.events.add("MAP_REMOVE_CALL_BEGIN_EFFECT_NOT_YET_KNOWN");
        }
    }
    /** 原Map.put已发生；previous必须是原返回值，绝不根据latest猜旧payload。 */
    public synchronized void putReturned(Attempt a,Object compound,Object actualPrevious) {
        producer(a);exactPayload(a,compound);
        Attempt previous=queued.get(a.slot);
        changed();a.actualPrevious=actualPrevious;a.generation=++acceptedGeneration;
        queued.put(a.slot,a);a.events.add("MAP_PUT_ACCEPTED");
        if (a.primary==null && a.progress!=Progress.FAULTED) a.progress=Progress.QUEUED;
        if ((previous==null && actualPrevious!=null) || (previous!=null && previous.compound!=actualPrevious))
            throw defect(a,"Map.put previous exact payload differs; actual accepted replacement retained");
        if (previous!=null) { previous.events.add("SUPERSEDED_BY_ACTUAL_MAP_PUT");if(previous.primary==null && previous.progress!=Progress.FAULTED)previous.progress=Progress.SUPERSEDED; }
    }
    public synchronized void endProducer(Attempt a) {
        producer(a);
        if (a.producerEnded) throw defect(a,"Producer ended twice");
        changed();a.producerEnded=true;producers--;a.events.add("PRODUCER_RETURNED");
        if (a.compound==null && a.primary==null) fail(a,new IllegalStateException("Save returned without final SaveEvent lineage"));
    }
    public synchronized Attempt removed(Object actualLoader, Slot slot,Object actualRemoved) {
        requireIo(actualLoader);
        Attempt a=queued.get(slot);
        if (actualRemoved==null) {
            if(a==null)return null;
            IllegalStateException cause=new IllegalStateException("Actual Map.remove returned null while an exact accepted row remained queued; no flight was created");
            fail(a,cause);throw cause;
        }
        if (a==null || a.compound!=actualRemoved) {
            IllegalStateException cause=new IllegalStateException("Actual Map.remove returned an unobserved/replaced payload after the original queue side effect; source responsibility is unresolved");
            sticky(cause);throw cause;
        }
        if(a.pendingFlightId==null) a.pendingFlightId=UUID.randomUUID();
        queued.remove(slot);changed();a.consumer=Thread.currentThread();flights++;a.flightId=a.pendingFlightId;a.pendingFlightId=null;a.events.add("MAP_REMOVE_EXACT_INFLIGHT");
        if (a.primary==null && a.progress!=Progress.FAULTED) a.progress=Progress.INFLIGHT;
        return a;
    }
    public synchronized void requireIo(Object actualLoader) {
        if(consumerDetached)throw new IllegalStateException("Original source save consumer detached");
        if (actualLoader!=loader || Thread.currentThread()==installer) throw new IllegalStateException("Managed writer must be actual off-installer IO owner");
        if (ioThread!=Thread.currentThread()) throw new IllegalStateException("Managed writer thread changed");
    }
    public synchronized void beforeWrite(Attempt a,Object actualCompound,byte[] actualCompleteBytes) {
        consumer(a);exactPayload(a,actualCompound);
        if (!Arrays.equals(a.payload.bytes,actualCompleteBytes)) throw defect(a,"Accepted payload mutated before write; original lineage retained");
    }
    public synchronized void mutationBeginning(Attempt a) {
        consumer(a);
        if (observationSealed) throw defect(a,"Late mutation after observation seal");
        a.events.add("MUTATION_CALL_BEGIN_NO_PERMIT");
        // 仅记录进入真实mutation边界；没有durable undo，因此从不签write permit。
    }
    public synchronized void dataWriteReturned(Attempt a) { consumer(a);a.writes++;a.events.add("DATA_WRITE_CALL_RETURNED"); }
    public synchronized void headerWriteReturned(Attempt a) { consumer(a);a.headers++;a.events.add("HEADER_WRITE_CALL_RETURNED"); }
    public synchronized void regionReturned(Attempt a,int writesAtEntry) {
        consumer(a);a.events.add("REGION_CALL_RETURNED");if(a.writes==writesAtEntry)a.noWrite=true;
    }
    public synchronized int writes(Attempt a) { consumer(a);return a.writes; }
    public synchronized void endFlight(Attempt a) {
        consumer(a);
        if (a.flightEnded) throw defect(a,"Flight ended twice");
        changed();a.flightEnded=true;flights--;a.events.add("FLIGHT_CALL_RETURNED_NO_ACK");
        if (a.primary==null && a.progress!=Progress.FAULTED) a.progress=Progress.CALLS_RETURNED;
    }
    public synchronized void failure(Attempt a,Throwable cause) {
        owned(a);if(!((Thread.currentThread()==a.producer && !a.producerEnded) || (Thread.currentThread()==a.consumer && !a.flightEnded)))throw new IllegalStateException("Exact active observer actor required");Objects.requireNonNull(cause);fail(a,cause);
    }
    public synchronized void reserveOutput(Attempt a) {
        consumer(a);
        if(a.outputReserved || a.outputObservationId!=null || a.pendingOutputObservationId!=null || (capacity!=null && outputReservations>=capacity.maximumRetainedOutputs()) || reservedOutputBytes>limits.maxRetainedBytes()-retainedPayloadBytes-sourceReadLeaseBytes-limits.maxDecompressedBytes())
            throw defect(a,"Output retention reservation exhausted before stream opening");
        chargeOriginal(limits.maxDecompressedBytes(),"actual original Region output reservation BEFORE opening");changed();
        try { a.pendingOutputObservationId=UUID.randomUUID(); }
        catch(RuntimeException|Error cause) { releaseOriginal(limits.maxDecompressedBytes(),"failed actual Region output ID reservation dropped");throw cause; }
        a.outputReserved=true;a.events.add("OUTPUT_RESERVED");outputReservations++;reservedOutputBytes+=limits.maxDecompressedBytes();
    }
    public synchronized void openedOutput(Attempt a,Object actualOutput) {
        consumer(a);Objects.requireNonNull(actualOutput);
        if(!a.outputReserved || a.actualOutput!=null || a.pendingOutputObservationId==null || a.outputObservationId!=null)throw defect(a,"Exact reserved first actual output required");
        a.outputObservationId=a.pendingOutputObservationId;a.pendingOutputObservationId=null;a.actualOutput=actualOutput;
    }
    public synchronized void outputOpeningFailed(Attempt a) { consumer(a);if(a.actualOutput!=null)throw defect(a,"Already opened output remains retained");a.pendingOutputObservationId=null;releaseOutput(a); }
    public synchronized void encoderBytes(Attempt a,long observed) {
        consumer(a);if(observed<0 || observed>limits.maxDecompressedBytes())throw defect(a,"Actual encoder bytes exceed explicit budget");a.encoderBytes=observed;
    }
    public synchronized void outputCloseReturned(Attempt a) { consumer(a);if(a.actualOutput==null)throw defect(a,"No actual opened output close return");a.events.add("OUTPUT_CLOSE_CALL_RETURNED_NO_ACK");releaseOutput(a);a.actualOutput=null; }
    public synchronized void retainAbandonedOutput(Attempt a,Object actualOutput) {
        consumer(a);Objects.requireNonNull(actualOutput);
        if(!a.outputReserved || a.actualOutput!=actualOutput)throw defect(a,"Foreign/unreserved actual output cannot replace retained responsibility");
        if(!a.abandonedOutput){a.abandonedOutput=true;a.events.add("ABANDONED_ENCODER_OR_FAILED_CLOSE_STREAM");}
    }
    public synchronized void bindRegion(Object actualRegion) {
        if(Thread.currentThread()!=ioThread)throw new IllegalStateException("Exact IO binds actual Region");Objects.requireNonNull(actualRegion);
        if(!regions.containsKey(actualRegion)){chargeOriginal(256L,"actual Region save identity BEFORE allocation");changed();regions.put(actualRegion,new RegionState(++regionGeneration));}
    }
    public synchronized UUID regionInstanceId(Object actualRegion) {
        if(Thread.currentThread()!=ioThread)throw new IllegalStateException("Exact IO reads actual Region instance identity");
        return region(actualRegion).id;
    }
    public synchronized void regionWriteBeginning(Object actualRegion,Attempt a) {
        consumer(a);RegionState r=regions.get(actualRegion);
        if(r==null || r.closeBegun)throw defect(a,"Unknown/closed exact Region object cannot write; new object must bind separately");
    }
    public synchronized void largeDisabledReturn(Attempt a) { consumer(a);a.largeDisabled=true;a.events.add("ACTUAL_LARGE_DISABLED_RETURN_NO_WRITE"); }
    private void releaseOutput(Attempt a) { if(!a.outputReserved)throw defect(a,"No actual output reservation");releaseOriginal(limits.maxDecompressedBytes(),"actual closed/unopened Region output dropped");changed();a.outputReserved=false;outputReservations--;reservedOutputBytes-=limits.maxDecompressedBytes(); }
    public synchronized void beginStoppingObservation() {
        if(Thread.currentThread()!=installer)throw new IllegalStateException("Exact installer declares stopping only");changed();closeStarted=true;
    }
    /** 原queue锁内传入实际map/set数量；仅封本地consumer，历史写入仍保持UNRESOLVED。 */
    public synchronized boolean tryDetachStoppedConsumer(Object actualLoader,int actualPending,int actualInflight){
        assertInstallation(actualLoader);
        if(consumerDetached)throw new IllegalStateException("Original source save consumer already detached");
        if(!closeStarted||primary!=null)throw new IllegalStateException("Original source save stop missing or failed",primary);
        if(actualPending<0||actualInflight<0)throw new IllegalArgumentException("Actual nonnegative source queue counts required");
        // IO可能已结束flight但尚未退lexical entry；在该窗口不能判断实际容器或提前解除。
        if(activeAllocationEntries!=0||producers!=0||flights!=0||outputReservations!=0||sourceReadLeaseBytes!=0)return false;
        if(actualPending!=queued.size()||actualInflight!=flights)
            throw new IllegalStateException("Actual source queue differs from original stopped ledger");
        if(!queued.isEmpty())return false;
        consumerDetached=true;
        return true;
    }
    public synchronized void closeBeginning(Object actualRegion) { observerActor();RegionState r=region(actualRegion);changed();r.closeBegun=true; }
    public synchronized void closeReturned(Object actualRegion) { observerActor();region(actualRegion).closeReturned++;closeReturned++; }
    public synchronized void closeFailure(Object actualRegion,Throwable cause) {
        observerActor();RegionState r=region(actualRegion);Objects.requireNonNull(cause);sticky(cause);if(r.primary==null)r.primary=cause;else suppressSecondary(r.primary,cause);
    }
    public synchronized void closeFailure(Throwable cause) { observerActor();sticky(Objects.requireNonNull(cause)); }
    private void observerActor(){if(Thread.currentThread()!=installer && Thread.currentThread()!=ioThread)throw new IllegalStateException("Exact Main or IO observer actor required");}
    private RegionState region(Object actualRegion) { RegionState r=regions.get(actualRegion);if(r==null)throw new IllegalArgumentException("Exact previously bound Region required");return r; }
    /** 只封观察快照，仍UNRESOLVED；由exactinstaller显式宣布producer结束，不能凭queue empty封。 */
    public synchronized Snapshot sealObservation() {
        if(Thread.currentThread()!=installer)throw new IllegalStateException("Exact installer only");
        if(!closeStarted || primary!=null || outputReservations!=0 || sourceReadLeaseBytes!=0 || producers!=0 || flights!=0 || !queued.isEmpty())throw new IllegalStateException("Producer/accepted/flight obligations are not quiescent");
        observationSealed=true;return snapshot();
    }
    public synchronized Snapshot snapshot() {
        List<Observation> rows=new ArrayList<>();
        for(Attempt a:history)rows.add(new Observation(a.id,a.slot,a.generation,a.progress,a.payload,
                a.encoderFalse,a.writes,a.headers,a.noWrite,a.encoderBytes,a.primary,(a.actualOutput==null ? 0 : 1),a.largeDisabled,a.events,Resolution.UNRESOLVED));
        List<RegionObservation> regionRows=new ArrayList<>();for(RegionState r:regions.values())regionRows.add(new RegionObservation(r.id,r.generation,r.closeBegun,r.closeReturned,r.primary,Resolution.UNRESOLVED));
        regionRows.sort(java.util.Comparator.comparingLong(RegionObservation::generation));
        return new Snapshot(boot,limits,rows,producers,flights,closeStarted,closeReturned,primary,observationSealed,outputReservations,reservedOutputBytes,regionRows,retainedPayloadBytes);
    }
    /** Same queue lock caller captures actual maps; this value is observation, never write/absence permission. */
    public static final class GenerationCapture {
        private final ManagedSourceSaveLedger ledger;private final long revision;private final int pending,inflight;
        private GenerationCapture(ManagedSourceSaveLedger ledger,long revision,int pending,int inflight){this.ledger=ledger;this.revision=revision;this.pending=pending;this.inflight=inflight;}
    }
    public synchronized GenerationCapture captureGeneration(Object actualLoader,int actualPending,int actualInflight){
        assertInstallation(actualLoader);
        if(actualPending!=queued.size() || actualInflight!=flights || primary!=null)throw new IllegalStateException("Actual source queue/flight observation differs",primary);
        chargeOriginal(128L,"actual source generation capture BEFORE allocation");
        return new GenerationCapture(this,sourceRevision,actualPending,actualInflight);
    }
    public synchronized void requireGeneration(Object actualLoader,GenerationCapture original,int actualPending,int actualInflight){
        assertInstallation(actualLoader);
        if(original==null || original.ledger!=this || original.revision!=sourceRevision || original.pending!=actualPending ||
                original.inflight!=actualInflight || actualPending!=queued.size() || actualInflight!=flights || primary!=null)
            throw new IllegalStateException("Original source save/queue/flight generation changed",primary);
    }
    private void changed(){sourceRevision=Math.addExact(sourceRevision,1L);}
    private void chargeOriginal(long bytes,String context){
        if(allocation==null)return;
        try{allocation.reserveTemporary(bytes,context);}catch(java.io.IOException cause){retainAllocationFailure(cause);throw new java.io.UncheckedIOException(cause);}
    }
    private void releaseOriginal(long bytes,String context){
        if(allocation==null)return;
        try{allocation.releaseTemporary(bytes,context);}catch(java.io.IOException cause){retainAllocationFailure(cause);throw new java.io.UncheckedIOException(cause);}
    }
    private void producer(Attempt a) { owned(a);if(Thread.currentThread()!=a.producer || a.producerEnded)throw defect(a,"Exact active producer required"); }
    private void consumer(Attempt a) { owned(a);if(Thread.currentThread()!=a.consumer || a.flightEnded)throw defect(a,"Exact active consumer required"); }
    private void owned(Attempt a) { if(a==null || a.ledger!=this)throw new IllegalArgumentException("Foreign observation attempt"); }
    private void exactPayload(Attempt a,Object compound) { if(a.compound!=compound || a.payload==null)throw defect(a,"Exact final payload reference required"); }
    private IllegalStateException defect(Attempt a,String message) { IllegalStateException cause=new IllegalStateException(message);fail(a,cause);return cause; }
    /** FAULTED先于有界诊断记录；primary==null表示该fault cause未被ledger保留，责任仍unknown。 */
    private void fail(Attempt a,Throwable cause) { a.progress=Progress.FAULTED;sticky(cause);if(a.primary==null)a.primary=cause;else suppressSecondary(a.primary,cause); }
    private void sticky(Throwable cause) {
        if(cause==causeOverflow)throw causeOverflow;
        if(causes.contains(cause))return;
        if(capacity!=null && causes.size()>=capacity.maximumCauses()) {
            if(causeOverflow==null) { causeOverflow=new IllegalStateException("Observer Throwable retention budget exhausted; additional faults remain unresolved at their actual caller boundary");if(primary!=null)suppressSecondary(primary,causeOverflow); }
            throw causeOverflow;
        }
        chargeOriginal(128L,"actual save cause retained association BEFORE allocation");changed();causes.add(cause);
        if(primary==null)primary=cause;else suppressSecondary(primary,cause); }
    /** 次级诊断不能反向引用原fault形成cause/suppressed cycle，也不重复附加已有graph。 */
    public static void suppressSecondary(Throwable actualPrimary,Throwable secondary) {
        Objects.requireNonNull(actualPrimary);Objects.requireNonNull(secondary);
        if(actualPrimary!=secondary && !reaches(actualPrimary,secondary) && !reaches(secondary,actualPrimary))actualPrimary.addSuppressed(secondary);
    }
    private static boolean reaches(Throwable root,Throwable target) {
        java.util.Set<Throwable> seen=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        java.util.ArrayDeque<Throwable> pending=new java.util.ArrayDeque<>();pending.push(root);
        while(!pending.isEmpty()) {
            Throwable next=pending.pop();if(next==target)return true;if(!seen.add(next))continue;
            if(next.getCause()!=null)pending.push(next.getCause());for(Throwable child:next.getSuppressed())pending.push(child);
        }
        return false;
    }
}

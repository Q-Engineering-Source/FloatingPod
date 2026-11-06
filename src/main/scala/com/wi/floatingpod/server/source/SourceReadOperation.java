package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.BoundedNbtStructurePreflight;
import com.wi.floatingpod.server.save.ManagedSaveContext;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import net.minecraft.nbt.NBTBase;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;
import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** 实际 read→Main decode 的节点身份观察。没有 complete/placement/native 权利。 */
public final class SourceReadOperation {
    public enum Origin { DISK_READ, QUEUED_COPY, NO_ROOT }
    public enum Lineage { SAME_ACTUAL_NODE, OWNED_COPY_CALL, UNPROVEN }
    public enum DecodeOutcome { NOT_CALLED, UNKNOWN_ENTITY_ID, REGISTERED_FACTORY_NULL, FACTORY_FAILED,
        READ_FAILED, BODY_READ_NOT_SERIALIZED, SERIALIZATION_FAILED, BODY_DECODED_NOT_PLACED, HELPER_FAILED, OUTCOME_UNPROVEN }
    public enum Stage { RAW, FIXED, ACTUAL }
    /** UUID仅为本operation私有actual引用的观察关联号，不是注册/来源能力。 */
    public record RegistryObservation(UUID registryReference,UUID entryReference,String queriedKey,String registeredKey) {}
    public record NodeRelation(Stage from,UUID fromCapture,Stage to,UUID toCapture,Lineage relation) {}
    public record BodyObservation(UUID bodyReference,String actualClass,long actualMainThreadId,int serializationCalls,
            SourceForestCodec.Bytes actualBodyNbt,Throwable serializationFailure) {}
    public record DomainObservation(long observedGeneration,long registryGeneration,SourceDomainCapture.GenerationObservation hooks,
            List<SourceDomainCapture.Unresolved> unresolved) {public DomainObservation{unresolved=List.copyOf(unresolved);}}
    public record Occurrence(UUID captureId,List<Integer> path,SourceForestCodec.Bytes rawSubtree,
            String observedRegistryKey,String observedEntityClass,SourceForestCodec.Classification classification,
            int registryCalls,int factoryCalls,int readCalls,DecodeOutcome outcome,Throwable originalCause,
            boolean partialBodyRetained,RegistryObservation registryObservation,BodyObservation actualBody) {
        public Occurrence {path=List.copyOf(path);}
    }
    public record Tree(UUID captureId,SourceForestCodec.Bytes bytes,List<Occurrence> occurrences) {
        public Tree {occurrences=List.copyOf(occurrences);}
    }
    public record Observation(UUID operation,SourceDomainIdentity domain,int x,int z,long readGeneration,
            Origin origin,long acceptedSaveGeneration,Lineage fixerLineage,Tree raw,Tree fixed,Tree actualDecodeInput,
            Throwable primary,List<String> unresolved,List<NodeRelation> nodeRelations,DomainObservation sourceDomain,SourceLimitProfile exactProfile) {
        public Observation {unresolved=List.copyOf(unresolved);nodeRelations=List.copyOf(nodeRelations);}
    }
    private static final ThreadLocal<PhaseScope> PHASE=new ThreadLocal<>();
    private static final ThreadLocal<DecoderScope> DECODER=new ThreadLocal<>();
    private static final ThreadLocal<DecoderScope> INNER=new ThreadLocal<>();
    private static final ThreadLocal<DecoderScope> LOOKUP=new ThreadLocal<>();
    private final SourceLoadManifest holder;
    private final UUID operation=UUID.randomUUID();
    private final SourceLimitProfile limits;
    private final Object world;
    private final int x,z;
    private final long generation;
    private final Thread reader;
    private NBTTagCompound rawRoot,fixedRoot,returnedRoot;
    private Object returnedLevel;
    private final java.util.Set<NBTTagCompound> ambiguousNodes=java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private final IdentityHashMap<NBTTagCompound,Node> rawNodes=new IdentityHashMap<>(),fixedNodes=new IdentityHashMap<>(),actualNodes=new IdentityHashMap<>();
    private Tree rawTree,fixedTree,actualTree;
    private Origin origin=Origin.NO_ROOT;
    private long acceptedGeneration;
    private Lineage fixerLineage=Lineage.UNPROVEN;
    private final List<String> unresolved=new ArrayList<>();
    private final IdentityHashMap<Object,UUID> observedReferences=new IdentityHashMap<>();
    private final IdentityHashMap<NBTTagCompound,NBTTagCompound> copiedNodes=new IdentityHashMap<>();
    private static final ThreadLocal<CopyScope> COPY=new ThreadLocal<>();
    private Throwable primary;
    private boolean rawEnded,decoded;
    private DataInputStream actualStream;
    private boolean streamCloseAttempted;
    private long retained;
    private static final class Node {
        final UUID capture=UUID.randomUUID();final NBTTagCompound actual;final List<Integer> path;final SourceForestCodec.Bytes bytes;
        String key="",entityClass="";Object registry,entry,body,serializationTarget,serializationReturned;RegistryObservation registryObservation;BodyObservation actualBody;SourceForestCodec.Classification classification=SourceForestCodec.Classification.UNKNOWN;
        int serializationCalls;
        int registryCalls,factoryCalls,readCalls;DecodeOutcome outcome=DecodeOutcome.NOT_CALLED;Throwable cause;
        Node(NBTTagCompound actual,List<Integer> path,byte[] bytes){this.actual=actual;this.path=List.copyOf(path);this.bytes=new SourceForestCodec.Bytes(bytes);}
        Occurrence observation(){return new Occurrence(capture,path,bytes,key,entityClass,classification,registryCalls,factoryCalls,readCalls,outcome,cause,body!=null&&outcome!=DecodeOutcome.BODY_DECODED_NOT_PLACED,registryObservation,actualBody);}
    }
    SourceReadOperation(SourceLoadManifest holder,Object world,int x,int z,long generation){
        this.holder=Objects.requireNonNull(holder);this.world=Objects.requireNonNull(world);this.x=x;this.z=z;this.generation=generation;
        limits=holder.limits();reader=Thread.currentThread();
    }
    boolean mainAllocationActor(){return holder.mainAllocationActor();}
    public void requireAllocationOwner(){
        if(holder.mainAllocationActor()&&!decoded)return;
        if(Thread.currentThread()!=reader||rawEnded)throw new IllegalStateException("Exact active source allocation reader required");
        holder.requireReadEntry();
    }
    public void requireQueuedAllocationOwner(Object actualLoader){
        if(currentRead()!=this)throw new IllegalStateException("Exact original queued read phase required");holder.requireReadLoader(actualLoader);requireAllocationOwner();
    }
    public static SourceReadOperation currentRead(){PhaseScope s=PHASE.get();return s!=null&&!s.main?s.operation:null;}
    public static SourceReadOperation currentMain(){PhaseScope s=PHASE.get();return s!=null&&s.main?s.operation:null;}
    public SourceLimitProfile limits(){return limits;}
    Object exactWorld(){return world;}
    Object exactReturnedRoot(){return returnedRoot;}
    Object exactReturnedLevel(){return returnedLevel;}
    int x(){return x;}int z(){return z;}
    public PhaseScope enterRead(){if(Thread.currentThread()!=reader||rawEnded)throw new IllegalStateException("Exact active source read actor required");return new PhaseScope(this,false);}
    public PhaseScope enterMain(Object actualWorld,Object actualLevel)throws IOException{
        holder.requireCurrentDomain();if(!rawEnded||decoded||actualWorld!=world||returnedRoot==null||returnedRoot.getTag("Level")!=actualLevel)throw new IllegalStateException("Exact retained async root/level/world required");
        // checkedRead may relocate/change nodes after fixer; retain ACTUAL input, never infer replacement lineage.
        if(returnedRoot!=fixedRoot)unproven("CHECKED_READ_REPLACED_ROOT");
        actualTree=capture(returnedRoot,actualNodes);return new PhaseScope(this,true);
    }
    public void openedDiskStream(DataInputStream stream){readActor();if(actualStream!=null)throw new IllegalStateException("Repeated disk stream");actualStream=stream;origin=Origin.DISK_READ;}
    private byte[] originalDiskBytes;
    /** 原 Region stream一次有界读到EOF，先真实shared preflight，再调用原NBT reader一次。 */
    public DataInputStream ownedDiskInput(DataInputStream exact)throws IOException{
        readActor();if(exact!=actualStream||originalDiskBytes!=null)throw new IllegalStateException("Foreign/repeated actual read stream");
        ByteArrayOutputStream output=new ByteArrayOutputStream();byte[] buffer=new byte[4096];
        long count=0;int n;while((n=exact.read(buffer))!=-1){if(n==0)throw new IOException("Actual source input made no progress");count=Math.addExact(count,n);if(count>limits.maxDecompressedBytes())throw new IOException("Source RAW stream byte budget exhausted");output.write(buffer,0,n);}
        originalDiskBytes=output.toByteArray();reserve(Math.multiplyExact(originalDiskBytes.length,2L));
        BoundedNbtStructurePreflight.extract(originalDiskBytes,limits);
        return new DataInputStream(new java.io.ByteArrayInputStream(originalDiskBytes));
    }
    public void diskRootReturned(NBTTagCompound root)throws IOException{
        readActor();if(originalDiskBytes==null)throw new IllegalStateException("Missing actual disk input lineage");
        rawRoot=Objects.requireNonNull(root);BoundedNbtStructurePreflight.Node originalTree=BoundedNbtStructurePreflight.extract(originalDiskBytes,limits);
        rawTree=capture(root,rawNodes);List<Occurrence> exactRaw=new ArrayList<>();
        for(Occurrence occurrence:rawTree.occurrences()){byte[] payload=rawAt(originalTree,occurrence.path()).payloadBytes();byte[] standalone=new byte[payload.length+3];standalone[0]=10;System.arraycopy(payload,0,standalone,3,payload.length);reserve(Math.multiplyExact(standalone.length,2L));exactRaw.add(new Occurrence(occurrence.captureId(),occurrence.path(),new SourceForestCodec.Bytes(standalone),occurrence.observedRegistryKey(),occurrence.observedEntityClass(),occurrence.classification(),0,0,0,occurrence.outcome(),null,false,null,null));}
        rawTree=new Tree(rawTree.captureId(),new SourceForestCodec.Bytes(originalDiskBytes),exactRaw);
        unproven("DISK_NATIVE_IDENTITY_UNOBSERVED");
    }
    private static BoundedNbtStructurePreflight.Node rawAt(BoundedNbtStructurePreflight.Node root,List<Integer> path){
        BoundedNbtStructurePreflight.Node level=root.child("Level"),entities=level.child("Entities");
        BoundedNbtStructurePreflight.Node node=entities.children().get(path.getFirst());for(int i=1;i<path.size();i++)node=node.child("Passengers").children().get(path.get(i));return node;
    }
    public NBTTagCompound queuedRoot(Object actual,NBTTagCompound compound,long acceptedSaveGeneration)throws IOException{
        readActor();if(actual!=compound)throw new IllegalArgumentException("Foreign queued read reference");origin=Origin.QUEUED_COPY;acceptedGeneration=acceptedSaveGeneration;
        rawRoot=compound;rawTree=capture(compound,rawNodes);NBTTagCompound copy;
        try(CopyScope scope=new CopyScope(this)){copy=compound.copy();}
        IdentityHashMap<NBTTagCompound,Node> copied=new IdentityHashMap<>();Tree copiedTree=capture(copy,copied);
        // Actual copy call establishes a candidate mapping; no samehash/readlease can exclude external mutable references/ABA.
        fixedRoot=copy;fixedTree=copiedTree;fixedNodes.putAll(copied);fixerLineage=Lineage.OWNED_COPY_CALL;
        for(Node source:rawNodes.values())if(!fixedNodes.containsKey(copiedNodes.get(source.actual)))unproven("QUEUED_NODE_COPY_MAPPING_UNOBSERVED");
        unproven("QUEUED_MUTATOR_EXCLUSIVITY_UNPROVEN");if(acceptedSaveGeneration==0)unproven("QUEUED_ACCEPTED_GENERATION_UNOBSERVED");
        return copy;
    }
    public void fixerBeginning(NBTTagCompound actual){readActor();if(actual!=rawRoot)throw new IllegalStateException("Fixer input not actual raw read root");}
    public void acceptedQueuedFaultWithoutCause(){readActor();unproven("QUEUED_ACCEPTED_FAULT_WITH_UNRETAINED_CAUSE");}
    public void fixerReturned(NBTTagCompound input,NBTTagCompound output)throws IOException{
        readActor();fixedRoot=Objects.requireNonNull(output);fixedTree=capture(output,fixedNodes);
        boolean same=input==rawRoot&&output==input&&rawNodes.size()==fixedNodes.size();
        for(Node node:fixedNodes.values())if(!rawNodes.containsKey(node.actual))same=false;
        fixerLineage=same?Lineage.SAME_ACTUAL_NODE:Lineage.UNPROVEN;
        if(!same)unproven("FIXER_NODE_TRANSFORMATION_MAPPING_UNPROVEN");
    }
    public void returnedAsync(Object[] actual)throws IOException{
        readActor();if(actual==null){origin=Origin.NO_ROOT;unproven("NO_ROOT_RETURN_IS_NOT_ABSENCE");return;}
        if(actual.length!=2||!(actual[1] instanceof NBTTagCompound root))throw new IllegalStateException("Actual async result root missing");
        returnedRoot=root;returnedLevel=root.getTag("Level");if(fixedTree==null){fixedRoot=root;fixedTree=capture(root,fixedNodes);unproven("FIXER_OR_QUEUE_LINEAGE_UNOBSERVED");}
    }
    public void actualStreamClosing(DataInputStream stream){readActor();if(stream!=actualStream||streamCloseAttempted)throw new IllegalStateException("Foreign/repeated source input close");streamCloseAttempted=true;}
    public void readEnded(Throwable cause){
        if(Thread.currentThread()!=reader||rawEnded)throw new IllegalStateException("Exact read end required");
        if(cause!=null){recordFailure(cause);if(actualStream!=null&&!streamCloseAttempted){streamCloseAttempted=true;try{actualStream.close();}catch(Throwable cleanup){ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}}}
        rawEnded=true;
    }
    public void mainEnded(Throwable cause){holder.assertMain();if(decoded)throw new IllegalStateException("Source decode ended twice");if(cause!=null)recordFailure(cause);decoded=true;}
    public void recordFailure(Throwable original){Objects.requireNonNull(original);if(primary==null)primary=original;else ManagedSourceSaveLedger.suppressSecondary(primary,original);}
    private void unproven(String code){if(!unresolved.contains(code)){reserve(128);unresolved.add(code);}}
    private void reserve(long bytes){long next=Math.addExact(retained,bytes);if(bytes<0||next>holder.operationBudget())throw new IllegalStateException("Source operation retained budget exhausted");holder.reserveAdditional(this,bytes);retained=next;}
    private void readActor(){if(Thread.currentThread()!=reader||rawEnded||currentRead()!=this)throw new IllegalStateException("Exact scoped read required");}
    /** 仅实际NBTTagCompound.copy返回hook可记录edge；外部未激活时不补造映射。 */
    public static void actualCompoundCopyReturned(NBTTagCompound input,NBTTagCompound output){
        CopyScope scope=COPY.get();if(scope==null)return;SourceReadOperation operation=scope.operation;operation.readActor();
        if(input==null||output==null||input==output||operation.copiedNodes.containsKey(input)){operation.unproven("COMPOUND_COPY_IDENTITY_AMBIGUOUS");return;}
        operation.reserve(128);operation.copiedNodes.put(input,output);
    }
    private static final class CopyScope implements AutoCloseable {
        final SourceReadOperation operation;final CopyScope previous;final Thread owner=Thread.currentThread();
        CopyScope(SourceReadOperation operation){this.operation=operation;previous=COPY.get();COPY.set(this);}
        public void close(){if(Thread.currentThread()!=owner||COPY.get()!=this)throw new IllegalStateException("Exact copy scope required");if(previous==null)COPY.remove();else COPY.set(previous);}
    }
    private Tree capture(NBTTagCompound root,IdentityHashMap<NBTTagCompound,Node> target)throws IOException{
        byte[] complete=ManagedSaveContext.encode(root,limits);reserve(Math.multiplyExact(complete.length,2L));
        BoundedNbtStructurePreflight.extract(complete,limits);List<Occurrence> nodes=new ArrayList<>();
        NBTBase level=root.getTag("Level");if(!(level instanceof NBTTagCompound compound))throw new IOException("Actual chunk Level missing");NBTBase entities=compound.getTag("Entities");
        if(entities==null)return new Tree(UUID.randomUUID(),new SourceForestCodec.Bytes(complete),nodes);
        if(!(entities instanceof NBTTagList list)||list.tagCount()>0&&list.getTagType()!=10)throw new IOException("Actual Entities must be compound list");
        for(int i=0;i<list.tagCount();i++)captureNode(list.getCompoundTagAt(i),List.of(i),target,nodes);
        return new Tree(UUID.randomUUID(),new SourceForestCodec.Bytes(complete),nodes);
    }
    private void captureNode(NBTTagCompound node,List<Integer> path,IdentityHashMap<NBTTagCompound,Node> target,List<Occurrence> observations)throws IOException{
        if(path.size()>limits.maxNbtDepth()||target.size()>=limits.maxMembers())throw new IOException("Source occurrence/depth budget exhausted");
        byte[] bytes=ManagedSaveContext.encode(node,limits);reserve(Math.addExact(512L,Math.multiplyExact(bytes.length,2L)));
        Node observation=new Node(node,path,bytes);if(target.put(node,observation)!=null){ambiguousNodes.add(node);unproven("ALIASED_NODE_OCCURRENCE_UNPROVEN");}observations.add(observation.observation());
        NBTBase passengers=node.getTag("Passengers");if(passengers==null)return;
        if(!(passengers instanceof NBTTagList list)||list.tagCount()>0&&list.getTagType()!=10)throw new IOException("Actual Passengers must be compound list");
        for(int i=0;i<list.tagCount();i++){List<Integer> next=new ArrayList<>(path);next.add(i);captureNode(list.getCompoundTagAt(i),next,target,observations);}
    }
    public DecoderScope enterDecoder(NBTTagCompound node,Object actualWorld){
        holder.assertMain();if(currentMain()!=this||actualWorld!=world)throw new IllegalStateException("Exact Main source phase/world required");
        Node record=ambiguousNodes.contains(node)?null:actualNodes.get(node);
        if(record!=null&&!fixedNodes.containsKey(node))unproven("ACTUAL_NODE_FIXED_LINEAGE_UNPROVEN");if(record==null){unproven("ACTUAL_DECODER_NODE_NOT_FIXED_IDENTITY");return new DecoderScope(this,null,node);}
        return new DecoderScope(this,record,node);
    }
    public static DecoderScope decoder(){return DECODER.get();}
    public static DecoderScope inner(){return INNER.get();}
    public static DecoderScope lookup(){return LOOKUP.get();}
    public static Mask enterEntityList(Object node,Object world){DecoderScope scope=DECODER.get();boolean valid=scope!=null&&scope.record!=null&&!scope.entityListEntered&&scope.exact(node,world);if(valid)scope.entityListEntered=true;return new Mask(INNER,valid?scope:null);}
    public static Mask enterLookup(String key,Object world){DecoderScope scope=INNER.get();
        boolean valid=scope!=null&&scope.record!=null&&!scope.lookupEntered&&scope.operation.world==world&&scope.node.getString("id").equals(key);
        if(valid)scope.lookupEntered=true;return new Mask(LOOKUP,valid?scope:null);
    }
    public static final class Mask implements AutoCloseable {
        final Thread owner=Thread.currentThread();final ThreadLocal<DecoderScope> slot;final DecoderScope previous;boolean closed;
        private Mask(ThreadLocal<DecoderScope> slot,DecoderScope next){this.slot=slot;previous=slot.get();slot.set(next);}
        public void close(){if(closed||Thread.currentThread()!=owner)throw new IllegalStateException("Exact once decoder mask owner");closed=true;if(previous==null)slot.remove();else slot.set(previous);}
    }
    public static final class PhaseScope implements AutoCloseable {
        final SourceReadOperation operation;final boolean main;final Thread owner=Thread.currentThread();final PhaseScope previous;boolean closed;
        private PhaseScope(SourceReadOperation operation,boolean main){this.operation=operation;this.main=main;previous=PHASE.get();PHASE.set(this);}
        public void close(){if(closed||Thread.currentThread()!=owner||PHASE.get()!=this)throw new IllegalStateException("Exact once source phase owner");closed=true;if(previous==null)PHASE.remove();else PHASE.set(previous);}
    }
    public static final class DecoderScope implements AutoCloseable {
        final SourceReadOperation operation;final Node record;final NBTTagCompound node;final DecoderScope previous;final Thread owner=Thread.currentThread();boolean closed,lookupEntered,entityListEntered;
        private DecoderScope(SourceReadOperation operation,Node record,NBTTagCompound node){this.operation=operation;this.record=record;this.node=node;previous=DECODER.get();DECODER.set(this);}
        public boolean exact(Object actual,Object world){return !closed&&Thread.currentThread()==owner&&node==actual&&operation.world==world;}
        private void check(){if(closed||Thread.currentThread()!=owner||DECODER.get()!=this)throw new IllegalStateException("Exact active decoder scope required");}
        public void registry(Object actualRegistry,String key,Object actualEntry,String registeredKey,String entityClass,SourceForestCodec.Classification classification){
            registryLookupReturned(actualRegistry,key,actualEntry);registryClassified(registeredKey,entityClass,classification);
        }
        public void registryLookupReturned(Object actualRegistry,String key,Object actualEntry){
            check();if(record==null)return;if(record.registryCalls!=0)throw new IllegalStateException("Repeated source registry lookup");record.registry=actualRegistry;record.entry=actualEntry;record.key=key;record.registryCalls=1;
            record.registryObservation=new RegistryObservation(operation.reference(actualRegistry),actualEntry==null?null:operation.reference(actualEntry),key,"");
            if(actualEntry==null)record.outcome=DecodeOutcome.UNKNOWN_ENTITY_ID;
        }
        public void registryClassified(String registeredKey,String entityClass,SourceForestCodec.Classification classification){
            check();if(record==null)return;if(record.registryCalls!=1)throw new IllegalStateException("Actual lookup required before classification");record.entityClass=entityClass;record.classification=classification;
            record.registryObservation=new RegistryObservation(operation.reference(record.registry),record.entry==null?null:operation.reference(record.entry),record.key,registeredKey);
            if(record.entry!=null&&!record.key.equals(registeredKey))operation.unproven("ACTUAL_REGISTRY_KEY_MISMATCH");
        }
        public void factoryBeginning(Object entry,Object world){check();if(record==null)return;if(entry!=record.entry||world!=operation.world||record.factoryCalls++!=0)throw new IllegalStateException("Foreign/repeated actual registered factory");}
        public void factoryReturned(Object body){check();if(record==null)return;record.body=body;if(body==null)record.outcome=DecodeOutcome.REGISTERED_FACTORY_NULL;}
        public void factoryFailed(Throwable cause){fail(DecodeOutcome.FACTORY_FAILED,cause);}
        public void readBeginning(Object body,Object nbt){check();if(record==null)return;if(body!=record.body||nbt!=node||record.readCalls++!=0)throw new IllegalStateException("Foreign/repeated virtual read");}
        public void readReturned(){check();if(record!=null)record.outcome=DecodeOutcome.BODY_READ_NOT_SERIALIZED;}
        /** 真实read返回后、Chunk.addEntity之前，原body序列化一次。新target失败时强引用仍在record。 */
        public void serializationBeginning(Object body,NBTTagCompound target){
            check();if(record==null)return;if(body!=record.body||record.outcome!=DecodeOutcome.BODY_READ_NOT_SERIALIZED||record.serializationCalls!=0)throw new IllegalStateException("Exact successfully read body serialization required");
            record.serializationTarget=Objects.requireNonNull(target);record.serializationCalls=1;operation.reserve(512);
            record.actualBody=new BodyObservation(operation.reference(body),body.getClass().getName(),Thread.currentThread().threadId(),1,null,null);
        }
        public void serializationReturned(Object body,NBTTagCompound actualReturned)throws IOException{
            check();if(record==null)return;if(body!=record.body||record.serializationCalls!=1||actualReturned==null)throw new IllegalStateException("Exact actual serialization result required");
            record.serializationReturned=actualReturned;if(actualReturned!=record.serializationTarget)operation.unproven("BODY_SERIALIZATION_RETURNED_OTHER_TAG");byte[] bytes=ManagedSaveContext.encode(actualReturned,operation.limits);BoundedNbtStructurePreflight.extract(bytes,operation.limits);operation.reserve(Math.multiplyExact(bytes.length,2L));
            record.actualBody=new BodyObservation(operation.reference(body),body.getClass().getName(),Thread.currentThread().threadId(),1,new SourceForestCodec.Bytes(bytes),null);record.outcome=DecodeOutcome.BODY_DECODED_NOT_PLACED;
        }
        public void serializationFailed(Throwable cause){fail(DecodeOutcome.SERIALIZATION_FAILED,cause);if(record!=null){BodyObservation prior=record.actualBody;record.actualBody=new BodyObservation(prior==null?null:prior.bodyReference(),record.body==null?"":record.body.getClass().getName(),Thread.currentThread().threadId(),record.serializationCalls,null,cause);}}
        public void readFailed(Throwable cause){fail(DecodeOutcome.READ_FAILED,cause);}
        public void helperFailed(Throwable cause){fail(DecodeOutcome.HELPER_FAILED,cause);}
        private void fail(DecodeOutcome outcome,Throwable cause){check();operation.recordFailure(cause);if(record!=null){if(record.cause==null){record.cause=cause;record.outcome=outcome;}else ManagedSourceSaveLedger.suppressSecondary(record.cause,cause);}}
        public Throwable primary(){return record==null?operation.primary:record.cause;}
        public void helperReturned(Object result){check();if(record==null)return;if(record.cause!=null&&result==null)return;
            if(result!=record.body||result==null&&record.outcome==DecodeOutcome.BODY_DECODED_NOT_PLACED||result!=null&&record.outcome!=DecodeOutcome.BODY_DECODED_NOT_PLACED){
                if(record.cause==null)record.outcome=DecodeOutcome.OUTCOME_UNPROVEN;operation.unproven("HELPER_RETURN_INNER_OUTCOME_MISMATCH");}
        }
        public void close(){check();closed=true;if(previous==null)DECODER.remove();else DECODER.set(previous);}
    }
    Observation observation(){
        List<Occurrence> observed=fixedNodes.values().stream().sorted((a,b)->comparePath(a.path,b.path)).map(Node::observation).toList();
        Tree fixed=fixedTree==null?null:new Tree(fixedTree.captureId(),fixedTree.bytes(),observed);
        Tree actual=actualTree==null?null:new Tree(actualTree.captureId(),actualTree.bytes(),actualNodes.values().stream().sorted((a,b)->comparePath(a.path,b.path)).map(Node::observation).toList());
        List<NodeRelation> relations=new ArrayList<>();
        for(Node raw:rawNodes.values()){Node next=fixedNodes.get(raw.actual);Lineage relation=Lineage.SAME_ACTUAL_NODE;
            if(next==null){next=fixedNodes.get(copiedNodes.get(raw.actual));relation=Lineage.OWNED_COPY_CALL;}
            if(next!=null&&!ambiguousNodes.contains(raw.actual)&&!ambiguousNodes.contains(next.actual))relations.add(new NodeRelation(Stage.RAW,raw.capture,Stage.FIXED,next.capture,relation));}
        for(Node node:fixedNodes.values()){Node next=actualNodes.get(node.actual);if(next!=null&&!ambiguousNodes.contains(node.actual))relations.add(new NodeRelation(Stage.FIXED,node.capture,Stage.ACTUAL,next.capture,Lineage.SAME_ACTUAL_NODE));}
        return new Observation(operation,holder.identity(),x,z,generation,origin,acceptedGeneration,fixerLineage,rawTree,fixed,actual,primary,unresolved,relations,holder.domainProjection(),limits);
    }
    private UUID reference(Object actual){Objects.requireNonNull(actual);UUID id=observedReferences.get(actual);if(id==null){reserve(128);id=UUID.randomUUID();observedReferences.put(actual,id);}return id;}
    private static int comparePath(List<Integer> a,List<Integer> b){for(int i=0;i<Math.min(a.size(),b.size());i++){int c=Integer.compare(a.get(i),b.get(i));if(c!=0)return c;}return Integer.compare(a.size(),b.size());}
}

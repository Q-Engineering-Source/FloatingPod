package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget.Scope;
import com.wi.floatingpod.server.BoundedNbtStructurePreflight;
import com.wi.floatingpod.server.BoundedNbtStructurePreflight.Node;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

/** 完整森林的纯数据协议。验证结构不发行来源、完整coverage、seal或absence能力。 */
public final class SourceForestCodec {
    public static final String DOMAIN = "FP_SOURCE_FOREST_BE_V2";
    private SourceForestCodec() { }
    public enum StageKind { RAW, FIXED, ACTUAL }
    public enum Classification { FLOATING_POD, OTHER, UNKNOWN }
    public enum UuidRepresentation { ABSENT, EXACT_LONG_PAIR, MALFORMED }
    public enum PoseRepresentation { ABSENT, EXACT_LIST_BITS, MALFORMED }
    public enum Observation { NOT_OBSERVED, RETURNED, FAILED }
    public enum Disposition { INDEXED, ORDINARILY_UNLOADED, LEGITIMATELY_REMOVED, PENDING, FAULTED, UNKNOWN }
    public enum Coverage { SUPPLIED_ROOTS_ONLY, DECLARED_FOREST_DATA }
    public enum CarrierDisposition { CURRENT, TARGET_STAGED, OLD_RETAINED, ORDINARILY_UNLOADED, LEGITIMATELY_RETIRED, PENDING, UNKNOWN }
    public enum LineageKind { RAW_TO_FIXED, FIXED_TO_ACTUAL }

    /** 不把数组、NBT对象或游戏对象留给调用者。 */
    public static final class Bytes {
        private final byte[] value;
        public Bytes(byte[] value) { this.value = Objects.requireNonNull(value).clone(); }
        public byte[] copy() { return value.clone(); }
        public int length() { return value.length; }
        public Bytes sha256() { return new Bytes(hash(value)); }
        @Override public boolean equals(Object other) { return other instanceof Bytes b && Arrays.equals(value,b.value); }
        @Override public int hashCode() { return Arrays.hashCode(value); }
    }
    public record Identity(UUID bindingId, UUID unitId, UUID forestId, long sourceEpoch,
                           UUID createdOperationId, String worldIdentity, String saveIdentity,
                           String loaderProfile, UUID catalogGeneration, long readGeneration) {
        public Identity {
            Objects.requireNonNull(bindingId); Objects.requireNonNull(unitId); Objects.requireNonNull(forestId);
            Objects.requireNonNull(createdOperationId); Objects.requireNonNull(catalogGeneration);
            requireText(worldIdentity); requireText(saveIdentity); requireText(loaderProfile);
            require(sourceEpoch > 0 && readGeneration >= 0,"Invalid source epoch/read generation");
        }
    }
    /** FileId字段只是被编码的观察数据；不等于原descriptor/native custody证明。 */
    public record Carrier(UUID id, String regionKey, int regionX, int regionZ, int dimension, int chunkX, int chunkZ,
                          int slot, long volumeSerialBits, Bytes fileId128,
                          long saveGeneration, long originalRegionLength, int compressionType, Bytes originalCompressedChunk,
                          int sectorOffset, int sectorCount, int timestampBits, CarrierDisposition disposition, UUID dispositionObservation, List<CarrierStage> stages) {
        public Carrier { Objects.requireNonNull(id); requireText(regionKey); Objects.requireNonNull(fileId128);
            require(fileId128.length()==16 && slot>=0 && slot<1024 && saveGeneration>=0,"Invalid carrier data");
            require(chunkX==Math.addExact(Math.multiplyExact(regionX,32),slot%32) && chunkZ==Math.addExact(Math.multiplyExact(regionZ,32),slot/32),"Exact region/slot/chunk coordinate mismatch");
            Objects.requireNonNull(originalCompressedChunk);
            require(originalRegionLength>=8192 && compressionType>=1 && compressionType<=2 && sectorOffset>=2 && sectorCount>0 && sectorCount<=255,"Invalid original region/slot data");
            Objects.requireNonNull(disposition);
            require(dispositionObservation!=null||disposition==CarrierDisposition.PENDING||disposition==CarrierDisposition.UNKNOWN,"Carrier disposition observation required");
            stages=List.copyOf(stages); }
    }
    /** 整个chunk NBT，包括非FP、tile、Caps和未识别字段。 */
    public record BackgroundOccurrence(UUID captureId, List<Integer> entityPath, Classification classification, Bytes entityNbt) {
        public BackgroundOccurrence {Objects.requireNonNull(captureId);Objects.requireNonNull(classification);Objects.requireNonNull(entityNbt);entityPath=List.copyOf(entityPath);
            require(!entityPath.isEmpty()&&entityPath.stream().allMatch(i->i!=null&&i>=0),"Invalid background occurrence path");}
    }
    public record CarrierStage(StageKind kind, UUID captureId, Bytes chunkNbt, List<BackgroundOccurrence> background) {
        public CarrierStage { Objects.requireNonNull(kind); Objects.requireNonNull(captureId); Objects.requireNonNull(chunkNbt);background=List.copyOf(background); }
        public CarrierStage(StageKind kind,UUID captureId,Bytes chunkNbt){this(kind,captureId,chunkNbt,List.of());}
    }
    /** 每个阶段保留路径、原始完整子树和观察次数。UNKNOWN可保责任，但不能派index。 */
    public record Stage(StageKind kind, UUID captureId, UUID carrierId, UUID parentMemberId,
                        List<Integer> entityPath, Classification classification, Bytes entityNbt,
                        Observation factory, int factoryCalls, Observation read, int readCalls,
                        Observation caps, int capsCalls, Observation placement, int placementCalls,
                        Observation index, int indexCalls, String failureCode, UUID failureCorrelation) {
        public Stage {
            Objects.requireNonNull(kind); Objects.requireNonNull(captureId); Objects.requireNonNull(carrierId);
            entityPath=List.copyOf(entityPath); Objects.requireNonNull(classification); Objects.requireNonNull(entityNbt);
            Objects.requireNonNull(factory); Objects.requireNonNull(read); Objects.requireNonNull(caps);
            Objects.requireNonNull(placement); Objects.requireNonNull(index); Objects.requireNonNull(failureCode);
            require(!entityPath.isEmpty() && entityPath.stream().allMatch(i->i!=null && i>=0),"Invalid entity path");
            require(factoryCalls>=0 && readCalls>=0 && capsCalls>=0 && placementCalls>=0 && indexCalls>=0,"Negative call count");
            require(failureCode.isEmpty()==(failureCorrelation==null),"Failure code/correlation pair required");
            for(var pair:List.of(Map.entry(factory,factoryCalls),Map.entry(read,readCalls),Map.entry(caps,capsCalls),Map.entry(placement,placementCalls),Map.entry(index,indexCalls)))
                require(pair.getKey()==Observation.NOT_OBSERVED?pair.getValue()==0:pair.getValue()>0,"Observation/call count contradiction");
            if(factory==Observation.FAILED||read==Observation.FAILED||caps==Observation.FAILED||placement==Observation.FAILED||index==Observation.FAILED)require(!failureCode.isEmpty(),"Failed stage requires diagnostic correlation");
        }
    }
    public record Lineage(LineageKind kind, UUID fromCaptureId, UUID toCaptureId, UUID observationId) {
        public Lineage { Objects.requireNonNull(kind); Objects.requireNonNull(fromCaptureId);
            Objects.requireNonNull(toCaptureId); Objects.requireNonNull(observationId); }
    }
    public record Member(UUID id, Disposition disposition, UUID dispositionObservation,
                         Observation dispositionAction, int dispositionCalls, UUID dispositionCaptureId, List<Stage> stages, List<Lineage> lineage) {
        public Member { Objects.requireNonNull(id); Objects.requireNonNull(disposition);Objects.requireNonNull(dispositionAction);
            require(dispositionCalls>=0 && (dispositionAction==Observation.NOT_OBSERVED?dispositionCalls==0:dispositionCalls>0),"Disposition action/count contradiction");
            require(dispositionAction==Observation.NOT_OBSERVED?dispositionCaptureId==null:dispositionCaptureId!=null,"Disposition action capture mismatch");
            stages=List.copyOf(stages); lineage=List.copyOf(lineage);
            require(dispositionObservation!=null || disposition==Disposition.PENDING || disposition==Disposition.FAULTED || disposition==Disposition.UNKNOWN,
                    "Settled disposition requires observation identity"); }
    }
    /** public DATA不能证明调用者声明来自真实registry/native/factory。 */
    public record Forest(Identity identity, Coverage coverage, Bytes sourceObservation,
                         List<UUID> rawRoots, List<UUID> fixedRoots, List<UUID> actualRoots, List<Carrier> carriers, List<Member> members) {
        public Forest { Objects.requireNonNull(identity); Objects.requireNonNull(coverage); Objects.requireNonNull(sourceObservation);
            rawRoots=List.copyOf(rawRoots);fixedRoots=List.copyOf(fixedRoots);actualRoots=List.copyOf(actualRoots); carriers=List.copyOf(carriers); members=List.copyOf(members); }
    }
    public record Encoded(Forest data, Bytes bytes, Bytes sha256, long memberCount, long rootCount,
                          long carrierCount, long retainedBytes) {
        public Encoded {Objects.requireNonNull(data);Objects.requireNonNull(bytes);Objects.requireNonNull(sha256);
            require(sha256.length()==32&&memberCount==data.members.size()&&rootCount==data.actualRoots.size()&&carrierCount==data.carriers.size()&&retainedBytes==bytes.length(),"Encoded DATA scalar mismatch");}
    }
    /** 原NBT派生的值，double/float保持原bits，不做NaN规范化。 */
    public record EntityFields(String registryType, UUID uuid, UuidRepresentation uuidRepresentation, PoseRepresentation poseRepresentation, List<Long> positionBits,
                               List<Integer> rotationBits, Bytes completePayload) {
        public EntityFields {Objects.requireNonNull(uuidRepresentation);Objects.requireNonNull(poseRepresentation);Objects.requireNonNull(completePayload);positionBits=List.copyOf(positionBits);rotationBits=List.copyOf(rotationBits);}
    }

    public static Encoded encode(Forest forest, SourceLimitProfile limits) throws IOException {
        try(Scope work=Scope.untracked()){return encodeWithExternalReservation(forest,limits,0,work);}
    }
    static Encoded encodeWithin(Forest forest,SourceLimitProfile limits,Scope work)throws IOException{return encodeWithExternalReservation(forest,limits,0,work);}
    private static Encoded encodeWithExternalReservation(Forest forest,SourceLimitProfile limits,long external,Scope work)throws IOException {
        Objects.requireNonNull(limits);prebudget(forest,limits,external);
        work.charge(192L,"forest counter/output objects");Counter measured=new Counter(limit(limits)); write(new DataOutputStream(measured),forest,limits,work);
        int size=Math.toIntExact(measured.count);
        long accounted=validateBudget(forest,limits,false,Math.max(external,(long)size),work);
        // 保encoding三份与一次decode输入的roundtrip预算；不是JVM精确heap-size声称。
        require(Math.addExact(Math.addExact(accounted,Math.max(external,(long)size)),Math.addExact(Math.multiplyExact((long)size,3L),512L))<=limits.maxRetainedBytes(),"Whole graph/encoded/copy retention budget exhausted");
        work.charge(Math.addExact(Math.multiplyExact((long)size,3L),256L),"forest canonical buffer and defensive copies");ByteArrayOutputStream buffer=new ByteArrayOutputStream(size);
        write(new DataOutputStream(buffer),forest,limits,work); byte[] bytes=buffer.toByteArray();
        return new Encoded(forest,new Bytes(bytes),new Bytes(hash(bytes,work)),forest.members.size(),forest.actualRoots.size(),forest.carriers.size(),bytes.length);
    }
    public static Encoded decode(byte[] bytes,byte[] expectedSha256,SourceLimitProfile limits)throws IOException {
        Objects.requireNonNull(expectedSha256);
        try(Scope work=Scope.untracked()){return decodeWithin(bytes,expectedSha256,limits,work);}
    }
    public static Encoded decode(byte[] bytes,SourceLimitProfile limits)throws IOException {
        try(Scope work=Scope.untracked()){return decodeWithin(bytes,null,limits,work);}
    }
    static Encoded decodeWithin(byte[] bytes,byte[] expectedSha256,SourceLimitProfile limits,Scope work)throws IOException {
        byte[] owned=ownDecodeInput(bytes,limits,work);
        if(expectedSha256!=null)require(expectedSha256.length==32&&Arrays.equals(hash(owned,work),expectedSha256),"Forest SHA mismatch");
        return decodeOwned(owned,limits,work);
    }
    private static byte[] ownDecodeInput(byte[] bytes,SourceLimitProfile limits,Scope work) {
        Objects.requireNonNull(bytes);Objects.requireNonNull(limits);
        require(bytes.length<=limit(limits)&&Math.multiplyExact((long)bytes.length,3L)<=limits.maxRetainedBytes(),"Forest input/workspace budget exhausted");
        work.charge(Math.addExact(bytes.length,64L),"forest owned decode input");return bytes.clone();
    }
    private static Encoded decodeOwned(byte[] bytes,SourceLimitProfile limits,Scope work)throws IOException {
        work.charge(512L,"forest reader and outer graph lists");Reader r=new Reader(bytes,limits,work);
        require(r.text().equals(DOMAIN),"Wrong forest domain");
        byte[] profile=r.blob(); require(Arrays.equals(profile,profileBytes(limits,work)),"Exact shared limit profile mismatch");
        require(Arrays.equals(r.fixed(32),profileSha(limits,work)),"Profile SHA mismatch");
        Identity identity=new Identity(r.uuid(),r.uuid(),r.uuid(),r.i64(),r.uuid(),r.text(),r.text(),r.text(),r.uuid(),r.i64());
        Coverage coverage=r.enumeration(Coverage.values()); Bytes observation=r.checkedBytes();
        List<List<UUID>> stageRoots=new ArrayList<>();for(int stage=0;stage<3;stage++){int rootCount=r.count(16,32);List<UUID> roots=new ArrayList<>();for(int i=0;i<rootCount;i++)roots.add(r.uuid());stageRoots.add(roots);}
        int carrierCount=r.count(49,512);List<Carrier> carriers=new ArrayList<>();
        for(int i=0;i<carrierCount;i++) {
            UUID id=r.uuid();String key=r.text();int regionX=r.i32(),regionZ=r.i32(),dimension=r.i32(),x=r.i32(),z=r.i32(),slot=r.i32();long volume=r.i64();
            Bytes fileId=new Bytes(r.fixed(16));long generation=r.i64(),regionLength=r.i64();int compression=r.i32();Bytes compressed=r.checkedBytes();
            int sectorOffset=r.i32(),sectorCount=r.i32(),timestamp=r.i32();CarrierDisposition disposition=r.enumeration(CarrierDisposition.values());UUID dispositionObservation=r.optionalUuid();int n=r.stageCount();List<CarrierStage> stages=new ArrayList<>();
            for(int j=0;j<n;j++){StageKind kind=r.enumeration(StageKind.values());UUID capture=r.uuid();Bytes payload=r.checkedBytes();int backgroundCount=r.count(37,256);List<BackgroundOccurrence> background=new ArrayList<>();
                for(int k=0;k<backgroundCount;k++){UUID observed=r.uuid();int depth=r.boundedCount(limits.maxNbtDepth(),4,32);List<Integer> path=new ArrayList<>();for(int l=0;l<depth;l++)path.add(r.i32());background.add(new BackgroundOccurrence(observed,path,r.enumeration(Classification.values()),r.checkedBytes()));}
                stages.add(new CarrierStage(kind,capture,payload,background));}
            carriers.add(new Carrier(id,key,regionX,regionZ,dimension,x,z,slot,volume,fileId,generation,regionLength,compression,compressed,sectorOffset,sectorCount,timestamp,disposition,dispositionObservation,stages));
        }
        int memberCount=r.count(25,2048);List<Member> members=new ArrayList<>();
        for(int i=0;i<memberCount;i++) {
            UUID id=r.uuid();Disposition disposition=r.enumeration(Disposition.values());UUID evidence=r.optionalUuid();
            Observation action=r.enumeration(Observation.values());int actionCalls=r.i32();UUID actionCapture=r.optionalUuid();
            int n=r.stageCount();List<Stage> stages=new ArrayList<>();
            for(int j=0;j<n;j++) {
                StageKind kind=r.enumeration(StageKind.values());UUID capture=r.uuid(),carrier=r.uuid(),parent=r.optionalUuid();
                int pathCount=r.boundedCount(limits.maxNbtDepth(),4,32);require(pathCount<=limits.maxNbtDepth(),"Path depth budget exhausted");List<Integer> path=new ArrayList<>();
                for(int k=0;k<pathCount;k++)path.add(r.i32());Classification classification=r.enumeration(Classification.values());Bytes payload=r.checkedBytes();
                stages.add(new Stage(kind,capture,carrier,parent,path,classification,payload,r.enumeration(Observation.values()),r.i32(),
                    r.enumeration(Observation.values()),r.i32(),r.enumeration(Observation.values()),r.i32(),r.enumeration(Observation.values()),r.i32(),
                    r.enumeration(Observation.values()),r.i32(),r.text(),r.optionalUuid()));
            }
            int ln=r.boundedCount(2,49,80);require(ln<=2,"Only raw/fixed/actual lineage supported");List<Lineage> lineage=new ArrayList<>();
            for(int j=0;j<ln;j++)lineage.add(new Lineage(r.enumeration(LineageKind.values()),r.uuid(),r.uuid(),r.uuid()));
            members.add(new Member(id,disposition,evidence,action,actionCalls,actionCapture,stages,lineage));
        }
        require(r.in.available()==0,"Trailing forest bytes");
        Encoded encoded=encodeWithExternalReservation(new Forest(identity,coverage,observation,stageRoots.get(0),stageRoots.get(1),stageRoots.get(2),carriers,members),limits,bytes.length,work);
        require(Arrays.equals(bytes,encoded.bytes.value),"Noncanonical forest encoding");return encoded;
    }
    public static EntityFields entityFields(Bytes payload,SourceLimitProfile limits) throws IOException {
        try(Scope work=Scope.untracked()){return entityFields(payload,limits,limits.maxRetainedBytes(),work);}
    }
    static EntityFields entityFields(Bytes payload,SourceLimitProfile limits,long workspace)throws IOException {
        try(Scope work=Scope.untracked()){return entityFields(payload,limits,workspace,work);}}
    static EntityFields entityFields(Bytes payload,SourceLimitProfile limits,long workspace,Scope work)throws IOException {
        work.charge(768L,"entity fields UUID and fixed pose representations");Node root=BoundedNbtStructurePreflight.extract(payload.value,limits,workspace,work);Node type=root.child("id");
        String registryType=type!=null&&type.type()==8?type.stringValue():null;
        Node most=root.child("UUIDMost"),least=root.child("UUIDLeast");UUID uuid=null;
        UuidRepresentation representation=most==null&&least==null?UuidRepresentation.ABSENT:UuidRepresentation.MALFORMED;
        if(most!=null&&least!=null&&most.type()==4&&least.type()==4) {representation=UuidRepresentation.EXACT_LONG_PAIR;uuid=new UUID(most.integerBits(),least.integerBits());}
        Node pos=root.child("Pos"),rotation=root.child("Rotation");List<Long> positions=longList(pos,6,3);List<Integer> rotations=intList(rotation,5,2);
        PoseRepresentation pose=pos==null||rotation==null?PoseRepresentation.ABSENT:
            positions.size()==3&&rotations.size()==2?PoseRepresentation.EXACT_LIST_BITS:PoseRepresentation.MALFORMED;
        return new EntityFields(registryType,uuid,representation,pose,positions,rotations,payload);
    }
    private static List<Long> longList(Node node,int type,int count) {
        if(node==null||node.type()!=9||node.children().size()!=count)return List.of();
        List<Long> result=new ArrayList<>();for(Node n:node.children()){if(n.type()!=type)return List.of();result.add(n.integerBits());}return List.copyOf(result);
    }
    private static List<Integer> intList(Node node,int type,int count) {
        if(node==null||node.type()!=9||node.children().size()!=count)return List.of();
        List<Integer> result=new ArrayList<>();for(Node n:node.children()){if(n.type()!=type)return List.of();result.add(n.intBits());}return List.copyOf(result);
    }
    static void validate(Forest forest,SourceLimitProfile limits,boolean classified) throws IOException {try(Scope work=Scope.untracked()){validateBudget(forest,limits,classified,0,work);}}
    static long validateWithExternal(Forest forest,SourceLimitProfile limits,boolean classified,long external)throws IOException {try(Scope work=Scope.untracked()){return validateWithExternal(forest,limits,classified,external,work);}}
    static long validateWithExternal(Forest forest,SourceLimitProfile limits,boolean classified,long external,Scope work)throws IOException {long accounted=validateBudget(forest,limits,classified,external,work);return limits.maxRetainedBytes()-accounted-external;}
    private static long prebudget(Forest forest,SourceLimitProfile limits,long external) {
        require(external>=0&&external<=limits.maxRetainedBytes(),"Invalid external reservation");
        Objects.requireNonNull(forest);for(List<UUID> roots:List.of(forest.rawRoots,forest.fixedRoots,forest.actualRoots))require(roots.size()<=limits.maxMembers(),"Stage root cardinality exhausted");require(forest.members.size()<=limits.maxMembers() && forest.carriers.size()<=limits.maxMembers(),"Forest cardinality budget exhausted");
        long payloadBudget=forest.sourceObservation.length();long textBudget=0;
        Identity identity=forest.identity;
        for(String text:List.of(identity.worldIdentity,identity.saveIdentity,identity.loaderProfile))textBudget=Math.addExact(textBudget,Math.multiplyExact((long)text.length(),3L));
        for(Carrier carrier:forest.carriers){require(carrier.stages.size()<=3,"Too many carrier stages");payloadBudget=Math.addExact(payloadBudget,carrier.originalCompressedChunk.length());textBudget=Math.addExact(textBudget,Math.multiplyExact((long)carrier.regionKey.length(),3L));for(CarrierStage stage:carrier.stages){require(stage.background.size()<=limits.maxMembers(),"Background cardinality exhausted");payloadBudget=Math.addExact(payloadBudget,stage.chunkNbt.length());for(BackgroundOccurrence background:stage.background){require(background.entityPath.size()<=limits.maxNbtDepth(),"Background path depth exhausted");payloadBudget=Math.addExact(payloadBudget,background.entityNbt.length());}}}
        for(Member member:forest.members){require(member.stages.size()<=3&&member.lineage.size()<=2,"Too many member stages/lineage");for(Stage stage:member.stages){payloadBudget=Math.addExact(payloadBudget,stage.entityNbt.length());textBudget=Math.addExact(textBudget,Math.multiplyExact((long)stage.failureCode.length(),3L));}}
        long backgroundCount=0,backgroundMetadata=0;for(Carrier carrier:forest.carriers)for(CarrierStage stage:carrier.stages)for(BackgroundOccurrence background:stage.background){backgroundCount=Math.addExact(backgroundCount,1);backgroundMetadata=Math.addExact(backgroundMetadata,Math.addExact(256L,Math.multiplyExact((long)background.entityPath.size(),32L)));}
        require(backgroundCount/3L+(backgroundCount%3L==0?0L:1L)<=limits.maxMembers(),"Aggregate three-stage background cardinality exhausted");
        long metadata=Math.addExact(backgroundMetadata,Math.addExact(Math.multiplyExact((long)forest.members.size(),2048L),Math.multiplyExact((long)forest.carriers.size(),512L)));
        for(Member member:forest.members)for(Stage stage:member.stages)metadata=Math.addExact(metadata,Math.multiplyExact((long)stage.entityPath.size(),16L));
        long accounted=Math.addExact(metadata,Math.multiplyExact(Math.addExact(payloadBudget,textBudget),2L));
        require(Math.addExact(accounted,external)<=limits.maxRetainedBytes(),"Whole graph payload/text budget exhausted before semantic allocation");
        return accounted;
    }
    private static long validateBudget(Forest forest,SourceLimitProfile limits,boolean classified,long external,Scope allocationParent)throws IOException {
        try(Scope work=allocationParent.child()){work.charge(512L,"validation reservation computation tuples");work.charge(validationScratch(forest),"forest topology collection and path workspace");
        long accounted=prebudget(forest,limits,external);
        long workspace=limits.maxRetainedBytes()-accounted-external;
        Map<UUID,Carrier> carriers=unique(forest.carriers,Carrier::id);Map<UUID,Member> members=unique(forest.members,Member::id);
        if(classified)require(!forest.members.isEmpty()&&!forest.rawRoots.isEmpty(),"No observed forest roots/members; cannot derive classified data index");
        for(List<UUID> roots:List.of(forest.rawRoots,forest.fixedRoots,forest.actualRoots))require(new HashSet<>(roots).size()==roots.size(),"Duplicate stage root");
        long retained=forest.sourceObservation.length();Set<UUID> captures=new HashSet<>();
        for(Carrier carrier:forest.carriers) {
            if(classified)require(carrier.disposition!=CarrierDisposition.PENDING&&carrier.disposition!=CarrierDisposition.UNKNOWN,"Carrier disposition unresolved");
            require(carrier.originalRegionLength<=limits.maxRegionBytes() && carrier.originalCompressedChunk.length()<=limits.maxChunkBytes(),"Original region/chunk budget exhausted");
            require(Math.multiplyExact((long)carrier.sectorOffset+carrier.sectorCount,4096L)<=carrier.originalRegionLength,"Original slot extends beyond region");
            require((long)carrier.originalCompressedChunk.length()+5L<=(long)carrier.sectorCount*4096L,"Original chunk cannot fit allocated slot");
            CarrierStage rawCarrier=carrier.stages.stream().filter(s->s.kind==StageKind.RAW).findFirst().orElse(null);
            if(rawCarrier!=null&&(classified||carrier.disposition!=CarrierDisposition.PENDING&&carrier.disposition!=CarrierDisposition.UNKNOWN))verifyOriginalCompression(carrier,rawCarrier,limits,workspace,work);
            else if(classified)require(false,"Original carrier decode unresolved");
            retained=Math.addExact(retained,carrier.originalCompressedChunk.length());
            uniqueKinds(carrier.stages.stream().map(CarrierStage::kind).toList());
            for(CarrierStage stage:carrier.stages) {require(captures.add(stage.captureId),"Duplicate capture identity");retained=Math.addExact(retained,stage.chunkNbt.length());
                require(stage.chunkNbt.length()<=limits.maxDecompressedBytes(),"Chunk decompressed budget exhausted");
                Set<List<Integer>> backgroundPaths=new HashSet<>();for(BackgroundOccurrence background:stage.background){require(captures.add(background.captureId),"Duplicate background capture identity");require(backgroundPaths.add(background.entityPath),"Duplicate background occurrence path");}
            }
        }
        for(Member member:forest.members) {
            uniqueKinds(member.stages.stream().map(Stage::kind).toList());
            for(Stage stage:member.stages) {require(captures.add(stage.captureId),"Duplicate capture identity");require(carriers.containsKey(stage.carrierId),"Missing carrier");
                retained=Math.addExact(retained,stage.entityNbt.length());require(stage.entityPath.size()<=limits.maxNbtDepth(),"Entity path depth budget exhausted");
                if(stage.parentMemberId!=null)require(members.containsKey(stage.parentMemberId)&&!stage.parentMemberId.equals(member.id),"Missing/self parent");
                if(stage.entityNbt.length()>0&&(classified||stage.classification!=Classification.UNKNOWN))entityFields(stage.entityNbt,limits,workspace,work);
            }
            Set<LineageKind> kinds=new HashSet<>();for(Lineage lineage:member.lineage) {
                require(kinds.add(lineage.kind),"Duplicate lineage kind");Stage from=find(member,lineage.kind==LineageKind.RAW_TO_FIXED?StageKind.RAW:StageKind.FIXED);
                Stage to=find(member,lineage.kind==LineageKind.RAW_TO_FIXED?StageKind.FIXED:StageKind.ACTUAL);
                require(from!=null&&to!=null&&lineage.fromCaptureId.equals(from.captureId)&&lineage.toCaptureId.equals(to.captureId),"Lineage does not bind exact captures");
            }
            if(classified) {
                require(member.stages.size()==3 && member.lineage.size()==2,"Unresolved stage/lineage");
                require(member.disposition==Disposition.INDEXED||member.disposition==Disposition.ORDINARILY_UNLOADED||member.disposition==Disposition.LEGITIMATELY_REMOVED,"Unresolved disposition");
                for(Stage stage:member.stages)require(stage.classification!=Classification.UNKNOWN && stage.entityNbt.length()>0 && stage.failureCode.isEmpty(),"Unresolved stage classification");
                Stage actual=find(member,StageKind.ACTUAL);
                if(member.disposition==Disposition.INDEXED&&actual.parentMemberId!=null)require(members.get(actual.parentMemberId).disposition==Disposition.INDEXED,"Indexed member has non-indexed actual parent");
                if(member.disposition==Disposition.INDEXED) {
                    require(actual.factory==Observation.RETURNED&&actual.factoryCalls==1&&actual.read==Observation.RETURNED&&actual.readCalls==1
                        &&actual.caps==Observation.RETURNED&&actual.capsCalls==1&&actual.placement==Observation.RETURNED&&actual.placementCalls==1
                        &&actual.index==Observation.RETURNED&&actual.indexCalls==1,"Unresolved factory/read/Caps/placement/index");
                    require(member.dispositionAction==Observation.NOT_OBSERVED,"Indexed member cannot claim terminal removal action");
                } else {
                    require(actual.index==Observation.NOT_OBSERVED&&actual.indexCalls==0,"Unloaded/removed member cannot claim index");
                    require(member.dispositionAction==Observation.RETURNED&&member.dispositionCalls==1&&actual.captureId.equals(member.dispositionCaptureId),"Unresolved exact terminal disposition action");
                }
                for(Stage stage:member.stages) {EntityFields fields=entityFields(stage.entityNbt,limits,workspace,work);
                    require(fields.registryType!=null && !fields.registryType.isEmpty() && fields.uuidRepresentation==UuidRepresentation.EXACT_LONG_PAIR,"Stage type/UUID unresolved");
                    require(fields.poseRepresentation==PoseRepresentation.EXACT_LIST_BITS,"Stage pose unresolved");
                    if(stage.classification==Classification.FLOATING_POD)require(fields.uuid!=null,"FP UUID unresolved");}
            }
        }
        require(Math.multiplyExact(retained,2L)<=limits.maxRetainedBytes(),"Complete payload retention budget exhausted");
        for(StageKind kind:StageKind.values()) {
            Set<UUID> expectedRoots=new HashSet<>();for(Member member:forest.members){Stage stage=find(member,kind);if(stage!=null&&stage.parentMemberId==null)expectedRoots.add(member.id);}
            List<UUID> declared=switch(kind){case RAW->forest.rawRoots;case FIXED->forest.fixedRoots;case ACTUAL->forest.actualRoots;};
            require(expectedRoots.equals(new HashSet<>(declared)),"Stage roots do not cover exact topology");
        }
        for(StageKind kind:StageKind.values()) {
            Map<String,Member> paths=new HashMap<>();
            for(Member member:forest.members) {Stage stage=find(member,kind);if(stage==null)continue;
                require(paths.put(stage.carrierId+":"+stage.entityPath,member)==null,"Duplicate stage occurrence path");
                Set<UUID> chain=new HashSet<>();Stage current=stage;
                while(current.parentMemberId!=null) {require(chain.add(current.parentMemberId),"Stage parent cycle");Member parent=members.get(current.parentMemberId);
                    Stage parentStage=find(parent,kind);require(parentStage!=null && parentStage.carrierId.equals(current.carrierId)
                        && current.entityPath.size()==parentStage.entityPath.size()+1 && current.entityPath.subList(0,parentStage.entityPath.size()).equals(parentStage.entityPath),"Parent path/carrier mismatch");current=parentStage;}
            }
            for(Carrier carrier:forest.carriers) {CarrierStage carrierStage=carrier.stages.stream().filter(s->s.kind==kind).findFirst().orElse(null);
                List<Member> attached=forest.members.stream().filter(m->{Stage s=find(m,kind);return s!=null&&s.carrierId.equals(carrier.id);}).toList();
                require(attached.isEmpty()||carrierStage!=null,"Missing exact carrier stage");if(carrierStage==null)continue;
                // 未决DATA保原bytes；没有资格绕过classified分支的真实semantic检查。
                if(!classified&&(carrier.disposition==CarrierDisposition.PENDING||carrier.disposition==CarrierDisposition.UNKNOWN
                    ||attached.stream().anyMatch(m->find(m,kind).classification==Classification.UNKNOWN)||carrierStage.background.stream().anyMatch(b->b.classification==Classification.UNKNOWN)))continue;
                Node chunk=BoundedNbtStructurePreflight.extract(carrierStage.chunkNbt.value,limits,workspace,work);
                Map<List<Integer>,BackgroundOccurrence> background=new HashMap<>();for(BackgroundOccurrence occurrence:carrierStage.background){require(background.put(occurrence.entityPath,occurrence)==null,"Duplicate background occurrence");
                    require(!paths.containsKey(carrier.id+":"+occurrence.entityPath),"Included/background occurrence overlap");require(at(chunk,occurrence.entityPath).matchesStandaloneCompound(occurrence.entityNbt.value),"Background payload differs from exact carrier occurrence");}
                if(classified){Node entities=entities(chunk);Set<List<Integer>> observedBackground=new HashSet<>();
                    for(int index=0;index<entities.children().size();index++)checkCarrierClosure(element(entities,index),List.of(index),carrier.id,paths,background,observedBackground,kind,limits);
                    require(observedBackground.size()==background.size(),"Background contains non-occurring paths");}
                for(Member member:attached) {Stage stage=find(member,kind);Node entity=at(chunk,stage.entityPath);
                    if(stage.entityNbt.length()>0)require(entity.matchesStandaloneCompound(stage.entityNbt.value),"Member payload differs from exact carrier occurrence");
                    if(classified&&stage.parentMemberId==null){for(int depth=1;depth<stage.entityPath.size();depth++){
                        BackgroundOccurrence ancestor=background.get(stage.entityPath.subList(0,depth));require(ancestor!=null&&ancestor.classification==Classification.OTHER,"External ancestor is missing/FP/unknown");}
                        checkDescendants(entity,stage.entityPath,carrier.id,paths,limits,kind);}
                }
            }
        }
        return accounted;
        }
    }
    /** 使用JDK真实解压器逐byte对比原compressed与RAW carrier，不构造第二个NBT parser。 */
    private static void verifyOriginalCompression(Carrier carrier,CarrierStage raw,SourceLimitProfile limits,long workspace,Scope work)throws IOException {
        work.charge(4096L,"compression verification workspace");require(workspace>=4096,"Compression verification workspace exhausted");byte[] buffer=new byte[512];int offset=0;
        byte[] original=carrier.originalCompressedChunk.value,expected=raw.chunkNbt.value;
        if(carrier.compressionType==1) {
            try(InputStream input=new GZIPInputStream(new ByteArrayInputStream(original),512)) {
                int count;while((count=input.read(buffer))!=-1){require(count>0,"Gzip decoder made no progress");offset=compareInflated(buffer,count,expected,offset,limits);}
            }
        } else {
            try(Inflater inflater=new Inflater(false)) {
                inflater.setInput(original);
                while(!inflater.finished()) {int count=inflater.inflate(buffer);if(count>0)offset=compareInflated(buffer,count,expected,offset,limits);
                    else if(!inflater.finished())throw new IOException("Original zlib is truncated, needs dictionary, or stalled");}
                require(inflater.getRemaining()==0,"Trailing original zlib bytes");
            } catch(DataFormatException cause){throw new IOException("Original compressed carrier malformed",cause);}
        }
        require(offset==expected.length,"Original compressed carrier differs from full RAW bytes");
    }
    private static int compareInflated(byte[] buffer,int count,byte[] expected,int offset,SourceLimitProfile limits) {
        long end=Math.addExact((long)offset,count);require(end<=limits.maxDecompressedBytes()&&end<=expected.length,"Original decompressed carrier length mismatch/budget exhausted");
        for(int i=0;i<count;i++)require(buffer[i]==expected[offset+i],"Original compressed carrier differs from RAW bytes");return Math.toIntExact(end);
    }
    private static void checkDescendants(Node entity,List<Integer> path,UUID carrier,Map<String,Member> paths,SourceLimitProfile limits,StageKind kind) {
        require(paths.containsKey(carrier+":"+path),"Passenger coverage incomplete");Node passengers=entity.child("Passengers");if(passengers==null)return;
        require(passengers.type()==9,"Passengers must be list");if(!passengers.children().isEmpty())require(path.size()<limits.maxNbtDepth(),"Passenger depth exhausted");
        for(int i=0;i<passengers.children().size();i++){Node child=passengers.children().get(i);require(child.type()==10,"Passenger must be compound");List<Integer> next=new ArrayList<>(path);next.add(i);Member childMember=paths.get(carrier+":"+next);
            require(childMember!=null&&Objects.equals(find(childMember,kind).parentMemberId,paths.get(carrier+":"+path).id),"Passenger parent identity mismatch");
            checkDescendants(child,next,carrier,paths,limits,kind);}
    }
    private static Node entities(Node root){Node level=root.child("Level");Node entities=(level==null?root:level).child("Entities");require(entities!=null&&entities.type()==9,"Carrier Entities list required");return entities;}
    private static void checkCarrierClosure(Node entity,List<Integer> path,UUID carrier,Map<String,Member> included,Map<List<Integer>,BackgroundOccurrence> background,Set<List<Integer>> observed,StageKind kind,SourceLimitProfile limits) {
        Member member=included.get(carrier+":"+path);BackgroundOccurrence external=background.get(path);
        require((member!=null)!=(external!=null),"Actual carrier occurrence has missing/overlapping unit/background classification");
        if(external!=null){require(external.classification==Classification.OTHER,"Uncovered FP or UNKNOWN carrier branch");observed.add(path);}
        else {Stage stage=find(member,kind);if(path.size()>1){List<Integer> prefix=path.subList(0,path.size()-1);Member parent=included.get(carrier+":"+prefix);
            require(parent==null?stage.parentMemberId==null:Objects.equals(stage.parentMemberId,parent.id),"Actual carrier immediate parent mismatch");}}
        Node passengers=entity.child("Passengers");if(passengers==null)return;require(passengers.type()==9,"Passengers must be list");
        if(!passengers.children().isEmpty())require(path.size()<limits.maxNbtDepth(),"Passenger depth exhausted");
        for(int index=0;index<passengers.children().size();index++){Node child=element(passengers,index);List<Integer> next=new ArrayList<>(path);next.add(index);checkCarrierClosure(child,next,carrier,included,background,observed,kind,limits);}
    }
    private static Node at(Node root,List<Integer> path) {
        Node entities=entities(root);
        Node current=element(entities,path.getFirst());for(int i=1;i<path.size();i++){Node passengers=current.child("Passengers");require(passengers!=null&&passengers.type()==9,"Missing Passengers path");current=element(passengers,path.get(i));}return current;
    }
    private static Node element(Node list,int index) {require(index>=0&&index<list.children().size(),"Entity path out of bounds");Node node=list.children().get(index);require(node.type()==10,"Entity path not compound");return node;}
    static Stage find(Member member,StageKind kind) {return member.stages.stream().filter(s->s.kind==kind).findFirst().orElse(null);}
    private static <T> Map<UUID,T> unique(List<T> values,java.util.function.Function<T,UUID> key) {Map<UUID,T> result=new HashMap<>();for(T value:values)require(result.put(key.apply(value),value)==null,"Duplicate stable identity");return result;}
    private static void uniqueKinds(List<StageKind> kinds){require(new HashSet<>(kinds).size()==kinds.size(),"Duplicate stage");}
    private static long validationScratch(Forest f) {
        long n=2048L;
        for(List<UUID> roots:List.of(f.rawRoots,f.fixedRoots,f.actualRoots))n=Math.addExact(n,Math.multiplyExact(roots.size(),128L));
        for(Carrier c:f.carriers){n=Math.addExact(n,2048L);for(CarrierStage stage:c.stages){n=Math.addExact(n,512L);for(BackgroundOccurrence b:stage.background)n=Math.addExact(n,Math.addExact(1024L,Math.multiplyExact(b.entityPath.size(),256L)));}}
        for(Member m:f.members){n=Math.addExact(n,4096L);for(Stage stage:m.stages)n=Math.addExact(n,Math.addExact(2048L,Math.multiplyExact(stage.entityPath.size(),256L)));}
        return n;
    }
    private static long writerScratch(Forest f) {
        long n=Math.addExact(4096L,Math.multiplyExact((long)f.members.size()+f.carriers.size(),512L));
        for(String text:List.of(f.identity.worldIdentity,f.identity.saveIdentity,f.identity.loaderProfile))n=Math.addExact(n,Math.addExact(256L,Math.multiplyExact(text.length(),6L)));
        for(List<UUID> roots:List.of(f.rawRoots,f.fixedRoots,f.actualRoots))n=Math.addExact(n,Math.multiplyExact(roots.size(),32L));
        for(Carrier c:f.carriers){n=Math.addExact(n,Math.addExact(512L,Math.multiplyExact(c.regionKey.length(),6L)));for(CarrierStage stage:c.stages)n=Math.addExact(n,Math.addExact(512L,Math.multiplyExact(stage.background.size(),256L)));}
        for(Member m:f.members)for(Stage stage:m.stages)n=Math.addExact(n,Math.addExact(512L,Math.multiplyExact(stage.failureCode.length(),6L)));
        return n;
    }
    private static byte[] profileBytes(SourceLimitProfile p,Scope work){work.charge(192L,"profile canonical copy");return p.canonicalBytes();}
    private static byte[] profileSha(SourceLimitProfile p,Scope work){work.charge(96L,"profile SHA copy");return p.sha256();}
    static byte[] hash(byte[] bytes,Scope parent){parent.charge(64L,"source forest SHA256 result");try(Scope work=parent.child()){work.charge(1600L,"source forest SHA256 workspace");return hash(bytes);}}
    static byte[] hash(byte[] bytes){try{return MessageDigest.getInstance("SHA-256").digest(bytes);}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException("SHA-256 unavailable",impossible);}}
    static void require(boolean condition,String detail){if(!condition)throw new IllegalArgumentException(detail);}
    private static void requireText(String text){require(text!=null&&!text.isEmpty(),"Exact nonempty identity text required");}
    private static long limit(SourceLimitProfile p){return Math.min(Integer.MAX_VALUE,Math.min(p.maxRetainedBytes(),p.maxSqlPacketBytes()));}
    static int compareUuid(UUID left,UUID right){int high=Long.compareUnsigned(left.getMostSignificantBits(),right.getMostSignificantBits());return high!=0?high:Long.compareUnsigned(left.getLeastSignificantBits(),right.getLeastSignificantBits());}
    private static void write(DataOutputStream out,Forest f,SourceLimitProfile limits,Scope parent)throws IOException {
        try(Scope work=parent.child()){work.charge(512L,"writer reservation computation tuples");work.charge(writerScratch(f),"forest sorted-list UTF8 stream and SHA workspace");
        text(out,DOMAIN);blob(out,profileBytes(limits,work));out.write(profileSha(limits,work));Identity i=f.identity;
        uuid(out,i.bindingId);uuid(out,i.unitId);uuid(out,i.forestId);out.writeLong(i.sourceEpoch);uuid(out,i.createdOperationId);
        text(out,i.worldIdentity);text(out,i.saveIdentity);text(out,i.loaderProfile);uuid(out,i.catalogGeneration);out.writeLong(i.readGeneration);
        out.writeByte(f.coverage.ordinal());checkedBlob(out,f.sourceObservation);
        for(List<UUID> stageRoots:List.of(f.rawRoots,f.fixedRoots,f.actualRoots)){List<UUID> roots=stageRoots.stream().sorted(SourceForestCodec::compareUuid).toList();out.writeInt(roots.size());for(UUID root:roots)uuid(out,root);}
        List<Carrier> carriers=f.carriers.stream().sorted(Comparator.comparing(Carrier::id,SourceForestCodec::compareUuid)).toList();out.writeInt(carriers.size());
        for(Carrier c:carriers){uuid(out,c.id);text(out,c.regionKey);out.writeInt(c.regionX);out.writeInt(c.regionZ);out.writeInt(c.dimension);out.writeInt(c.chunkX);out.writeInt(c.chunkZ);out.writeInt(c.slot);out.writeLong(c.volumeSerialBits);out.write(c.fileId128.value);out.writeLong(c.saveGeneration);out.writeLong(c.originalRegionLength);out.writeInt(c.compressionType);checkedBlob(out,c.originalCompressedChunk);out.writeInt(c.sectorOffset);out.writeInt(c.sectorCount);out.writeInt(c.timestampBits);out.writeByte(c.disposition.ordinal());optionalUuid(out,c.dispositionObservation);
            List<CarrierStage> stages=c.stages.stream().sorted(Comparator.comparing(CarrierStage::kind)).toList();out.writeInt(stages.size());for(CarrierStage s:stages){out.writeByte(s.kind.ordinal());uuid(out,s.captureId);checkedBlob(out,s.chunkNbt);List<BackgroundOccurrence> background=s.background.stream().sorted((a,b)->comparePath(a.entityPath,b.entityPath)).toList();out.writeInt(background.size());for(BackgroundOccurrence occurrence:background){uuid(out,occurrence.captureId);out.writeInt(occurrence.entityPath.size());for(int part:occurrence.entityPath)out.writeInt(part);out.writeByte(occurrence.classification.ordinal());checkedBlob(out,occurrence.entityNbt);}}}
        List<Member> members=f.members.stream().sorted(Comparator.comparing(Member::id,SourceForestCodec::compareUuid)).toList();out.writeInt(members.size());
        for(Member m:members){uuid(out,m.id);out.writeByte(m.disposition.ordinal());optionalUuid(out,m.dispositionObservation);observation(out,m.dispositionAction,m.dispositionCalls);optionalUuid(out,m.dispositionCaptureId);List<Stage> stages=m.stages.stream().sorted(Comparator.comparing(Stage::kind)).toList();out.writeInt(stages.size());
            for(Stage s:stages){out.writeByte(s.kind.ordinal());uuid(out,s.captureId);uuid(out,s.carrierId);optionalUuid(out,s.parentMemberId);out.writeInt(s.entityPath.size());for(int part:s.entityPath)out.writeInt(part);
                out.writeByte(s.classification.ordinal());checkedBlob(out,s.entityNbt);observation(out,s.factory,s.factoryCalls);observation(out,s.read,s.readCalls);observation(out,s.caps,s.capsCalls);observation(out,s.placement,s.placementCalls);observation(out,s.index,s.indexCalls);text(out,s.failureCode);optionalUuid(out,s.failureCorrelation);}
            List<Lineage> lineage=m.lineage.stream().sorted(Comparator.comparing(Lineage::kind)).toList();out.writeInt(lineage.size());for(Lineage l:lineage){out.writeByte(l.kind.ordinal());uuid(out,l.fromCaptureId);uuid(out,l.toCaptureId);uuid(out,l.observationId);}}
        }
    }
    private static int comparePath(List<Integer> a,List<Integer> b){for(int i=0;i<Math.min(a.size(),b.size());i++){int difference=Integer.compare(a.get(i),b.get(i));if(difference!=0)return difference;}return Integer.compare(a.size(),b.size());}
    private static void observation(DataOutputStream o,Observation observation,int calls)throws IOException{o.writeByte(observation.ordinal());o.writeInt(calls);}
    private static void uuid(DataOutputStream o,UUID id)throws IOException{o.writeLong(id.getMostSignificantBits());o.writeLong(id.getLeastSignificantBits());}
    private static void optionalUuid(DataOutputStream o,UUID id)throws IOException{o.writeByte(id==null?0:1);if(id!=null)uuid(o,id);}
    private static void blob(DataOutputStream o,byte[] bytes)throws IOException{o.writeInt(bytes.length);o.write(bytes);}
    private static void checkedBlob(DataOutputStream o,Bytes bytes)throws IOException{blob(o,bytes.value);o.write(hash(bytes.value));}
    private static void text(DataOutputStream o,String text)throws IOException{blob(o,strictUtf8(text));}
    private static byte[] strictUtf8(String text)throws CharacterCodingException {ByteBuffer encoded=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(text));byte[] bytes=new byte[encoded.remaining()];encoded.get(bytes);return bytes;}
    private static final class Counter extends OutputStream {long count;final long maximum;Counter(long maximum){this.maximum=maximum;}private void add(long n)throws IOException{try{count=Math.addExact(count,n);}catch(ArithmeticException cause){throw new IOException("Forest size overflow",cause);}if(count>maximum)throw new IOException("Forest encoded budget exhausted");}@Override public void write(int b)throws IOException{add(1);}@Override public void write(byte[] b,int off,int len)throws IOException{Objects.checkFromIndexSize(off,len,b.length);add(len);}}
    private static final class Reader {final DataInputStream in;final SourceLimitProfile limits;final Scope work;long retained;
        Reader(byte[] bytes,SourceLimitProfile limits,Scope work){this.work=work;in=new DataInputStream(new ByteArrayInputStream(bytes));this.limits=limits;retained=bytes.length;}
        private void reserve(long n){retained=Math.addExact(retained,n);require(retained<=limits.maxRetainedBytes(),"Decode retained/metadata budget exhausted");work.charge(n,"forest Reader decoded representation");}
        int i32()throws IOException{return in.readInt();}long i64()throws IOException{return in.readLong();}
        UUID uuid()throws IOException{work.charge(32L,"forest decoded UUID");return new UUID(i64(),i64());}UUID optionalUuid()throws IOException{int flag=in.readUnsignedByte();require(flag<=1,"Noncanonical optional UUID");return flag==0?null:uuid();}
        byte[] fixed(int n)throws IOException{require(n>=0,"Negative fixed byte length");if(n>in.available())throw new EOFException("Truncated fixed bytes");reserve(Math.multiplyExact((long)n,2L));work.charge(128L,"forest fixed-array object representation");return in.readNBytes(n);}
        byte[] blob()throws IOException{int n=i32();require(n>=0,"Negative byte length");return fixed(n);}
        Bytes checkedBytes()throws IOException{byte[] raw=blob(),sha=fixed(32);require(Arrays.equals(hash(raw,work),sha),"Payload SHA mismatch");return new Bytes(raw);}
        String text()throws IOException{byte[] bytes=blob();reserve(Math.multiplyExact((long)bytes.length,2L));return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();}
        int count(int minimum,long perItem)throws IOException{return boundedCount(limits.maxMembers(),minimum,perItem);}
        int boundedCount(long maximum,int minimum,long perItem)throws IOException{int n=i32();require(n>=0&&n<=maximum,"Invalid/budgeted collection count");if(n>in.available()/minimum)throw new EOFException("Truncated collection framing");reserve(Math.multiplyExact((long)n,perItem));return n;}
        int stageCount()throws IOException{int n=i32();require(n>=0&&n<=3,"Invalid stage count");if(n>in.available())throw new EOFException("Truncated stage framing");reserve(Math.multiplyExact((long)n,128L));return n;}
        <T> T enumeration(T[] values)throws IOException{int n=in.readUnsignedByte();require(n<values.length,"Unknown protocol enum");return values[n];}
    }
}

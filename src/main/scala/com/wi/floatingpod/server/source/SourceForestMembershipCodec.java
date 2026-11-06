package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget.Scope;
import com.wi.floatingpod.server.source.SourceForestCodec.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;

/** 固定74-byte header的纯DATA索引。没有SQL seal、absence或ProducedSourceForest发行权。 */
public final class SourceForestMembershipCodec {
    public static final String DOMAIN="FP_FOREST_POD_UUID_INDEX_BE_V1";
    public static final int HEADER_BYTES=74;
    private SourceForestMembershipCodec() { }
    public static final class IndexData {
        private final Bytes bytes,sha256,forestSha256;
        private final List<UUID> identities;
        private IndexData(byte[] bytes,byte[] forestSha,List<UUID> identities) {
            this.bytes=new Bytes(bytes);this.sha256=new Bytes(SourceForestCodec.hash(bytes));
            this.forestSha256=new Bytes(forestSha);this.identities=List.copyOf(identities);
        }
        public Bytes bytes(){return bytes;}public Bytes sha256(){return sha256;}
        public Bytes forestSha256(){return forestSha256;}public List<UUID> identities(){return identities;}
        /** 仅对已完整解码的DATA查询，不授absence权。 */
        public boolean contains(UUID uuid){return Collections.binarySearch(identities,Objects.requireNonNull(uuid),SourceForestCodec::compareUuid)>=0;}
    }
    public static IndexData derive(Forest data,SourceLimitProfile limits)throws IOException {
        try(Scope work=Scope.untracked()){return deriveWithin(data,limits,work);}}
    static IndexData deriveWithin(Forest data,SourceLimitProfile limits,Scope work)throws IOException {
        Encoded forest=SourceForestCodec.encodeWithin(data,limits,work);
        long identityBudget=Math.multiplyExact(Math.multiplyExact((long)data.members().size(),3L),128L);
        long maximumItems=Math.multiplyExact((long)data.members().size(),3L);long maximumIndexLength=Math.addExact(HEADER_BYTES,Math.multiplyExact(maximumItems,16L));
        long external=Math.addExact(forest.bytes().length(),Math.addExact(identityBudget,Math.multiplyExact(maximumIndexLength,3L)));
        long workspace=SourceForestCodec.validateWithExternal(data,limits,true,external,work);
        work.charge(Math.addExact(identityBudget,1024L),"membership TreeSet identities and framing objects");TreeSet<UUID> values=new TreeSet<>(SourceForestCodec::compareUuid);
        for(Member member:data.members()) {
            // 同一stable member任一阶段DATA分类为FP，则保全三个阶段aliases；实际分类由未来issuer活核。
            if(member.stages().stream().anyMatch(stage->stage.classification()==Classification.FLOATING_POD))
                for(Stage stage:member.stages())values.add(Objects.requireNonNull(SourceForestCodec.entityFields(stage.entityNbt(),limits,workspace,work).uuid()));
        }
        long maximum=Math.multiplyExact((long)data.members().size(),3L);
        SourceForestCodec.require(values.size()<=maximum,"Raw/fixed/actual UUID cardinality exceeded");
        long length=Math.addExact(HEADER_BYTES,Math.multiplyExact((long)values.size(),16L));
        SourceForestCodec.require(length<=Integer.MAX_VALUE && length<=limits.maxSqlPacketBytes()
            && Math.multiplyExact(length,2L)<=limits.maxRetainedBytes(),"Membership budget exhausted");
        work.charge(Math.addExact(Math.multiplyExact(length,3L),Math.multiplyExact(values.size(),64L)),"membership canonical array defensive copies and result list");ByteBuffer bytes=ByteBuffer.allocate((int)length);byte[] tag=DOMAIN.getBytes(StandardCharsets.US_ASCII);
        bytes.putInt(tag.length).put(tag).put(forest.sha256().copy()).putLong(values.size());
        for(UUID uuid:values)bytes.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        work.charge(1920L,"membership digest workspace and forest SHA/list copies");return new IndexData(bytes.array(),forest.sha256().copy(),new ArrayList<>(values));
    }
    public static IndexData decode(byte[] indexBytes,byte[] expectedIndexSha,byte[] fullForestBytes,SourceLimitProfile limits)throws IOException {
        Objects.requireNonNull(expectedIndexSha);Objects.requireNonNull(indexBytes);Objects.requireNonNull(limits);
        SourceForestCodec.require(indexBytes.length<=limits.maxSqlPacketBytes()&&Math.multiplyExact((long)indexBytes.length,2L)<=limits.maxRetainedBytes(),"Index hash input budget exhausted");
        byte[] owned=indexBytes.clone();SourceForestCodec.require(expectedIndexSha.length==32&&Arrays.equals(SourceForestCodec.hash(owned),expectedIndexSha),"Index SHA mismatch");
        return decode(owned,fullForestBytes,limits);
    }
    public static IndexData decode(byte[] indexBytes,byte[] fullForestBytes,SourceLimitProfile limits)throws IOException {
        Encoded forest=SourceForestCodec.decode(fullForestBytes,limits);
        IndexData parsed=parseFrame(indexBytes,forest.sha256().copy(),forest.memberCount(),limits);
        IndexData derived=derive(forest.data(),limits);
        SourceForestCodec.require(parsed.bytes.equals(derived.bytes),"Index union differs from complete typed graph");
        return parsed;
    }
    /** SQL scalar DATA expectation; it does not issue authority. */
    public record ForestExpectation(UUID binding,UUID unit,UUID forest,long sourceEpoch,UUID createdOperation,
                                    String codec,long memberCount,long rootCount,long carrierCount,long retainedBytes) {
        public ForestExpectation {Objects.requireNonNull(binding);Objects.requireNonNull(unit);Objects.requireNonNull(forest);Objects.requireNonNull(createdOperation);Objects.requireNonNull(codec);}
    }
    private record VerifiedGraph(Encoded forest,IndexData index) { }
    /** 完整纯DATA消费；结果不代表数据库/世界absence能力。 */
    public static boolean containsVerified(byte[] forestBytes,byte[] forestSha,byte[] indexBytes,byte[] indexSha,
            SourceLimitProfile limits,SourceForestAllocationBudget budget,UUID proposed,ForestExpectation expected)throws IOException {
        Objects.requireNonNull(forestBytes);Objects.requireNonNull(forestSha);Objects.requireNonNull(indexBytes);
        Objects.requireNonNull(indexSha);Objects.requireNonNull(limits);Objects.requireNonNull(budget);
        Objects.requireNonNull(proposed);Objects.requireNonNull(expected);
        try(Scope work=Scope.open(budget,limits)) {
            VerifiedGraph graph=verifyGraphWithin(forestBytes,forestSha,indexBytes,indexSha,limits,expected,work);
            return graph.index().contains(proposed);
        }
    }
    /**
     * 在原 owner 的同步回调中消费完整 DATA 图；不授数据库 transfer、来源、absence 或写入资格。
     * 回调若在返回后保留 graph/index，调用方必须先向原 owner 账本预留对应保留费用；此 Scope 只覆盖临时 workspace。
     */
    public static void consumeVerifiedGraph(byte[] forestBytes,byte[] forestSha,byte[] indexBytes,byte[] indexSha,
            SourceLimitProfile limits,SourceForestAllocationBudget budget,ForestExpectation expected,
            BiConsumer<Encoded,IndexData> consumer)throws IOException {
        Objects.requireNonNull(forestBytes);Objects.requireNonNull(forestSha);Objects.requireNonNull(indexBytes);
        Objects.requireNonNull(indexSha);Objects.requireNonNull(limits);Objects.requireNonNull(budget);
        Objects.requireNonNull(expected);Objects.requireNonNull(consumer);
        try(Scope work=Scope.open(budget,limits)) {
            VerifiedGraph graph=verifyGraphWithin(forestBytes,forestSha,indexBytes,indexSha,limits,expected,work);
            consumer.accept(graph.forest(),graph.index());
        }
    }
    private static VerifiedGraph verifyGraphWithin(byte[] forestBytes,byte[] forestSha,byte[] indexBytes,byte[] indexSha,
            SourceLimitProfile limits,ForestExpectation expected,Scope work)throws IOException {
        Encoded forest=SourceForestCodec.decodeWithin(forestBytes,forestSha,limits,work);
        Identity identity=forest.data().identity();
        SourceForestCodec.require(identity.bindingId().equals(expected.binding())&&identity.unitId().equals(expected.unit())
            &&identity.forestId().equals(expected.forest())&&identity.sourceEpoch()==expected.sourceEpoch()
            &&identity.createdOperationId().equals(expected.createdOperation())&&DOMAIN_FOREST.equals(expected.codec())
            &&forest.memberCount()==expected.memberCount()&&forest.rootCount()==expected.rootCount()
            &&forest.carrierCount()==expected.carrierCount()&&forest.retainedBytes()==expected.retainedBytes(),"SQL forest identity/count/codec differs from complete decoded graph");
        SourceForestCodec.require(indexSha.length==32&&Arrays.equals(SourceForestCodec.hash(indexBytes,work),indexSha),"Index SHA mismatch");
        work.charge(64L,"forest SHA defensive copy for membership frame");
        IndexData parsed=parseFrameWithin(indexBytes,forest.sha256().copy(),forest.memberCount(),limits,work);
        IndexData derived=deriveWithin(forest.data(),limits,work);
        SourceForestCodec.require(parsed.bytes.equals(derived.bytes),"Index union differs from complete typed graph");
        work.charge(64L,"verified forest and membership index callback view");
        return new VerifiedGraph(forest,parsed);
    }
    private static final String DOMAIN_FOREST=SourceForestCodec.DOMAIN;
    /** 包内frame验证seam仍是DATA，不能以caller hash/count发生产能力。 */
    static IndexData parseFrame(byte[] bytes,byte[] expectedForestSha,long fullMemberCount,SourceLimitProfile limits) {
        try(Scope work=Scope.untracked()){return parseFrameWithin(bytes,expectedForestSha,fullMemberCount,limits,work);}}
    static IndexData parseFrameWithin(byte[] bytes,byte[] expectedForestSha,long fullMemberCount,SourceLimitProfile limits,Scope work) {
        Objects.requireNonNull(bytes);Objects.requireNonNull(expectedForestSha);Objects.requireNonNull(limits);
        SourceForestCodec.require(expectedForestSha.length==32 && fullMemberCount>=0 && fullMemberCount<=limits.maxMembers(),"Invalid forest hash/member count");
        SourceForestCodec.require(bytes.length>=HEADER_BYTES && bytes.length<=limits.maxSqlPacketBytes()
            && Math.multiplyExact((long)bytes.length,2L)<=limits.maxRetainedBytes(),"Index input budget exhausted");
        work.charge(Math.addExact(Math.multiplyExact(bytes.length,2L),1024L),"membership parse clone defensive result and hash framing");bytes=bytes.clone();ByteBuffer input=ByteBuffer.wrap(bytes);byte[] tag=DOMAIN.getBytes(StandardCharsets.US_ASCII);
        SourceForestCodec.require(input.getInt()==tag.length,"Wrong membership domain length");
        for(byte value:tag)SourceForestCodec.require(input.get()==value,"Wrong membership domain");
        byte[] sha=new byte[32];input.get(sha);SourceForestCodec.require(Arrays.equals(sha,expectedForestSha),"Embedded forest hash mismatch");
        long count=input.getLong();
        // 先实际长度除法界定数量，随后才乘法；negative/high-bit count明确拒绝。
        SourceForestCodec.require(count>=0 && count<=input.remaining()/16L,"Invalid membership count/length");
        SourceForestCodec.require(count<=Math.multiplyExact(fullMemberCount,3L),"Index count exceeds three identities/member");
        SourceForestCodec.require(Math.multiplyExact(count,16L)==input.remaining(),"Short/trailing membership data");
        SourceForestCodec.require(Math.addExact(Math.multiplyExact((long)bytes.length,2L),Math.multiplyExact(count,64L))<=limits.maxRetainedBytes(),"Index identity workspace budget exhausted");
        work.charge(Math.addExact(Math.multiplyExact(count,96L),256L),"membership UUIDs mutable and immutable list representations");List<UUID> values=new ArrayList<>();UUID previous=null;
        for(long i=0;i<count;i++) {UUID current=new UUID(input.getLong(),input.getLong());
            SourceForestCodec.require(previous==null||SourceForestCodec.compareUuid(previous,current)<0,"Duplicate/unsorted membership identities");
            values.add(current);previous=current;
        }
        // 全部项已核后才可返回查询对象，不能found-first提前成功。
        work.charge(1920L,"membership digest and result framing");return new IndexData(bytes,sha,values);
    }
}

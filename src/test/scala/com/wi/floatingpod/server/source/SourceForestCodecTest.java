package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestCodec.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.io.EOFException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static com.wi.floatingpod.server.source.SourceForestFixtures.*;

final class SourceForestCodecTest {
    @Test void completeRoundtripPreservesFourOccurrencesMixedPassengersAndAllCarrierPayloads()throws Exception {
        Forest forest=forest();Encoded encoded=SourceForestCodec.encode(forest,LIMITS);Encoded decoded=SourceForestCodec.decode(encoded.bytes().copy(),LIMITS);
        assertArrayEquals(encoded.bytes().copy(),decoded.bytes().copy());assertEquals(4,decoded.memberCount());assertEquals(2,decoded.rootCount());assertEquals(2,decoded.carrierCount());assertEquals(encoded.bytes().length(),decoded.retainedBytes());
        for(Member member:forest.members())for(Stage stage:member.stages()) {
            Member actual=decoded.data().members().stream().filter(m->m.id().equals(member.id())).findFirst().orElseThrow();
            Stage actualStage=SourceForestCodec.find(actual,stage.kind());assertEquals(stage.entityNbt(),actualStage.entityNbt());assertEquals(stage.entityPath(),actualStage.entityPath());assertEquals(stage.parentMemberId(),actualStage.parentMemberId());assertEquals(member.lineage(),actual.lineage());
        }
        assertEquals(forest.carriers(),decoded.data().carriers());
        EntityFields fields=SourceForestCodec.entityFields(forest.members().getFirst().stages().getFirst().entityNbt(),LIMITS);
        assertEquals(0x8000000000000000L,fields.positionBits().get(0).longValue());assertEquals(0x7ff8000000000042L,fields.positionBits().get(1).longValue());assertEquals(0x7fc00042,fields.rotationBits().get(1).intValue());
    }
    @Test void arraysAndCollectionsAreDefensiveAndCanonicalOrderIsIndependentOfInputOrder()throws Exception {
        Forest original=forest();List<Member> reverse=new ArrayList<>(original.members());Collections.reverse(reverse);List<Carrier> carriers=new ArrayList<>(original.carriers());Collections.reverse(carriers);
        Forest reordered=new Forest(original.identity(),original.coverage(),original.sourceObservation(),List.of(id(4),id(1)),List.of(id(4),id(1)),List.of(id(4),id(1)),carriers,reverse);
        assertArrayEquals(SourceForestCodec.encode(original,LIMITS).bytes().copy(),SourceForestCodec.encode(reordered,LIMITS).bytes().copy());
        byte[] copy=original.sourceObservation().copy();copy[0]=99;assertEquals(1,original.sourceObservation().copy()[0]);assertThrows(UnsupportedOperationException.class,()->original.members().clear());
    }
    @Test void unresolvedClassificationRoundtripsButHasNoIndex()throws Exception {
        Forest unresolved=forest(false,true);Encoded data=SourceForestCodec.encode(unresolved,LIMITS);assertArrayEquals(data.bytes().copy(),SourceForestCodec.decode(data.bytes().copy(),LIMITS).bytes().copy());
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(unresolved,LIMITS));
    }
    @Test void unresolvedCarrierRetainsOriginalMalformedStageBytesWithoutIssuingAnIndex()throws Exception {
        Forest original=forest(false,true);Carrier carrier=original.carriers().getFirst();List<CarrierStage> stages=new ArrayList<>(carrier.stages());
        byte[] malformed=new byte[]{10,0,0,99};stages.set(1,new CarrierStage(StageKind.FIXED,stages.get(1).captureId(),new Bytes(malformed)));
        Carrier unknown=new Carrier(carrier.id(),carrier.regionKey(),carrier.regionX(),carrier.regionZ(),carrier.dimension(),carrier.chunkX(),carrier.chunkZ(),carrier.slot(),carrier.volumeSerialBits(),carrier.fileId128(),carrier.saveGeneration(),carrier.originalRegionLength(),carrier.compressionType(),carrier.originalCompressedChunk(),carrier.sectorOffset(),carrier.sectorCount(),carrier.timestampBits(),CarrierDisposition.UNKNOWN,null,stages);
        Forest data=new Forest(original.identity(),original.coverage(),original.sourceObservation(),original.rawRoots(),original.fixedRoots(),original.actualRoots(),List.of(unknown,original.carriers().get(1)),original.members());
        Encoded encoded=SourceForestCodec.encode(data,LIMITS);Encoded decoded=SourceForestCodec.decode(encoded.bytes().copy(),LIMITS);
        assertArrayEquals(malformed,decoded.data().carriers().getFirst().stages().get(1).chunkNbt().copy());assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(data,LIMITS));
    }
    @Test void rejectsDuplicateIdentityWrongRootsAndLineageRatherThanDeduplicatingMembers()throws Exception {
        Forest original=forest();List<Member> duplicate=new ArrayList<>(original.members());duplicate.add(duplicate.getFirst());
        assertThrows(IllegalArgumentException.class,()->SourceForestCodec.encode(members(original,duplicate),LIMITS));
        Forest noRoots=new Forest(original.identity(),original.coverage(),original.sourceObservation(),List.of(),original.fixedRoots(),original.actualRoots(),original.carriers(),original.members());assertThrows(IllegalArgumentException.class,()->SourceForestCodec.encode(noRoots,LIMITS));
        Member member=original.members().getFirst();Member wrong=new Member(member.id(),member.disposition(),member.dispositionObservation(),member.dispositionAction(),member.dispositionCalls(),member.dispositionCaptureId(),member.stages(),List.of(new Lineage(LineageKind.RAW_TO_FIXED,id(900),id(901),id(902))));
        List<Member> changed=new ArrayList<>(original.members());changed.set(0,wrong);assertThrows(IllegalArgumentException.class,()->SourceForestCodec.encode(members(original,changed),LIMITS));
    }
    @Test void missingPassengerCanBeRetainedAsDraftDataButCannotDeriveCompleteDataIndex()throws Exception {
        Forest original=forest();List<Member> subset=original.members().stream().filter(m->!m.id().equals(id(3))).toList();Forest draft=members(original,subset);
        assertNotNull(SourceForestCodec.encode(draft,LIMITS));assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(draft,LIMITS));
    }
    @Test void rejectsWrongCarrierPayload()throws Exception {
        Forest original=forest();Member member=original.members().getFirst();Stage stage=member.stages().getFirst();List<Stage> stages=new ArrayList<>(member.stages());
        stages.set(0,new Stage(stage.kind(),stage.captureId(),stage.carrierId(),stage.parentMemberId(),stage.entityPath(),stage.classification(),new Bytes(entity("different",id(7),List.of())),stage.factory(),stage.factoryCalls(),stage.read(),stage.readCalls(),stage.caps(),stage.capsCalls(),stage.placement(),stage.placementCalls(),stage.index(),stage.indexCalls(),"",null));
        List<Member> changed=new ArrayList<>(original.members());changed.set(0,new Member(member.id(),member.disposition(),member.dispositionObservation(),member.dispositionAction(),member.dispositionCalls(),member.dispositionCaptureId(),stages,member.lineage()));assertThrows(IllegalArgumentException.class,()->SourceForestCodec.encode(members(original,changed),LIMITS));
    }
    @Test void parentReferenceMustMatchExactCarrierAndImmediatePathPrefix()throws Exception {
        Forest original=forest();Member member=original.members().get(1);Stage stage=member.stages().getFirst();List<Stage> stages=new ArrayList<>(member.stages());
        stages.set(0,new Stage(stage.kind(),stage.captureId(),stage.carrierId(),id(4),stage.entityPath(),stage.classification(),stage.entityNbt(),stage.factory(),stage.factoryCalls(),stage.read(),stage.readCalls(),stage.caps(),stage.capsCalls(),stage.placement(),stage.placementCalls(),stage.index(),stage.indexCalls(),"",null));
        List<Member> changed=new ArrayList<>(original.members());changed.set(1,new Member(member.id(),member.disposition(),member.dispositionObservation(),member.dispositionAction(),member.dispositionCalls(),member.dispositionCaptureId(),stages,member.lineage()));
        assertThrows(IllegalArgumentException.class,()->SourceForestCodec.encode(members(original,changed),LIMITS));
    }
    @Test void originalCompressedBytesMustDecodeToExactRawCarrierRatherThanSiblingPayload()throws Exception {
        Forest original=forest();Carrier carrier=original.carriers().getFirst(),other=original.carriers().get(1);
        Carrier mismatched=new Carrier(carrier.id(),carrier.regionKey(),carrier.regionX(),carrier.regionZ(),carrier.dimension(),carrier.chunkX(),carrier.chunkZ(),carrier.slot(),carrier.volumeSerialBits(),carrier.fileId128(),carrier.saveGeneration(),carrier.originalRegionLength(),carrier.compressionType(),other.originalCompressedChunk(),carrier.sectorOffset(),carrier.sectorCount(),carrier.timestampBits(),carrier.disposition(),carrier.dispositionObservation(),carrier.stages());
        Forest wrong=new Forest(original.identity(),original.coverage(),original.sourceObservation(),original.rawRoots(),original.fixedRoots(),original.actualRoots(),List.of(mismatched,other),original.members());
        assertThrows(IllegalArgumentException.class,()->SourceForestCodec.encode(wrong,LIMITS));
    }
    @Test void truncationTrailingAndProfileMismatchAreExplicit()throws Exception {
        byte[] bytes=SourceForestCodec.encode(forest(),LIMITS).bytes().copy();
        assertThrows(IOException.class,()->SourceForestCodec.decode(Arrays.copyOf(bytes,bytes.length-1),LIMITS));
        assertThrows(IllegalArgumentException.class,()->SourceForestCodec.decode(Arrays.copyOf(bytes,bytes.length+1),LIMITS));
        SourceLimitProfile other=SourceLimitProfile.of(1_000_000,100_000,100_000,64,4097,10_000_000,1_000_000,65536);
        assertThrows(IllegalArgumentException.class,()->SourceForestCodec.decode(bytes,other));
    }
    @Test void smallestAcceptedRetentionBudgetStillRoundtripsUnderExactSameProfile()throws Exception {
        Forest forest=forest();assertNotNull(SourceForestCodec.encode(forest,LIMITS));long low=1,high=LIMITS.maxRetainedBytes();
        while(low<high) {long middle=low+(high-low)/2;SourceLimitProfile profile=withRetention(middle);
            try {SourceForestCodec.encode(forest,profile);high=middle;}catch(IOException|IllegalArgumentException budgetRejected){low=middle+1;}}
        SourceLimitProfile exact=withRetention(low);Encoded encoded=SourceForestCodec.encode(forest,exact);
        assertArrayEquals(encoded.bytes().copy(),SourceForestCodec.decode(encoded.bytes().copy(),exact).bytes().copy());
    }
    @Test void declaredBlobWithInsufficientPayloadIsEofBeforeAllocation()throws Exception {
        byte[] bytes=SourceForestCodec.encode(forest(),LIMITS).bytes().copy();
        int domainLength=ByteBuffer.wrap(bytes).getInt();
        assertThrows(EOFException.class,()->SourceForestCodec.decode(Arrays.copyOf(bytes,4+domainLength-1),LIMITS));
        byte[] negative=Arrays.copyOf(bytes,4);ByteBuffer.wrap(negative).putInt(-1);
        assertThrows(IllegalArgumentException.class,()->SourceForestCodec.decode(negative,LIMITS));
    }
    @Test void validCollectionCountWithMissingElementsIsEofButInvalidCountRemainsSemantic()throws Exception {
        byte[] bytes=SourceForestCodec.encode(forest(),LIMITS).bytes().copy();int offset=framingOffsets(bytes)[0];
        assertThrows(EOFException.class,()->SourceForestCodec.decode(Arrays.copyOf(bytes,offset+4),LIMITS));
        for(int invalid:new int[]{-1,Math.toIntExact(LIMITS.maxMembers()+1)}) {
            byte[] changed=Arrays.copyOf(bytes,offset+4);ByteBuffer.wrap(changed).putInt(offset,invalid);
            assertThrows(IllegalArgumentException.class,()->SourceForestCodec.decode(changed,LIMITS));
        }
    }
    @Test void validStageCountWithoutStagesIsEofButInvalidStageCountRemainsSemantic()throws Exception {
        byte[] bytes=SourceForestCodec.encode(forest(),LIMITS).bytes().copy();int offset=framingOffsets(bytes)[1];
        assertThrows(EOFException.class,()->SourceForestCodec.decode(Arrays.copyOf(bytes,offset+4),LIMITS));
        for(int invalid:new int[]{-1,4}) {
            byte[] changed=Arrays.copyOf(bytes,offset+4);ByteBuffer.wrap(changed).putInt(offset,invalid);
            assertThrows(IllegalArgumentException.class,()->SourceForestCodec.decode(changed,LIMITS));
        }
    }
    /** Locate framing fields in an actual production encoding; no replacement decoder/semantic validator. */
    private static int[] framingOffsets(byte[] bytes)throws IOException {
        DataInputStream input=new DataInputStream(new ByteArrayInputStream(bytes));
        skipBlob(input);skipBlob(input);input.skipNBytes(32+16*3+8+16);
        skipBlob(input);skipBlob(input);skipBlob(input);input.skipNBytes(16+8+1);
        skipBlob(input);input.skipNBytes(32);int roots=bytes.length-input.available();
        for(int stage=0;stage<3;stage++)input.skipNBytes(input.readInt()*16L);
        assertTrue(input.readInt()>0);input.skipNBytes(16);skipBlob(input);
        input.skipNBytes(6*4+8+16+8+8+4);skipBlob(input);input.skipNBytes(32+3*4+1);
        if(input.readUnsignedByte()==1)input.skipNBytes(16);
        return new int[]{roots,bytes.length-input.available()};
    }
    private static void skipBlob(DataInputStream input)throws IOException {input.skipNBytes(input.readInt());}
    private static SourceLimitProfile withRetention(long retained) {return SourceLimitProfile.of(LIMITS.maxRegionBytes(),LIMITS.maxChunkBytes(),LIMITS.maxDecompressedBytes(),LIMITS.maxNbtDepth(),LIMITS.maxMembers(),retained,LIMITS.maxSqlPacketBytes(),LIMITS.maxNativeArtifactBytes());}
    @Test void explicitByteAndMemberBudgetsRefuseOversizeBeforeReturningData()throws Exception {
        Forest forest=forest();SourceLimitProfile tiny=SourceLimitProfile.of(1_000_000,100_000,100_000,64,3,10_000_000,1_000_000,65536);
        assertThrows(IllegalArgumentException.class,()->SourceForestCodec.encode(forest,tiny));
        SourceLimitProfile packet=SourceLimitProfile.of(1_000_000,100_000,100_000,64,4096,10_000_000,64,65536);
        assertThrows(IOException.class,()->SourceForestCodec.encode(forest,packet));
    }
}

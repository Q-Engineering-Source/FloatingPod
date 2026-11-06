package com.wi.floatingpod.server.source;

import com.wi.floatingpod.server.source.SourceForestCodec.*;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static com.wi.floatingpod.server.source.SourceForestFixtures.*;

final class SourceForestMembershipCodecTest {
    @Test void derivesThreeStageUnionWithoutDeduplicatingFourFullGraphMembers()throws Exception {
        Forest forest=forest();Encoded encoded=SourceForestCodec.encode(forest,LIMITS);var index=SourceForestMembershipCodec.derive(forest,LIMITS);
        assertEquals(List.of(id(1),id(2),id(3)),index.identities());assertEquals(4,forest.members().size());assertEquals(74+3*16,index.bytes().length());
        var decoded=SourceForestMembershipCodec.decode(index.bytes().copy(),index.sha256().copy(),encoded.bytes().copy(),LIMITS);
        assertTrue(decoded.contains(id(2)));assertFalse(decoded.contains(id(9)));assertArrayEquals(encoded.sha256().copy(),decoded.forestSha256().copy());
    }
    @Test void memberClassifiedFpInAnyStageRetainsAllThreeAliasesAcrossTypeChanges()throws Exception {
        Forest changed=forest(false,false,true);assertEquals(Classification.OTHER,changed.members().getFirst().stages().get(1).classification());
        assertEquals("minecraft:pig",SourceForestCodec.entityFields(changed.members().getFirst().stages().get(1).entityNbt(),LIMITS).registryType());
        assertEquals(List.of(id(1),id(2),id(3)),SourceForestMembershipCodec.derive(changed,LIMITS).identities());
    }
    @Test void completeOtherOnlyGraphDerivesEmptyDataIndexButUnknownGraphDoesNot()throws Exception {
        Forest other=forest(true,false);var empty=SourceForestMembershipCodec.derive(other,LIMITS);assertEquals(74,empty.bytes().length());assertTrue(empty.identities().isEmpty());
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(forest(false,true),LIMITS));
    }
    @Test void frozenEmptyAndUnsignedTwoItemFramingVectorsMatchExactBytesAndHashes() {
        byte[] sha=HexFormat.of().parseHex("403AA78B03D88332222A68FA12E504E7F40829D4AA5826875CCC5DEF027B6731");
        byte[] empty=frame(sha,List.of());assertEquals("B6BF8E695D0C8A3BC7975ECC5B376160CC0FB02CB6E16403F049FC27774E7030",HexFormat.of().withUpperCase().formatHex(SourceForestCodec.hash(empty)));
        var emptyData=SourceForestMembershipCodec.parseFrame(empty,sha,0,LIMITS);assertTrue(emptyData.identities().isEmpty());
        List<UUID> ids=List.of(id(1),new UUID(Long.MIN_VALUE,0));byte[] two=frame(sha,ids);
        assertEquals("739765FBF4051A3F1BFFE61DCB6FD71DD9DA8EE22CBD2442E571070EF010F610",HexFormat.of().withUpperCase().formatHex(SourceForestCodec.hash(two)));
        assertEquals(ids,SourceForestMembershipCodec.parseFrame(two,sha,2,LIMITS).identities());
    }
    @Test void foundFirstTargetCannotHideBadLaterDuplicateOrUnsortedIdentity()throws Exception {
        Forest forest=forest();var index=SourceForestMembershipCodec.derive(forest,LIMITS);byte[] bytes=index.bytes().copy();ByteBuffer.wrap(bytes).putLong(74+32,0).putLong(74+40,1);
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(bytes,index.forestSha256().copy(),4,LIMITS));
        byte[] unsorted=frame(index.forestSha256().copy(),List.of(id(2),id(1)));assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(unsorted,index.forestSha256().copy(),4,LIMITS));
    }
    @Test void rejectsTagNegativeHugeCountTruncationTrailingAndWrongEmbeddedHash()throws Exception {
        var index=SourceForestMembershipCodec.derive(forest(),LIMITS);byte[] good=index.bytes().copy(),sha=index.forestSha256().copy();
        byte[] tag=good.clone();tag[4]^=1;assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(tag,sha,4,LIMITS));
        byte[] negative=good.clone();ByteBuffer.wrap(negative).putLong(66,-1);assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(negative,sha,4,LIMITS));
        byte[] huge=good.clone();ByteBuffer.wrap(huge).putLong(66,Long.MAX_VALUE);assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(huge,sha,4,LIMITS));
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(Arrays.copyOf(good,good.length-1),sha,4,LIMITS));
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(Arrays.copyOf(good,good.length+1),sha,4,LIMITS));
        byte[] wrong=good.clone();wrong[34]^=1;assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.parseFrame(wrong,sha,4,LIMITS));
    }
    @Test void validFrameStillMustEqualActualFullGraphUnionAndActualDigest()throws Exception {
        Forest forest=forest();Encoded encoded=SourceForestCodec.encode(forest,LIMITS);var index=SourceForestMembershipCodec.derive(forest,LIMITS);
        byte[] other=frame(encoded.sha256().copy(),List.of(id(1),id(2),id(4)));assertNotNull(SourceForestMembershipCodec.parseFrame(other,encoded.sha256().copy(),4,LIMITS));
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.decode(other,encoded.bytes().copy(),LIMITS));
        byte[] badHash=index.sha256().copy();badHash[0]^=1;assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.decode(index.bytes().copy(),badHash,encoded.bytes().copy(),LIMITS));
        byte[] badForestHash=encoded.sha256().copy();badForestHash[0]^=1;assertThrows(IllegalArgumentException.class,()->SourceForestCodec.decode(encoded.bytes().copy(),badForestHash,LIMITS));
    }
    @Test void missingActualIndexOrUnresolvedDispositionCannotProduceIndex()throws Exception {
        Forest original=forest();Member member=original.members().getFirst();Member pending=new Member(member.id(),Disposition.PENDING,null,Observation.NOT_OBSERVED,0,null,member.stages(),member.lineage());
        List<Member> changed=new ArrayList<>(original.members());changed.set(0,pending);assertNotNull(SourceForestCodec.encode(members(original,changed),LIMITS));assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(members(original,changed),LIMITS));
    }
    @Test void legitimateRemovalUsesExactObservedTerminalActionAndPreservesHistoricalMembership()throws Exception {
        Forest original=forest();Member member=original.members().get(2);Stage old=SourceForestCodec.find(member,StageKind.ACTUAL);
        Stage terminal=new Stage(old.kind(),old.captureId(),old.carrierId(),old.parentMemberId(),old.entityPath(),old.classification(),old.entityNbt(),Observation.NOT_OBSERVED,0,Observation.NOT_OBSERVED,0,Observation.NOT_OBSERVED,0,Observation.NOT_OBSERVED,0,Observation.NOT_OBSERVED,0,"",null);
        List<Stage> stages=new ArrayList<>(member.stages());stages.set(2,terminal);
        Member removed=new Member(member.id(),Disposition.LEGITIMATELY_REMOVED,id(999),Observation.RETURNED,1,terminal.captureId(),stages,member.lineage());List<Member> changed=new ArrayList<>(original.members());changed.set(2,removed);
        assertEquals(List.of(id(1),id(2),id(3)),SourceForestMembershipCodec.derive(members(original,changed),LIMITS).identities());
        Member wrong=new Member(member.id(),Disposition.LEGITIMATELY_REMOVED,id(999),Observation.RETURNED,1,id(998),stages,member.lineage());changed.set(2,wrong);
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(members(original,changed),LIMITS));
    }
    @Test void nestedGrandchildCannotDeclareItselfRootAndHideActualFpAncestorAliases()throws Exception {
        Forest original=forest();Member grandchild=original.members().get(2);List<Stage> stages=new ArrayList<>();List<CarrierStage> carrierStages=new ArrayList<>();
        for(Stage old:grandchild.stages()) {
            stages.add(new Stage(old.kind(),old.captureId(),old.carrierId(),null,old.entityPath(),old.classification(),old.entityNbt(),old.factory(),old.factoryCalls(),old.read(),old.readCalls(),old.caps(),old.capsCalls(),old.placement(),old.placementCalls(),old.index(),old.indexCalls(),"",null));
            byte[] middle=entity("minecraft:pig",id(90),List.of(old.entityNbt().copy()));
            byte[] ancestor=entity("floatingpod:floating_pod",id(101+old.kind().ordinal()),List.of(middle));
            carrierStages.add(new CarrierStage(old.kind(),id(100+old.kind().ordinal()),new Bytes(chunk(List.of(ancestor)))));
        }
        Member omitted=new Member(grandchild.id(),grandchild.disposition(),grandchild.dispositionObservation(),grandchild.dispositionAction(),grandchild.dispositionCalls(),grandchild.dispositionCaptureId(),stages,grandchild.lineage());
        Carrier old=original.carriers().getFirst();java.io.ByteArrayOutputStream compressed=new java.io.ByteArrayOutputStream();
        try(java.util.zip.DeflaterOutputStream output=new java.util.zip.DeflaterOutputStream(compressed)){output.write(carrierStages.getFirst().chunkNbt().copy());}
        Carrier carrier=new Carrier(old.id(),old.regionKey(),old.regionX(),old.regionZ(),old.dimension(),old.chunkX(),old.chunkZ(),old.slot(),old.volumeSerialBits(),old.fileId128(),old.saveGeneration(),old.originalRegionLength(),old.compressionType(),new Bytes(compressed.toByteArray()),old.sectorOffset(),old.sectorCount(),old.timestampBits(),old.disposition(),old.dispositionObservation(),carrierStages);
        Forest hidden=new Forest(original.identity(),original.coverage(),original.sourceObservation(),List.of(grandchild.id()),List.of(grandchild.id()),List.of(grandchild.id()),List.of(carrier),List.of(omitted));
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(hidden,LIMITS));
    }
    @Test void explicitOtherAncestorsAndSiblingsRemainOutsideUnitAndRoundtripFullCarrier()throws Exception {
        Forest data=externalAncestors(Classification.OTHER,false,false);Encoded encoded=SourceForestCodec.encode(data,LIMITS);Encoded decoded=SourceForestCodec.decode(encoded.bytes().copy(),LIMITS);
        assertEquals(1,decoded.memberCount());assertEquals(1,decoded.rootCount());assertEquals(List.of(0,0,0),decoded.data().members().getFirst().stages().getFirst().entityPath());
        assertEquals(4,decoded.data().carriers().getFirst().stages().getFirst().background().size());assertEquals(data.carriers(),decoded.data().carriers());
        assertEquals(List.of(id(1),id(2),id(3)),SourceForestMembershipCodec.derive(data,LIMITS).identities());
    }
    @Test void actualCarrierRequiresEveryExternalOccurrenceKnownOtherIncludingSiblingDescendants()throws Exception {
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(externalAncestors(Classification.FLOATING_POD,false,false),LIMITS));
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(externalAncestors(Classification.UNKNOWN,false,false),LIMITS));
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(externalAncestors(Classification.OTHER,true,false),LIMITS));
        Forest unknown=externalAncestors(Classification.OTHER,false,true);assertNotNull(SourceForestCodec.encode(unknown,LIMITS));assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(unknown,LIMITS));
        Forest full=externalAncestors(Classification.OTHER,false,false);Carrier carrier=full.carriers().getFirst();List<CarrierStage> changed=new ArrayList<>(carrier.stages());CarrierStage stage=changed.getFirst();
        changed.set(0,new CarrierStage(stage.kind(),stage.captureId(),stage.chunkNbt(),stage.background().subList(0,3)));
        Carrier missing=new Carrier(carrier.id(),carrier.regionKey(),carrier.regionX(),carrier.regionZ(),carrier.dimension(),carrier.chunkX(),carrier.chunkZ(),carrier.slot(),carrier.volumeSerialBits(),carrier.fileId128(),carrier.saveGeneration(),carrier.originalRegionLength(),carrier.compressionType(),carrier.originalCompressedChunk(),carrier.sectorOffset(),carrier.sectorCount(),carrier.timestampBits(),carrier.disposition(),carrier.dispositionObservation(),changed);
        Forest incomplete=new Forest(full.identity(),full.coverage(),full.sourceObservation(),full.rawRoots(),full.fixedRoots(),full.actualRoots(),List.of(missing),full.members());
        assertThrows(IllegalArgumentException.class,()->SourceForestMembershipCodec.derive(incomplete,LIMITS));
    }
    private static byte[] frame(byte[] sha,List<UUID> ids) {
        byte[] tag=SourceForestMembershipCodec.DOMAIN.getBytes(StandardCharsets.US_ASCII);ByteBuffer bytes=ByteBuffer.allocate(74+16*ids.size());bytes.putInt(tag.length).put(tag).put(sha).putLong(ids.size());for(UUID id:ids)bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());return bytes.array();
    }
}
